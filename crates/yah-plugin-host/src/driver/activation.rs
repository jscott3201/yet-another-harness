use std::{
    any::Any,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

use yah_compose::{
    ComponentSlot, ComponentSlotError, ComponentSlotOutcome, ComponentStateKind,
    DesiredComponentState, ProviderSelectionEpoch, ReconcileError, ReconcileOutcome,
    ScopeCancellation, ServiceRegistry, StopRecord,
};

use crate::DriverKind;

use super::{
    DriverActivationError, DriverFuture, DriverStartPermit, HostPluginActivationError,
    PluginActivationId, PluginActivationRequest, PluginDriver, PluginHealth, PluginHealthError,
    PluginStartError, PreparedDriverActivation,
    control::{DriverDeactivation, PreparedControl},
};

type DriverStartFuture = DriverFuture<Result<(), DriverActivationError>>;
type PluginStartResult = Result<PluginActivationHandle, PluginStartError>;

/// Host-owned guard for one prepared plugin activation.
///
/// Preparation transfers exact deactivation into the component effect scope
/// before any start future can be constructed or polled. The guard temporarily
/// owns the mutable component slot while start is unsettled, so every desired
/// change and failure seals cancellation before the stored start future is
/// destroyed. Dropping only an [`ActivatePlugin`] waiter preserves that future.
///
/// Dropping this whole guard before [`Self::release_active`] records an exact
/// activation failure and requests cancellation synchronously, but it cannot
/// drive asynchronous cleanup. The caller regains the borrowed slot and must
/// still resume its normal `finish_stop` path.
#[must_use = "prepared plugin activation must be driven, failed, or released"]
pub struct HostPluginActivation<'slot> {
    slot: Option<&'slot mut ComponentSlot>,
    id: PluginActivationId,
    kind: DriverKind,
    cancellation: ScopeCancellation,
    control: Arc<PreparedControl>,
    start: StartState,
}

impl<'slot> HostPluginActivation<'slot> {
    /// Prepare one exact activation and admit its deactivation finalizer.
    ///
    /// The component must already be `Starting` at `selection_epoch`. Driver
    /// preparation is required to be inert; on any admission error no start or
    /// deactivation operation is invoked. Errors before cleanup admission leave
    /// that exact component `Starting`; its owner must retry, reconcile a newer
    /// desired state, or explicitly report activation failure.
    pub fn prepare(
        slot: &'slot mut ComponentSlot,
        selection_epoch: ProviderSelectionEpoch,
        driver: Arc<dyn PluginDriver>,
    ) -> Result<Self, HostPluginActivationError> {
        let control = Arc::new(PreparedControl::new(driver));
        let state = slot
            .live_state()
            .map_or(ComponentStateKind::Removed, |state| state.kind());
        if state != ComponentStateKind::Starting {
            return Err(reject_control(
                &control,
                ComponentSlotError::Reconcile(ReconcileError::InvalidState {
                    operation: "prepare a plugin activation",
                    state,
                })
                .into(),
            ));
        }
        let cancellation = match slot.cancellation(selection_epoch) {
            Ok(cancellation) => cancellation,
            Err(error) => return Err(reject_control(&control, error.into())),
        };
        let (kind, revision) = match catch_unwind(AssertUnwindSafe(|| {
            control.with_driver(|driver| (driver.kind(), driver.revision_id().clone()))
        })) {
            Ok(Some(metadata)) => metadata,
            Ok(None) => {
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::DriverPanicked {
                        summary: "plugin driver control was already released".to_owned(),
                    },
                ));
            }
            Err(payload) => {
                let summary = consume_panic_payload(payload);
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::DriverPanicked { summary },
                ));
            }
        };
        let id = PluginActivationId::new(revision, selection_epoch);
        let request = PluginActivationRequest::new(id.clone(), cancellation.clone());
        let prepared = match catch_unwind(AssertUnwindSafe(|| {
            control.with_driver(|driver| driver.prepare(request))
        })) {
            Ok(Some(Ok(prepared))) => prepared,
            Ok(Some(Err(error))) => {
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::Driver(error),
                ));
            }
            Ok(None) => {
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::DriverPanicked {
                        summary: "plugin driver control was already released".to_owned(),
                    },
                ));
            }
            Err(payload) => {
                let summary = consume_panic_payload(payload);
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::DriverPanicked { summary },
                ));
            }
        };
        control.install_prepared(prepared);

        let received = match catch_unwind(AssertUnwindSafe(|| {
            control.with_prepared(|prepared| prepared.id().clone())
        })) {
            Ok(Some(received)) => received,
            Ok(None) => {
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::DriverPanicked {
                        summary: "prepared plugin control was already released".to_owned(),
                    },
                ));
            }
            Err(payload) => {
                let summary = consume_panic_payload(payload);
                return Err(reject_control(
                    &control,
                    HostPluginActivationError::DriverPanicked { summary },
                ));
            }
        };
        if received != id {
            return Err(reject_control(
                &control,
                HostPluginActivationError::PreparedIdentityMismatch {
                    expected: id,
                    received,
                },
            ));
        }

        let cleanup_control = Arc::clone(&control);
        let cleanup_id = id.clone();
        let admission = slot.defer_async(
            selection_epoch,
            format!("deactivate plugin {id}"),
            move || DriverDeactivation::new(cleanup_control, cleanup_id),
        );
        if let Err(error) = admission {
            return Err(reject_control(
                &control,
                HostPluginActivationError::Composition(error),
            ));
        }

        Ok(Self {
            slot: Some(slot),
            id: id.clone(),
            kind,
            cancellation,
            control,
            start: StartState::Prepared(Some(DriverStartPermit::new(id))),
        })
    }

    pub const fn id(&self) -> &PluginActivationId {
        &self.id
    }

    pub const fn kind(&self) -> DriverKind {
        self.kind
    }

    pub const fn cancellation(&self) -> &ScopeCancellation {
        &self.cancellation
    }

    /// Drive start and composition readiness for this exact activation.
    ///
    /// Dropping the returned waiter while pending leaves the same driver future
    /// in this guard. Calling `activate` again resumes it without constructing
    /// another operation. Driver failures and ordinary unwind panics are
    /// contained, record an exact component failure, and synchronously seal the
    /// activation before its future is destroyed.
    pub fn activate<'host>(
        &'host mut self,
        registry: &'host ServiceRegistry,
    ) -> ActivatePlugin<'host, 'slot> {
        ActivatePlugin {
            activation: self,
            registry,
        }
    }

    /// Reconcile a desired snapshot while start is pending.
    ///
    /// If the snapshot or current provider inventory invalidates this
    /// activation, the slot seals cancellation first and this method then
    /// destroys the pending start future under unwind containment.
    pub fn reconcile(
        &mut self,
        registry: &ServiceRegistry,
        desired: DesiredComponentState,
    ) -> Result<ComponentSlotOutcome, ComponentSlotError> {
        let outcome = self.slot_mut().reconcile(registry, desired)?;
        if self.cancellation.is_cancelled() {
            self.discard_after_seal();
        }
        Ok(outcome)
    }

    /// Record a host-observed activation failure and seal before destroying
    /// any pending start future.
    pub fn fail_activation(
        &mut self,
        summary: impl Into<String>,
    ) -> Result<ReconcileOutcome, ComponentSlotError> {
        let epoch = self.id.selection_epoch();
        let outcome = self.slot_mut().fail_activation(epoch, summary)?;
        self.discard_after_seal();
        Ok(outcome)
    }

    /// Resume teardown of the exact activation after it has been sealed.
    ///
    /// The pending start future is destroyed before cleanup is polled. Dropping
    /// this waiter leaves the component slot's resumable close state intact.
    pub async fn finish_stop(&mut self) -> Result<StopRecord, ComponentSlotError> {
        let epoch = self.id.selection_epoch();
        if !self.cancellation.is_cancelled() {
            return self.slot_mut().finish_stop(epoch).await;
        }
        self.discard_after_seal();
        self.slot_mut().finish_stop(epoch).await
    }

    /// Release a successfully active slot and return its exact health handle.
    ///
    /// Calling this before a successful `activate` consumes the guard; its Drop
    /// path seals the still-live activation before returning the error.
    pub fn release_active(
        mut self,
    ) -> Result<(&'slot mut ComponentSlot, PluginActivationHandle), PluginStartError> {
        let result = match &self.start {
            StartState::Complete(result) => result.clone(),
            StartState::Prepared(_) | StartState::Running(_) | StartState::Transitioning => {
                Err(PluginStartError::Composition {
                    activation_id: self.id.clone(),
                    error: Box::new(self.release_before_completion_error()),
                    stop: None,
                })
            }
        }?;
        let slot = self
            .slot
            .take()
            .expect("an unreleased activation guard owns its component slot");
        Ok((slot, result))
    }

    fn poll_activate(
        &mut self,
        registry: &ServiceRegistry,
        cx: &mut Context<'_>,
    ) -> Poll<PluginStartResult> {
        loop {
            let state = std::mem::replace(&mut self.start, StartState::Transitioning);
            match state {
                StartState::Prepared(mut permit) => {
                    if self.cancellation.is_cancelled() {
                        return Poll::Ready(self.complete_superseded(None));
                    }
                    let permit = permit
                        .take()
                        .expect("prepared activation owns one start permit");
                    match catch_unwind(AssertUnwindSafe(|| {
                        self.control
                            .with_prepared(|prepared| prepared.start(permit))
                    })) {
                        Ok(Some(future)) => self.start = StartState::Running(future),
                        Ok(None) => {
                            let failure = DriverActivationError::failed(
                                "prepared plugin control was released before activation",
                            );
                            return Poll::Ready(self.complete_driver_failure(failure, None));
                        }
                        Err(payload) => {
                            let failure =
                                DriverActivationError::panicked(consume_panic_payload(payload));
                            return Poll::Ready(self.complete_driver_failure(failure, None));
                        }
                    }
                }
                StartState::Running(mut future) => {
                    if self.cancellation.is_cancelled() {
                        let drop_panic = drop_future(future);
                        let failure = drop_panic.map_or_else(
                            || DriverActivationError::cancelled("activation was superseded"),
                            |summary| {
                                DriverActivationError::panicked(format!(
                                    "cancelled activation future destructor panicked: {summary}"
                                ))
                            },
                        );
                        let result = Err(PluginStartError::Driver {
                            activation_id: self.id.clone(),
                            failure,
                            stop: Err(Box::new(self.already_stopping_error())),
                        });
                        self.start = StartState::Complete(result.clone());
                        return Poll::Ready(result);
                    }
                    match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                        Ok(Poll::Pending) => {
                            self.start = StartState::Running(future);
                            return Poll::Pending;
                        }
                        Ok(Poll::Ready(Ok(()))) => {
                            if let Some(summary) = drop_future(future) {
                                let failure = DriverActivationError::panicked(format!(
                                    "activation future destructor panicked: {summary}"
                                ));
                                return Poll::Ready(self.complete_driver_failure(failure, None));
                            }
                            return Poll::Ready(self.complete_ready(registry));
                        }
                        Ok(Poll::Ready(Err(failure))) => {
                            let stop = self.seal_driver_failure(&failure);
                            let failure = match drop_future(future) {
                                Some(summary) => failure.with_future_drop_panic(summary),
                                None => failure,
                            };
                            return Poll::Ready(self.store_driver_failure(failure, stop));
                        }
                        Err(payload) => {
                            let failure =
                                DriverActivationError::panicked(consume_panic_payload(payload));
                            let stop = self.seal_driver_failure(&failure);
                            let failure = match drop_future(future) {
                                Some(summary) => failure.with_future_drop_panic(summary),
                                None => failure,
                            };
                            return Poll::Ready(self.store_driver_failure(failure, stop));
                        }
                    }
                }
                StartState::Complete(result) => {
                    self.start = StartState::Complete(result.clone());
                    return Poll::Ready(result);
                }
                StartState::Transitioning => {
                    unreachable!("activation state transitions are not reentrant")
                }
            }
        }
    }

    fn complete_ready(&mut self, registry: &ServiceRegistry) -> PluginStartResult {
        let epoch = self.id.selection_epoch();
        let result = match self.slot_mut().complete_start(epoch, registry) {
            Ok(ReconcileOutcome::Active { .. }) => Ok(PluginActivationHandle {
                id: self.id.clone(),
                kind: self.kind,
                cancellation: self.cancellation.clone(),
                control: Arc::downgrade(&self.control),
            }),
            Ok(stop) => Err(PluginStartError::Superseded {
                activation_id: self.id.clone(),
                stop: Some(Box::new(stop)),
            }),
            Err(error) => {
                let stop = self.slot_mut().fail_activation(
                    epoch,
                    format!("composition rejected driver readiness: {error}"),
                );
                Err(PluginStartError::Composition {
                    activation_id: self.id.clone(),
                    error: Box::new(error),
                    stop: Some(stop.map(Box::new).map_err(Box::new)),
                })
            }
        };
        self.start = StartState::Complete(result.clone());
        result
    }

    fn complete_driver_failure(
        &mut self,
        failure: DriverActivationError,
        future: Option<DriverStartFuture>,
    ) -> PluginStartResult {
        let stop = self.seal_driver_failure(&failure);
        let failure = match future.and_then(drop_future) {
            Some(summary) => failure.with_future_drop_panic(summary),
            None => failure,
        };
        self.store_driver_failure(failure, stop)
    }

    fn seal_driver_failure(
        &mut self,
        failure: &DriverActivationError,
    ) -> Result<ReconcileOutcome, ComponentSlotError> {
        let epoch = self.id.selection_epoch();
        self.slot_mut().fail_activation(epoch, failure.to_string())
    }

    fn store_driver_failure(
        &mut self,
        failure: DriverActivationError,
        stop: Result<ReconcileOutcome, ComponentSlotError>,
    ) -> PluginStartResult {
        let result = Err(PluginStartError::Driver {
            activation_id: self.id.clone(),
            failure,
            stop: stop.map(Box::new).map_err(Box::new),
        });
        self.start = StartState::Complete(result.clone());
        result
    }

    fn complete_superseded(&mut self, stop: Option<ReconcileOutcome>) -> PluginStartResult {
        let result = Err(PluginStartError::Superseded {
            activation_id: self.id.clone(),
            stop: stop.map(Box::new),
        });
        self.start = StartState::Complete(result.clone());
        result
    }

    fn discard_after_seal(&mut self) {
        let state = std::mem::replace(&mut self.start, StartState::Transitioning);
        self.start = match state {
            StartState::Prepared(_) => StartState::Complete(Err(PluginStartError::Superseded {
                activation_id: self.id.clone(),
                stop: None,
            })),
            StartState::Running(future) => {
                let result = if let Some(summary) = drop_future(future) {
                    Err(PluginStartError::Driver {
                        activation_id: self.id.clone(),
                        failure: DriverActivationError::panicked(format!(
                            "cancelled activation future destructor panicked: {summary}"
                        )),
                        stop: Err(Box::new(self.already_stopping_error())),
                    })
                } else {
                    Err(PluginStartError::Superseded {
                        activation_id: self.id.clone(),
                        stop: None,
                    })
                };
                StartState::Complete(result)
            }
            StartState::Complete(Ok(_)) => {
                StartState::Complete(Err(PluginStartError::Superseded {
                    activation_id: self.id.clone(),
                    stop: None,
                }))
            }
            StartState::Complete(Err(error)) => StartState::Complete(Err(error)),
            StartState::Transitioning => {
                unreachable!("activation state transitions are not reentrant")
            }
        };
    }

    fn slot_mut(&mut self) -> &mut ComponentSlot {
        self.slot
            .as_deref_mut()
            .expect("an unreleased activation guard owns its component slot")
    }

    fn release_before_completion_error(&self) -> ComponentSlotError {
        ComponentSlotError::Reconcile(ReconcileError::InvalidState {
            operation: "release an unsettled plugin activation",
            state: self
                .slot
                .as_deref()
                .and_then(ComponentSlot::live_state)
                .map_or(ComponentStateKind::Removed, |state| state.kind()),
        })
    }

    fn already_stopping_error(&self) -> ComponentSlotError {
        ComponentSlotError::Reconcile(ReconcileError::InvalidState {
            operation: "fail an already cancelled plugin activation",
            state: self
                .slot
                .as_deref()
                .and_then(ComponentSlot::live_state)
                .map_or(ComponentStateKind::Removed, |state| state.kind()),
        })
    }

    fn seal_on_drop(&mut self) {
        if !self.cancellation.is_cancelled() {
            let epoch = self.id.selection_epoch();
            let _ = catch_unwind(AssertUnwindSafe(|| {
                self.slot_mut().fail_activation(
                    epoch,
                    "plugin activation owner dropped before controlled release",
                )
            }))
            .map_err(consume_panic_payload);
        }
        self.discard_after_seal();
    }
}

impl fmt::Debug for HostPluginActivation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostPluginActivation")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("cancelled", &self.cancellation.is_cancelled())
            .field("start_state", &self.start.label())
            .finish_non_exhaustive()
    }
}

impl Drop for HostPluginActivation<'_> {
    fn drop(&mut self) {
        if self.slot.is_some() {
            self.seal_on_drop();
        }
    }
}

/// Cancellation-safe borrowing waiter for [`HostPluginActivation::activate`].
#[must_use = "plugin activation advances only while this future is polled"]
pub struct ActivatePlugin<'host, 'slot> {
    activation: &'host mut HostPluginActivation<'slot>,
    registry: &'host ServiceRegistry,
}

impl Future for ActivatePlugin<'_, '_> {
    type Output = PluginStartResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        this.activation.poll_activate(this.registry, cx)
    }
}

/// Cloneable exact-activation handle for advisory health observation.
#[derive(Clone)]
pub struct PluginActivationHandle {
    id: PluginActivationId,
    kind: DriverKind,
    cancellation: ScopeCancellation,
    control: Weak<PreparedControl>,
}

impl PluginActivationHandle {
    pub const fn id(&self) -> &PluginActivationId {
        &self.id
    }

    pub const fn kind(&self) -> DriverKind {
        self.kind
    }

    /// Read the driver's current nonblocking health snapshot.
    ///
    /// Cancellation is checked before and after the driver call so a stopped
    /// or replaced activation cannot surface a fresh observation. Health never
    /// mutates lifecycle state. A probe may race teardown, but its panic payload
    /// is normalized before cancellation can suppress the stale result.
    pub fn health(&self) -> Result<PluginHealth, PluginHealthError> {
        if self.cancellation.is_cancelled() {
            return Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            });
        }
        let Some(control) = self.control.upgrade() else {
            return Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            });
        };
        let observed = match catch_unwind(AssertUnwindSafe(|| {
            control.with_prepared(PreparedDriverActivation::health)
        })) {
            Ok(Some(Ok(health))) => Ok(health),
            Ok(Some(Err(error))) => Err(PluginHealthError::Driver {
                activation_id: self.id.clone(),
                error,
            }),
            Ok(None) => Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            }),
            Err(payload) => Err(PluginHealthError::DriverPanicked {
                activation_id: self.id.clone(),
                summary: consume_panic_payload(payload),
            }),
        };
        if self.cancellation.is_cancelled() {
            return Err(PluginHealthError::Inactive {
                activation_id: self.id.clone(),
            });
        }
        observed
    }
}

impl fmt::Debug for PluginActivationHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginActivationHandle")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

enum StartState {
    Prepared(Option<DriverStartPermit>),
    Running(DriverStartFuture),
    Complete(PluginStartResult),
    Transitioning,
}

impl StartState {
    const fn label(&self) -> &'static str {
        match self {
            Self::Prepared(_) => "prepared",
            Self::Running(_) => "running",
            Self::Complete(_) => "complete",
            Self::Transitioning => "transitioning",
        }
    }
}

fn reject_control(
    control: &PreparedControl,
    error: HostPluginActivationError,
) -> HostPluginActivationError {
    match control.dispose() {
        None => error,
        Some(summary) => HostPluginActivationError::DriverControlDropPanicked {
            summary: format!("{error}; control destructor panic: {summary}"),
        },
    }
}

fn drop_future(future: DriverStartFuture) -> Option<String> {
    catch_unwind(AssertUnwindSafe(|| drop(future)))
        .err()
        .map(consume_panic_payload)
}

fn panic_summary(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "driver panicked with a non-string payload".to_owned()
    }
}

pub(super) fn consume_panic_payload(payload: Box<dyn Any + Send>) -> String {
    let summary = panic_summary(payload.as_ref());
    match catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        Ok(()) => summary,
        Err(drop_payload) => {
            let drop_summary = panic_summary(drop_payload.as_ref());
            std::mem::forget(drop_payload);
            format!("{summary}; panic payload destructor panicked: {drop_summary}")
        }
    }
}
