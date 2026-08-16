use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use crate::{
    DriverActivationError, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginRevisionId,
    PreparedDriverActivation,
};
use yah_compose::ScopeCancellation;

use super::{
    DriverActivationObservation, DriverConformanceResourceState, DriverConformanceTerminal,
};

pub(super) fn observe_driver(
    inner: Arc<dyn PluginDriver>,
) -> (Arc<dyn PluginDriver>, ObservationHandle) {
    let observations = Arc::new(ObservationStore::default());
    let driver = Arc::new(ObservedDriver {
        inner,
        observations: Arc::clone(&observations),
    });
    (driver, ObservationHandle { observations })
}

#[derive(Clone)]
pub(super) struct ObservationHandle {
    observations: Arc<ObservationStore>,
}

impl ObservationHandle {
    pub(super) fn mark_later_cleanup(&self) {
        self.observations
            .later_cleanup
            .store(true, Ordering::Release);
    }

    pub(super) fn snapshot(
        &self,
        activation: &PluginActivationId,
        resource_state: Option<DriverConformanceResourceState>,
        resource_probe_error: Option<String>,
    ) -> DriverActivationObservation {
        self.observations
            .snapshot(activation, resource_state, resource_probe_error)
    }
}

struct ObservedDriver {
    inner: Arc<dyn PluginDriver>,
    observations: Arc<ObservationStore>,
}

impl PluginDriver for ObservedDriver {
    fn kind(&self) -> DriverKind {
        self.inner.kind()
    }

    fn revision_id(&self) -> &PluginRevisionId {
        self.inner.revision_id()
    }

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError> {
        let activation = request.id().clone();
        let cancellation = request.cancellation().clone();
        self.observations.update(&activation, |entry| {
            increment(&mut entry.prepare_calls);
        });
        match self.inner.prepare(request) {
            Ok(prepared) => {
                self.observations.update(&activation, |entry| {
                    entry.prepare_succeeded = true;
                });
                Ok(Arc::new(ObservedPreparedActivation {
                    inner: prepared,
                    activation,
                    cancellation,
                    observations: Arc::clone(&self.observations),
                }))
            }
            Err(error) => Err(error),
        }
    }
}

struct ObservedPreparedActivation {
    inner: Arc<dyn PreparedDriverActivation>,
    activation: PluginActivationId,
    cancellation: ScopeCancellation,
    observations: Arc<ObservationStore>,
}

impl PreparedDriverActivation for ObservedPreparedActivation {
    fn id(&self) -> &PluginActivationId {
        self.inner.id()
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        let matched = permit.id() == &self.activation;
        self.observations.update(&self.activation, |entry| {
            increment(&mut entry.start_factory_calls);
            entry.start_permit_matched =
                Some(entry.start_permit_matched.unwrap_or(true) && matched);
        });
        let inner = self.inner.start(permit);
        Box::pin(ObservedStartFuture {
            inner,
            activation: self.activation.clone(),
            cancellation: self.cancellation.clone(),
            observations: Arc::clone(&self.observations),
        })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        self.observations.update(&self.activation, |entry| {
            increment(&mut entry.health_calls);
        });
        self.inner.health()
    }

    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        let matched = permit.id() == &self.activation;
        let cancelled = self.cancellation.is_cancelled();
        let later_cleanup = self.observations.later_cleanup.load(Ordering::Acquire);
        self.observations.update(&self.activation, |entry| {
            increment(&mut entry.deactivation_factory_calls);
            entry.deactivation_saw_cancellation |= cancelled;
            entry.deactivation_saw_later_cleanup |= later_cleanup;
            entry.deactivation_permit_matched =
                Some(entry.deactivation_permit_matched.unwrap_or(true) && matched);
        });
        let inner = self.inner.deactivate(permit);
        Box::pin(ObservedDeactivationFuture {
            inner,
            activation: self.activation.clone(),
            cancellation: self.cancellation.clone(),
            observations: Arc::clone(&self.observations),
        })
    }
}

struct ObservedStartFuture {
    inner: DriverFuture<Result<(), DriverActivationError>>,
    activation: PluginActivationId,
    cancellation: ScopeCancellation,
    observations: Arc<ObservationStore>,
}

impl Future for ObservedStartFuture {
    type Output = Result<(), DriverActivationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.observations.update(&this.activation, |entry| {
            increment(&mut entry.start_polls);
        });
        match this.inner.as_mut().poll(cx) {
            Poll::Pending => {
                this.observations.update(&this.activation, |entry| {
                    increment(&mut entry.start_pending_polls);
                });
                Poll::Pending
            }
            Poll::Ready(result) => {
                let terminal = if result.is_ok() {
                    DriverConformanceTerminal::Succeeded
                } else {
                    DriverConformanceTerminal::ReturnedError
                };
                this.observations.update(&this.activation, |entry| {
                    entry.start_terminal = Some(terminal);
                });
                Poll::Ready(result)
            }
        }
    }
}

impl Drop for ObservedStartFuture {
    fn drop(&mut self) {
        let cancelled = self.cancellation.is_cancelled();
        self.observations.update(&self.activation, |entry| {
            increment(&mut entry.start_drops);
            entry.start_drop_saw_cancellation |= cancelled;
        });
    }
}

struct ObservedDeactivationFuture {
    inner: DriverFuture<Result<(), DriverDeactivationError>>,
    activation: PluginActivationId,
    cancellation: ScopeCancellation,
    observations: Arc<ObservationStore>,
}

impl Future for ObservedDeactivationFuture {
    type Output = Result<(), DriverDeactivationError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let cancelled = this.cancellation.is_cancelled();
        this.observations.update(&this.activation, |entry| {
            increment(&mut entry.deactivation_polls);
            entry.deactivation_saw_cancellation |= cancelled;
        });
        match this.inner.as_mut().poll(cx) {
            Poll::Pending => {
                this.observations.update(&this.activation, |entry| {
                    increment(&mut entry.deactivation_pending_polls);
                });
                Poll::Pending
            }
            Poll::Ready(result) => {
                let terminal = if result.is_ok() {
                    DriverConformanceTerminal::Succeeded
                } else {
                    DriverConformanceTerminal::ReturnedError
                };
                this.observations.update(&this.activation, |entry| {
                    entry.deactivation_terminal = Some(terminal);
                });
                Poll::Ready(result)
            }
        }
    }
}

impl Drop for ObservedDeactivationFuture {
    fn drop(&mut self) {
        let cancelled = self.cancellation.is_cancelled();
        self.observations.update(&self.activation, |entry| {
            increment(&mut entry.deactivation_drops);
            entry.deactivation_saw_cancellation |= cancelled;
        });
    }
}

#[derive(Default)]
struct ObservationStore {
    entries: Mutex<HashMap<PluginActivationId, MutableObservation>>,
    later_cleanup: AtomicBool,
}

impl ObservationStore {
    fn update(
        &self,
        activation: &PluginActivationId,
        update: impl FnOnce(&mut MutableObservation),
    ) {
        let mut entries = lock(&self.entries);
        update(entries.entry(activation.clone()).or_default());
    }

    fn snapshot(
        &self,
        activation: &PluginActivationId,
        resource_state: Option<DriverConformanceResourceState>,
        resource_probe_error: Option<String>,
    ) -> DriverActivationObservation {
        let entry = lock(&self.entries)
            .get(activation)
            .cloned()
            .unwrap_or_default();
        DriverActivationObservation::new(
            activation.clone(),
            entry.prepare_calls,
            entry.prepare_succeeded,
            entry.start_factory_calls,
            entry.start_polls,
            entry.start_pending_polls,
            entry.start_terminal,
            entry.start_drops,
            entry.start_drop_saw_cancellation,
            entry.start_permit_matched,
            entry.health_calls,
            entry.deactivation_factory_calls,
            entry.deactivation_polls,
            entry.deactivation_pending_polls,
            entry.deactivation_terminal,
            entry.deactivation_drops,
            entry.deactivation_saw_cancellation,
            entry.deactivation_saw_later_cleanup,
            entry.deactivation_permit_matched,
            resource_state,
            resource_probe_error,
        )
    }
}

#[derive(Clone, Default)]
struct MutableObservation {
    prepare_calls: u64,
    prepare_succeeded: bool,
    start_factory_calls: u64,
    start_polls: u64,
    start_pending_polls: u64,
    start_terminal: Option<DriverConformanceTerminal>,
    start_drops: u64,
    start_drop_saw_cancellation: bool,
    start_permit_matched: Option<bool>,
    health_calls: u64,
    deactivation_factory_calls: u64,
    deactivation_polls: u64,
    deactivation_pending_polls: u64,
    deactivation_terminal: Option<DriverConformanceTerminal>,
    deactivation_drops: u64,
    deactivation_saw_cancellation: bool,
    deactivation_saw_later_cleanup: bool,
    deactivation_permit_matched: Option<bool>,
}

fn increment(value: &mut u64) {
    *value = value.saturating_add(1);
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
