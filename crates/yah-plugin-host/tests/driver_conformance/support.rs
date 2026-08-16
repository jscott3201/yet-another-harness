use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    task::{Context, Poll},
};

use yah_plugin_host::{
    DriverActivationError, DriverConformanceCase, DriverConformanceProbe,
    DriverConformanceProbeError, DriverConformanceResourceState, DriverConformanceSetupError,
    DriverConformanceSubject, DriverConformanceTarget, DriverDeactivationError, DriverFuture,
    DriverHealthError, DriverKind, DriverPrepareError, DriverStartPermit, DriverStopPermit,
    PackageDigest, PluginActivationId, PluginActivationRequest, PluginDriver, PluginEntrypoint,
    PluginHealth, PluginManifest, PluginPackageId, PluginRevision, PluginRevisionId, PluginVersion,
    PreparedDriverActivation, SdkVersionRequirement,
};

pub(super) struct ReferenceTarget {
    violation: Violation,
    states: Arc<Mutex<Option<Arc<Mutex<ActivationStates>>>>>,
}

impl ReferenceTarget {
    pub(super) fn new() -> Self {
        Self::with_violation(Violation::None)
    }

    pub(super) fn with_mismatched_revision() -> Self {
        Self::with_violation(Violation::RevisionMismatch)
    }

    pub(super) fn with_pending_start_that_never_acquires() -> Self {
        Self::with_violation(Violation::PendingStartNeverAcquires)
    }

    pub(super) fn with_pending_start_completion() -> Self {
        Self::with_violation(Violation::PendingStartCompletes)
    }

    pub(super) fn with_unhealthy_ready_activation() -> Self {
        Self::with_violation(Violation::UnhealthyReady)
    }

    pub(super) fn with_probe_failure_after_acquisition() -> Self {
        Self::with_violation(Violation::ProbeFailureAfterAcquire)
    }

    pub(super) fn with_transient_probe_failure_after_acquisition() -> Self {
        Self::with_violation(Violation::TransientProbeFailureAfterAcquire)
    }

    pub(super) fn with_clean_deactivation() -> Self {
        Self::with_violation(Violation::CleanDeactivation)
    }

    pub(super) fn with_second_shared_start_failure() -> Self {
        Self::with_violation(Violation::SecondSharedStartFailure)
    }

    pub(super) fn with_pending_first_cleanup_after_second_start_failure() -> Self {
        Self::with_violation(Violation::PendingFirstCleanupAfterSecondStartFailure)
    }

    pub(super) fn with_shared_driver_crosstalk() -> Self {
        Self::with_violation(Violation::SharedCrossTalk)
    }

    pub(super) fn with_reverse_shared_driver_crosstalk() -> Self {
        Self::with_violation(Violation::ReverseSharedCrossTalk)
    }

    pub(super) fn with_driver_drop_panic() -> Self {
        Self::with_violation(Violation::DriverDropPanic)
    }

    pub(super) fn with_setup_kind_mismatch_and_drop_payload_panic() -> Self {
        Self::with_violation(Violation::SetupKindMismatchDropPayloadPanic)
    }

    pub(super) fn activation_states(&self) -> Vec<(bool, DriverConformanceResourceState)> {
        let states = self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(states) = states else {
            return Vec::new();
        };
        states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .map(|state| {
                (
                    state.cancellation.is_cancelled(),
                    state.resource_state().expect("reference state is valid"),
                )
            })
            .collect()
    }

    fn with_violation(violation: Violation) -> Self {
        Self {
            violation,
            states: Arc::new(Mutex::new(None)),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Violation {
    None,
    RevisionMismatch,
    PendingStartCompletes,
    PendingStartNeverAcquires,
    UnhealthyReady,
    ProbeFailureAfterAcquire,
    TransientProbeFailureAfterAcquire,
    CleanDeactivation,
    SecondSharedStartFailure,
    PendingFirstCleanupAfterSecondStartFailure,
    SharedCrossTalk,
    ReverseSharedCrossTalk,
    DriverDropPanic,
    SetupKindMismatchDropPayloadPanic,
}

impl DriverConformanceTarget for ReferenceTarget {
    fn name(&self) -> &str {
        match self.violation {
            Violation::None => "reference-scripted-driver",
            Violation::RevisionMismatch => "reference-metadata-mismatch",
            Violation::PendingStartCompletes => "reference-pending-completes",
            Violation::PendingStartNeverAcquires => "reference-pending-never-acquires",
            Violation::UnhealthyReady => "reference-unhealthy-ready",
            Violation::ProbeFailureAfterAcquire => "reference-probe-failure",
            Violation::TransientProbeFailureAfterAcquire => "reference-transient-probe-failure",
            Violation::CleanDeactivation => "reference-clean-deactivation",
            Violation::SecondSharedStartFailure => "reference-second-start-failure",
            Violation::PendingFirstCleanupAfterSecondStartFailure => {
                "reference-paired-pending-cleanup"
            }
            Violation::SharedCrossTalk => "reference-shared-crosstalk",
            Violation::ReverseSharedCrossTalk => "reference-reverse-shared-crosstalk",
            Violation::DriverDropPanic => "reference-driver-drop-panic",
            Violation::SetupKindMismatchDropPayloadPanic => {
                "reference-setup-kind-mismatch-drop-payload-panic"
            }
        }
    }

    fn kind(&self) -> DriverKind {
        if self.violation == Violation::SetupKindMismatchDropPayloadPanic {
            DriverKind::NodeProcess
        } else {
            DriverKind::BuiltinRust
        }
    }

    fn subject(
        &self,
        case: DriverConformanceCase,
    ) -> Result<DriverConformanceSubject, DriverConformanceSetupError> {
        let expected = revision(case, 'a');
        let driver_revision = if self.violation == Violation::RevisionMismatch {
            revision(case, 'b').id().clone()
        } else {
            expected.id().clone()
        };
        let states = Arc::new(Mutex::new(HashMap::new()));
        *self
            .states
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&states));
        let mut plans = match case {
            DriverConformanceCase::ReadyLifecycle => vec![Plan::ready()],
            DriverConformanceCase::PendingStartCancellation => vec![Plan {
                start: StartBehavior::Pending,
                health: HealthBehavior::Healthy,
                deactivate: DeactivationBehavior::Ready,
            }],
            DriverConformanceCase::ReturnedStartFailure => vec![Plan {
                start: StartBehavior::Failure,
                health: HealthBehavior::Healthy,
                deactivate: DeactivationBehavior::Ready,
            }],
            DriverConformanceCase::ReturnedDeactivationFailure => vec![Plan {
                start: StartBehavior::Ready,
                health: HealthBehavior::Healthy,
                deactivate: DeactivationBehavior::Failure,
            }],
            DriverConformanceCase::SharedDriverIsolation => {
                vec![Plan::ready(), Plan::ready()]
            }
            _ => {
                return Err(DriverConformanceSetupError::new(
                    "reference target does not implement this future case",
                ));
            }
        };
        match (self.violation, case) {
            (Violation::PendingStartCompletes, DriverConformanceCase::PendingStartCancellation) => {
                plans[0].start = StartBehavior::Ready;
            }
            (
                Violation::PendingStartNeverAcquires,
                DriverConformanceCase::PendingStartCancellation,
            ) => {
                plans[0].start = StartBehavior::PendingWithoutAcquire;
            }
            (Violation::UnhealthyReady, DriverConformanceCase::ReadyLifecycle) => {
                plans[0].health = HealthBehavior::Unhealthy;
            }
            (Violation::CleanDeactivation, DriverConformanceCase::ReturnedDeactivationFailure) => {
                plans[0].deactivate = DeactivationBehavior::Ready;
            }
            (Violation::SecondSharedStartFailure, DriverConformanceCase::SharedDriverIsolation) => {
                plans[1].start = StartBehavior::Failure;
            }
            (
                Violation::PendingFirstCleanupAfterSecondStartFailure,
                DriverConformanceCase::SharedDriverIsolation,
            ) => {
                plans[0].deactivate = DeactivationBehavior::Pending;
                plans[1].start = StartBehavior::Failure;
            }
            (Violation::SharedCrossTalk, DriverConformanceCase::SharedDriverIsolation) => {
                plans[0].deactivate = DeactivationBehavior::ReleaseAll;
            }
            (Violation::ReverseSharedCrossTalk, DriverConformanceCase::SharedDriverIsolation) => {
                plans[1].deactivate = DeactivationBehavior::ReviveOthers;
            }
            _ => {}
        }
        let driver = Arc::new(ReferenceDriver {
            revision: driver_revision,
            plans: Mutex::new(plans.into()),
            states: Arc::clone(&states),
            drop_behavior: match self.violation {
                Violation::DriverDropPanic => DriverDropBehavior::StringPanic,
                Violation::SetupKindMismatchDropPayloadPanic => {
                    DriverDropBehavior::PayloadDropPanic
                }
                _ => DriverDropBehavior::None,
            },
        });
        let probe = Arc::new(ReferenceProbe {
            states: Arc::clone(&states),
            fail_after_acquire: self.violation == Violation::ProbeFailureAfterAcquire,
            fail_once_after_acquire: AtomicBool::new(
                self.violation == Violation::TransientProbeFailureAfterAcquire,
            ),
        });
        Ok(DriverConformanceSubject::new(expected, driver, probe))
    }
}

fn revision(case: DriverConformanceCase, digest: char) -> PluginRevision {
    let package = PluginPackageId::new(format!("test.conformance.{}", package_suffix(case)))
        .expect("reference package ID is canonical");
    let manifest = PluginManifest::new(
        package,
        PluginVersion::new("1.0.0").unwrap(),
        SdkVersionRequirement::new(">=0.1.0, <0.2.0").unwrap(),
        PluginEntrypoint::BuiltinRust {},
        vec![],
        vec![],
        vec![],
    )
    .unwrap();
    PluginRevision::new(
        manifest,
        PackageDigest::new(format!("blake3:{}", digest.to_string().repeat(64))).unwrap(),
    )
}

fn package_suffix(case: DriverConformanceCase) -> &'static str {
    match case {
        DriverConformanceCase::ReadyLifecycle => "ready",
        DriverConformanceCase::PendingStartCancellation => "pending",
        DriverConformanceCase::ReturnedStartFailure => "start.failure",
        DriverConformanceCase::ReturnedDeactivationFailure => "stop.failure",
        DriverConformanceCase::SharedDriverIsolation => "shared",
        _ => "unsupported",
    }
}

#[derive(Clone, Copy)]
struct Plan {
    start: StartBehavior,
    health: HealthBehavior,
    deactivate: DeactivationBehavior,
}

impl Plan {
    const fn ready() -> Self {
        Self {
            start: StartBehavior::Ready,
            health: HealthBehavior::Healthy,
            deactivate: DeactivationBehavior::Ready,
        }
    }
}

#[derive(Clone, Copy)]
enum StartBehavior {
    Ready,
    Pending,
    /// Pends without ever acquiring, which no conforming driver does.
    ///
    /// The pending-start case polls until the driver acquires, so this is what
    /// makes that loop's bound reachable: without it nothing distinguishes a
    /// bounded loop from one that would spin forever.
    PendingWithoutAcquire,
    Failure,
}

#[derive(Clone, Copy)]
enum HealthBehavior {
    Healthy,
    Unhealthy,
}

#[derive(Clone, Copy)]
enum DeactivationBehavior {
    Ready,
    Failure,
    Pending,
    ReleaseAll,
    ReviveOthers,
}

struct ReferenceDriver {
    revision: PluginRevisionId,
    plans: Mutex<VecDeque<Plan>>,
    states: Arc<Mutex<ActivationStates>>,
    drop_behavior: DriverDropBehavior,
}

impl Drop for ReferenceDriver {
    fn drop(&mut self) {
        match self.drop_behavior {
            DriverDropBehavior::None => {}
            DriverDropBehavior::StringPanic => panic!("reference driver destructor panic"),
            DriverDropBehavior::PayloadDropPanic => std::panic::panic_any(DropPanickingPayload),
        }
    }
}

#[derive(Clone, Copy)]
enum DriverDropBehavior {
    None,
    StringPanic,
    PayloadDropPanic,
}

struct DropPanickingPayload;

impl Drop for DropPanickingPayload {
    fn drop(&mut self) {
        panic!("reference panic payload destructor panic");
    }
}

impl PluginDriver for ReferenceDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::BuiltinRust
    }

    fn revision_id(&self) -> &PluginRevisionId {
        &self.revision
    }

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError> {
        let plan = self
            .plans
            .lock()
            .unwrap()
            .pop_front()
            .ok_or_else(|| DriverPrepareError::new("reference plan exhausted"))?;
        let state = Arc::new(ActivationState {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
            cancellation: request.cancellation().clone(),
        });
        self.states
            .lock()
            .unwrap()
            .insert(request.id().clone(), Arc::clone(&state));
        Ok(Arc::new(ReferencePrepared {
            id: request.id().clone(),
            plan,
            state,
            states: Arc::clone(&self.states),
        }))
    }
}

struct ReferencePrepared {
    id: PluginActivationId,
    plan: Plan,
    state: Arc<ActivationState>,
    states: Arc<Mutex<ActivationStates>>,
}

impl PreparedDriverActivation for ReferencePrepared {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        debug_assert_eq!(permit.id(), &self.id);
        if !matches!(self.plan.start, StartBehavior::PendingWithoutAcquire) {
            self.state
                .resource
                .store(ResourceState::Live as u8, Ordering::Release);
        }
        Box::pin(StartFuture {
            behavior: self.plan.start,
        })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        if matches!(self.plan.health, HealthBehavior::Healthy)
            && self.state.resource.load(Ordering::Acquire) == ResourceState::Live as u8
        {
            Ok(PluginHealth::Healthy)
        } else {
            Err(DriverHealthError::new("reference resource is not live"))
        }
    }

    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        debug_assert_eq!(permit.id(), &self.id);
        Box::pin(DeactivateFuture {
            behavior: self.plan.deactivate,
            state: Arc::clone(&self.state),
            states: Arc::clone(&self.states),
        })
    }
}

struct StartFuture {
    behavior: StartBehavior,
}

impl Future for StartFuture {
    type Output = Result<(), DriverActivationError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.behavior {
            StartBehavior::Ready => Poll::Ready(Ok(())),
            StartBehavior::Pending | StartBehavior::PendingWithoutAcquire => Poll::Pending,
            StartBehavior::Failure => Poll::Ready(Err(DriverActivationError::failed(
                "reference start failure",
            ))),
        }
    }
}

struct DeactivateFuture {
    behavior: DeactivationBehavior,
    state: Arc<ActivationState>,
    states: Arc<Mutex<ActivationStates>>,
}

impl Future for DeactivateFuture {
    type Output = Result<(), DriverDeactivationError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.behavior {
            DeactivationBehavior::Ready => {
                self.state
                    .resource
                    .store(ResourceState::Released as u8, Ordering::Release);
                Poll::Ready(Ok(()))
            }
            DeactivationBehavior::Failure => {
                self.state
                    .resource
                    .store(ResourceState::Released as u8, Ordering::Release);
                Poll::Ready(Err(DriverDeactivationError::new(
                    "reference deactivation failure",
                )))
            }
            DeactivationBehavior::Pending => Poll::Pending,
            DeactivationBehavior::ReleaseAll => {
                for state in self
                    .states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                {
                    state
                        .resource
                        .store(ResourceState::Released as u8, Ordering::Release);
                }
                Poll::Ready(Ok(()))
            }
            DeactivationBehavior::ReviveOthers => {
                for state in self
                    .states
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .values()
                {
                    state
                        .resource
                        .store(ResourceState::Live as u8, Ordering::Release);
                }
                self.state
                    .resource
                    .store(ResourceState::Released as u8, Ordering::Release);
                Poll::Ready(Ok(()))
            }
        }
    }
}

struct ActivationState {
    resource: AtomicU8,
    cancellation: yah_compose::ScopeCancellation,
}

impl ActivationState {
    fn resource_state(
        &self,
    ) -> Result<DriverConformanceResourceState, DriverConformanceProbeError> {
        match self.resource.load(Ordering::Acquire) {
            value if value == ResourceState::NotAcquired as u8 => {
                Ok(DriverConformanceResourceState::NotAcquired)
            }
            value if value == ResourceState::Live as u8 => Ok(DriverConformanceResourceState::Live),
            value if value == ResourceState::Released as u8 => {
                Ok(DriverConformanceResourceState::Released)
            }
            _ => Err(DriverConformanceProbeError::new(
                "reference resource state was invalid",
            )),
        }
    }
}

#[repr(u8)]
enum ResourceState {
    NotAcquired = 0,
    Live = 1,
    Released = 2,
}

struct ReferenceProbe {
    states: Arc<Mutex<ActivationStates>>,
    fail_after_acquire: bool,
    fail_once_after_acquire: AtomicBool,
}

type ActivationStates = HashMap<PluginActivationId, Arc<ActivationState>>;

impl DriverConformanceProbe for ReferenceProbe {
    fn resource_state(
        &self,
        activation: &PluginActivationId,
    ) -> Result<DriverConformanceResourceState, DriverConformanceProbeError> {
        let state = self
            .states
            .lock()
            .unwrap()
            .get(activation)
            .cloned()
            .ok_or_else(|| DriverConformanceProbeError::new("unknown activation identity"))?;
        let resource = state.resource.load(Ordering::Acquire);
        if self.fail_after_acquire && resource != ResourceState::NotAcquired as u8 {
            return Err(DriverConformanceProbeError::new(
                "injected probe failure after resource acquisition",
            ));
        }
        if resource != ResourceState::NotAcquired as u8
            && self.fail_once_after_acquire.swap(false, Ordering::AcqRel)
        {
            return Err(DriverConformanceProbeError::new(
                "injected transient probe failure after resource acquisition",
            ));
        }
        state.resource_state()
    }
}
