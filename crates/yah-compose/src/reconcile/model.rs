use std::{collections::BTreeMap, error::Error, fmt};

use crate::{
    ActivationEpoch, CloseReport, ComponentInstanceId, ComponentStateKind, EffectScopeError,
    LifecycleError, ProviderCandidate, ProviderRegistrationId, ServiceId, ServiceRegistryError,
};

/// Fence for one immutable provider assignment owned by one activation.
///
/// A reconciled activation never changes providers in place, so its activation
/// identity is also the exact lifetime of its provider selection.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProviderSelectionEpoch(ActivationEpoch);

impl ProviderSelectionEpoch {
    pub(crate) const fn new(activation: ActivationEpoch) -> Self {
        Self(activation)
    }

    pub const fn activation(self) -> ActivationEpoch {
        self.0
    }
}

impl fmt::Display for ProviderSelectionEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "selection:{}", self.0)
    }
}

/// Caller-selected exact providers for a component's required services.
///
/// Assignments are desired input. A reconciled activation clones and freezes a
/// complete assignment; mutating this value never changes a live activation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProviderAssignments {
    providers: BTreeMap<ServiceId, ProviderRegistrationId>,
}

impl ProviderAssignments {
    pub fn new() -> Self {
        Self::default()
    }

    /// Assign the candidate to its semantic service, returning any old choice.
    pub fn assign(&mut self, candidate: &ProviderCandidate) -> Option<ProviderRegistrationId> {
        self.providers
            .insert(candidate.service_id().clone(), candidate.id())
    }

    pub fn remove(&mut self, service_id: &ServiceId) -> Option<ProviderRegistrationId> {
        self.providers.remove(service_id)
    }

    pub fn provider_for(&self, service_id: &ServiceId) -> Option<ProviderRegistrationId> {
        self.providers.get(service_id).copied()
    }

    pub fn len(&self) -> usize {
        self.providers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&ServiceId, &ProviderRegistrationId)> {
        self.providers.iter()
    }
}

/// One immutable, activation-bound provider selection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderSelection {
    consumer_instance_id: ComponentInstanceId,
    epoch: ProviderSelectionEpoch,
    assignments: ProviderAssignments,
    providers: Vec<ProviderCandidate>,
}

impl ProviderSelection {
    pub(crate) fn new(
        consumer_instance_id: ComponentInstanceId,
        activation: ActivationEpoch,
        assignments: ProviderAssignments,
        providers: Vec<ProviderCandidate>,
    ) -> Self {
        Self {
            consumer_instance_id,
            epoch: ProviderSelectionEpoch::new(activation),
            assignments,
            providers,
        }
    }

    pub fn consumer_instance_id(&self) -> &ComponentInstanceId {
        &self.consumer_instance_id
    }

    pub const fn epoch(&self) -> ProviderSelectionEpoch {
        self.epoch
    }

    pub fn assignments(&self) -> &ProviderAssignments {
        &self.assignments
    }

    /// Selected candidates in requirement-declaration order.
    pub fn providers(&self) -> &[ProviderCandidate] {
        &self.providers
    }

    pub fn provider_for(&self, service_id: &ServiceId) -> Option<&ProviderCandidate> {
        self.providers
            .iter()
            .find(|provider| provider.service_id() == service_id)
    }
}

/// Why an exact assignment is not currently startable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DependencyIssue {
    MissingProvider {
        service_id: ServiceId,
    },
    Unassigned {
        service_id: ServiceId,
        candidate: ProviderRegistrationId,
    },
    Ambiguous {
        service_id: ServiceId,
        candidates: Vec<ProviderRegistrationId>,
    },
    AssignedProviderUnavailable {
        service_id: ServiceId,
        assigned: ProviderRegistrationId,
        available: Vec<ProviderRegistrationId>,
    },
}

impl DependencyIssue {
    pub fn service_id(&self) -> &ServiceId {
        match self {
            Self::MissingProvider { service_id }
            | Self::Unassigned { service_id, .. }
            | Self::Ambiguous { service_id, .. }
            | Self::AssignedProviderUnavailable { service_id, .. } => service_id,
        }
    }
}

/// Result of validating desired assignments against one registry inventory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DependencyReadiness {
    Ready,
    Pending(Vec<DependencyIssue>),
}

impl DependencyReadiness {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn issues(&self) -> &[DependencyIssue] {
        match self {
            Self::Ready => &[],
            Self::Pending(issues) => issues,
        }
    }
}

/// One desired provider identity change that requires recomposition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderChange {
    service_id: ServiceId,
    previous: Option<ProviderRegistrationId>,
    next: Option<ProviderRegistrationId>,
}

impl ProviderChange {
    pub(crate) fn new(
        service_id: ServiceId,
        previous: Option<ProviderRegistrationId>,
        next: Option<ProviderRegistrationId>,
    ) -> Self {
        Self {
            service_id,
            previous,
            next,
        }
    }

    pub fn service_id(&self) -> &ServiceId {
        &self.service_id
    }

    pub const fn previous(&self) -> Option<ProviderRegistrationId> {
        self.previous
    }

    pub const fn next(&self) -> Option<ProviderRegistrationId> {
        self.next
    }
}

/// Reason the current activation entered controlled teardown.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DependencyStopReason {
    AssignmentChanged(Vec<ProviderChange>),
    ProviderUnavailable(Vec<DependencyIssue>),
}

/// Observable result of one level-triggered reconciliation pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconcileOutcome {
    Pending {
        readiness: DependencyReadiness,
    },
    StartBegun {
        selection: ProviderSelection,
    },
    AwaitingStart {
        selection_epoch: ProviderSelectionEpoch,
    },
    Active {
        selection_epoch: ProviderSelectionEpoch,
    },
    StopBegun {
        selection_epoch: ProviderSelectionEpoch,
        reason: DependencyStopReason,
    },
    Stopping {
        selection_epoch: ProviderSelectionEpoch,
        reason: DependencyStopReason,
        cleanup_blocked: bool,
    },
    Removed,
}

/// Terminal observation from driving one activation's effect-scope close.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StopCompletion {
    Completed {
        selection_epoch: ProviderSelectionEpoch,
        report: CloseReport,
    },
    Blocked {
        selection_epoch: ProviderSelectionEpoch,
        report: CloseReport,
    },
}

impl StopCompletion {
    pub fn report(&self) -> &CloseReport {
        match self {
            Self::Completed { report, .. } | Self::Blocked { report, .. } => report,
        }
    }

    pub fn is_completed(&self) -> bool {
        matches!(self, Self::Completed { .. })
    }
}

/// Rejected dependency-reconciliation operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReconcileError {
    UndeclaredAssignment {
        service_id: ServiceId,
    },
    UndeclaredRequirement {
        service_id: ServiceId,
    },
    RequirementContractMismatch {
        service_id: ServiceId,
        expected: &'static str,
        received: &'static str,
    },
    NoCurrentSelection {
        received: ProviderSelectionEpoch,
    },
    StaleSelection {
        expected: ProviderSelectionEpoch,
        received: ProviderSelectionEpoch,
    },
    InvalidState {
        operation: &'static str,
        state: ComponentStateKind,
    },
    Lifecycle(LifecycleError),
    EffectScope(EffectScopeError),
    Registry(ServiceRegistryError),
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UndeclaredAssignment { service_id } => {
                write!(f, "provider assignment for undeclared service {service_id}")
            }
            Self::UndeclaredRequirement { service_id } => {
                write!(f, "component does not require service {service_id}")
            }
            Self::RequirementContractMismatch {
                service_id,
                expected,
                received,
            } => write!(
                f,
                "required service {service_id} uses Rust contract {expected}, not {received}"
            ),
            Self::NoCurrentSelection { received } => {
                write!(f, "selection epoch {received} has no current activation")
            }
            Self::StaleSelection { expected, received } => write!(
                f,
                "stale provider selection epoch {received}; current epoch is {expected}"
            ),
            Self::InvalidState { operation, state } => {
                write!(
                    f,
                    "cannot {operation} while reconciled component is {state}"
                )
            }
            Self::Lifecycle(error) => error.fmt(f),
            Self::EffectScope(error) => error.fmt(f),
            Self::Registry(error) => error.fmt(f),
        }
    }
}

impl Error for ReconcileError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Lifecycle(error) => Some(error),
            Self::EffectScope(error) => Some(error),
            Self::Registry(error) => Some(error),
            _ => None,
        }
    }
}

impl From<LifecycleError> for ReconcileError {
    fn from(error: LifecycleError) -> Self {
        Self::Lifecycle(error)
    }
}

impl From<EffectScopeError> for ReconcileError {
    fn from(error: EffectScopeError) -> Self {
        Self::EffectScope(error)
    }
}

impl From<ServiceRegistryError> for ReconcileError {
    fn from(error: ServiceRegistryError) -> Self {
        Self::Registry(error)
    }
}
