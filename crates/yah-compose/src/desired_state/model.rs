use std::{error::Error, fmt};

use crate::{
    CloseReport, ComponentDefinition, ComponentInstanceId, ComponentRevisionId,
    ComponentStopReason, ProviderAssignments, ProviderSelectionEpoch, ReconcileError,
    ReconcileOutcome, Scope, StopTarget,
};

/// Opaque process-local fence for one desired-state snapshot.
///
/// The composition authority chooses the sequence, while
/// [`crate::ComponentSlot::generation`] binds it to one process-unique slot
/// incarnation. It is not a durable epoch and does not make process-local
/// provider IDs persistent.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DesiredGeneration {
    slot_incarnation: u64,
    sequence: u64,
}

impl DesiredGeneration {
    pub(crate) const fn new(slot_incarnation: u64, sequence: u64) -> Self {
        Self {
            slot_incarnation,
            sequence,
        }
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    pub(crate) const fn slot_incarnation(self) -> u64 {
        self.slot_incarnation
    }
}

impl fmt::Display for DesiredGeneration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.slot_incarnation, self.sequence)
    }
}

/// One immutable component definition, scope, and configuration revision.
///
/// The loader owns actual configuration and factory inputs. Reusing this
/// revision ID with a different definition or scope is rejected by a slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentRevision {
    id: ComponentRevisionId,
    definition: ComponentDefinition,
    scope: Scope,
}

impl ComponentRevision {
    pub fn new(
        id: impl Into<ComponentRevisionId>,
        definition: ComponentDefinition,
        scope: Scope,
    ) -> Self {
        Self {
            id: id.into(),
            definition,
            scope,
        }
    }

    pub fn id(&self) -> &ComponentRevisionId {
        &self.id
    }

    pub fn definition(&self) -> &ComponentDefinition {
        &self.definition
    }

    pub fn scope(&self) -> &Scope {
        &self.scope
    }
}

/// Latest desired state supplied to one stable component slot.
///
/// Disabled revisions remain inspectable but own no mounted component. Exact
/// provider assignments are process-local and therefore exist only while the
/// revision is enabled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DesiredComponentState {
    Enabled {
        generation: DesiredGeneration,
        revision: ComponentRevision,
        assignments: ProviderAssignments,
    },
    Disabled {
        generation: DesiredGeneration,
        revision: ComponentRevision,
    },
    Removed {
        generation: DesiredGeneration,
    },
}

impl DesiredComponentState {
    pub fn enabled(
        generation: DesiredGeneration,
        revision: ComponentRevision,
        assignments: ProviderAssignments,
    ) -> Self {
        Self::Enabled {
            generation,
            revision,
            assignments,
        }
    }

    pub fn disabled(generation: DesiredGeneration, revision: ComponentRevision) -> Self {
        Self::Disabled {
            generation,
            revision,
        }
    }

    pub const fn removed(generation: DesiredGeneration) -> Self {
        Self::Removed { generation }
    }

    pub const fn generation(&self) -> DesiredGeneration {
        match self {
            Self::Enabled { generation, .. }
            | Self::Disabled { generation, .. }
            | Self::Removed { generation } => *generation,
        }
    }

    pub fn revision(&self) -> Option<&ComponentRevision> {
        match self {
            Self::Enabled { revision, .. } | Self::Disabled { revision, .. } => Some(revision),
            Self::Removed { .. } => None,
        }
    }

    pub fn assignments(&self) -> Option<&ProviderAssignments> {
        match self {
            Self::Enabled { assignments, .. } => Some(assignments),
            Self::Disabled { .. } | Self::Removed { .. } => None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled { .. })
    }
}

/// Observable result of one synchronous desired-state reconciliation pass.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComponentSlotOutcome {
    Removed {
        generation: DesiredGeneration,
    },
    Disabled {
        generation: DesiredGeneration,
        desired_revision: ComponentRevisionId,
    },
    Mounted {
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        component: ReconcileOutcome,
    },
    Reconciled {
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        component: ReconcileOutcome,
    },
    Unmounted {
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        reason: crate::DesiredStopReason,
    },
    StopBegun {
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        selection_epoch: ProviderSelectionEpoch,
        target: StopTarget,
        reason: ComponentStopReason,
    },
    Stopping {
        generation: DesiredGeneration,
        applied_revision: ComponentRevisionId,
        selection_epoch: ProviderSelectionEpoch,
        target: StopTarget,
        reason: ComponentStopReason,
        cleanup_blocked: bool,
    },
    /// The desired snapshot is accepted, but the slot cannot currently mount
    /// or reconcile it. Desired content changes require a newer generation;
    /// an identical snapshot may recheck external convergence conditions.
    ConvergenceBlocked {
        generation: DesiredGeneration,
        desired_revision: Option<ComponentRevisionId>,
        applied_revision: Option<ComponentRevisionId>,
        error: ReconcileError,
    },
}

/// How the latest terminal close report was handled by the slot authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StopDisposition {
    Completed,
    Blocked,
    Abandoned,
}

/// Last terminal cleanup observation, retained after an instance is removed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StopRecord {
    applied_revision: ComponentRevisionId,
    selection_epoch: ProviderSelectionEpoch,
    target: StopTarget,
    reason: ComponentStopReason,
    disposition: StopDisposition,
    report: CloseReport,
}

impl StopRecord {
    pub(crate) fn new(
        applied_revision: ComponentRevisionId,
        selection_epoch: ProviderSelectionEpoch,
        target: StopTarget,
        reason: ComponentStopReason,
        disposition: StopDisposition,
        report: CloseReport,
    ) -> Self {
        Self {
            applied_revision,
            selection_epoch,
            target,
            reason,
            disposition,
            report,
        }
    }

    pub fn applied_revision(&self) -> &ComponentRevisionId {
        &self.applied_revision
    }

    pub const fn selection_epoch(&self) -> ProviderSelectionEpoch {
        self.selection_epoch
    }

    pub const fn target(&self) -> StopTarget {
        self.target
    }

    pub fn reason(&self) -> &ComponentStopReason {
        &self.reason
    }

    pub const fn disposition(&self) -> StopDisposition {
        self.disposition
    }

    pub fn report(&self) -> &CloseReport {
        &self.report
    }
}

/// Rejected desired-state or slot operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComponentSlotError {
    SlotIncarnationExhausted,
    ForeignDesiredGeneration {
        expected: DesiredGeneration,
        received: DesiredGeneration,
    },
    StaleDesired {
        current: DesiredGeneration,
        received: DesiredGeneration,
    },
    DesiredGenerationConflict {
        generation: DesiredGeneration,
    },
    RevisionIdentityConflict {
        revision: ComponentRevisionId,
    },
    NoMountedComponent {
        instance_id: ComponentInstanceId,
    },
    MountedRevisionNotEnabled {
        desired: Option<ComponentRevisionId>,
        applied: ComponentRevisionId,
    },
    Reconcile(ReconcileError),
}

impl fmt::Display for ComponentSlotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SlotIncarnationExhausted => {
                f.write_str("component slot incarnation space exhausted")
            }
            Self::ForeignDesiredGeneration { expected, received } => write!(
                f,
                "desired generation {received} belongs to another slot incarnation; expected {expected}"
            ),
            Self::StaleDesired { current, received } => write!(
                f,
                "stale desired generation {received}; current generation is {current}"
            ),
            Self::DesiredGenerationConflict { generation } => write!(
                f,
                "desired generation {generation} was reused with different content"
            ),
            Self::RevisionIdentityConflict { revision } => write!(
                f,
                "component revision {revision} was reused with a different definition or scope"
            ),
            Self::NoMountedComponent { instance_id } => {
                write!(f, "component slot {instance_id} has no mounted component")
            }
            Self::MountedRevisionNotEnabled { desired, applied } => match desired {
                Some(desired) if desired == applied => {
                    write!(f, "mounted revision {applied} is currently disabled")
                }
                Some(desired) => write!(
                    f,
                    "mounted revision {applied} has been superseded by desired revision {desired}"
                ),
                None => write!(f, "mounted revision {applied} is desired as removed"),
            },
            Self::Reconcile(error) => error.fmt(f),
        }
    }
}

impl Error for ComponentSlotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Reconcile(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ReconcileError> for ComponentSlotError {
    fn from(error: ReconcileError) -> Self {
        Self::Reconcile(error)
    }
}
