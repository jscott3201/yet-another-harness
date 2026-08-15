//! Epoch-fenced component lifecycle vocabulary.

use std::{error::Error, fmt};

/// Identity of one process-local activation attempt.
///
/// The opaque token combines a process-unique component-instance incarnation
/// with a sequence that increases for each activation of that instance. This
/// is not a durable work-attempt epoch; its purpose is to reject late work from
/// either an older activation or a replaced live instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ActivationEpoch {
    incarnation: u64,
    sequence: u64,
}

impl ActivationEpoch {
    pub(crate) const fn new(incarnation: u64, sequence: u64) -> Self {
        Self {
            incarnation,
            sequence,
        }
    }

    /// Return the sequence within this component-instance incarnation.
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

impl fmt::Display for ActivationEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.incarnation, self.sequence)
    }
}

/// Stable target reached after controlled teardown.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StopTarget {
    Pending,
    Removed,
}

/// Phase whose callback or live work failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FailurePhase {
    Starting,
    Active,
}

/// Diagnostic attached to a valid lifecycle failure transition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ComponentFailure {
    phase: FailurePhase,
    summary: String,
}

impl ComponentFailure {
    pub(crate) fn new(phase: FailurePhase, summary: impl Into<String>) -> Self {
        Self {
            phase,
            summary: summary.into(),
        }
    }

    pub fn phase(&self) -> FailurePhase {
        self.phase
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }
}

/// Live state of one mounted component instance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ComponentState {
    Pending,
    Starting {
        activation: ActivationEpoch,
    },
    Active {
        activation: ActivationEpoch,
    },
    Failed {
        activation: ActivationEpoch,
        failure: ComponentFailure,
    },
    Stopping {
        activation: ActivationEpoch,
        target: StopTarget,
        /// Preserved while cleaning up a failed instance so diagnostics remain
        /// observable until teardown completes.
        prior_failure: Option<ComponentFailure>,
    },
    Removed,
}

impl ComponentState {
    pub fn kind(&self) -> ComponentStateKind {
        match self {
            ComponentState::Pending => ComponentStateKind::Pending,
            ComponentState::Starting { .. } => ComponentStateKind::Starting,
            ComponentState::Active { .. } => ComponentStateKind::Active,
            ComponentState::Failed { .. } => ComponentStateKind::Failed,
            ComponentState::Stopping { .. } => ComponentStateKind::Stopping,
            ComponentState::Removed => ComponentStateKind::Removed,
        }
    }

    pub fn activation(&self) -> Option<ActivationEpoch> {
        match self {
            ComponentState::Pending | ComponentState::Removed => None,
            ComponentState::Starting { activation }
            | ComponentState::Active { activation }
            | ComponentState::Failed { activation, .. }
            | ComponentState::Stopping { activation, .. } => Some(*activation),
        }
    }

    pub(crate) fn invalid(&self, action: LifecycleAction) -> LifecycleError {
        LifecycleError::InvalidTransition {
            from: self.kind(),
            action,
        }
    }

    pub(crate) fn require_action(&self, action: LifecycleAction) -> Result<(), LifecycleError> {
        match (self, action) {
            (ComponentState::Pending, LifecycleAction::BeginStart) => Ok(()),
            _ => Err(self.invalid(action)),
        }
    }

    pub(crate) fn require_activation(
        &self,
        action: LifecycleAction,
        received: ActivationEpoch,
    ) -> Result<(), LifecycleError> {
        let Some(expected) = self.activation() else {
            return Err(self.invalid(action));
        };
        if expected != received {
            return Err(LifecycleError::StaleActivation { expected, received });
        }
        Ok(())
    }
}

/// State name used in typed transition errors without cloning state payloads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ComponentStateKind {
    Pending,
    Starting,
    Active,
    Failed,
    Stopping,
    Removed,
}

impl fmt::Display for ComponentStateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ComponentStateKind::Pending => "pending",
            ComponentStateKind::Starting => "starting",
            ComponentStateKind::Active => "active",
            ComponentStateKind::Failed => "failed",
            ComponentStateKind::Stopping => "stopping",
            ComponentStateKind::Removed => "removed",
        })
    }
}

/// Operation rejected by a lifecycle transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum LifecycleAction {
    BeginStart,
    CompleteStart,
    MarkFailed,
    BeginStop,
    CompleteStop,
    RemovePending,
}

impl fmt::Display for LifecycleAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LifecycleAction::BeginStart => "begin_start",
            LifecycleAction::CompleteStart => "complete_start",
            LifecycleAction::MarkFailed => "mark_failed",
            LifecycleAction::BeginStop => "begin_stop",
            LifecycleAction::CompleteStop => "complete_stop",
            LifecycleAction::RemovePending => "remove_pending",
        })
    }
}

/// Misuse or stale completion at the live lifecycle boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleError {
    InvalidTransition {
        from: ComponentStateKind,
        action: LifecycleAction,
    },
    StaleActivation {
        expected: ActivationEpoch,
        received: ActivationEpoch,
    },
    InstanceIncarnationExhausted,
    ActivationEpochExhausted,
}

impl fmt::Display for LifecycleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LifecycleError::InvalidTransition { from, action } => {
                write!(f, "cannot {action} while component is {from}")
            }
            LifecycleError::StaleActivation { expected, received } => write!(
                f,
                "stale activation epoch {received}; current epoch is {expected}"
            ),
            LifecycleError::InstanceIncarnationExhausted => {
                f.write_str("component instance incarnation space exhausted")
            }
            LifecycleError::ActivationEpochExhausted => f.write_str("activation epoch exhausted"),
        }
    }
}

impl Error for LifecycleError {}
