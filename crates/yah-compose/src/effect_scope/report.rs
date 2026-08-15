use std::{
    any::Any,
    error::Error,
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
};

use crate::ActivationEpoch;

/// Process-unique identity of one activation-bound effect scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EffectScopeId {
    activation: ActivationEpoch,
    incarnation: u64,
}

impl EffectScopeId {
    pub(super) const fn new(activation: ActivationEpoch, incarnation: u64) -> Self {
        Self {
            activation,
            incarnation,
        }
    }

    pub const fn activation(self) -> ActivationEpoch {
        self.activation
    }

    /// Return this scope's process-unique incarnation.
    pub const fn incarnation(self) -> u64 {
        self.incarnation
    }
}

impl fmt::Display for EffectScopeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/scope:{}", self.activation, self.incarnation)
    }
}

/// Identity assigned to one cleanup registration.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EffectRegistrationId {
    scope_id: EffectScopeId,
    sequence: u64,
}

impl EffectRegistrationId {
    pub(super) const fn new(scope_id: EffectScopeId, sequence: u64) -> Self {
        Self { scope_id, sequence }
    }

    pub const fn activation(self) -> ActivationEpoch {
        self.scope_id.activation()
    }

    pub const fn scope_id(self) -> EffectScopeId {
        self.scope_id
    }

    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

/// Error returned deliberately by one local cleanup callback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupError {
    summary: String,
}

impl CleanupError {
    pub fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
        }
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }
}

impl fmt::Display for CleanupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.summary)
    }
}

impl Error for CleanupError {}

/// How a cleanup attempt failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CleanupFailureKind {
    ReturnedError,
    Panicked,
}

/// Observable failure from one cleanup attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupFailure {
    kind: CleanupFailureKind,
    summary: String,
}

impl CleanupFailure {
    pub fn kind(&self) -> CleanupFailureKind {
        self.kind
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub(super) fn returned(error: CleanupError) -> Self {
        Self {
            kind: CleanupFailureKind::ReturnedError,
            summary: error.summary,
        }
    }

    pub(super) fn panicked(payload: Box<dyn Any + Send>) -> Self {
        Self {
            kind: CleanupFailureKind::Panicked,
            summary: consume_panic_payload(payload),
        }
    }
}

/// Result of attempting one registered cleanup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CleanupOutcome {
    Succeeded,
    Failed(CleanupFailure),
}

impl CleanupOutcome {
    pub fn failure(&self) -> Option<&CleanupFailure> {
        match self {
            Self::Succeeded => None,
            Self::Failed(failure) => Some(failure),
        }
    }

    pub(super) fn with_future_drop_panic(self, payload: Box<dyn Any + Send>) -> Self {
        let drop_summary = consume_panic_payload(payload);
        let summary = match self {
            Self::Succeeded => format!("cleanup future destructor panicked: {drop_summary}"),
            Self::Failed(failure) => format!(
                "cleanup failed before its future destructor panicked: {}; destructor panic: {drop_summary}",
                failure.summary
            ),
        };
        Self::Failed(CleanupFailure {
            kind: CleanupFailureKind::Panicked,
            summary,
        })
    }
}

/// One cleanup result in actual teardown order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CleanupRecord {
    registration_id: EffectRegistrationId,
    label: String,
    outcome: CleanupOutcome,
}

impl CleanupRecord {
    pub(super) fn new(
        registration_id: EffectRegistrationId,
        label: String,
        outcome: CleanupOutcome,
    ) -> Self {
        Self {
            registration_id,
            label,
            outcome,
        }
    }

    pub fn registration_id(&self) -> EffectRegistrationId {
        self.registration_id
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn outcome(&self) -> &CleanupOutcome {
        &self.outcome
    }
}

/// One step in a tree-preserving close report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CloseStep {
    Cleanup(CleanupRecord),
    Child {
        report: Box<CloseReport>,
        /// The child was already terminal when its parent reached this entry.
        already_closed: bool,
    },
}

/// Immutable, cached observation of one scope teardown.
///
/// A report for an already-closed child retains the child's earlier terminal
/// evidence. Its counts do not imply that the child callbacks ran again.
#[must_use = "cleanup failures should be inspected or deliberately recorded"]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseReport {
    scope_id: EffectScopeId,
    scope_label: String,
    steps: Vec<CloseStep>,
}

impl CloseReport {
    pub(super) fn new(scope_id: EffectScopeId, scope_label: String, steps: Vec<CloseStep>) -> Self {
        Self {
            scope_id,
            scope_label,
            steps,
        }
    }

    pub const fn scope_id(&self) -> EffectScopeId {
        self.scope_id
    }

    pub fn scope_label(&self) -> &str {
        &self.scope_label
    }

    pub const fn activation(&self) -> ActivationEpoch {
        self.scope_id.activation()
    }

    pub fn steps(&self) -> &[CloseStep] {
        &self.steps
    }

    pub fn cleanup_count(&self) -> usize {
        self.steps
            .iter()
            .map(|step| match step {
                CloseStep::Cleanup(_) => 1,
                CloseStep::Child { report, .. } => report.cleanup_count(),
            })
            .sum()
    }

    pub fn failure_count(&self) -> usize {
        self.steps
            .iter()
            .map(|step| match step {
                CloseStep::Cleanup(record) => usize::from(record.outcome.failure().is_some()),
                CloseStep::Child { report, .. } => report.failure_count(),
            })
            .sum()
    }

    pub fn is_clean(&self) -> bool {
        self.failure_count() == 0
    }
}

/// Admission and cleanup phase of an effect scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum EffectScopeState {
    Open,
    Closing,
    Closed,
}

impl fmt::Display for EffectScopeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Open => "open",
            Self::Closing => "closing",
            Self::Closed => "closed",
        })
    }
}

/// Rejected operation at the effect-scope boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectScopeError {
    ScopeIncarnationExhausted,
    NotOpen {
        scope_id: EffectScopeId,
        state: EffectScopeState,
    },
    RegistrationIdExhausted {
        scope_id: EffectScopeId,
    },
    WrongActivation {
        expected: ActivationEpoch,
        received: ActivationEpoch,
    },
    UnknownScope {
        scope_id: EffectScopeId,
    },
}

impl fmt::Display for EffectScopeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ScopeIncarnationExhausted => {
                f.write_str("effect scope incarnation space exhausted")
            }
            Self::NotOpen { scope_id, state } => {
                write!(f, "effect scope {scope_id} is {state}")
            }
            Self::RegistrationIdExhausted { scope_id } => {
                write!(
                    f,
                    "effect scope {scope_id} exhausted registration identities"
                )
            }
            Self::WrongActivation { expected, received } => write!(
                f,
                "effect scope activation mismatch: expected {expected}, received {received}"
            ),
            Self::UnknownScope { scope_id } => {
                write!(f, "effect scope {scope_id} is not in this subtree")
            }
        }
    }
}

impl Error for EffectScopeError {}

fn panic_summary(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "cleanup panicked with a non-string payload".to_owned()
    }
}

fn consume_panic_payload(payload: Box<dyn Any + Send>) -> String {
    let summary = panic_summary(payload.as_ref());
    match catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        Ok(()) => summary,
        Err(drop_payload) => {
            let drop_summary = panic_summary(drop_payload.as_ref());
            // A panic payload is arbitrary user data. Leaking this secondary,
            // pathological payload prevents its destructor from unwinding the
            // cleanup reporter again.
            std::mem::forget(drop_payload);
            format!("{summary}; panic payload destructor panicked: {drop_summary}")
        }
    }
}
