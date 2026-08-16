use std::{
    error::Error,
    fmt,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio_util::sync::CancellationToken;

const REVOKED: usize = 1 << (usize::BITS - 1);
const ACTIVE_CALLS: usize = REVOKED - 1;

pub(crate) type ActivityDrain = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Hierarchical admission fence for operations borrowing activation resources.
pub(crate) struct ActivityGate {
    state: AtomicUsize,
    drained: CancellationToken,
    parent: Option<Arc<ActivityGate>>,
}

impl ActivityGate {
    pub(crate) fn root() -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
            drained: CancellationToken::new(),
            parent: None,
        })
    }

    pub(crate) fn child(parent: &Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            state: AtomicUsize::new(0),
            drained: CancellationToken::new(),
            parent: Some(Arc::clone(parent)),
        })
    }

    /// Admit against every ancestor before the local scope.
    ///
    /// Root-to-leaf order ensures an ancestor cannot observe a drained count
    /// and then be incremented by a descendant. Partial admission rolls back
    /// when any gate is revoked or exhausted.
    pub(crate) fn admit(self: &Arc<Self>) -> Result<ActivityAdmission, ActivityAdmissionError> {
        let mut lineage = Vec::new();
        let mut current = Some(Arc::clone(self));
        while let Some(gate) = current {
            current = gate.parent.clone();
            lineage.push(gate);
        }
        lineage.reverse();

        let mut guards = Vec::with_capacity(lineage.len());
        for gate in lineage {
            guards.push(gate.admit_local()?);
        }
        Ok(ActivityAdmission { _guards: guards })
    }

    pub(crate) fn is_open(&self) -> bool {
        let mut current = Some(self);
        while let Some(gate) = current {
            if gate.state.load(Ordering::Acquire) & REVOKED != 0 {
                return false;
            }
            current = gate.parent.as_deref();
        }
        true
    }

    /// Revoke new subtree admission and arm the persistent zero-count latch.
    pub(crate) fn revoke(&self) {
        let previous = self.state.fetch_or(REVOKED, Ordering::AcqRel);
        if previous & ACTIVE_CALLS == 0 {
            self.drained.cancel();
        }
    }

    pub(crate) fn drain(&self) -> ActivityDrain {
        let drained = self.drained.clone();
        Box::pin(async move {
            drained.cancelled().await;
        })
    }

    fn admit_local(self: Arc<Self>) -> Result<GateAdmission, ActivityAdmissionError> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & REVOKED != 0 {
                return Err(ActivityAdmissionError::Revoked);
            }
            if current & ACTIVE_CALLS == ACTIVE_CALLS {
                return Err(ActivityAdmissionError::Exhausted);
            }
            match self.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(GateAdmission { gate: self }),
                Err(observed) => current = observed,
            }
        }
    }
}

/// One admitted operation across a scope and all of its ancestors.
pub(crate) struct ActivityAdmission {
    _guards: Vec<GateAdmission>,
}

struct GateAdmission {
    gate: Arc<ActivityGate>,
}

impl Drop for GateAdmission {
    fn drop(&mut self) {
        let previous = self.gate.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous & ACTIVE_CALLS, 0, "activity count underflow");
        if previous & REVOKED != 0 && previous & ACTIVE_CALLS == 1 {
            self.gate.drained.cancel();
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ActivityAdmissionError {
    Revoked,
    Exhausted,
}

/// Cloneable admission token for synchronous work borrowing activation state.
///
/// The token carries no cancellation or cleanup authority. Entering it joins
/// the owning effect scope's pre-cleanup activity drain: explicit close rejects
/// new entries and waits for already-entered callbacks before running cleanup.
/// The callback must not wait for closure of the same scope.
#[derive(Clone)]
pub struct ScopeActivity {
    activation: crate::ActivationEpoch,
    gate: Arc<ActivityGate>,
}

impl ScopeActivity {
    pub(crate) fn new(activation: crate::ActivationEpoch, gate: Arc<ActivityGate>) -> Self {
        Self { activation, gate }
    }

    pub const fn activation(&self) -> crate::ActivationEpoch {
        self.activation
    }

    pub fn is_open(&self) -> bool {
        self.gate.is_open()
    }

    /// Run one synchronous operation in this activation subtree.
    ///
    /// Admission cannot escape this callback, so it cannot be moved into an
    /// asynchronous task or retained after the operation returns.
    pub fn try_with<R>(&self, operation: impl FnOnce() -> R) -> Result<R, ScopeActivityError> {
        let admission = self.gate.admit().map_err(ScopeActivityError::from)?;
        let result = operation();
        drop(admission);
        Ok(result)
    }
}

impl fmt::Debug for ScopeActivity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScopeActivity")
            .field("activation", &self.activation)
            .field("is_open", &self.is_open())
            .finish()
    }
}

/// Why an activation activity entry was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeActivityError {
    Revoked,
    Exhausted,
}

impl From<ActivityAdmissionError> for ScopeActivityError {
    fn from(error: ActivityAdmissionError) -> Self {
        match error {
            ActivityAdmissionError::Revoked => Self::Revoked,
            ActivityAdmissionError::Exhausted => Self::Exhausted,
        }
    }
}

impl fmt::Display for ScopeActivityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Revoked => "activation activity is revoked",
            Self::Exhausted => "activation activity admission is exhausted",
        })
    }
}

impl Error for ScopeActivityError {}
