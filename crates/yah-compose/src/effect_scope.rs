//! Activation-owned reversible local effects.
//!
//! Cleanup failures include returned errors and ordinary unwind panics from
//! callback invocation, future polling, and terminal future destruction.
//! Panic payload disposal is also contained; a pathological secondary payload
//! is leaked if its own destructor panics. Process aborts, `panic = "abort"`,
//! FFI failures, executor loss, and panics while abandoning a scope are outside
//! this report boundary. Panics from executor-supplied wakers while requesting
//! cancellation or signaling activity drain are likewise outside it.

mod activity;
mod cancellation;
mod report;

use std::{
    any::Any,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use tokio_util::sync::CancellationToken;

use crate::ActivationEpoch;

pub(crate) use activity::{ActivityAdmission, ActivityAdmissionError, ActivityDrain, ActivityGate};
pub use cancellation::ScopeCancellation;
pub use report::{
    CleanupError, CleanupFailure, CleanupFailureKind, CleanupOutcome, CleanupRecord, CloseReport,
    CloseStep, EffectRegistrationId, EffectScopeError, EffectScopeId, EffectScopeState,
};

static NEXT_SCOPE_INCARNATION: AtomicU64 = AtomicU64::new(1);

/// Result returned by a synchronous or asynchronous cleanup callback.
pub type CleanupResult = Result<(), CleanupError>;
type BoxCleanupFuture = Pin<Box<dyn Future<Output = CleanupResult> + Send + 'static>>;
type SyncCleanup = Box<dyn FnOnce() -> CleanupResult + Send + 'static>;
type AsyncCleanup = Box<dyn FnOnce() -> BoxCleanupFuture + Send + 'static>;

/// One activation-owned tree of reversible in-process effects.
///
/// The scope is intentionally uniquely owned. Cleanup is sequential and may
/// be resumed after a pending [`CloseScope`] future is dropped, provided the
/// scope itself survives. Concurrent effect registration and multi-owner close
/// policy remain outside this slice. Service handles use a private hierarchical
/// admission fence: explicit close rejects new calls and waits for already
/// admitted synchronous calls in this subtree before running any cleanup.
///
/// Dropping a scope requests cancellation as a fail-safe, then abandons any
/// cleanup that has not run. Owners must explicitly drive [`Self::close`] to
/// completion on every normal, error, and unwind path that requires cleanup.
#[must_use = "effect scopes must be explicitly closed to run registered cleanup"]
pub struct EffectScope {
    id: EffectScopeId,
    label: String,
    cancellation: CancellationToken,
    activity: Arc<ActivityGate>,
    next_registration: u64,
    storage: ScopeStorage,
}

impl EffectScope {
    /// Create a root scope with a diagnostic label.
    ///
    /// # Errors
    ///
    /// Returns [`EffectScopeError::ScopeIncarnationExhausted`] only if the
    /// process-wide scope-incarnation counter is exhausted.
    pub fn new(
        label: impl Into<String>,
        activation: ActivationEpoch,
    ) -> Result<Self, EffectScopeError> {
        Ok(Self {
            id: allocate_scope_id(activation)?,
            label: label.into(),
            cancellation: CancellationToken::new(),
            activity: ActivityGate::root(),
            next_registration: 1,
            storage: ScopeStorage::Open {
                entries: Vec::new(),
            },
        })
    }

    pub const fn id(&self) -> EffectScopeId {
        self.id
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub const fn activation(&self) -> ActivationEpoch {
        self.id.activation()
    }

    pub fn state(&self) -> EffectScopeState {
        match self.storage {
            ScopeStorage::Open { .. } => EffectScopeState::Open,
            ScopeStorage::Closing { .. } => EffectScopeState::Closing,
            ScopeStorage::Closed { .. } => EffectScopeState::Closed,
        }
    }

    pub fn cancellation(&self) -> ScopeCancellation {
        ScopeCancellation::new(self.cancellation.clone())
    }

    pub(crate) fn activity_gate(&self) -> Arc<ActivityGate> {
        Arc::clone(&self.activity)
    }

    /// Create a child whose teardown position is its creation position.
    pub fn child(
        &mut self,
        label: impl Into<String>,
    ) -> Result<&mut EffectScope, EffectScopeError> {
        self.require_open()?;
        let child = Self {
            id: allocate_scope_id(self.activation())?,
            label: label.into(),
            cancellation: self.cancellation.child_token(),
            activity: ActivityGate::child(&self.activity),
            next_registration: 1,
            storage: ScopeStorage::Open {
                entries: Vec::new(),
            },
        };
        let ScopeStorage::Open { entries } = &mut self.storage else {
            unreachable!("open state checked before child insertion")
        };
        entries.push(EffectEntry::Child(Box::new(child)));
        let Some(EffectEntry::Child(child)) = entries.last_mut() else {
            unreachable!("the inserted entry is a child")
        };
        Ok(child)
    }

    /// Find a scope in this open owned subtree by its generated identity.
    ///
    /// This retains unique ownership: the returned mutable borrow cannot
    /// coexist with another operation on the tree. Sealing an ancestor also
    /// seals lookup admission into every descendant.
    pub fn scope_mut(
        &mut self,
        scope_id: EffectScopeId,
    ) -> Result<&mut EffectScope, EffectScopeError> {
        if scope_id.activation() != self.activation() {
            return Err(EffectScopeError::WrongActivation {
                expected: self.activation(),
                received: scope_id.activation(),
            });
        }
        self.require_open()?;
        self.find_scope_mut(scope_id)
            .ok_or(EffectScopeError::UnknownScope { scope_id })
    }

    /// Transfer one synchronous cleanup callback into this scope.
    ///
    /// Higher layers must admit cleanup before publishing the resource it will
    /// reverse. A rejected registration was never owned or run by this scope.
    ///
    /// Synchronous callbacks must be short and nonblocking. Panics are recorded
    /// when the build uses unwinding; `panic = "abort"` cannot be contained.
    pub fn defer_sync<F>(
        &mut self,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<EffectRegistrationId, EffectScopeError>
    where
        F: FnOnce() -> CleanupResult + Send + 'static,
    {
        let registration_id = self.allocate_registration()?;
        let ScopeStorage::Open { entries } = &mut self.storage else {
            unreachable!("registration allocation checks open state")
        };
        entries.push(EffectEntry::Sync {
            registration_id,
            label: label.into(),
            cleanup: Box::new(cleanup),
        });
        Ok(registration_id)
    }

    /// Transfer one asynchronous cleanup callback into this scope.
    ///
    /// Higher layers must admit cleanup before publishing the resource it will
    /// reverse. A rejected registration was never owned or run by this scope.
    pub fn defer_async<F, Fut>(
        &mut self,
        label: impl Into<String>,
        cleanup: F,
    ) -> Result<EffectRegistrationId, EffectScopeError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = CleanupResult> + Send + 'static,
    {
        let registration_id = self.allocate_registration()?;
        let cleanup: AsyncCleanup = Box::new(move || Box::pin(cleanup()));
        let ScopeStorage::Open { entries } = &mut self.storage else {
            unreachable!("registration allocation checks open state")
        };
        entries.push(EffectEntry::Async {
            registration_id,
            label: label.into(),
            cleanup,
        });
        Ok(registration_id)
    }

    /// Seal admission, request subtree cancellation, drain service calls, and
    /// drive cleanup.
    ///
    /// Calling this method seals admission and requests cancellation
    /// synchronously. While polled, it waits for already-admitted synchronous
    /// service calls in this subtree before the first cleanup callback. If the
    /// returned future is dropped while pending, calling `close` again resumes
    /// the same drain or cleanup without rerunning completed callbacks.
    pub fn close(&mut self) -> CloseScope<'_> {
        self.begin_close();
        CloseScope { scope: self }
    }

    pub fn closed_report(&self) -> Option<&CloseReport> {
        match &self.storage {
            ScopeStorage::Closed { report } => Some(report),
            ScopeStorage::Open { .. } | ScopeStorage::Closing { .. } => None,
        }
    }

    fn require_open(&self) -> Result<(), EffectScopeError> {
        let state = self.state();
        if state == EffectScopeState::Open {
            Ok(())
        } else {
            Err(EffectScopeError::NotOpen {
                scope_id: self.id,
                state,
            })
        }
    }

    fn allocate_registration(&mut self) -> Result<EffectRegistrationId, EffectScopeError> {
        self.require_open()?;
        let sequence = self.next_registration;
        self.next_registration = sequence
            .checked_add(1)
            .ok_or(EffectScopeError::RegistrationIdExhausted { scope_id: self.id })?;
        Ok(EffectRegistrationId::new(self.id, sequence))
    }

    fn find_scope_mut(&mut self, scope_id: EffectScopeId) -> Option<&mut EffectScope> {
        if self.id == scope_id {
            return Some(self);
        }
        match &mut self.storage {
            ScopeStorage::Open { entries } => find_in_entries(entries, scope_id),
            ScopeStorage::Closing { .. } | ScopeStorage::Closed { .. } => None,
        }
    }

    fn begin_close(&mut self) {
        if !matches!(self.storage, ScopeStorage::Open { .. }) {
            return;
        }
        let quiescence = self.activity.drain();
        let previous = std::mem::replace(
            &mut self.storage,
            ScopeStorage::Closing {
                quiescence: None,
                remaining: Vec::new(),
                current: None,
                steps: Vec::new(),
            },
        );
        let ScopeStorage::Open { entries } = previous else {
            unreachable!("open state checked before close transition")
        };
        self.storage = ScopeStorage::Closing {
            quiescence: Some(quiescence),
            remaining: entries,
            current: None,
            steps: Vec::new(),
        };
        self.activity.revoke();
        self.cancellation.cancel();
    }

    fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<CloseReport> {
        self.begin_close();
        loop {
            if let ScopeStorage::Closed { report } = &self.storage {
                return Poll::Ready(report.clone());
            }

            if self.poll_quiescence(cx).is_pending() {
                return Poll::Pending;
            }

            if let Some(current) = self.take_current() {
                match self.poll_current(current, cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(step) => self.push_step(step),
                }
                continue;
            }

            let Some(entry) = self.pop_entry() else {
                let ScopeStorage::Closing { steps, .. } = &mut self.storage else {
                    unreachable!("non-closed scope is closing")
                };
                let report = CloseReport::new(self.id, self.label.clone(), std::mem::take(steps));
                self.storage = ScopeStorage::Closed {
                    report: report.clone(),
                };
                return Poll::Ready(report);
            };

            match entry {
                EffectEntry::Sync {
                    registration_id,
                    label,
                    cleanup,
                } => {
                    let result = catch_unwind(AssertUnwindSafe(cleanup));
                    self.push_step(CloseStep::Cleanup(CleanupRecord::new(
                        registration_id,
                        label,
                        outcome_from_invocation(result),
                    )));
                }
                EffectEntry::Async {
                    registration_id,
                    label,
                    cleanup,
                } => match catch_unwind(AssertUnwindSafe(cleanup)) {
                    Ok(future) => self.put_current(CurrentCleanup::Async {
                        registration_id,
                        label,
                        future,
                    }),
                    Err(payload) => self.push_step(CloseStep::Cleanup(CleanupRecord::new(
                        registration_id,
                        label,
                        CleanupOutcome::Failed(CleanupFailure::panicked(payload)),
                    ))),
                },
                EffectEntry::Child(mut scope) => {
                    let already_closed = scope.state() == EffectScopeState::Closed;
                    scope.begin_close();
                    self.put_current(CurrentCleanup::Child {
                        scope,
                        already_closed,
                    });
                }
            }
        }
    }

    fn poll_current(&mut self, current: CurrentCleanup, cx: &mut Context<'_>) -> Poll<CloseStep> {
        match current {
            CurrentCleanup::Async {
                registration_id,
                label,
                mut future,
            } => {
                let polled = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx)));
                match polled {
                    Ok(Poll::Pending) => {
                        self.put_current(CurrentCleanup::Async {
                            registration_id,
                            label,
                            future,
                        });
                        Poll::Pending
                    }
                    Ok(Poll::Ready(result)) => Poll::Ready(CloseStep::Cleanup(CleanupRecord::new(
                        registration_id,
                        label,
                        outcome_after_future_drop(outcome_from_result(result), future),
                    ))),
                    Err(payload) => Poll::Ready(CloseStep::Cleanup(CleanupRecord::new(
                        registration_id,
                        label,
                        outcome_after_future_drop(
                            CleanupOutcome::Failed(CleanupFailure::panicked(payload)),
                            future,
                        ),
                    ))),
                }
            }
            CurrentCleanup::Child {
                mut scope,
                already_closed,
            } => match scope.poll_close(cx) {
                Poll::Pending => {
                    self.put_current(CurrentCleanup::Child {
                        scope,
                        already_closed,
                    });
                    Poll::Pending
                }
                Poll::Ready(report) => Poll::Ready(CloseStep::Child {
                    report: Box::new(report),
                    already_closed,
                }),
            },
        }
    }

    fn take_current(&mut self) -> Option<CurrentCleanup> {
        let ScopeStorage::Closing { current, .. } = &mut self.storage else {
            return None;
        };
        current.take()
    }

    fn poll_quiescence(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let ScopeStorage::Closing { quiescence, .. } = &mut self.storage else {
            return Poll::Ready(());
        };
        let Some(drain) = quiescence else {
            return Poll::Ready(());
        };
        match drain.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                *quiescence = None;
                Poll::Ready(())
            }
        }
    }

    fn put_current(&mut self, cleanup: CurrentCleanup) {
        let ScopeStorage::Closing { current, .. } = &mut self.storage else {
            unreachable!("current cleanup belongs to a closing scope")
        };
        *current = Some(cleanup);
    }

    fn pop_entry(&mut self) -> Option<EffectEntry> {
        let ScopeStorage::Closing { remaining, .. } = &mut self.storage else {
            return None;
        };
        remaining.pop()
    }

    fn push_step(&mut self, step: CloseStep) {
        let ScopeStorage::Closing { steps, .. } = &mut self.storage else {
            unreachable!("close steps belong to a closing scope")
        };
        steps.push(step);
    }

    #[cfg(test)]
    fn set_next_registration_for_test(&mut self, sequence: u64) {
        self.next_registration = sequence;
    }
}

impl fmt::Debug for EffectScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EffectScope")
            .field("id", &self.id)
            .field("label", &self.label)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl Drop for EffectScope {
    fn drop(&mut self) {
        self.activity.revoke();
        self.cancellation.cancel();
    }
}

/// Future returned by [`EffectScope::close`].
#[must_use = "cleanup runs only while the close future is polled"]
pub struct CloseScope<'a> {
    scope: &'a mut EffectScope,
}

impl Future for CloseScope<'_> {
    type Output = CloseReport;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.scope.poll_close(cx)
    }
}

enum ScopeStorage {
    Open {
        entries: Vec<EffectEntry>,
    },
    Closing {
        quiescence: Option<ActivityDrain>,
        remaining: Vec<EffectEntry>,
        current: Option<CurrentCleanup>,
        steps: Vec<CloseStep>,
    },
    Closed {
        report: CloseReport,
    },
}

enum EffectEntry {
    Sync {
        registration_id: EffectRegistrationId,
        label: String,
        cleanup: SyncCleanup,
    },
    Async {
        registration_id: EffectRegistrationId,
        label: String,
        cleanup: AsyncCleanup,
    },
    Child(Box<EffectScope>),
}

enum CurrentCleanup {
    Async {
        registration_id: EffectRegistrationId,
        label: String,
        future: BoxCleanupFuture,
    },
    Child {
        scope: Box<EffectScope>,
        already_closed: bool,
    },
}

fn allocate_scope_id(activation: ActivationEpoch) -> Result<EffectScopeId, EffectScopeError> {
    let incarnation = NEXT_SCOPE_INCARNATION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| EffectScopeError::ScopeIncarnationExhausted)?;
    Ok(EffectScopeId::new(activation, incarnation))
}

fn find_in_entries(
    entries: &mut [EffectEntry],
    scope_id: EffectScopeId,
) -> Option<&mut EffectScope> {
    entries.iter_mut().find_map(|entry| match entry {
        EffectEntry::Child(scope) => scope.find_scope_mut(scope_id),
        EffectEntry::Sync { .. } | EffectEntry::Async { .. } => None,
    })
}

fn outcome_from_invocation(result: Result<CleanupResult, Box<dyn Any + Send>>) -> CleanupOutcome {
    match result {
        Ok(result) => outcome_from_result(result),
        Err(payload) => CleanupOutcome::Failed(CleanupFailure::panicked(payload)),
    }
}

fn outcome_from_result(result: CleanupResult) -> CleanupOutcome {
    match result {
        Ok(()) => CleanupOutcome::Succeeded,
        Err(error) => CleanupOutcome::Failed(CleanupFailure::returned(error)),
    }
}

fn outcome_after_future_drop(outcome: CleanupOutcome, future: BoxCleanupFuture) -> CleanupOutcome {
    match catch_unwind(AssertUnwindSafe(|| drop(future))) {
        Ok(()) => outcome,
        Err(payload) => outcome.with_future_drop_panic(payload),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComponentDefinition, ComponentInstance, Scope};

    fn activation() -> ActivationEpoch {
        let definition = ComponentDefinition::new("test.component");
        let scope = Scope::root("root");
        let mut instance = ComponentInstance::new("instance", &definition, &scope).unwrap();
        instance.begin_start().unwrap()
    }

    #[test]
    fn registration_identity_overflow_rejects_without_admission() {
        let mut scope = EffectScope::new("effects", activation()).unwrap();
        let scope_id = scope.id();
        scope.set_next_registration_for_test(u64::MAX);

        assert_eq!(
            scope.defer_sync("never admitted", || Ok(())),
            Err(EffectScopeError::RegistrationIdExhausted { scope_id })
        );
        assert_eq!(scope.state(), EffectScopeState::Open);
    }
}
