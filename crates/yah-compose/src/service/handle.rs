use std::{
    error::Error,
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{ActivationEpoch, ComponentInstanceId, ScopeCancellation, ScopeId};

use super::ProviderCandidate;

const REVOKED: usize = 1 << (usize::BITS - 1);
const ACTIVE_CALLS: usize = REVOKED - 1;

/// A revocable, exact binding to one provider publication.
///
/// The handle never follows a replacement provider and does not expose the
/// provider value through `Deref`, `AsRef`, or an `Arc` getter. Every access is
/// mediated by [`Self::try_with`]. A call admitted before revocation may finish;
/// calls admitted afterward fail closed.
///
/// Service contract methods must likewise avoid returning raw authority that
/// would bypass this gate. Sandboxed plugin resources require a future brokered
/// handle layer in addition to this trusted in-process boundary.
#[must_use = "service handles must be checked at each use"]
pub struct ServiceHandle<T: ?Sized + Send + Sync + 'static> {
    provider_value: Weak<T>,
    gate: Arc<ProviderGate>,
    candidate: ProviderCandidate,
    consumer_instance_id: ComponentInstanceId,
    consumer_scope_id: ScopeId,
    consumer_activation: ActivationEpoch,
    consumer_cancellation: ScopeCancellation,
    invariant: PhantomData<fn(&T) -> &T>,
}

impl<T: ?Sized + Send + Sync + 'static> ServiceHandle<T> {
    pub(super) fn new(
        provider_value: Weak<T>,
        gate: Arc<ProviderGate>,
        candidate: ProviderCandidate,
        consumer_instance_id: ComponentInstanceId,
        consumer_scope_id: ScopeId,
        consumer_activation: ActivationEpoch,
        consumer_cancellation: ScopeCancellation,
    ) -> Self {
        Self {
            provider_value,
            gate,
            candidate,
            consumer_instance_id,
            consumer_scope_id,
            consumer_activation,
            consumer_cancellation,
            invariant: PhantomData,
        }
    }

    pub fn provider(&self) -> &ProviderCandidate {
        &self.candidate
    }

    pub fn consumer_instance_id(&self) -> &ComponentInstanceId {
        &self.consumer_instance_id
    }

    pub fn consumer_scope_id(&self) -> &ScopeId {
        &self.consumer_scope_id
    }

    pub const fn consumer_activation(&self) -> ActivationEpoch {
        self.consumer_activation
    }

    /// Run one operation if both provider and consumer activations remain live.
    ///
    /// The provider reference cannot escape through the result type. A service
    /// contract can still deliberately clone or return authority of its own;
    /// trusted contract authors must keep that authority mediated.
    pub fn try_with<R>(&self, operation: impl FnOnce(&T) -> R) -> Result<R, ServiceHandleError> {
        if self.consumer_cancellation.is_cancelled() || !self.gate.is_available() {
            return Err(self.revoked());
        }

        let Some(provider) = self.provider_value.upgrade() else {
            return Err(self.revoked());
        };
        let admission = self.gate.admit().map_err(|failure| match failure {
            AdmissionFailure::Revoked => self.revoked(),
            AdmissionFailure::Exhausted => ServiceHandleError::AdmissionExhausted {
                provider_id: self.candidate.id(),
            },
        })?;

        if self.consumer_cancellation.is_cancelled() || !self.gate.is_available() {
            drop(admission);
            return Err(self.revoked());
        }

        let result = operation(provider.as_ref());
        drop(admission);
        Ok(result)
    }

    fn revoked(&self) -> ServiceHandleError {
        ServiceHandleError::Revoked {
            provider_id: self.candidate.id(),
            consumer_activation: self.consumer_activation,
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for ServiceHandle<T> {
    fn clone(&self) -> Self {
        Self {
            provider_value: self.provider_value.clone(),
            gate: Arc::clone(&self.gate),
            candidate: self.candidate.clone(),
            consumer_instance_id: self.consumer_instance_id.clone(),
            consumer_scope_id: self.consumer_scope_id.clone(),
            consumer_activation: self.consumer_activation,
            consumer_cancellation: self.consumer_cancellation.clone(),
            invariant: PhantomData,
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for ServiceHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceHandle")
            .field("provider", &self.candidate)
            .field("consumer_instance_id", &self.consumer_instance_id)
            .field("consumer_scope_id", &self.consumer_scope_id)
            .field("consumer_activation", &self.consumer_activation)
            .finish_non_exhaustive()
    }
}

/// A fail-closed service-call rejection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceHandleError {
    Revoked {
        provider_id: super::ProviderRegistrationId,
        consumer_activation: ActivationEpoch,
    },
    AdmissionExhausted {
        provider_id: super::ProviderRegistrationId,
    },
}

impl fmt::Display for ServiceHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Revoked {
                provider_id,
                consumer_activation,
            } => write!(
                f,
                "service handle for provider {provider_id} and consumer activation {consumer_activation} is revoked"
            ),
            Self::AdmissionExhausted { provider_id } => {
                write!(
                    f,
                    "provider {provider_id} exhausted concurrent call admission"
                )
            }
        }
    }
}

impl Error for ServiceHandleError {}

pub(super) struct ProviderGate {
    calls: AtomicUsize,
    provider_cancellation: ScopeCancellation,
}

impl ProviderGate {
    pub(super) fn new(provider_cancellation: ScopeCancellation) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            provider_cancellation,
        }
    }

    pub(super) fn revoke(&self) {
        self.calls.fetch_or(REVOKED, Ordering::AcqRel);
    }

    pub(super) fn is_available(&self) -> bool {
        !self.provider_cancellation.is_cancelled()
            && self.calls.load(Ordering::Acquire) & REVOKED == 0
    }

    fn admit(&self) -> Result<CallAdmission<'_>, AdmissionFailure> {
        let mut current = self.calls.load(Ordering::Acquire);
        loop {
            if current & REVOKED != 0 || self.provider_cancellation.is_cancelled() {
                return Err(AdmissionFailure::Revoked);
            }
            if current & ACTIVE_CALLS == ACTIVE_CALLS {
                return Err(AdmissionFailure::Exhausted);
            }
            match self.calls.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(CallAdmission { gate: self }),
                Err(observed) => current = observed,
            }
        }
    }
}

struct CallAdmission<'a> {
    gate: &'a ProviderGate,
}

impl Drop for CallAdmission<'_> {
    fn drop(&mut self) {
        self.gate.calls.fetch_sub(1, Ordering::Release);
    }
}

enum AdmissionFailure {
    Revoked,
    Exhausted,
}
