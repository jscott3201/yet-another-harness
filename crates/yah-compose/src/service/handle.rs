use std::{
    error::Error,
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    ActivationEpoch, ComponentInstanceId, ScopeCancellation, ScopeId,
    effect_scope::{ActivityAdmission, ActivityAdmissionError, ActivityGate},
};

use super::ProviderCandidate;

/// A revocable, exact binding to one provider publication.
///
/// The handle never follows a replacement provider and does not expose the
/// provider value through `Deref`, `AsRef`, or an `Arc` getter. Every access is
/// mediated by [`Self::try_with`]. Explicit provider or consumer scope close
/// rejects new calls and drains already-admitted calls before running any
/// cleanup in those activation subtrees.
///
/// Service contract methods must likewise avoid returning raw authority that
/// would bypass this gate. Sandboxed plugin resources require a future brokered
/// handle layer in addition to this trusted in-process boundary.
#[must_use = "service handles must be checked at each use"]
pub struct ServiceHandle<T: ?Sized + Send + Sync + 'static> {
    provider_value: Weak<T>,
    gate: Arc<ProviderGate>,
    provider_activity: Arc<ActivityGate>,
    candidate: ProviderCandidate,
    consumer_instance_id: ComponentInstanceId,
    consumer_scope_id: ScopeId,
    consumer_activation: ActivationEpoch,
    consumer_cancellation: ScopeCancellation,
    consumer_activity: Arc<ActivityGate>,
    invariant: PhantomData<fn(&T) -> &T>,
}

impl<T: ?Sized + Send + Sync + 'static> ServiceHandle<T> {
    pub(super) fn new(
        provider: ProviderBinding<T>,
        consumer_instance_id: ComponentInstanceId,
        consumer_scope_id: ScopeId,
        consumer_activation: ActivationEpoch,
        consumer_cancellation: ScopeCancellation,
        consumer_activity: Arc<ActivityGate>,
    ) -> Self {
        let ProviderBinding {
            provider_value,
            gate,
            activity: provider_activity,
            candidate,
        } = provider;
        Self {
            provider_value,
            gate,
            provider_activity,
            candidate,
            consumer_instance_id,
            consumer_scope_id,
            consumer_activation,
            consumer_cancellation,
            consumer_activity,
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
    /// trusted contract authors must keep that authority mediated. The callback
    /// is synchronous and must not wait for closure of its own provider or
    /// consumer scope. A callback panic propagates after both activation
    /// admissions and the temporary provider reference are released.
    pub fn try_with<R>(&self, operation: impl FnOnce(&T) -> R) -> Result<R, ServiceHandleError> {
        if self.consumer_cancellation.is_cancelled()
            || !self.gate.is_available()
            || !self.provider_activity.is_open()
            || !self.consumer_activity.is_open()
        {
            return Err(self.revoked());
        }

        let provider_activity = self
            .provider_activity
            .admit()
            .map_err(|error| self.admission_error(error))?;
        let consumer_activity = self
            .consumer_activity
            .admit()
            .map_err(|error| self.admission_error(error))?;
        let Some(provider) = self.provider_value.upgrade() else {
            return Err(self.revoked());
        };
        let lease = ServiceCallLease {
            provider,
            _consumer_activity: consumer_activity,
            _provider_activity: provider_activity,
        };

        if self.consumer_cancellation.is_cancelled()
            || !self.gate.is_available()
            || !self.provider_activity.is_open()
            || !self.consumer_activity.is_open()
        {
            drop(lease);
            return Err(self.revoked());
        }

        let result = operation(lease.provider.as_ref());
        drop(lease);
        Ok(result)
    }

    fn admission_error(&self, error: ActivityAdmissionError) -> ServiceHandleError {
        match error {
            ActivityAdmissionError::Revoked => self.revoked(),
            ActivityAdmissionError::Exhausted => ServiceHandleError::AdmissionExhausted {
                provider_id: self.candidate.id(),
            },
        }
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
            provider_activity: Arc::clone(&self.provider_activity),
            candidate: self.candidate.clone(),
            consumer_instance_id: self.consumer_instance_id.clone(),
            consumer_scope_id: self.consumer_scope_id.clone(),
            consumer_activation: self.consumer_activation,
            consumer_cancellation: self.consumer_cancellation.clone(),
            consumer_activity: Arc::clone(&self.consumer_activity),
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
                    "service call for provider {provider_id} exhausted activation activity admission"
                )
            }
        }
    }
}

impl Error for ServiceHandleError {}

pub(super) struct ProviderBinding<T: ?Sized + Send + Sync + 'static> {
    provider_value: Weak<T>,
    gate: Arc<ProviderGate>,
    activity: Arc<ActivityGate>,
    candidate: ProviderCandidate,
}

impl<T: ?Sized + Send + Sync + 'static> ProviderBinding<T> {
    pub(super) fn new(
        provider_value: Weak<T>,
        gate: Arc<ProviderGate>,
        activity: Arc<ActivityGate>,
        candidate: ProviderCandidate,
    ) -> Self {
        Self {
            provider_value,
            gate,
            activity,
            candidate,
        }
    }

    pub(super) fn is_available(&self) -> bool {
        self.gate.is_available() && self.activity.is_open()
    }
}

pub(super) struct ProviderGate {
    revoked: AtomicBool,
    provider_cancellation: ScopeCancellation,
}

impl ProviderGate {
    pub(super) fn new(provider_cancellation: ScopeCancellation) -> Self {
        Self {
            revoked: AtomicBool::new(false),
            provider_cancellation,
        }
    }

    pub(super) fn revoke(&self) {
        self.revoked.store(true, Ordering::Release);
    }

    pub(super) fn is_available(&self) -> bool {
        !self.provider_cancellation.is_cancelled() && !self.revoked.load(Ordering::Acquire)
    }
}

/// Field order is intentional: the temporary strong provider reference is
/// released before either activation admission on both success and unwind.
struct ServiceCallLease<T: ?Sized + Send + Sync + 'static> {
    provider: Arc<T>,
    _consumer_activity: ActivityAdmission,
    _provider_activity: ActivityAdmission,
}
