use std::{
    error::Error,
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

use yah_compose::{ScopeActivity, ScopeActivityError, ScopeCancellation};

use crate::{CapabilityId, PluginActivationId};

use super::CapabilityRegistrationId;

const REVOKED: usize = 1 << (usize::BITS - 1);
const ACTIVE_CALLS: usize = REVOKED - 1;

/// Weak, mediated authority for one exact capability registration and activation.
///
/// Handles never follow replacement registrations and expose neither `Deref`
/// nor an `Arc` getter. Each synchronous use joins the activation effect scope's
/// pre-cleanup drain. A call admitted before revocation may finish; calls that
/// lose the admission race fail closed.
#[must_use = "capability handles must be checked at each use"]
pub struct CapabilityHandle<T: ?Sized + Send + Sync + 'static> {
    capability_id: CapabilityId,
    registration_id: CapabilityRegistrationId,
    activation_id: PluginActivationId,
    cancellation: ScopeCancellation,
    activity: ScopeActivity,
    provider: Weak<T>,
    gate: Arc<RegistrationGate>,
    invariant: PhantomData<fn(&T) -> &T>,
}

impl<T: ?Sized + Send + Sync + 'static> CapabilityHandle<T> {
    pub(super) fn new(
        capability_id: CapabilityId,
        registration_id: CapabilityRegistrationId,
        activation_id: PluginActivationId,
        cancellation: ScopeCancellation,
        activity: ScopeActivity,
        provider: Weak<T>,
        gate: Arc<RegistrationGate>,
    ) -> Self {
        Self {
            capability_id,
            registration_id,
            activation_id,
            cancellation,
            activity,
            provider,
            gate,
            invariant: PhantomData,
        }
    }

    pub const fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }

    pub const fn registration_id(&self) -> CapabilityRegistrationId {
        self.registration_id
    }

    pub const fn activation_id(&self) -> &PluginActivationId {
        &self.activation_id
    }

    /// Run one synchronous operation while this exact authority remains live.
    ///
    /// The capability contract must not return or clone raw authority that
    /// bypasses this gate. The callback must not wait for closure of its own
    /// activation. Panics propagate only after the provider reference and both
    /// admissions are released.
    pub fn try_with<R>(&self, operation: impl FnOnce(&T) -> R) -> Result<R, CapabilityHandleError> {
        self.require_available()?;
        let provider_admission = self
            .gate
            .admit()
            .map_err(|error| self.admission_error(error))?;
        let activity_result = self.activity.try_with(|| {
            let Some(provider) = self.provider.upgrade() else {
                return Err(self.revoked());
            };
            self.require_available()?;
            let result = operation(provider.as_ref());
            drop(provider);
            self.require_available()?;
            Ok(result)
        });
        drop(provider_admission);
        match activity_result {
            Ok(result) => result,
            Err(error) => Err(self.activity_error(error)),
        }
    }

    fn require_available(&self) -> Result<(), CapabilityHandleError> {
        if self.cancellation.is_cancelled() || !self.activity.is_open() || !self.gate.is_open() {
            Err(self.revoked())
        } else {
            Ok(())
        }
    }

    fn activity_error(&self, error: ScopeActivityError) -> CapabilityHandleError {
        match error {
            ScopeActivityError::Revoked => self.revoked(),
            ScopeActivityError::Exhausted => CapabilityHandleError::AdmissionExhausted {
                capability_id: self.capability_id.clone(),
                registration_id: self.registration_id,
            },
        }
    }

    fn admission_error(&self, error: RegistrationAdmissionError) -> CapabilityHandleError {
        match error {
            RegistrationAdmissionError::Revoked => self.revoked(),
            RegistrationAdmissionError::Exhausted => CapabilityHandleError::AdmissionExhausted {
                capability_id: self.capability_id.clone(),
                registration_id: self.registration_id,
            },
        }
    }

    fn revoked(&self) -> CapabilityHandleError {
        CapabilityHandleError::Revoked {
            capability_id: self.capability_id.clone(),
            registration_id: self.registration_id,
            activation_id: self.activation_id.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for CapabilityHandle<T> {
    fn clone(&self) -> Self {
        Self {
            capability_id: self.capability_id.clone(),
            registration_id: self.registration_id,
            activation_id: self.activation_id.clone(),
            cancellation: self.cancellation.clone(),
            activity: self.activity.clone(),
            provider: self.provider.clone(),
            gate: Arc::clone(&self.gate),
            invariant: PhantomData,
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for CapabilityHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityHandle")
            .field("capability_id", &self.capability_id)
            .field("registration_id", &self.registration_id)
            .field("activation_id", &self.activation_id)
            .finish_non_exhaustive()
    }
}

/// Fail-closed rejection from a capability handle use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapabilityHandleError {
    Revoked {
        capability_id: CapabilityId,
        registration_id: CapabilityRegistrationId,
        activation_id: PluginActivationId,
    },
    AdmissionExhausted {
        capability_id: CapabilityId,
        registration_id: CapabilityRegistrationId,
    },
}

impl fmt::Display for CapabilityHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Revoked {
                capability_id,
                registration_id,
                activation_id,
            } => write!(
                f,
                "capability {capability_id} registration {registration_id} is revoked for activation {activation_id}"
            ),
            Self::AdmissionExhausted {
                capability_id,
                registration_id,
            } => write!(
                f,
                "capability {capability_id} registration {registration_id} exhausted call admission"
            ),
        }
    }
}

impl Error for CapabilityHandleError {}

pub(super) struct RegistrationGate {
    state: AtomicUsize,
}

impl RegistrationGate {
    pub(super) fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
        }
    }

    pub(super) fn revoke(&self) {
        self.state.fetch_or(REVOKED, Ordering::AcqRel);
    }

    pub(super) fn is_open(&self) -> bool {
        self.state.load(Ordering::Acquire) & REVOKED == 0
    }

    fn admit(self: &Arc<Self>) -> Result<RegistrationAdmission, RegistrationAdmissionError> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & REVOKED != 0 {
                return Err(RegistrationAdmissionError::Revoked);
            }
            if current & ACTIVE_CALLS == ACTIVE_CALLS {
                return Err(RegistrationAdmissionError::Exhausted);
            }
            match self.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(RegistrationAdmission {
                        gate: Arc::clone(self),
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

struct RegistrationAdmission {
    gate: Arc<RegistrationGate>,
}

impl Drop for RegistrationAdmission {
    fn drop(&mut self) {
        let previous = self.gate.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(
            previous & ACTIVE_CALLS,
            0,
            "capability call count underflow"
        );
    }
}

enum RegistrationAdmissionError {
    Revoked,
    Exhausted,
}
