use std::{
    any::Any,
    collections::BTreeMap,
    error::Error,
    fmt,
    marker::PhantomData,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};

use yah_compose::{ScopeActivity, ScopeCancellation};

use crate::{CapabilityId, PluginActivationId};

use super::{
    CapabilityDefinition, CapabilityGrant, CapabilityHandle, CapabilityRegistrationId,
    EffectiveCapabilityGrants, handle::RegistrationGate, model::ContractType,
};

static NEXT_BROKER_INCARNATION: AtomicU64 = AtomicU64::new(1);

/// Process-local typed host capability registry.
///
/// Registrations are exact and singular per capability ID. Registration owners
/// retain one strong provider reference, while broker entries and plugin
/// handles are weak; callers remain responsible for any other `Arc` clones they
/// keep. Dropping the broker or a registration revokes old handles, and a later
/// replacement receives a new registration identity.
pub struct CapabilityBroker {
    core: Arc<BrokerCore>,
    next_registration: u64,
}

impl CapabilityBroker {
    /// Create an empty broker with a fresh opaque incarnation.
    ///
    /// # Errors
    ///
    /// Returns [`CapabilityBrokerError::BrokerIncarnationExhausted`] if the
    /// process-wide broker-incarnation counter is exhausted.
    pub fn new() -> Result<Self, CapabilityBrokerError> {
        let incarnation = NEXT_BROKER_INCARNATION
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(1)
            })
            .map_err(|_| CapabilityBrokerError::BrokerIncarnationExhausted)?;
        Ok(Self {
            core: Arc::new(BrokerCore {
                incarnation,
                state: Mutex::new(BrokerState::default()),
            }),
            next_registration: 1,
        })
    }

    /// Register one exact typed provider and retain its lifetime in the owner.
    pub fn register<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        definition: &CapabilityDefinition<T>,
        provider: Arc<T>,
    ) -> Result<CapabilityProviderRegistration<T>, CapabilityBrokerError> {
        let id = CapabilityRegistrationId::new(self.core.incarnation, self.next_registration);
        let next = self
            .next_registration
            .checked_add(1)
            .ok_or(CapabilityBrokerError::RegistrationIdExhausted)?;
        let gate = Arc::new(RegistrationGate::new());
        self.core.register(
            id,
            definition.id().clone(),
            definition.contract(),
            Box::new(Arc::downgrade(&provider)),
            Arc::clone(&gate),
        )?;
        self.next_registration = next;
        Ok(CapabilityProviderRegistration {
            id,
            capability_id: definition.id().clone(),
            provider: Some(provider),
            gate,
            broker: Arc::downgrade(&self.core),
            invariant: PhantomData,
        })
    }

    pub(crate) fn prepare_bindings(
        &self,
        grants: &EffectiveCapabilityGrants,
    ) -> Result<PreparedCapabilityBindings, CapabilityBrokerError> {
        self.core.validate_grants(grants)?;
        Ok(PreparedCapabilityBindings {
            broker: Arc::downgrade(&self.core),
            registrations: Arc::new(grants.registrations().clone()),
        })
    }
}

impl fmt::Debug for CapabilityBroker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.core.lock();
        f.debug_struct("CapabilityBroker")
            .field("incarnation", &self.core.incarnation)
            .field("registrations", &state.registrations.len())
            .finish_non_exhaustive()
    }
}

/// Unique owner of one broker registration and its strong provider reference.
#[must_use = "dropping a capability registration revokes all exact handles"]
pub struct CapabilityProviderRegistration<T: ?Sized + Send + Sync + 'static> {
    id: CapabilityRegistrationId,
    capability_id: CapabilityId,
    provider: Option<Arc<T>>,
    gate: Arc<RegistrationGate>,
    broker: Weak<BrokerCore>,
    invariant: PhantomData<fn(&T) -> &T>,
}

impl<T: ?Sized + Send + Sync + 'static> CapabilityProviderRegistration<T> {
    pub const fn id(&self) -> CapabilityRegistrationId {
        self.id
    }

    pub const fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }

    pub fn grant(&self) -> CapabilityGrant {
        CapabilityGrant::new(self.capability_id.clone(), self.id)
    }

    /// Revoke this exact registration and return provider ownership.
    pub fn withdraw(mut self) -> Arc<T> {
        self.revoke();
        self.provider
            .take()
            .expect("a live registration retains its provider")
    }

    fn revoke(&self) {
        self.gate.revoke();
        if let Some(broker) = self.broker.upgrade() {
            broker.remove(self.id);
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Drop for CapabilityProviderRegistration<T> {
    fn drop(&mut self) {
        self.revoke();
    }
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for CapabilityProviderRegistration<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityProviderRegistration")
            .field("id", &self.id)
            .field("capability_id", &self.capability_id)
            .finish_non_exhaustive()
    }
}

/// Exact activation context delivered only inside a host-minted start permit.
///
/// The context is weak: retaining it cannot keep the broker or any registered
/// provider alive. It can resolve only the immutable exact registrations chosen
/// for this activation.
#[derive(Clone)]
pub struct PluginStartContext {
    activation_id: PluginActivationId,
    cancellation: ScopeCancellation,
    activity: ScopeActivity,
    broker: Weak<BrokerCore>,
    registrations: Arc<BTreeMap<CapabilityId, CapabilityRegistrationId>>,
}

impl PluginStartContext {
    pub(crate) fn new(
        activation_id: PluginActivationId,
        cancellation: ScopeCancellation,
        activity: ScopeActivity,
        prepared: PreparedCapabilityBindings,
    ) -> Self {
        Self {
            activation_id,
            cancellation,
            activity,
            broker: prepared.broker,
            registrations: prepared.registrations,
        }
    }

    pub const fn activation_id(&self) -> &PluginActivationId {
        &self.activation_id
    }

    pub const fn cancellation(&self) -> &ScopeCancellation {
        &self.cancellation
    }

    /// Capability IDs selected in the immutable snapshot, regardless of
    /// current activation or provider liveness.
    pub fn granted_capabilities(&self) -> impl ExactSizeIterator<Item = &CapabilityId> {
        self.registrations.keys()
    }

    /// Whether this immutable snapshot includes a capability ID.
    ///
    /// Membership does not imply that the activation or exact provider remains
    /// live; [`Self::handle`] performs those checks.
    pub fn is_granted(&self, capability_id: &CapabilityId) -> bool {
        self.registrations.contains_key(capability_id)
    }

    pub fn handle<T: ?Sized + Send + Sync + 'static>(
        &self,
        definition: &CapabilityDefinition<T>,
    ) -> Result<CapabilityHandle<T>, CapabilityBrokerError> {
        if self.cancellation.is_cancelled() || !self.activity.is_open() {
            return Err(CapabilityBrokerError::ActivationInactive {
                activation_id: self.activation_id.clone(),
            });
        }
        let Some(registration_id) = self.registrations.get(definition.id()).copied() else {
            return Err(CapabilityBrokerError::NotGranted {
                capability_id: definition.id().clone(),
                activation_id: self.activation_id.clone(),
            });
        };
        let Some(broker) = self.broker.upgrade() else {
            return Err(CapabilityBrokerError::ProviderUnavailable {
                capability_id: definition.id().clone(),
                registration_id,
            });
        };
        let (provider, gate) = broker.binding(registration_id, definition)?;
        Ok(CapabilityHandle::new(
            definition.id().clone(),
            registration_id,
            self.activation_id.clone(),
            self.cancellation.clone(),
            self.activity.clone(),
            provider,
            gate,
        ))
    }
}

impl fmt::Debug for PluginStartContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginStartContext")
            .field("activation_id", &self.activation_id)
            .field("grant_count", &self.registrations.len())
            .field("cancelled", &self.cancellation.is_cancelled())
            .finish_non_exhaustive()
    }
}

pub(crate) struct PreparedCapabilityBindings {
    broker: Weak<BrokerCore>,
    registrations: Arc<BTreeMap<CapabilityId, CapabilityRegistrationId>>,
}

struct BrokerCore {
    incarnation: u64,
    state: Mutex<BrokerState>,
}

#[derive(Default)]
struct BrokerState {
    contracts: BTreeMap<CapabilityId, ContractType>,
    registrations: BTreeMap<CapabilityRegistrationId, RegistrationEntry>,
    active: BTreeMap<CapabilityId, CapabilityRegistrationId>,
}

struct RegistrationEntry {
    capability_id: CapabilityId,
    contract: ContractType,
    provider: Box<dyn Any + Send + Sync>,
    gate: Arc<RegistrationGate>,
}

impl BrokerCore {
    fn register(
        &self,
        id: CapabilityRegistrationId,
        capability_id: CapabilityId,
        contract: ContractType,
        provider: Box<dyn Any + Send + Sync>,
        gate: Arc<RegistrationGate>,
    ) -> Result<(), CapabilityBrokerError> {
        let mut state = self.lock();
        if let Some(existing) = state.contracts.get(&capability_id)
            && existing.id() != contract.id()
        {
            return Err(CapabilityBrokerError::ContractTypeMismatch {
                capability_id,
                expected: existing.name(),
                received: contract.name(),
            });
        }
        if let Some(existing_id) = state.active.get(&capability_id).copied() {
            return Err(CapabilityBrokerError::DuplicateProvider {
                capability_id,
                existing_id,
            });
        }
        state.contracts.insert(capability_id.clone(), contract);
        state.active.insert(capability_id.clone(), id);
        state.registrations.insert(
            id,
            RegistrationEntry {
                capability_id,
                contract,
                provider,
                gate,
            },
        );
        Ok(())
    }

    fn validate_grants(
        &self,
        grants: &EffectiveCapabilityGrants,
    ) -> Result<(), CapabilityBrokerError> {
        let state = self.lock();
        for (capability_id, registration_id) in grants.registrations() {
            if registration_id.broker_incarnation() != self.incarnation {
                return Err(CapabilityBrokerError::ForeignRegistration {
                    capability_id: capability_id.clone(),
                    registration_id: *registration_id,
                });
            }
            let Some(entry) = state.registrations.get(registration_id) else {
                return Err(CapabilityBrokerError::ProviderUnavailable {
                    capability_id: capability_id.clone(),
                    registration_id: *registration_id,
                });
            };
            if &entry.capability_id != capability_id || !entry.gate.is_open() {
                return Err(CapabilityBrokerError::ProviderUnavailable {
                    capability_id: capability_id.clone(),
                    registration_id: *registration_id,
                });
            }
        }
        Ok(())
    }

    fn binding<T: ?Sized + Send + Sync + 'static>(
        &self,
        registration_id: CapabilityRegistrationId,
        definition: &CapabilityDefinition<T>,
    ) -> Result<(Weak<T>, Arc<RegistrationGate>), CapabilityBrokerError> {
        let state = self.lock();
        let Some(entry) = state.registrations.get(&registration_id) else {
            return Err(CapabilityBrokerError::ProviderUnavailable {
                capability_id: definition.id().clone(),
                registration_id,
            });
        };
        if &entry.capability_id != definition.id() || !entry.gate.is_open() {
            return Err(CapabilityBrokerError::ProviderUnavailable {
                capability_id: definition.id().clone(),
                registration_id,
            });
        }
        if entry.contract.id() != definition.contract().id() {
            return Err(CapabilityBrokerError::ContractTypeMismatch {
                capability_id: definition.id().clone(),
                expected: entry.contract.name(),
                received: definition.contract().name(),
            });
        }
        let provider = entry
            .provider
            .downcast_ref::<Weak<T>>()
            .cloned()
            .ok_or_else(|| CapabilityBrokerError::ContractTypeMismatch {
                capability_id: definition.id().clone(),
                expected: entry.contract.name(),
                received: definition.contract().name(),
            })?;
        Ok((provider, Arc::clone(&entry.gate)))
    }

    fn remove(&self, id: CapabilityRegistrationId) {
        let mut state = self.lock();
        let Some(entry) = state.registrations.remove(&id) else {
            return;
        };
        if state.active.get(&entry.capability_id) == Some(&id) {
            state.active.remove(&entry.capability_id);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BrokerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Drop for BrokerCore {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for entry in state.registrations.values() {
            entry.gate.revoke();
        }
        state.registrations.clear();
        state.active.clear();
    }
}

/// Why broker registration, grant binding, or typed handle resolution failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapabilityBrokerError {
    BrokerIncarnationExhausted,
    RegistrationIdExhausted,
    DuplicateProvider {
        capability_id: CapabilityId,
        existing_id: CapabilityRegistrationId,
    },
    ForeignRegistration {
        capability_id: CapabilityId,
        registration_id: CapabilityRegistrationId,
    },
    ProviderUnavailable {
        capability_id: CapabilityId,
        registration_id: CapabilityRegistrationId,
    },
    ContractTypeMismatch {
        capability_id: CapabilityId,
        expected: &'static str,
        received: &'static str,
    },
    NotGranted {
        capability_id: CapabilityId,
        activation_id: PluginActivationId,
    },
    ActivationInactive {
        activation_id: PluginActivationId,
    },
}

impl fmt::Display for CapabilityBrokerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BrokerIncarnationExhausted => {
                f.write_str("capability broker incarnation space is exhausted")
            }
            Self::RegistrationIdExhausted => {
                f.write_str("capability registration identity space is exhausted")
            }
            Self::DuplicateProvider {
                capability_id,
                existing_id,
            } => write!(
                f,
                "capability {capability_id} already has active registration {existing_id}"
            ),
            Self::ForeignRegistration {
                capability_id,
                registration_id,
            } => write!(
                f,
                "capability {capability_id} registration {registration_id} belongs to another broker"
            ),
            Self::ProviderUnavailable {
                capability_id,
                registration_id,
            } => write!(
                f,
                "capability {capability_id} registration {registration_id} is unavailable"
            ),
            Self::ContractTypeMismatch {
                capability_id,
                expected,
                received,
            } => write!(
                f,
                "capability {capability_id} uses Rust contract {expected}, not {received}"
            ),
            Self::NotGranted {
                capability_id,
                activation_id,
            } => write!(
                f,
                "capability {capability_id} is not granted to activation {activation_id}"
            ),
            Self::ActivationInactive { activation_id } => {
                write!(f, "plugin activation {activation_id} is inactive")
            }
        }
    }
}

impl Error for CapabilityBrokerError {}
