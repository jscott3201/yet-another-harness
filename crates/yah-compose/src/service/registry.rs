use std::{
    any::Any,
    collections::{BTreeMap, HashMap},
    fmt,
    sync::{Arc, Mutex, MutexGuard},
};

use crate::{
    ComponentDefinition, ComponentInstance, ComponentState, EffectScope, EffectScopeState,
    ServiceId,
};

use super::{
    ProviderCandidate, ProviderRegistrationId, RequiredService, RequirementStatus, ServiceHandle,
    ServiceProvider, ServiceRegistryError, ServiceRequirement, handle::ProviderGate,
    model::ContractType,
};

/// One process-local visibility domain for typed service providers.
///
/// The registry is intentionally not `Clone`: provider publication remains
/// under one composition authority. It may inventory multiple candidates for
/// a service, but callers must bind an exact [`ProviderRegistrationId`]. Scope
/// inheritance, selection policy, and reactive reconciliation are later layers.
/// The first declaration, discovery, or publication of a [`ServiceId`] fixes
/// its exact Rust contract type for this registry's lifetime.
pub struct ServiceRegistry {
    core: Arc<RegistryCore>,
}

impl ServiceRegistry {
    pub fn new() -> Self {
        Self {
            core: Arc::new(RegistryCore::default()),
        }
    }

    /// Publish one provider after its withdrawal callback is admitted to the
    /// owning activation's effect scope.
    ///
    /// Providers are visible only once their component is active. A future
    /// callback runner may stage provider values while starting, complete the
    /// activation, and then call this method as its readiness point.
    pub fn provide<T: ?Sized + Send + Sync + 'static>(
        &mut self,
        owner: &ComponentInstance,
        effects: &mut EffectScope,
        provider: ServiceProvider<T>,
    ) -> Result<ProviderCandidate, ServiceRegistryError> {
        let activation = match owner.state() {
            ComponentState::Active { activation } => *activation,
            state => {
                return Err(ServiceRegistryError::ProviderOwnerNotActive {
                    instance_id: owner.id().clone(),
                    state: state.kind(),
                });
            }
        };
        if effects.activation() != activation {
            return Err(ServiceRegistryError::ActivationMismatch {
                instance_id: owner.id().clone(),
                component_activation: activation,
                effect_activation: effects.activation(),
            });
        }

        let service_id = provider.definition.id().clone();
        let contract = provider.definition.contract();
        self.core.check_contract(&service_id, contract)?;

        let marker = Arc::new(());
        let gate = Arc::new(ProviderGate::new(effects.cancellation()));
        let value = provider.value;
        let cleanup_value = Arc::clone(&value);
        let cleanup_marker = Arc::clone(&marker);
        let weak_core = Arc::downgrade(&self.core);
        let cleanup_label = format!("withdraw service {service_id} from {}", owner.id());
        let cleanup_registration_id = effects.defer_sync(cleanup_label, move || {
            if let Some(core) = weak_core.upgrade() {
                core.withdraw(&cleanup_marker);
            }
            drop(cleanup_value);
            Ok(())
        })?;

        let provider_id = ProviderRegistrationId::new(cleanup_registration_id);
        let candidate = ProviderCandidate {
            id: provider_id,
            service_id: service_id.clone(),
            owner_instance_id: owner.id().clone(),
            owner_scope_id: owner.scope_id().clone(),
            contract_name: contract.name(),
        };
        let entry = ProviderEntry {
            candidate: candidate.clone(),
            contract,
            marker,
            gate,
            value: Box::new(Arc::clone(&value)),
        };
        self.core.publish(service_id, contract, entry);
        Ok(candidate)
    }

    /// Return discoverable candidates matching one exact typed requirement.
    ///
    /// Deterministic provider-registration-ID order is an inventory, not a
    /// ranking or publication-order promise.
    pub fn candidates<T: ?Sized + Send + Sync + 'static>(
        &self,
        requirement: &ServiceRequirement<T>,
    ) -> Result<Vec<ProviderCandidate>, ServiceRegistryError> {
        self.core
            .candidates(requirement.service_id(), requirement.contract())
    }

    /// Check whether every required service declared by `definition` has at
    /// least one discoverable provider candidate.
    ///
    /// This reports readiness without mutating component lifecycle. The future
    /// reconciler will keep missing consumers pending and start ready ones.
    pub fn requirement_status(
        &self,
        definition: &ComponentDefinition,
    ) -> Result<RequirementStatus, ServiceRegistryError> {
        self.core.requirement_status(definition.requirements())
    }

    /// Bind one starting consumer activation to one exact provider candidate.
    ///
    /// Ordinary handles never switch to a replacement provider. Both provider
    /// and consumer effect-scope cancellation fence every subsequent call.
    /// Until the callback runner exists, composition authority must supply a
    /// requirement declared by the consumer's definition; this low-level bind
    /// operation cannot verify membership from the instance's definition ID.
    pub fn bind<T: ?Sized + Send + Sync + 'static>(
        &self,
        consumer: &ComponentInstance,
        consumer_effects: &EffectScope,
        requirement: &ServiceRequirement<T>,
        provider_id: ProviderRegistrationId,
    ) -> Result<ServiceHandle<T>, ServiceRegistryError> {
        let consumer_activation = match consumer.state() {
            ComponentState::Starting { activation } => *activation,
            state => {
                return Err(ServiceRegistryError::ConsumerNotStarting {
                    instance_id: consumer.id().clone(),
                    state: state.kind(),
                });
            }
        };
        if consumer_effects.activation() != consumer_activation {
            return Err(ServiceRegistryError::ActivationMismatch {
                instance_id: consumer.id().clone(),
                component_activation: consumer_activation,
                effect_activation: consumer_effects.activation(),
            });
        }
        if consumer_effects.state() != EffectScopeState::Open {
            return Err(ServiceRegistryError::ConsumerScopeNotOpen {
                scope_id: consumer_effects.id(),
                state: consumer_effects.state(),
            });
        }

        let (provider, gate, candidate) = self.core.binding::<T>(requirement, provider_id)?;
        if !gate.is_available() {
            return Err(ServiceRegistryError::ProviderUnavailable { provider_id });
        }

        Ok(ServiceHandle::new(
            Arc::downgrade(&provider),
            gate,
            candidate,
            consumer.id().clone(),
            consumer.scope_id().clone(),
            consumer_activation,
            consumer_effects.cancellation(),
        ))
    }
}

impl Default for ServiceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ServiceRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.core.lock();
        f.debug_struct("ServiceRegistry")
            .field("contract_count", &state.contracts.len())
            .field("provider_count", &state.providers.len())
            .finish()
    }
}

impl Drop for ServiceRegistry {
    fn drop(&mut self) {
        self.core.revoke_all();
    }
}

#[derive(Default)]
struct RegistryCore {
    state: Mutex<RegistryState>,
}

impl RegistryCore {
    fn lock(&self) -> MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn check_contract(
        &self,
        service_id: &ServiceId,
        received: ContractType,
    ) -> Result<(), ServiceRegistryError> {
        let state = self.lock();
        check_contract(&state, service_id, received)
    }

    fn publish(&self, service_id: ServiceId, contract: ContractType, entry: ProviderEntry) {
        let mut state = self.lock();
        state.contracts.entry(service_id).or_insert(contract);
        let displaced = state.providers.insert(entry.candidate.id(), entry);
        assert!(
            displaced.is_none(),
            "effect registration identities are process-unique"
        );
    }

    fn candidates(
        &self,
        service_id: &ServiceId,
        contract: ContractType,
    ) -> Result<Vec<ProviderCandidate>, ServiceRegistryError> {
        let mut state = self.lock();
        ensure_contract(&mut state, service_id, contract)?;
        Ok(state
            .providers
            .values()
            .filter(|entry| entry.candidate.service_id() == service_id && entry.gate.is_available())
            .map(|entry| entry.candidate.clone())
            .collect())
    }

    fn requirement_status(
        &self,
        requirements: &[RequiredService],
    ) -> Result<RequirementStatus, ServiceRegistryError> {
        let mut state = self.lock();
        for requirement in requirements {
            ensure_contract(&mut state, requirement.service_id(), requirement.contract())?;
        }

        let missing = requirements
            .iter()
            .filter(|requirement| {
                !state.providers.values().any(|entry| {
                    entry.candidate.service_id() == requirement.service_id()
                        && entry.contract == requirement.contract()
                        && entry.gate.is_available()
                })
            })
            .map(|requirement| requirement.service_id().clone())
            .collect::<Vec<_>>();
        if missing.is_empty() {
            Ok(RequirementStatus::Ready)
        } else {
            Ok(RequirementStatus::Missing(missing))
        }
    }

    fn binding<T: ?Sized + Send + Sync + 'static>(
        &self,
        requirement: &ServiceRequirement<T>,
        provider_id: ProviderRegistrationId,
    ) -> Result<(Arc<T>, Arc<ProviderGate>, ProviderCandidate), ServiceRegistryError> {
        let mut state = self.lock();
        ensure_contract(&mut state, requirement.service_id(), requirement.contract())?;
        let entry = state
            .providers
            .get(&provider_id)
            .ok_or(ServiceRegistryError::UnknownProvider { provider_id })?;
        if entry.candidate.service_id() != requirement.service_id() {
            return Err(ServiceRegistryError::ProviderDoesNotSatisfy {
                provider_id,
                required: requirement.service_id().clone(),
                provided: entry.candidate.service_id().clone(),
            });
        }
        if !entry.gate.is_available() {
            return Err(ServiceRegistryError::ProviderUnavailable { provider_id });
        }
        let provider = entry.value.downcast_ref::<Arc<T>>().cloned().ok_or(
            ServiceRegistryError::ProviderValueTypeMismatch {
                provider_id,
                expected: requirement.contract().name(),
                stored: entry.contract.name(),
            },
        )?;
        Ok((provider, Arc::clone(&entry.gate), entry.candidate.clone()))
    }

    fn withdraw(&self, marker: &Arc<()>) {
        let removed = {
            let mut state = self.lock();
            let provider_id = state.providers.iter().find_map(|(provider_id, entry)| {
                Arc::ptr_eq(&entry.marker, marker).then_some(*provider_id)
            });
            provider_id.and_then(|provider_id| {
                let entry = state.providers.remove(&provider_id);
                if let Some(entry) = &entry {
                    entry.gate.revoke();
                }
                entry
            })
        };
        drop(removed);
    }

    fn revoke_all(&self) {
        let providers = {
            let mut state = self.lock();
            for provider in state.providers.values() {
                provider.gate.revoke();
            }
            std::mem::take(&mut state.providers)
        };
        drop(providers);
    }
}

#[derive(Default)]
struct RegistryState {
    contracts: HashMap<ServiceId, ContractType>,
    providers: BTreeMap<ProviderRegistrationId, ProviderEntry>,
}

struct ProviderEntry {
    candidate: ProviderCandidate,
    contract: ContractType,
    marker: Arc<()>,
    gate: Arc<ProviderGate>,
    value: Box<dyn Any + Send + Sync>,
}

fn check_contract(
    state: &RegistryState,
    service_id: &ServiceId,
    received: ContractType,
) -> Result<(), ServiceRegistryError> {
    let Some(expected) = state.contracts.get(service_id) else {
        return Ok(());
    };
    if expected.rust_type_id() == received.rust_type_id() {
        Ok(())
    } else {
        Err(ServiceRegistryError::ContractTypeMismatch {
            service_id: service_id.clone(),
            expected: expected.name(),
            received: received.name(),
        })
    }
}

fn ensure_contract(
    state: &mut RegistryState,
    service_id: &ServiceId,
    received: ContractType,
) -> Result<(), ServiceRegistryError> {
    check_contract(state, service_id, received)?;
    state
        .contracts
        .entry(service_id.clone())
        .or_insert(received);
    Ok(())
}
