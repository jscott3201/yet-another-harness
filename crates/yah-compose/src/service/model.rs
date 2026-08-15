use std::{any::TypeId, error::Error, fmt, marker::PhantomData, sync::Arc};

use crate::{
    ActivationEpoch, ComponentInstanceId, ComponentStateKind, EffectRegistrationId,
    EffectScopeError, EffectScopeId, EffectScopeState, ScopeId, ServiceId,
};

/// A stable service identity paired with its exact Rust contract type.
///
/// The service ID is semantic identity; Rust's [`TypeId`] is only a local
/// consistency check. It is never a durable or wire identifier.
///
/// Typed service wrappers are invariant in `T`. In particular, function-type
/// subtyping cannot be used to change the contract associated with an ID.
///
/// ```compile_fail
/// use yah_compose::ServiceDefinition;
///
/// fn forge(
///     service: ServiceDefinition<fn(&str)>,
/// ) -> ServiceDefinition<fn(&'static str)> {
///     service
/// }
/// ```
pub struct ServiceDefinition<T: ?Sized + Send + Sync + 'static> {
    id: ServiceId,
    invariant: PhantomData<fn(&T) -> &T>,
}

impl<T: ?Sized + Send + Sync + 'static> ServiceDefinition<T> {
    /// Associate a caller-chosen semantic identity with `T`.
    ///
    /// The owning SDK or manifest layer is responsible for namespace and
    /// version syntax. This process-local layer performs exact ID matching.
    pub fn new(id: impl Into<ServiceId>) -> Self {
        Self {
            id: id.into(),
            invariant: PhantomData,
        }
    }

    pub fn id(&self) -> &ServiceId {
        &self.id
    }

    pub fn required(&self) -> ServiceRequirement<T> {
        ServiceRequirement {
            definition: self.clone(),
        }
    }

    /// Package an already shared, possibly unsized service implementation for
    /// publication.
    pub fn provider_arc(&self, value: Arc<T>) -> ServiceProvider<T> {
        ServiceProvider {
            definition: self.clone(),
            value,
        }
    }

    pub(crate) fn contract(&self) -> ContractType {
        ContractType::of::<T>()
    }
}

impl<T: Send + Sync + 'static> ServiceDefinition<T> {
    /// Package a sized service implementation for publication.
    pub fn provider(&self, value: T) -> ServiceProvider<T> {
        self.provider_arc(Arc::new(value))
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for ServiceDefinition<T> {
    fn clone(&self) -> Self {
        Self::new(self.id.clone())
    }
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for ServiceDefinition<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceDefinition")
            .field("id", &self.id)
            .field("contract", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T: ?Sized + Send + Sync + 'static> PartialEq for ServiceDefinition<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T: ?Sized + Send + Sync + 'static> Eq for ServiceDefinition<T> {}

/// A typed declaration that a component requires one exact service contract.
pub struct ServiceRequirement<T: ?Sized + Send + Sync + 'static> {
    definition: ServiceDefinition<T>,
}

impl<T: ?Sized + Send + Sync + 'static> ServiceRequirement<T> {
    pub fn service_id(&self) -> &ServiceId {
        self.definition.id()
    }

    pub fn definition(&self) -> &ServiceDefinition<T> {
        &self.definition
    }

    pub(crate) fn contract(&self) -> ContractType {
        self.definition.contract()
    }

    pub(crate) fn erased(&self) -> RequiredService {
        RequiredService {
            service_id: self.service_id().clone(),
            contract: self.contract(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for ServiceRequirement<T> {
    fn clone(&self) -> Self {
        Self {
            definition: self.definition.clone(),
        }
    }
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for ServiceRequirement<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceRequirement")
            .field("service_id", self.service_id())
            .field("contract", &self.contract().name())
            .finish()
    }
}

/// A typed provider value before it is published into a registry.
pub struct ServiceProvider<T: ?Sized + Send + Sync + 'static> {
    pub(crate) definition: ServiceDefinition<T>,
    pub(crate) value: Arc<T>,
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for ServiceProvider<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceProvider")
            .field("service_id", self.definition.id())
            .field("contract", &self.definition.contract().name())
            .finish_non_exhaustive()
    }
}

/// Type-erased required-service metadata stored on a component definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequiredService {
    service_id: ServiceId,
    contract: ContractType,
}

impl RequiredService {
    pub fn service_id(&self) -> &ServiceId {
        &self.service_id
    }

    /// A diagnostic Rust type name, not a durable contract identity.
    pub fn contract_name(&self) -> &'static str {
        self.contract.name()
    }

    pub(crate) const fn contract(&self) -> ContractType {
        self.contract
    }
}

/// Result of checking all required services declared by a component.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequirementStatus {
    Ready,
    Missing(Vec<ServiceId>),
}

impl RequirementStatus {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    pub fn missing(&self) -> &[ServiceId] {
        match self {
            Self::Ready => &[],
            Self::Missing(services) => services,
        }
    }
}

/// Identity of one exact, effect-owned provider publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProviderRegistrationId(EffectRegistrationId);

impl ProviderRegistrationId {
    pub(crate) const fn new(registration_id: EffectRegistrationId) -> Self {
        Self(registration_id)
    }

    pub const fn activation(self) -> ActivationEpoch {
        self.0.activation()
    }

    pub const fn effect_scope_id(self) -> EffectScopeId {
        self.0.scope_id()
    }

    pub const fn cleanup_registration_id(self) -> EffectRegistrationId {
        self.0
    }
}

impl fmt::Display for ProviderRegistrationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/provider:{}",
            self.effect_scope_id(),
            self.0.sequence()
        )
    }
}

/// Immutable metadata for one currently discoverable provider candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderCandidate {
    pub(crate) id: ProviderRegistrationId,
    pub(crate) service_id: ServiceId,
    pub(crate) owner_instance_id: ComponentInstanceId,
    pub(crate) owner_scope_id: ScopeId,
    pub(crate) contract_name: &'static str,
}

impl ProviderCandidate {
    pub const fn id(&self) -> ProviderRegistrationId {
        self.id
    }

    pub fn service_id(&self) -> &ServiceId {
        &self.service_id
    }

    pub fn owner_instance_id(&self) -> &ComponentInstanceId {
        &self.owner_instance_id
    }

    pub fn owner_scope_id(&self) -> &ScopeId {
        &self.owner_scope_id
    }

    pub const fn activation(&self) -> ActivationEpoch {
        self.id.activation()
    }

    pub const fn effect_scope_id(&self) -> EffectScopeId {
        self.id.effect_scope_id()
    }

    /// A diagnostic Rust type name, not a durable contract identity.
    pub const fn contract_name(&self) -> &'static str {
        self.contract_name
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ContractType {
    type_id: TypeId,
    name: &'static str,
}

impl ContractType {
    pub(crate) fn of<T: ?Sized + 'static>() -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    pub(crate) const fn rust_type_id(self) -> TypeId {
        self.type_id
    }

    pub(crate) const fn name(self) -> &'static str {
        self.name
    }
}

/// Rejected service publication, discovery, or exact binding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServiceRegistryError {
    ContractTypeMismatch {
        service_id: ServiceId,
        expected: &'static str,
        received: &'static str,
    },
    ProviderOwnerNotActive {
        instance_id: ComponentInstanceId,
        state: ComponentStateKind,
    },
    ConsumerNotStarting {
        instance_id: ComponentInstanceId,
        state: ComponentStateKind,
    },
    ActivationMismatch {
        instance_id: ComponentInstanceId,
        component_activation: ActivationEpoch,
        effect_activation: ActivationEpoch,
    },
    ConsumerScopeNotOpen {
        scope_id: EffectScopeId,
        state: EffectScopeState,
    },
    UnknownProvider {
        provider_id: ProviderRegistrationId,
    },
    ProviderDoesNotSatisfy {
        provider_id: ProviderRegistrationId,
        required: ServiceId,
        provided: ServiceId,
    },
    ProviderUnavailable {
        provider_id: ProviderRegistrationId,
    },
    ProviderValueTypeMismatch {
        provider_id: ProviderRegistrationId,
        expected: &'static str,
        stored: &'static str,
    },
    EffectScope(EffectScopeError),
}

impl fmt::Display for ServiceRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ContractTypeMismatch {
                service_id,
                expected,
                received,
            } => write!(
                f,
                "service {service_id} uses Rust contract {expected}, not {received}"
            ),
            Self::ProviderOwnerNotActive { instance_id, state } => write!(
                f,
                "component {instance_id} cannot publish a provider while {state}"
            ),
            Self::ConsumerNotStarting { instance_id, state } => write!(
                f,
                "component {instance_id} cannot bind required services while {state}"
            ),
            Self::ActivationMismatch {
                instance_id,
                component_activation,
                effect_activation,
            } => write!(
                f,
                "component {instance_id} activation {component_activation} does not own effect activation {effect_activation}"
            ),
            Self::ConsumerScopeNotOpen { scope_id, state } => {
                write!(f, "consumer effect scope {scope_id} is {state}")
            }
            Self::UnknownProvider { provider_id } => {
                write!(f, "provider registration {provider_id} is unknown")
            }
            Self::ProviderDoesNotSatisfy {
                provider_id,
                required,
                provided,
            } => write!(
                f,
                "provider {provider_id} publishes {provided}, not required service {required}"
            ),
            Self::ProviderUnavailable { provider_id } => {
                write!(f, "provider registration {provider_id} is unavailable")
            }
            Self::ProviderValueTypeMismatch {
                provider_id,
                expected,
                stored,
            } => write!(
                f,
                "provider {provider_id} stores Rust contract {stored}, not {expected}"
            ),
            Self::EffectScope(error) => write!(f, "provider cleanup admission failed: {error}"),
        }
    }
}

impl Error for ServiceRegistryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::EffectScope(error) => Some(error),
            _ => None,
        }
    }
}

impl From<EffectScopeError> for ServiceRegistryError {
    fn from(error: EffectScopeError) -> Self {
        Self::EffectScope(error)
    }
}
