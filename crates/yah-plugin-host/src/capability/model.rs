use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    marker::PhantomData,
};

use crate::{CapabilityId, DriverKind, PluginRevision, PluginRevisionId};

/// A capability identity paired with its exact in-process Rust contract.
///
/// The wire ID is semantic identity. Rust's `TypeId` is only a process-local
/// consistency check, and the wrapper is invariant in `T`.
///
/// ```compile_fail
/// use yah_plugin_host::{CapabilityDefinition, CapabilityId};
///
/// fn forge(
///     capability: CapabilityDefinition<fn(&str)>,
/// ) -> CapabilityDefinition<fn(&'static str)> {
///     capability
/// }
/// ```
pub struct CapabilityDefinition<T: ?Sized + Send + Sync + 'static> {
    id: CapabilityId,
    invariant: PhantomData<fn(&T) -> &T>,
}

impl<T: ?Sized + Send + Sync + 'static> CapabilityDefinition<T> {
    pub fn new(id: CapabilityId) -> Self {
        Self {
            id,
            invariant: PhantomData,
        }
    }

    pub const fn id(&self) -> &CapabilityId {
        &self.id
    }

    pub(crate) fn contract(&self) -> ContractType {
        ContractType::of::<T>()
    }
}

impl<T: ?Sized + Send + Sync + 'static> Clone for CapabilityDefinition<T> {
    fn clone(&self) -> Self {
        Self::new(self.id.clone())
    }
}

impl<T: ?Sized + Send + Sync + 'static> fmt::Debug for CapabilityDefinition<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CapabilityDefinition")
            .field("id", &self.id)
            .field("contract", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T: ?Sized + Send + Sync + 'static> PartialEq for CapabilityDefinition<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T: ?Sized + Send + Sync + 'static> Eq for CapabilityDefinition<T> {}

/// Exact process-local identity of one broker registration.
///
/// This value is diagnostic metadata, never bearer authority or a wire token.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CapabilityRegistrationId {
    broker_incarnation: u64,
    sequence: u64,
}

impl CapabilityRegistrationId {
    pub(crate) const fn new(broker_incarnation: u64, sequence: u64) -> Self {
        Self {
            broker_incarnation,
            sequence,
        }
    }

    pub(crate) const fn broker_incarnation(self) -> u64 {
        self.broker_incarnation
    }
}

impl fmt::Display for CapabilityRegistrationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "capability-provider:{}/{}",
            self.broker_incarnation, self.sequence
        )
    }
}

/// One authority-selected exact provider for a requested capability.
///
/// Registrations mint these values. The ID is still descriptive; only a
/// privately constructed start context can turn it into a live handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CapabilityGrant {
    capability_id: CapabilityId,
    registration_id: CapabilityRegistrationId,
}

impl CapabilityGrant {
    pub(crate) fn new(
        capability_id: CapabilityId,
        registration_id: CapabilityRegistrationId,
    ) -> Self {
        Self {
            capability_id,
            registration_id,
        }
    }

    pub const fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }

    pub const fn registration_id(&self) -> CapabilityRegistrationId {
        self.registration_id
    }
}

/// Immutable effective grants for one exact plugin package revision.
///
/// Construction proves only that every selected capability was requested by
/// this validated manifest. A trusted admission layer must already have
/// intersected policy, approval, and backend enforceability. Missing requests
/// are denied; no ambient fallback is added here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EffectiveCapabilityGrants {
    revision_id: PluginRevisionId,
    driver_kind: DriverKind,
    registrations: BTreeMap<CapabilityId, CapabilityRegistrationId>,
}

impl EffectiveCapabilityGrants {
    pub fn new(
        revision: &PluginRevision,
        grants: impl IntoIterator<Item = CapabilityGrant>,
    ) -> Result<Self, CapabilityGrantError> {
        let requested = revision
            .manifest()
            .requested_capabilities()
            .iter()
            .map(|request| request.capability())
            .collect::<BTreeSet<_>>();
        let mut registrations = BTreeMap::new();
        for grant in grants {
            if !requested.contains(&grant.capability_id) {
                return Err(CapabilityGrantError::NotRequested {
                    capability_id: grant.capability_id,
                });
            }
            if registrations
                .insert(grant.capability_id.clone(), grant.registration_id)
                .is_some()
            {
                return Err(CapabilityGrantError::Duplicate {
                    capability_id: grant.capability_id,
                });
            }
        }
        Ok(Self {
            revision_id: revision.id().clone(),
            driver_kind: revision.manifest().entrypoint().driver(),
            registrations,
        })
    }

    pub fn empty(revision: &PluginRevision) -> Self {
        Self::new(revision, std::iter::empty()).expect("an empty grant set is always valid")
    }

    pub const fn revision_id(&self) -> &PluginRevisionId {
        &self.revision_id
    }

    pub const fn driver_kind(&self) -> DriverKind {
        self.driver_kind
    }

    pub fn contains(&self, capability_id: &CapabilityId) -> bool {
        self.registrations.contains_key(capability_id)
    }

    pub fn granted_capabilities(&self) -> impl ExactSizeIterator<Item = &CapabilityId> {
        self.registrations.keys()
    }

    pub(crate) const fn registrations(&self) -> &BTreeMap<CapabilityId, CapabilityRegistrationId> {
        &self.registrations
    }
}

/// Why an effective grant snapshot was structurally invalid.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CapabilityGrantError {
    NotRequested { capability_id: CapabilityId },
    Duplicate { capability_id: CapabilityId },
}

impl fmt::Display for CapabilityGrantError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRequested { capability_id } => {
                write!(
                    f,
                    "capability {capability_id} was not requested by the plugin manifest"
                )
            }
            Self::Duplicate { capability_id } => {
                write!(f, "capability {capability_id} was granted more than once")
            }
        }
    }
}

impl Error for CapabilityGrantError {}

#[derive(Clone, Copy)]
pub(crate) struct ContractType {
    id: std::any::TypeId,
    name: &'static str,
}

impl ContractType {
    fn of<T: ?Sized + 'static>() -> Self {
        Self {
            id: std::any::TypeId::of::<T>(),
            name: std::any::type_name::<T>(),
        }
    }

    pub(crate) const fn id(self) -> std::any::TypeId {
        self.id
    }

    pub(crate) const fn name(self) -> &'static str {
        self.name
    }
}
