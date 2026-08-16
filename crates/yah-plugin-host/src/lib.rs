//! Runtime-neutral plugin package and driver contracts.
//!
//! This crate validates declarative package manifests and immutable revision
//! identities, defines the runtime-neutral host/driver lifecycle, and projects
//! authority-selected manifest requests into exact activation-scoped typed
//! handles. It does not load code, compute policy or approval, choose restart
//! behavior, or persist plugin state.

mod capability;
mod driver;
mod identity;
mod manifest;
mod revision;

pub use capability::{
    CapabilityBroker, CapabilityBrokerError, CapabilityDefinition, CapabilityGrant,
    CapabilityGrantError, CapabilityHandle, CapabilityHandleError, CapabilityProviderRegistration,
    CapabilityRegistrationId, EffectiveCapabilityGrants, PluginStartContext,
};
pub use driver::{
    ActivatePlugin, DriverActivationError, DriverActivationErrorKind, DriverDeactivationError,
    DriverFuture, DriverHealthError, DriverPrepareError, DriverStartPermit, DriverStopPermit,
    HostPluginActivation, HostPluginActivationError, PluginActivationHandle, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginHealthError, PluginStartError,
    PreparedDriverActivation,
};
pub use identity::{
    CapabilityId, IdentityError, PackageDigest, PackageRelativePath, PluginPackageId,
    PluginVersion, SdkVersion, SdkVersionRequirement, ServiceContractId,
};
pub use manifest::{
    CapabilityRequest, DeclarationKind, DriverKind, MANIFEST_SCHEMA_VERSION, MAX_MANIFEST_BYTES,
    ManifestError, PLUGIN_MANIFEST_FILE, PluginEntrypoint, PluginManifest,
};
pub use revision::{PluginRevision, PluginRevisionId};
