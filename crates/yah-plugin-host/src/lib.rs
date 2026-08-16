//! Runtime-neutral plugin package and driver contracts.
//!
//! This crate validates declarative package manifests and immutable revision
//! identities, and defines the runtime-neutral host/driver lifecycle. It does
//! not load code, grant capabilities, choose restart policy, or persist plugin
//! state. Those authority boundaries remain owned by later host layers.

mod driver;
mod identity;
mod manifest;
mod revision;

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
