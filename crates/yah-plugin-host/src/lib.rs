//! Runtime-neutral plugin package vocabulary.
//!
//! This crate validates declarative package manifests and immutable revision
//! identities. It intentionally does not load code, grant capabilities, run a
//! driver, or persist plugin state. Those authority boundaries remain owned by
//! later host layers.

mod identity;
mod manifest;
mod revision;

pub use identity::{
    CapabilityId, IdentityError, PackageDigest, PackageRelativePath, PluginPackageId,
    PluginVersion, SdkVersion, SdkVersionRequirement, ServiceContractId,
};
pub use manifest::{
    CapabilityRequest, DeclarationKind, DriverKind, MANIFEST_SCHEMA_VERSION, MAX_MANIFEST_BYTES,
    ManifestError, PLUGIN_MANIFEST_FILE, PluginEntrypoint, PluginManifest,
};
pub use revision::{PluginRevision, PluginRevisionId};
