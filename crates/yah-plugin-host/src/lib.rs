//! Runtime-neutral plugin package and driver contracts.
//!
//! This crate validates declarative package manifests and immutable revision
//! identities, defines the runtime-neutral host/driver lifecycle, and projects
//! authority-selected manifest requests into exact activation-scoped typed
//! handles. Its reusable host-side conformance runner tests those driver
//! lifecycle boundaries without choosing an executor. It does not load code,
//! compute policy or approval, choose restart behavior, or persist plugin
//! state.

mod capability;
pub mod conformance;
mod driver;
mod identity;
mod manifest;
mod revision;

pub use capability::{
    CapabilityBroker, CapabilityBrokerError, CapabilityDefinition, CapabilityGrant,
    CapabilityGrantError, CapabilityHandle, CapabilityHandleError, CapabilityProviderRegistration,
    CapabilityRegistrationId, EffectiveCapabilityGrants, PluginStartContext, TextCapability,
    TextCapabilityFailure, TextCapabilityFailureCode,
};
pub use conformance::{
    DriverActivationObservation, DriverConformanceCase, DriverConformanceCaseReport,
    DriverConformanceCaseResult, DriverConformanceFailure, DriverConformancePhase,
    DriverConformanceProbe, DriverConformanceProbeError, DriverConformanceReport,
    DriverConformanceResourceState, DriverConformanceSetupError, DriverConformanceSubject,
    DriverConformanceTarget, DriverConformanceTeardown, DriverConformanceTerminal,
    run_driver_conformance, run_driver_conformance_case,
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
