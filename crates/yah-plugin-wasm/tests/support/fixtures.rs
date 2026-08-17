//! Shared fixture identity for the wasm driver's test binaries.
//!
//! Each binary uses a different subset, so unused items here are expected.
//!
//! The manifest names a `wasm-component` entrypoint the driver never resolves:
//! nothing here loads a package, and the path exists only so the fixture is a
//! well-formed revision.

#![allow(dead_code)]

use yah_plugin_host::{
    CapabilityId, CapabilityRequest, DriverConformanceCase, DriverConformanceSetupError,
    PackageDigest, PackageRelativePath, PluginEntrypoint, PluginManifest, PluginPackageId,
    PluginRevision, PluginVersion, SdkVersionRequirement,
};

pub(super) fn revision(
    case: DriverConformanceCase,
    digest: char,
) -> Result<PluginRevision, DriverConformanceSetupError> {
    revision_requesting(case, digest, &[])
}

/// The same fixture identity, with capability requests in its manifest.
///
/// Effective grants admit only requested capability IDs, so a fixture that is
/// to be granted anything must say so here first - request-only, as a real
/// manifest would.
pub(super) fn revision_requesting(
    case: DriverConformanceCase,
    digest: char,
    capabilities: &[&str],
) -> Result<PluginRevision, DriverConformanceSetupError> {
    let setup = |error: String| DriverConformanceSetupError::new(error);
    let package = PluginPackageId::new(format!("test.wasm.{}", package_suffix(case)))
        .map_err(|error| setup(format!("fixture package ID is invalid: {error}")))?;
    let requested = capabilities
        .iter()
        .map(|capability| {
            CapabilityId::new(*capability)
                .map(CapabilityRequest::new)
                .map_err(|error| setup(format!("fixture capability request is invalid: {error}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let manifest = PluginManifest::new(
        package,
        PluginVersion::new("1.0.0")
            .map_err(|error| setup(format!("fixture version is invalid: {error}")))?,
        SdkVersionRequirement::new(">=0.1.0, <0.2.0")
            .map_err(|error| setup(format!("fixture SDK requirement is invalid: {error}")))?,
        PluginEntrypoint::WasmComponent {
            path: PackageRelativePath::new("plugin.wasm")
                .map_err(|error| setup(format!("fixture entrypoint path is invalid: {error}")))?,
        },
        vec![],
        vec![],
        requested,
    )
    .map_err(|error| setup(format!("fixture manifest is invalid: {error}")))?;
    let digest = PackageDigest::new(format!("blake3:{}", digest.to_string().repeat(64)))
        .map_err(|error| setup(format!("fixture digest is invalid: {error}")))?;
    Ok(PluginRevision::new(manifest, digest))
}

pub(super) fn package_suffix(case: DriverConformanceCase) -> &'static str {
    match case {
        DriverConformanceCase::ReadyLifecycle => "ready",
        DriverConformanceCase::PendingStartCancellation => "pending",
        DriverConformanceCase::ReturnedStartFailure => "start.failure",
        DriverConformanceCase::ReturnedDeactivationFailure => "stop.failure",
        DriverConformanceCase::SharedDriverIsolation => "shared",
        _ => "unsupported",
    }
}

pub(super) fn case_digest(case: DriverConformanceCase) -> char {
    match case {
        DriverConformanceCase::ReadyLifecycle => '1',
        DriverConformanceCase::PendingStartCancellation => '2',
        DriverConformanceCase::ReturnedStartFailure => '3',
        DriverConformanceCase::ReturnedDeactivationFailure => '4',
        DriverConformanceCase::SharedDriverIsolation => '5',
        _ => 'f',
    }
}
