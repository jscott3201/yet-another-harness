//! Shared fixture identity for the process driver's test binaries.
//!
//! Each binary uses a different subset, so unused items here are expected.
//!
//! The manifest names a `node-process` entrypoint the driver never resolves:
//! nothing here loads a package, and the path exists only so the fixture is
//! a well-formed revision. The program the tests actually spawn is the
//! compiled fake worker, reached through `CARGO_BIN_EXE_fake-worker`.

#![allow(dead_code)]

use std::path::PathBuf;

use yah_plugin_host::{
    CapabilityId, CapabilityRequest, DriverConformanceCase, DriverConformanceSetupError,
    PackageDigest, PackageRelativePath, PluginEntrypoint, PluginManifest, PluginPackageId,
    PluginRevision, PluginVersion, SdkVersionRequirement,
};

/// The compiled fake worker, built by cargo beside this test binary.
pub(super) fn worker_program() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_fake-worker"))
}

pub(super) fn revision(
    case: DriverConformanceCase,
    digest: char,
) -> Result<PluginRevision, DriverConformanceSetupError> {
    named_revision(&format!("test.proc.{}", package_suffix(case)), digest)
        .map_err(DriverConformanceSetupError::new)
}

/// A revision for the lifecycle tests, which are not case-keyed.
pub(super) fn lifecycle_revision(name: &str, digest: char) -> PluginRevision {
    named_revision(&format!("test.proc.{name}"), digest).expect("lifecycle fixture is valid")
}

/// The same, with capability requests in the manifest. Effective grants
/// admit only requested IDs, so a fixture that is to be granted anything
/// must say so here first.
pub(super) fn lifecycle_revision_requesting(
    name: &str,
    digest: char,
    capabilities: &[&str],
) -> PluginRevision {
    let requested = capabilities
        .iter()
        .map(|capability| {
            CapabilityRequest::new(CapabilityId::new(*capability).expect("fixture capability ID"))
        })
        .collect();
    let package =
        PluginPackageId::new(format!("test.proc.{name}")).expect("fixture package ID is valid");
    let manifest = PluginManifest::new(
        package,
        PluginVersion::new("1.0.0").expect("fixture version is valid"),
        SdkVersionRequirement::new(">=0.1.0, <0.2.0").expect("fixture SDK requirement is valid"),
        PluginEntrypoint::NodeProcess {
            path: PackageRelativePath::new("worker.mjs").expect("fixture path is valid"),
        },
        vec![],
        vec![],
        requested,
    )
    .expect("fixture manifest is valid");
    let digest = PackageDigest::new(format!("blake3:{}", digest.to_string().repeat(64)))
        .expect("fixture digest is valid");
    PluginRevision::new(manifest, digest)
}

fn named_revision(package: &str, digest: char) -> Result<PluginRevision, String> {
    let package = PluginPackageId::new(package)
        .map_err(|error| format!("fixture package ID is invalid: {error}"))?;
    let manifest = PluginManifest::new(
        package,
        PluginVersion::new("1.0.0").map_err(|error| format!("fixture version invalid: {error}"))?,
        SdkVersionRequirement::new(">=0.1.0, <0.2.0")
            .map_err(|error| format!("fixture SDK requirement invalid: {error}"))?,
        PluginEntrypoint::NodeProcess {
            path: PackageRelativePath::new("worker.mjs")
                .map_err(|error| format!("fixture entrypoint path invalid: {error}"))?,
        },
        vec![],
        vec![],
        vec![],
    )
    .map_err(|error| format!("fixture manifest invalid: {error}"))?;
    let digest = PackageDigest::new(format!("blake3:{}", digest.to_string().repeat(64)))
        .map_err(|error| format!("fixture digest invalid: {error}"))?;
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
