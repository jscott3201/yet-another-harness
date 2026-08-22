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

pub(super) fn capability_revision(name: &str, digest: char, capability: &str) -> PluginRevision {
    revision_with_capabilities(
        &format!("test.proc.{name}"),
        digest,
        vec![CapabilityRequest::new(
            CapabilityId::new(capability).expect("fixture capability id is valid"),
        )],
    )
    .expect("capability fixture is valid")
}

fn named_revision(package: &str, digest: char) -> Result<PluginRevision, String> {
    revision_with_capabilities(package, digest, Vec::new())
}

fn revision_with_capabilities(
    package: &str,
    digest: char,
    capabilities: Vec<CapabilityRequest>,
) -> Result<PluginRevision, String> {
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
        capabilities,
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
