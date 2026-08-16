//! The Wasmtime driver against the reusable host-side lifecycle corpus.
//!
//! Every case here drives a component Wasmtime actually compiled and
//! instantiated. The corpus proves host-facing lifecycle semantics, not guest
//! semantics: it says nothing about what the guest computed, only that the
//! driver acquired, reported, and released one activation correctly.

use std::sync::Arc;

use yah_plugin_host::{
    DriverConformanceCase, DriverConformanceProbe, DriverConformanceProbeError,
    DriverConformanceReport, DriverConformanceResourceState, DriverConformanceSetupError,
    DriverConformanceSubject, DriverConformanceTarget, DriverKind, PackageDigest,
    PackageRelativePath, PluginActivationId, PluginEntrypoint, PluginManifest, PluginPackageId,
    PluginRevision, PluginVersion, SdkVersionRequirement, run_driver_conformance,
    run_driver_conformance_case,
};
use yah_plugin_wasm::{ResourceState, WasmActivationPlan, WasmComponentDriver, WasmObserver};

struct WasmTarget;

impl DriverConformanceTarget for WasmTarget {
    fn name(&self) -> &str {
        "wasmtime-component-driver"
    }

    fn kind(&self) -> DriverKind {
        DriverKind::WasmComponent
    }

    fn subject(
        &self,
        case: DriverConformanceCase,
    ) -> Result<DriverConformanceSubject, DriverConformanceSetupError> {
        let plans = match case {
            DriverConformanceCase::ReadyLifecycle => vec![WasmActivationPlan::ready()],
            DriverConformanceCase::PendingStartCancellation => {
                vec![WasmActivationPlan::pending_start()]
            }
            DriverConformanceCase::ReturnedStartFailure => {
                vec![WasmActivationPlan::start_failure()]
            }
            DriverConformanceCase::ReturnedDeactivationFailure => {
                vec![WasmActivationPlan::deactivation_failure()]
            }
            DriverConformanceCase::SharedDriverIsolation => {
                vec![WasmActivationPlan::ready(), WasmActivationPlan::ready()]
            }
            _ => {
                return Err(DriverConformanceSetupError::new(
                    "the wasm driver does not implement this future conformance case",
                ));
            }
        };
        let revision = revision(case, case_digest(case))?;
        let (driver, observer) = WasmComponentDriver::scripted(revision.id().clone(), plans)
            .map_err(|error| {
                DriverConformanceSetupError::new(format!("wasm driver did not build: {error}"))
            })?;
        let probe: Arc<dyn DriverConformanceProbe> = Arc::new(WasmProbe { observer });
        Ok(DriverConformanceSubject::new(revision, driver, probe))
    }
}

struct WasmProbe {
    observer: WasmObserver,
}

impl DriverConformanceProbe for WasmProbe {
    fn resource_state(
        &self,
        activation: &PluginActivationId,
    ) -> Result<DriverConformanceResourceState, DriverConformanceProbeError> {
        self.observer
            .resource_state(activation)
            .map(|state| match state {
                ResourceState::NotAcquired => DriverConformanceResourceState::NotAcquired,
                ResourceState::Live => DriverConformanceResourceState::Live,
                ResourceState::Released => DriverConformanceResourceState::Released,
            })
            .map_err(DriverConformanceProbeError::new)
    }
}

fn revision(
    case: DriverConformanceCase,
    digest: char,
) -> Result<PluginRevision, DriverConformanceSetupError> {
    let setup = |error: String| DriverConformanceSetupError::new(error);
    let package = PluginPackageId::new(format!("test.wasm.{}", package_suffix(case)))
        .map_err(|error| setup(format!("fixture package ID is invalid: {error}")))?;
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
        vec![],
    )
    .map_err(|error| setup(format!("fixture manifest is invalid: {error}")))?;
    let digest = PackageDigest::new(format!("blake3:{}", digest.to_string().repeat(64)))
        .map_err(|error| setup(format!("fixture digest is invalid: {error}")))?;
    Ok(PluginRevision::new(manifest, digest))
}

fn package_suffix(case: DriverConformanceCase) -> &'static str {
    match case {
        DriverConformanceCase::ReadyLifecycle => "ready",
        DriverConformanceCase::PendingStartCancellation => "pending",
        DriverConformanceCase::ReturnedStartFailure => "start.failure",
        DriverConformanceCase::ReturnedDeactivationFailure => "stop.failure",
        DriverConformanceCase::SharedDriverIsolation => "shared",
        _ => "unsupported",
    }
}

fn case_digest(case: DriverConformanceCase) -> char {
    match case {
        DriverConformanceCase::ReadyLifecycle => '1',
        DriverConformanceCase::PendingStartCancellation => '2',
        DriverConformanceCase::ReturnedStartFailure => '3',
        DriverConformanceCase::ReturnedDeactivationFailure => '4',
        DriverConformanceCase::SharedDriverIsolation => '5',
        _ => 'f',
    }
}

#[tokio::test]
async fn wasm_component_driver_passes_the_ordered_portable_suite() {
    let report: DriverConformanceReport = run_driver_conformance(&WasmTarget).await;
    assert!(report.is_conformant(), "{report}");
}

#[tokio::test]
async fn ready_lifecycle_is_independently_runnable() {
    let result =
        run_driver_conformance_case(&WasmTarget, DriverConformanceCase::ReadyLifecycle).await;
    assert!(result.passed(), "{:?}", result.failure());
}

#[tokio::test]
async fn pending_start_cancellation_is_independently_runnable() {
    let result =
        run_driver_conformance_case(&WasmTarget, DriverConformanceCase::PendingStartCancellation)
            .await;
    assert!(result.passed(), "{:?}", result.failure());
}
