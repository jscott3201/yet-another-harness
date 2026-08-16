//! The Wasmtime driver against the reusable host-side lifecycle corpus.
//!
//! Every case here drives a component Wasmtime actually compiled and
//! instantiated. The corpus proves host-facing lifecycle semantics, not guest
//! semantics: it says nothing about what the guest computed, only that the
//! driver acquired, reported, and released one activation correctly.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::sync::Arc;

use fixtures::{case_digest, revision};
use yah_plugin_host::{
    DriverConformanceCase, DriverConformanceProbe, DriverConformanceProbeError,
    DriverConformanceReport, DriverConformanceResourceState, DriverConformanceSetupError,
    DriverConformanceSubject, DriverConformanceTarget, DriverKind, PluginActivationId,
    run_driver_conformance, run_driver_conformance_case,
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
