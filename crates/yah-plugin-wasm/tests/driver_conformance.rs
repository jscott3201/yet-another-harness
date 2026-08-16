//! The Wasmtime driver against the reusable host-side lifecycle corpus.
//!
//! Every case here drives a component Wasmtime actually compiled and
//! instantiated. The corpus proves host-facing lifecycle semantics, not guest
//! semantics: it says nothing about what the guest computed, only that the
//! driver acquired, reported, and released one activation correctly.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::{sync::Arc, time::Duration};

use fixtures::{case_digest, revision};
use yah_plugin_host::{
    DriverConformanceCase, DriverConformanceCaseResult, DriverConformanceProbe,
    DriverConformanceProbeError, DriverConformanceReport, DriverConformanceResourceState,
    DriverConformanceSetupError, DriverConformanceSubject, DriverConformanceTarget, DriverKind,
    PluginActivationId, run_driver_conformance, run_driver_conformance_case,
};
use yah_plugin_wasm::{
    ResourceState, WasmActivationPlan, WasmComponentDriver, WasmLimits, WasmObserver,
};

struct WasmTarget {
    name: &'static str,
    limits: WasmLimits,
    pending_start: WasmActivationPlan,
}

impl WasmTarget {
    /// The driver as a host builds it.
    fn standard() -> Self {
        Self {
            name: "wasmtime-component-driver",
            limits: WasmLimits::default(),
            pending_start: WasmActivationPlan::pending_start(),
        }
    }

    /// The same driver, against a guest that yields while it is starting.
    ///
    /// Instantiation is a guest call and a guest call yields at every epoch
    /// tick, so a start can hand the host back `Pending` before there is a
    /// store to observe. Forcing that by shortening the tick alone would be a
    /// race - a small component instantiates in microseconds - so the guest
    /// holds the fiber for milliseconds instead and the tick only has to be
    /// shorter than that.
    ///
    /// Only the yield is under test: the budget is effectively infinite so a
    /// loaded machine cannot fail this case for the unrelated reason that the
    /// call also ran out of deadline.
    fn yielding() -> Self {
        Self {
            name: "wasmtime-component-driver (yielding)",
            limits: WasmLimits {
                epoch_tick: Duration::from_micros(250),
                call_budget_ticks: u64::MAX,
                ..WasmLimits::default()
            },
            pending_start: WasmActivationPlan::slow_pending_start(),
        }
    }
}

impl DriverConformanceTarget for WasmTarget {
    fn name(&self) -> &str {
        self.name
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
            DriverConformanceCase::PendingStartCancellation => vec![self.pending_start],
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
        let (driver, observer) =
            WasmComponentDriver::scripted_with_limits(revision.id().clone(), plans, self.limits)
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
    let report: DriverConformanceReport = run_driver_conformance(&WasmTarget::standard()).await;
    assert!(report.is_conformant(), "{report}");
}

#[tokio::test]
async fn ready_lifecycle_is_independently_runnable() {
    let result = run_driver_conformance_case(
        &WasmTarget::standard(),
        DriverConformanceCase::ReadyLifecycle,
    )
    .await;
    assert!(result.passed(), "{:?}", result.failure());
}

#[tokio::test]
async fn pending_start_cancellation_is_independently_runnable() {
    let result = run_driver_conformance_case(
        &WasmTarget::standard(),
        DriverConformanceCase::PendingStartCancellation,
    )
    .await;
    assert!(result.passed(), "{:?}", result.failure());
}

/// A start that has acquired nothing by its first poll still conforms.
///
/// This is the flake that ran the case down: the pending-start case polled once
/// and then asserted the driver had a live store, which holds only when no
/// epoch tick lands inside instantiation. On an idle machine one rarely does,
/// so the suite passed; under load it did, about once in twenty runs, and the
/// case failed for a reason that had nothing to do with cancellation.
///
/// A guest that spends milliseconds in its start section makes the same path
/// happen on every run instead of on the unlucky ones, so this case fails
/// rather than quietly stops covering anything.
#[tokio::test]
async fn pending_start_conforms_when_instantiation_yields_before_it_acquires() {
    let result = run_driver_conformance_case(
        &WasmTarget::yielding(),
        DriverConformanceCase::PendingStartCancellation,
    )
    .await;
    let DriverConformanceCaseResult::Passed(report) = &result else {
        panic!("{:?}", result.failure());
    };
    // Assert the premise, not just the outcome. One pending poll to reach the
    // store and one for the resumed waiter is what a guest that does not yield
    // produces; anything beyond that is instantiation handing the thread back,
    // which is the whole reason this target exists. Without this the case would
    // pass by being an expensive duplicate of the one above on any machine
    // where the yield did not happen.
    let polls = report.activations()[0].start_pending_polls();
    assert!(
        polls > 2,
        "instantiation never yielded, so this case proved nothing the standard \
         target does not: {polls} pending start polls"
    );
}
