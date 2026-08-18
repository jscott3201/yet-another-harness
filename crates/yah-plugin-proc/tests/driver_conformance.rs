//! The process driver against the reusable host-side lifecycle corpus.
//!
//! Every case spawns a real child process speaking real protocol bytes over
//! its fd-3 channel. The corpus proves host-facing lifecycle semantics: the
//! driver acquired, reported, and released one process correctly — including
//! the two failure shapes, a worker the host's version gate refuses and a
//! deactivation the plan scripts to fail after its real release.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::sync::Arc;

use fixtures::{case_digest, revision, worker_program};
use yah_plugin_host::{
    DriverConformanceCase, DriverConformanceProbe, DriverConformanceProbeError,
    DriverConformanceReport, DriverConformanceResourceState, DriverConformanceSetupError,
    DriverConformanceSubject, DriverConformanceTarget, DriverKind, PluginActivationId,
    run_driver_conformance, run_driver_conformance_case,
};
use yah_plugin_proc::{ProcActivationPlan, ProcObserver, ProcessPluginDriver, ResourceState};

struct ProcTarget;

impl DriverConformanceTarget for ProcTarget {
    fn name(&self) -> &str {
        "process-worker-driver"
    }

    fn kind(&self) -> DriverKind {
        DriverKind::NodeProcess
    }

    fn subject(
        &self,
        case: DriverConformanceCase,
    ) -> Result<DriverConformanceSubject, DriverConformanceSetupError> {
        let plans = match case {
            DriverConformanceCase::ReadyLifecycle => vec![ProcActivationPlan::ready()],
            DriverConformanceCase::PendingStartCancellation => {
                vec![ProcActivationPlan::pending_start()]
            }
            DriverConformanceCase::ReturnedStartFailure => {
                vec![ProcActivationPlan::start_failure()]
            }
            DriverConformanceCase::ReturnedDeactivationFailure => {
                vec![ProcActivationPlan::deactivation_failure()]
            }
            DriverConformanceCase::SharedDriverIsolation => {
                vec![ProcActivationPlan::ready(), ProcActivationPlan::ready()]
            }
            _ => {
                return Err(DriverConformanceSetupError::new(
                    "the process driver does not implement this future conformance case",
                ));
            }
        };
        let revision = revision(case, case_digest(case))?;
        let (driver, observer) = ProcessPluginDriver::scripted(
            revision.id().clone(),
            DriverKind::NodeProcess,
            worker_program(),
            plans,
        );
        let probe: Arc<dyn DriverConformanceProbe> = Arc::new(ProcProbe { observer });
        Ok(DriverConformanceSubject::new(revision, driver, probe))
    }
}

struct ProcProbe {
    observer: ProcObserver,
}

impl DriverConformanceProbe for ProcProbe {
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
async fn process_driver_passes_the_ordered_portable_suite() {
    let report: DriverConformanceReport = run_driver_conformance(&ProcTarget).await;
    assert!(report.is_conformant(), "{report}");
}

#[tokio::test]
async fn pending_start_cancellation_is_independently_runnable() {
    let result =
        run_driver_conformance_case(&ProcTarget, DriverConformanceCase::PendingStartCancellation)
            .await;
    assert!(result.passed(), "{:?}", result.failure());
}
