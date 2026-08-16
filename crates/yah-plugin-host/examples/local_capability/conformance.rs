use std::sync::Arc;

use yah_plugin_host::{
    DriverConformanceCase, DriverConformanceProbe, DriverConformanceProbeError,
    DriverConformanceReport, DriverConformanceResourceState, DriverConformanceSetupError,
    DriverConformanceSubject, DriverConformanceTarget, DriverKind, PluginActivationId,
    run_driver_conformance,
};

use crate::{
    contract::example_revision,
    driver::{ActivationPlan, LocalCapabilityDriver, ResourceObserver, ResourceState},
};

pub struct LocalCapabilityTarget;

impl DriverConformanceTarget for LocalCapabilityTarget {
    fn name(&self) -> &str {
        "local-capability-builtin-driver"
    }

    fn kind(&self) -> DriverKind {
        DriverKind::BuiltinRust
    }

    fn subject(
        &self,
        case: DriverConformanceCase,
    ) -> Result<DriverConformanceSubject, DriverConformanceSetupError> {
        let revision =
            example_revision(case_digest(case)).map_err(DriverConformanceSetupError::new)?;
        let plans = match case {
            DriverConformanceCase::ReadyLifecycle => vec![ActivationPlan::ready()],
            DriverConformanceCase::PendingStartCancellation => {
                vec![ActivationPlan::pending_start()]
            }
            DriverConformanceCase::ReturnedStartFailure => {
                vec![ActivationPlan::start_failure()]
            }
            DriverConformanceCase::ReturnedDeactivationFailure => {
                vec![ActivationPlan::deactivation_failure()]
            }
            DriverConformanceCase::SharedDriverIsolation => {
                vec![ActivationPlan::ready(), ActivationPlan::ready()]
            }
            _ => {
                return Err(DriverConformanceSetupError::new(
                    "local example does not implement this future conformance case",
                ));
            }
        };
        let (driver, observer) = LocalCapabilityDriver::scripted(revision.id().clone(), plans);
        let probe: Arc<dyn DriverConformanceProbe> = Arc::new(LocalCapabilityProbe {
            resources: observer.resource_observer(),
        });
        Ok(DriverConformanceSubject::new(revision, driver, probe))
    }
}

pub async fn run_local_conformance() -> DriverConformanceReport {
    run_driver_conformance(&LocalCapabilityTarget).await
}

struct LocalCapabilityProbe {
    resources: ResourceObserver,
}

impl DriverConformanceProbe for LocalCapabilityProbe {
    fn resource_state(
        &self,
        activation: &PluginActivationId,
    ) -> Result<DriverConformanceResourceState, DriverConformanceProbeError> {
        self.resources
            .resource_state(activation)
            .map(|state| match state {
                ResourceState::NotAcquired => DriverConformanceResourceState::NotAcquired,
                ResourceState::Live => DriverConformanceResourceState::Live,
                ResourceState::Released => DriverConformanceResourceState::Released,
            })
            .map_err(DriverConformanceProbeError::new)
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
