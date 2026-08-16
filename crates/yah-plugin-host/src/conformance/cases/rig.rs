use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};

use crate::{
    CapabilityBroker, EffectiveCapabilityGrants, HostPluginActivation, PluginActivationId,
    driver::consume_panic_payload,
};

use super::{
    DriverActivationObservation, DriverConformanceCase, DriverConformanceCaseReport,
    DriverConformanceFailure, DriverConformancePhase, DriverConformanceProbe,
    DriverConformanceSubject, DriverConformanceTarget, DriverConformanceTeardown,
    ObservationHandle, observe_driver,
};

pub(super) struct CaseRig {
    target: String,
    kind: crate::DriverKind,
    case: DriverConformanceCase,
    pub(super) revision: crate::PluginRevision,
    driver: Mutex<Option<Arc<dyn crate::PluginDriver>>>,
    probe: Arc<dyn DriverConformanceProbe>,
    pub(super) observations: ObservationHandle,
}

impl CaseRig {
    pub(super) fn new(
        target: &dyn DriverConformanceTarget,
        case: DriverConformanceCase,
    ) -> Result<Self, DriverConformanceFailure> {
        let target_name = target.name().to_owned();
        let kind = target.kind();
        let DriverConformanceSubject {
            expected_revision,
            driver,
            probe,
        } = target.subject(case).map_err(|error| {
            DriverConformanceFailure::new(
                target_name.clone(),
                kind,
                case,
                DriverConformancePhase::Setup,
                format!("target setup failed: {error}"),
                vec![],
                DriverConformanceTeardown::NotStarted,
            )
        })?;
        let (driver, observations) = observe_driver(driver);
        let revision_kind = expected_revision.manifest().entrypoint().driver();
        if revision_kind != kind {
            let mut summary =
                format!("trusted revision selects {revision_kind:?}, but target declares {kind:?}");
            if let Some(drop_summary) = drop_untransferred_driver(driver) {
                summary.push_str("; untransferred driver destructor panicked: ");
                summary.push_str(&drop_summary);
            }
            return Err(DriverConformanceFailure::new(
                target_name,
                kind,
                case,
                DriverConformancePhase::Setup,
                summary,
                vec![],
                DriverConformanceTeardown::NotStarted,
            ));
        }
        Ok(Self {
            target: target_name,
            kind,
            case,
            revision: expected_revision,
            driver: Mutex::new(Some(driver)),
            probe,
            observations,
        })
    }

    pub(super) fn begin_component(
        &self,
        suffix: &str,
    ) -> Result<(ServiceRegistry, ComponentSlot, ProviderSelectionEpoch), DriverConformanceFailure>
    {
        let registry = ServiceRegistry::new();
        let mut slot =
            ComponentSlot::new(format!("conformance.{suffix}.slot")).map_err(|error| {
                self.failure(
                    DriverConformancePhase::Setup,
                    format!("component slot could not be created: {error}"),
                    &[],
                    DriverConformanceTeardown::NotStarted,
                )
            })?;
        let revision = ComponentRevision::new(
            format!("conformance.{suffix}.revision"),
            ComponentDefinition::new(format!("conformance.{suffix}.component")),
            Scope::root(format!("conformance.{suffix}.scope")),
        );
        let desired = DesiredComponentState::enabled(
            slot.generation(1),
            revision,
            ProviderAssignments::new(),
        );
        let outcome = slot.reconcile(&registry, desired).map_err(|error| {
            self.failure(
                DriverConformancePhase::Setup,
                format!("component could not begin start: {error}"),
                &[],
                DriverConformanceTeardown::NotStarted,
            )
        })?;
        let epoch = match outcome {
            ComponentSlotOutcome::Mounted {
                component: ReconcileOutcome::StartBegun { selection },
                ..
            } => selection.epoch(),
            other => {
                return Err(self.failure(
                    DriverConformancePhase::Setup,
                    format!("fresh component did not begin start: {other:?}"),
                    &[],
                    DriverConformanceTeardown::NotStarted,
                ));
            }
        };
        Ok((registry, slot, epoch))
    }

    pub(super) fn prepare<'slot>(
        &self,
        slot: &'slot mut ComponentSlot,
        epoch: ProviderSelectionEpoch,
        broker: &CapabilityBroker,
    ) -> Result<HostPluginActivation<'slot>, DriverConformanceFailure> {
        let driver = self.take_driver()?;
        self.prepare_with_driver(slot, epoch, broker, driver)
    }

    pub(super) fn prepare_with_driver<'slot>(
        &self,
        slot: &'slot mut ComponentSlot,
        epoch: ProviderSelectionEpoch,
        broker: &CapabilityBroker,
        driver: Arc<dyn crate::PluginDriver>,
    ) -> Result<HostPluginActivation<'slot>, DriverConformanceFailure> {
        let expected = PluginActivationId::new(self.revision.id().clone(), epoch);
        let grants = EffectiveCapabilityGrants::empty(&self.revision);
        HostPluginActivation::prepare(slot, epoch, broker, &grants, driver).map_err(|error| {
            self.failure(
                DriverConformancePhase::Preparation,
                format!("host rejected driver preparation: {error}"),
                &[expected],
                DriverConformanceTeardown::NotStarted,
            )
        })
    }

    pub(super) fn clone_driver(
        &self,
    ) -> Result<Arc<dyn crate::PluginDriver>, DriverConformanceFailure> {
        self.driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| {
                self.failure(
                    DriverConformancePhase::Setup,
                    "conformance subject driver was already transferred",
                    &[],
                    DriverConformanceTeardown::NotStarted,
                )
            })
    }

    pub(super) fn take_driver(
        &self,
    ) -> Result<Arc<dyn crate::PluginDriver>, DriverConformanceFailure> {
        self.driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                self.failure(
                    DriverConformancePhase::Setup,
                    "conformance subject driver was already transferred",
                    &[],
                    DriverConformanceTeardown::NotStarted,
                )
            })
    }

    pub(super) fn observation(
        &self,
        activation: &PluginActivationId,
        teardown: DriverConformanceTeardown,
    ) -> Result<DriverActivationObservation, DriverConformanceFailure> {
        let resource = self.probe.resource_state(activation).map_err(|error| {
            let probe_error = error.to_string();
            DriverConformanceFailure::new(
                self.target.clone(),
                self.kind,
                self.case,
                DriverConformancePhase::Probe,
                format!("fixture resource probe failed: {probe_error}"),
                vec![
                    self.observations
                        .snapshot(activation, None, Some(probe_error)),
                ],
                teardown,
            )
        })?;
        Ok(self.observations.snapshot(activation, Some(resource), None))
    }

    pub(super) fn failure(
        &self,
        phase: DriverConformancePhase,
        summary: impl Into<String>,
        activations: &[PluginActivationId],
        teardown: DriverConformanceTeardown,
    ) -> DriverConformanceFailure {
        let observations = activations
            .iter()
            .map(|activation| {
                let (resource, probe_error) = match self.probe.resource_state(activation) {
                    Ok(resource) => (Some(resource), None),
                    Err(error) => (None, Some(error.to_string())),
                };
                self.observations
                    .snapshot(activation, resource, probe_error)
            })
            .collect();
        DriverConformanceFailure::new(
            self.target.clone(),
            self.kind,
            self.case,
            phase,
            summary,
            observations,
            teardown,
        )
    }

    pub(super) fn finalized_failure(
        &self,
        primary: &DriverConformanceFailure,
        summary: impl Into<String>,
        activations: &[PluginActivationId],
        teardown: DriverConformanceTeardown,
    ) -> DriverConformanceFailure {
        let observations = activations
            .iter()
            .map(|activation| {
                let retained_probe_error = primary
                    .observations()
                    .iter()
                    .find(|observation| observation.activation() == activation)
                    .and_then(DriverActivationObservation::resource_probe_error)
                    .map(str::to_owned);
                let (resource, probe_error) = match retained_probe_error {
                    Some(error) => (None, Some(error)),
                    None => match self.probe.resource_state(activation) {
                        Ok(resource) => (Some(resource), None),
                        Err(error) => (None, Some(error.to_string())),
                    },
                };
                self.observations
                    .snapshot(activation, resource, probe_error)
            })
            .collect();
        DriverConformanceFailure::new(
            self.target.clone(),
            self.kind,
            self.case,
            primary.phase(),
            summary,
            observations,
            teardown,
        )
    }

    pub(super) fn report(
        &self,
        activations: Vec<DriverActivationObservation>,
        teardown: DriverConformanceTeardown,
    ) -> DriverConformanceCaseReport {
        DriverConformanceCaseReport::new(self.case, activations, teardown)
    }
}

impl Drop for CaseRig {
    fn drop(&mut self) {
        let driver = self
            .driver
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(driver) = driver {
            let _ = drop_untransferred_driver(driver);
        }
    }
}

fn drop_untransferred_driver(driver: Arc<dyn crate::PluginDriver>) -> Option<String> {
    catch_unwind(AssertUnwindSafe(|| drop(driver)))
        .err()
        .map(consume_panic_payload)
}
