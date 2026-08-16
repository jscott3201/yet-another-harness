mod conformance;
mod contract;
mod driver;

use std::sync::{Arc, atomic::Ordering};

use contract::{Greeting, example_revision};
use driver::{ActivationPlan, ExampleObserver, LocalCapabilityDriver, ResourceState};
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotError,
    ComponentSlotOutcome, DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch,
    ReconcileOutcome, Scope, ServiceRegistry, StopDisposition, StopRecord,
};
use yah_plugin_host::{
    CapabilityBroker, CapabilityDefinition, CapabilityGrantError, CapabilityHandle,
    CapabilityHandleError, CapabilityId, EffectiveCapabilityGrants, HostPluginActivation,
    PluginActivationId, PluginHealth, PluginRevision,
};

#[derive(Debug, PartialEq, Eq)]
struct DemoReport {
    denied_without_grant: bool,
    first_greeting: String,
    replacement_greeting: String,
    provider_calls: [usize; 3],
    clean_deactivations: usize,
    stale_handle_rejected: bool,
}

struct ActiveDemo {
    id: PluginActivationId,
    epoch: ProviderSelectionEpoch,
    observer: ExampleObserver,
    greeting: Option<String>,
    handle: Option<CapabilityHandle<dyn Greeting>>,
}

#[derive(Clone, Copy)]
enum GrantExpectation {
    Denied,
    Granted,
}

enum SlotRecovery {
    None,
    SealAndFinish,
    Finish,
}

struct ActivationAttemptError {
    summary: String,
    recovery: SlotRecovery,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match run_example().await {
        Ok((demo, conformance)) => {
            println!(
                "local capability: {:?} -> {:?}; {} lifecycle cases passed",
                demo.first_greeting,
                demo.replacement_greeting,
                conformance.cases().len()
            );
        }
        Err(error) => {
            eprintln!("local capability example failed: {error}");
            std::process::exit(1);
        }
    }
}

async fn run_example() -> Result<(DemoReport, yah_plugin_host::DriverConformanceReport), String> {
    let demo = run_capability_demo().await?;
    let report = conformance::run_local_conformance().await;
    if !report.is_conformant() {
        return Err(report.to_string());
    }
    Ok((demo, report))
}

async fn run_capability_demo() -> Result<DemoReport, String> {
    let revision = example_revision('a')?;
    contract::validate_manifest(&revision)?;
    let registry = ServiceRegistry::new();
    let component = component_revision();

    let mut broker = CapabilityBroker::new().map_err(|error| error.to_string())?;
    prove_unrequested_grant_is_rejected(&revision, &mut broker)?;

    let mut denied_slot = ComponentSlot::new("example.local-capability.denied-slot")
        .map_err(|error| error.to_string())?;
    let denied = activate_once(
        &mut denied_slot,
        &registry,
        &broker,
        &EffectiveCapabilityGrants::empty(&revision),
        &component,
        1,
        GrantExpectation::Denied,
        ActivationPlan::ready(),
    )
    .await?;
    stop_once(&mut denied_slot, &registry, 2, &denied).await?;

    let definition = contract::greeting_definition();
    let (first_provider, first_calls) = contract::provider("Hello");
    let first_registration = broker
        .register(&definition, first_provider)
        .map_err(|error| error.to_string())?;
    let first_grants = EffectiveCapabilityGrants::new(&revision, [first_registration.grant()])
        .map_err(|error| error.to_string())?;

    let mut slot =
        ComponentSlot::new("example.local-capability.slot").map_err(|error| error.to_string())?;
    let first = activate_once(
        &mut slot,
        &registry,
        &broker,
        &first_grants,
        &component,
        1,
        GrantExpectation::Granted,
        ActivationPlan::ready(),
    )
    .await?;
    let first_evidence = first.greeting.clone().zip(first.handle.clone());
    stop_once(&mut slot, &registry, 2, &first).await?;
    let (first_greeting, first_handle) = first_evidence.ok_or_else(|| {
        "granted activation produced incomplete evidence after cleanup".to_owned()
    })?;
    if !matches!(
        first_handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ) {
        return Err("stopped activation retained usable capability authority".to_owned());
    }
    drop(first_registration.withdraw());

    let (probe_provider, probe_calls) = contract::provider("Before replacement");
    let probe_registration = broker
        .register(&definition, probe_provider)
        .map_err(|error| error.to_string())?;
    let probe_grants = EffectiveCapabilityGrants::new(&revision, [probe_registration.grant()])
        .map_err(|error| error.to_string())?;
    let probe = activate_once(
        &mut slot,
        &registry,
        &broker,
        &probe_grants,
        &component,
        3,
        GrantExpectation::Granted,
        ActivationPlan::ready(),
    )
    .await?;
    let Some(probe_handle) = probe.handle.clone() else {
        stop_once(&mut slot, &registry, 4, &probe).await?;
        return Err("replacement probe retained no mediated handle".to_owned());
    };
    let probe_registration_id = probe_handle.registration_id();
    drop(probe_registration.withdraw());

    let (replacement_provider, replacement_calls) = contract::provider("Welcome");
    let replacement_registration = match broker.register(&definition, replacement_provider) {
        Ok(registration) => registration,
        Err(error) => {
            stop_once(&mut slot, &registry, 4, &probe).await?;
            return Err(format!("replacement provider registration failed: {error}"));
        }
    };
    let replacement_failure = if replacement_registration.id() == probe_registration_id {
        Some("replacement provider reused an exact registration identity".to_owned())
    } else if !matches!(
        probe_handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ) {
        Some("provider replacement retargeted a live activation's stale handle".to_owned())
    } else {
        None
    };
    stop_once(&mut slot, &registry, 4, &probe).await?;
    if let Some(error) = replacement_failure {
        drop(replacement_registration.withdraw());
        return Err(error);
    }

    let replacement_grants =
        EffectiveCapabilityGrants::new(&revision, [replacement_registration.grant()])
            .map_err(|error| error.to_string())?;
    let replacement = activate_once(
        &mut slot,
        &registry,
        &broker,
        &replacement_grants,
        &component,
        5,
        GrantExpectation::Granted,
        ActivationPlan::ready(),
    )
    .await?;
    let replacement_greeting = replacement.greeting.clone();
    let stale_handle_failure = if !matches!(
        probe_handle.try_with(|_| ()),
        Err(CapabilityHandleError::Revoked { .. })
    ) {
        Some("provider replacement retargeted a stale handle".to_owned())
    } else {
        None
    };
    stop_once(&mut slot, &registry, 6, &replacement).await?;
    drop(replacement_registration.withdraw());
    if let Some(error) = stale_handle_failure {
        return Err(error);
    }
    let replacement_greeting = replacement_greeting.ok_or_else(|| {
        "replacement activation produced incomplete evidence after cleanup".to_owned()
    })?;

    Ok(DemoReport {
        denied_without_grant: true,
        first_greeting,
        replacement_greeting,
        provider_calls: [
            first_calls.load(Ordering::Acquire),
            probe_calls.load(Ordering::Acquire),
            replacement_calls.load(Ordering::Acquire),
        ],
        clean_deactivations: denied.observer.deactivation_calls(&denied.id)
            + first.observer.deactivation_calls(&first.id)
            + probe.observer.deactivation_calls(&probe.id)
            + replacement.observer.deactivation_calls(&replacement.id),
        stale_handle_rejected: true,
    })
}

#[allow(clippy::too_many_arguments)]
async fn activate_once(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    broker: &CapabilityBroker,
    grants: &EffectiveCapabilityGrants,
    component: &ComponentRevision,
    generation: u64,
    expectation: GrantExpectation,
    plan: ActivationPlan,
) -> Result<ActiveDemo, String> {
    let (driver, observer) = LocalCapabilityDriver::scripted(grants.revision_id().clone(), [plan]);
    activate_scripted(
        slot,
        registry,
        broker,
        grants,
        component,
        generation,
        driver,
        observer,
        expectation,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn activate_scripted(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    broker: &CapabilityBroker,
    grants: &EffectiveCapabilityGrants,
    component: &ComponentRevision,
    generation: u64,
    driver: Arc<dyn yah_plugin_host::PluginDriver>,
    observer: ExampleObserver,
    expectation: GrantExpectation,
) -> Result<ActiveDemo, String> {
    let epoch = begin_start(slot, registry, component, generation)?;
    match activate_with_driver(
        slot,
        registry,
        broker,
        grants,
        epoch,
        driver,
        observer,
        expectation,
    )
    .await
    {
        Ok(active) => Ok(active),
        Err(error) => {
            let seal = match error.recovery {
                SlotRecovery::SealAndFinish => slot
                    .fail_activation(epoch, error.summary.clone())
                    .err()
                    .map(|failure| failure.to_string()),
                SlotRecovery::None | SlotRecovery::Finish => None,
            };
            let cleanup = match error.recovery {
                SlotRecovery::None => return Err(error.summary),
                SlotRecovery::SealAndFinish | SlotRecovery::Finish => slot.finish_stop(epoch).await,
            };
            Err(describe_cleanup(error.summary, seal, cleanup))
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn activate_with_driver(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    broker: &CapabilityBroker,
    grants: &EffectiveCapabilityGrants,
    epoch: ProviderSelectionEpoch,
    driver: Arc<dyn yah_plugin_host::PluginDriver>,
    observer: ExampleObserver,
    expectation: GrantExpectation,
) -> Result<ActiveDemo, ActivationAttemptError> {
    let prepared = HostPluginActivation::prepare(slot, epoch, broker, grants, driver);
    let mut activation = match prepared {
        Ok(activation) => activation,
        Err(error) => {
            return Err(ActivationAttemptError {
                summary: format!("local driver preparation failed: {error}"),
                recovery: SlotRecovery::SealAndFinish,
            });
        }
    };
    let id = activation.id().clone();
    let inert = observer.resource_state(&id).and_then(|state| {
        if state == ResourceState::NotAcquired && observer.greeting(&id).is_none() {
            Ok(())
        } else {
            Err("driver preparation acquired an activation resource".to_owned())
        }
    });
    if let Err(primary) = inert {
        return Err(ActivationAttemptError {
            summary: settle_guard(&mut activation, primary, true).await,
            recovery: SlotRecovery::None,
        });
    }
    let health = match activation.activate(registry).await {
        Ok(health) => health,
        Err(error) => {
            return Err(ActivationAttemptError {
                summary: settle_guard(
                    &mut activation,
                    format!("local driver start failed: {error}"),
                    false,
                )
                .await,
                recovery: SlotRecovery::None,
            });
        }
    };
    match health.health() {
        Ok(PluginHealth::Healthy) => {}
        Ok(other) => {
            return Err(ActivationAttemptError {
                summary: settle_guard(
                    &mut activation,
                    format!("activated local driver reported {other:?}"),
                    true,
                )
                .await,
                recovery: SlotRecovery::None,
            });
        }
        Err(error) => {
            return Err(ActivationAttemptError {
                summary: settle_guard(
                    &mut activation,
                    format!("local driver health failed: {error}"),
                    true,
                )
                .await,
                recovery: SlotRecovery::None,
            });
        }
    }
    if let Err(primary) = validate_grant_evidence(&observer, &id, expectation) {
        return Err(ActivationAttemptError {
            summary: settle_guard(&mut activation, primary, true).await,
            recovery: SlotRecovery::None,
        });
    }
    let greeting = observer.greeting(&id);
    let handle = observer.retained_handle(&id);
    match activation.release_active() {
        Ok((_, released_health)) => drop(released_health),
        Err(error) => {
            return Err(ActivationAttemptError {
                summary: format!("local activation release failed: {error}"),
                recovery: SlotRecovery::Finish,
            });
        }
    }
    Ok(ActiveDemo {
        id,
        epoch,
        observer,
        greeting,
        handle,
    })
}

async fn stop_once(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    generation: u64,
    active: &ActiveDemo,
) -> Result<StopRecord, String> {
    if let Err(error) = slot.reconcile(
        registry,
        DesiredComponentState::removed(slot.generation(generation)),
    ) {
        let primary = format!("local removal reconciliation failed: {error}");
        let seal = slot
            .fail_activation(active.epoch, primary.clone())
            .err()
            .map(|error| error.to_string());
        let cleanup = slot.finish_stop(active.epoch).await;
        return Err(describe_cleanup(primary, seal, cleanup));
    }
    let fence_failure = if let Some(handle) = &active.handle
        && !matches!(
            handle.try_with(|_| ()),
            Err(CapabilityHandleError::Revoked { .. })
        ) {
        Some("removal did not synchronously fence a capability handle".to_owned())
    } else {
        None
    };
    let record = match slot.finish_stop(active.epoch).await {
        Ok(record) => record,
        Err(error) => {
            return Err(describe_cleanup(
                fence_failure.unwrap_or_else(|| "local stop failed".to_owned()),
                None,
                Err(error),
            ));
        }
    };
    if let Some(primary) = fence_failure {
        return Err(describe_cleanup(primary, None, Ok(record)));
    }
    if record.disposition() != StopDisposition::Completed || !record.report().is_clean() {
        return Err(format!("local activation did not stop cleanly: {record:?}"));
    }
    if active.observer.resource_state(&active.id)? != ResourceState::Released
        || active.observer.deactivation_calls(&active.id) != 1
    {
        return Err("local activation resource was not released exactly once".to_owned());
    }
    Ok(record)
}

fn validate_grant_evidence(
    observer: &ExampleObserver,
    id: &PluginActivationId,
    expectation: GrantExpectation,
) -> Result<(), String> {
    let denied = observer.denied(id);
    let greeting = observer.greeting(id);
    let handle = observer.retained_handle(id);
    match expectation {
        GrantExpectation::Denied if denied && greeting.is_none() && handle.is_none() => Ok(()),
        GrantExpectation::Granted if !denied && greeting.is_some() && handle.is_some() => Ok(()),
        GrantExpectation::Denied => {
            Err("a manifest request was treated as a capability grant".to_owned())
        }
        GrantExpectation::Granted => {
            Err("an exact capability grant did not reach the driver".to_owned())
        }
    }
}

async fn settle_guard(
    activation: &mut HostPluginActivation<'_>,
    primary: String,
    seal: bool,
) -> String {
    let seal_error = if seal {
        activation
            .fail_activation(primary.clone())
            .err()
            .map(|error| error.to_string())
    } else {
        None
    };
    let cleanup = activation.finish_stop().await;
    describe_cleanup(primary, seal_error, cleanup)
}

fn describe_cleanup(
    primary: String,
    seal_error: Option<String>,
    cleanup: Result<StopRecord, ComponentSlotError>,
) -> String {
    let mut summary = primary;
    if let Some(error) = seal_error {
        summary.push_str(&format!("; lifecycle seal also failed: {error}"));
    }
    match cleanup {
        Ok(record) if record.report().is_clean() => {
            summary.push_str("; exact cleanup completed");
        }
        Ok(record) => {
            summary.push_str(&format!("; exact cleanup remains blocked: {record:?}"));
        }
        Err(error) => {
            summary.push_str(&format!("; exact cleanup could not complete: {error}"));
        }
    }
    summary
}

fn begin_start(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    component: &ComponentRevision,
    generation: u64,
) -> Result<ProviderSelectionEpoch, String> {
    let desired = DesiredComponentState::enabled(
        slot.generation(generation),
        component.clone(),
        ProviderAssignments::new(),
    );
    match slot
        .reconcile(registry, desired)
        .map_err(|error| error.to_string())?
    {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        }
        | ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => Ok(selection.epoch()),
        outcome => Err(format!("local component did not begin start: {outcome:?}")),
    }
}

fn component_revision() -> ComponentRevision {
    ComponentRevision::new(
        "example.local-capability.component-revision",
        ComponentDefinition::new("example.local-capability.component"),
        Scope::root("example.local-capability.scope"),
    )
}

fn prove_unrequested_grant_is_rejected(
    revision: &PluginRevision,
    broker: &mut CapabilityBroker,
) -> Result<(), String> {
    let definition = CapabilityDefinition::<usize>::new(
        CapabilityId::new("example.unrequested/v1").map_err(|error| error.to_string())?,
    );
    let registration = broker
        .register(&definition, Arc::new(1usize))
        .map_err(|error| error.to_string())?;
    let rejected = EffectiveCapabilityGrants::new(revision, [registration.grant()]);
    drop(registration.withdraw());
    if !matches!(rejected, Err(CapabilityGrantError::NotRequested { .. })) {
        return Err("host admitted a capability absent from the manifest request set".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{GREETING_CAPABILITY_ID, example_manifest};
    use yah_plugin_host::{DriverConformanceCase, DriverConformanceCaseResult, DriverKind};

    #[test]
    fn checked_manifest_declares_only_the_example_request() {
        let manifest = example_manifest().unwrap();
        assert_eq!(manifest.entrypoint().driver(), DriverKind::BuiltinRust);
        assert!(manifest.required_services().is_empty());
        assert!(manifest.provided_services().is_empty());
        assert_eq!(manifest.requested_capabilities().len(), 1);
        assert_eq!(
            manifest.requested_capabilities()[0].capability().as_str(),
            GREETING_CAPABILITY_ID
        );
    }

    #[tokio::test]
    async fn exact_grants_are_mediated_revocable_and_non_retargeting() {
        let report = run_capability_demo().await.unwrap();
        assert!(report.denied_without_grant);
        assert_eq!(report.first_greeting, "Hello, YAH!");
        assert_eq!(report.replacement_greeting, "Welcome, YAH!");
        assert_eq!(report.provider_calls, [1, 1, 1]);
        assert_eq!(report.clean_deactivations, 4);
        assert!(report.stale_handle_rejected);
    }

    #[tokio::test]
    async fn same_driver_family_passes_all_portable_lifecycle_cases() {
        let report = conformance::run_local_conformance().await;
        assert!(report.is_conformant(), "{report}");
        assert_eq!(report.cases().len(), DriverConformanceCase::ALL.len());
        assert_eq!(
            report
                .cases()
                .iter()
                .map(DriverConformanceCaseResult::case)
                .collect::<Vec<_>>(),
            DriverConformanceCase::ALL
        );
    }

    #[tokio::test]
    async fn failed_demo_assertions_still_release_exact_activation_resources() {
        let revision = example_revision('e').unwrap();
        let registry = ServiceRegistry::new();
        let broker = CapabilityBroker::new().unwrap();
        let grants = EffectiveCapabilityGrants::empty(&revision);
        let component = component_revision();

        for (index, plan, expectation, expected) in [
            (
                1,
                ActivationPlan::start_failure(),
                GrantExpectation::Denied,
                "start failed",
            ),
            (
                2,
                ActivationPlan::unhealthy(),
                GrantExpectation::Denied,
                "health failed",
            ),
            (
                3,
                ActivationPlan::ready(),
                GrantExpectation::Granted,
                "grant did not reach",
            ),
        ] {
            let mut slot = ComponentSlot::new(format!("example.failure.{index}")).unwrap();
            let (driver, observer) = LocalCapabilityDriver::scripted(revision.id().clone(), [plan]);
            let retained = observer.clone();
            let result = activate_scripted(
                &mut slot,
                &registry,
                &broker,
                &grants,
                &component,
                1,
                driver,
                observer,
                expectation,
            )
            .await;
            let error = match result {
                Ok(_) => panic!("injected example failure unexpectedly activated"),
                Err(error) => error,
            };
            assert!(error.contains(expected), "{error}");
            assert!(error.contains("exact cleanup completed"), "{error}");
            let ids = retained.activation_ids();
            assert_eq!(ids.len(), 1);
            assert_eq!(
                retained.resource_state(&ids[0]),
                Ok(ResourceState::Released)
            );
            assert_eq!(retained.deactivation_calls(&ids[0]), 1);
        }
    }

    #[tokio::test]
    async fn post_seal_demo_failure_finishes_cleanup_before_returning() {
        let revision = example_revision('d').unwrap();
        let registry = ServiceRegistry::new();
        let component = component_revision();
        let mut broker = CapabilityBroker::new().unwrap();
        let definition = contract::greeting_definition();
        let (provider, _) = contract::provider("Hello");
        let registration = broker.register(&definition, provider).unwrap();
        let grants = EffectiveCapabilityGrants::new(&revision, [registration.grant()]).unwrap();

        let mut slot_a = ComponentSlot::new("example.post-seal.a").unwrap();
        let mut active_a = activate_once(
            &mut slot_a,
            &registry,
            &broker,
            &grants,
            &component,
            1,
            GrantExpectation::Granted,
            ActivationPlan::ready(),
        )
        .await
        .unwrap();
        let mut slot_b = ComponentSlot::new("example.post-seal.b").unwrap();
        let active_b = activate_once(
            &mut slot_b,
            &registry,
            &broker,
            &grants,
            &component,
            1,
            GrantExpectation::Granted,
            ActivationPlan::ready(),
        )
        .await
        .unwrap();

        active_a.handle = active_b.handle.clone();
        let error = stop_once(&mut slot_a, &registry, 2, &active_a)
            .await
            .unwrap_err();
        assert!(error.contains("did not synchronously fence"), "{error}");
        assert!(error.contains("exact cleanup completed"), "{error}");
        assert_eq!(
            active_a.observer.resource_state(&active_a.id),
            Ok(ResourceState::Released)
        );
        assert_eq!(active_a.observer.deactivation_calls(&active_a.id), 1);

        stop_once(&mut slot_b, &registry, 2, &active_b)
            .await
            .unwrap();
        drop(registration.withdraw());
    }
}
