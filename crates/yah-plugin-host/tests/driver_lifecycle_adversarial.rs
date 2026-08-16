#[path = "driver_lifecycle/support.rs"]
mod support;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use support::{FakeDriver, FakePlan, Gate, HealthBlock, HealthMode, StartMode, prepare_activation};
use yah_compose::{
    CloseStep, ComponentDefinition, ComponentInstance, ComponentRevision, ComponentSlot,
    ComponentSlotOutcome, ComponentStateKind, DesiredComponentState, EffectScope,
    ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope, ServiceDefinition,
    ServiceRegistry, StopDisposition,
};
use yah_plugin_host::{
    HostPluginActivationError, PackageDigest, PluginHealthError, PluginPackageId, PluginRevisionId,
    PluginStartError, PluginVersion,
};

fn package_revision(name: &str, digest_byte: char) -> PluginRevisionId {
    PluginRevisionId::new(
        PluginPackageId::new(name).unwrap(),
        PluginVersion::new("1.0.0").unwrap(),
        PackageDigest::new(format!("blake3:{}", digest_byte.to_string().repeat(64))).unwrap(),
    )
}

fn component_revision(name: &str) -> ComponentRevision {
    ComponentRevision::new(
        format!("{name}.revision"),
        ComponentDefinition::new(format!("{name}.component")),
        Scope::root(format!("{name}.scope")),
    )
}

fn begin_start(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    revision: &ComponentRevision,
) -> ProviderSelectionEpoch {
    let desired = DesiredComponentState::enabled(
        slot.generation(1),
        revision.clone(),
        ProviderAssignments::new(),
    );
    match slot.reconcile(registry, desired).unwrap() {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected fresh start, got {outcome:?}"),
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

#[tokio::test]
async fn concurrent_health_panic_is_consumed_before_stop_suppresses_the_result() {
    let registry = ServiceRegistry::new();
    let revision = component_revision("health-race");
    let mut slot = ComponentSlot::new("health-race.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision);
    let block = HealthBlock::new();
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.health-race", '8'),
        [FakePlan {
            health: HealthMode::BlockedDropPanic(block.clone()),
            ..FakePlan::ready()
        }],
    ));
    let mut activation = prepare_activation(&mut slot, epoch, driver).unwrap();
    let handle = activation.activate(&registry).await.unwrap();
    let (slot, _) = activation.release_active().unwrap();

    let raced = handle.clone();
    let health = std::thread::spawn(move || raced.health());
    block.wait_until_called();
    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    block.release();
    assert!(matches!(
        health.join().expect("health panic stays contained"),
        Err(PluginHealthError::Inactive { .. })
    ));
    assert!(slot.finish_stop(epoch).await.unwrap().report().is_clean());
    assert!(matches!(
        handle.health(),
        Err(PluginHealthError::Inactive { .. })
    ));
}

#[tokio::test]
async fn prepared_control_destructor_is_reported_before_stale_handles_are_released() {
    let registry = ServiceRegistry::new();
    let revision = component_revision("control-drop");
    let mut slot = ComponentSlot::new("control-drop.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision);
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.control-drop", '9'),
        [FakePlan {
            prepared_drop_panics: true,
            ..FakePlan::ready()
        }],
    ));
    let mut activation = prepare_activation(&mut slot, epoch, driver).unwrap();
    let stale = activation.activate(&registry).await.unwrap();
    let (slot, _) = activation.release_active().unwrap();
    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();

    let record = slot.finish_stop(epoch).await.unwrap();
    assert_eq!(record.disposition(), StopDisposition::Blocked);
    assert_eq!(record.report().failure_count(), 1);
    assert!(matches!(
        stale.health(),
        Err(PluginHealthError::Inactive { .. })
    ));
    drop(stale);
    assert_eq!(
        slot.abandon_failed_cleanup(epoch).unwrap().disposition(),
        StopDisposition::Abandoned
    );
}

#[tokio::test]
async fn active_owner_drop_and_premature_release_both_seal_before_cleanup() {
    let registry = ServiceRegistry::new();
    let active_revision = component_revision("owner-active");
    let mut active_slot = ComponentSlot::new("owner-active.slot").unwrap();
    let active_epoch = begin_start(&mut active_slot, &registry, &active_revision);
    let active_driver = Arc::new(FakeDriver::new(
        package_revision("acme.owner-active", 'a'),
        [FakePlan::ready()],
    ));
    let active_probe;
    {
        let mut activation =
            prepare_activation(&mut active_slot, active_epoch, active_driver.clone()).unwrap();
        active_probe = active_driver.probe(activation.id());
        activation.activate(&registry).await.unwrap();
    }
    assert_eq!(
        active_slot.live_state().unwrap().kind(),
        ComponentStateKind::Stopping
    );
    assert_eq!(active_probe.deactivate_constructs(), 0);
    active_slot.finish_stop(active_epoch).await.unwrap();
    assert_eq!(active_probe.deactivate_constructs(), 1);

    let gate = Gate::default();
    let early_revision = component_revision("owner-early-release");
    let mut early_slot = ComponentSlot::new("owner-early-release.slot").unwrap();
    let early_epoch = begin_start(&mut early_slot, &registry, &early_revision);
    let early_driver = Arc::new(FakeDriver::new(
        package_revision("acme.owner-early-release", 'b'),
        [FakePlan {
            start: StartMode::Pending(gate),
            ..FakePlan::ready()
        }],
    ));
    let mut activation =
        prepare_activation(&mut early_slot, early_epoch, early_driver.clone()).unwrap();
    let probe = early_driver.probe(activation.id());
    let mut waiter = Box::pin(activation.activate(&registry));
    assert!(poll_once(waiter.as_mut()).is_pending());
    drop(waiter);
    assert!(matches!(
        activation.release_active(),
        Err(PluginStartError::Composition { .. })
    ));
    assert!(probe.start_drop_saw_cancellation());
    assert_eq!(
        early_slot.live_state().unwrap().kind(),
        ComponentStateKind::Stopping
    );
    early_slot.finish_stop(early_epoch).await.unwrap();
    assert_eq!(probe.deactivate_constructs(), 1);
}

#[derive(Debug)]
struct RequiredValue;

#[tokio::test]
async fn provider_withdrawal_wins_when_pending_driver_start_later_reports_ready() {
    let root = Scope::root("ready-race.root");
    let service = ServiceDefinition::<RequiredValue>::new("test.ready-race/v1");
    let provider_definition = ComponentDefinition::new("ready-race.provider");
    let mut provider =
        ComponentInstance::new("ready-race.provider.instance", &provider_definition, &root)
            .unwrap();
    let provider_epoch = provider.begin_start().unwrap();
    let mut provider_effects = EffectScope::new("ready-race.provider", provider_epoch).unwrap();
    provider.complete_start(provider_epoch).unwrap();
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &provider,
            &mut provider_effects,
            service.provider(RequiredValue),
        )
        .unwrap();

    let mut consumer_definition = ComponentDefinition::new("ready-race.consumer");
    consumer_definition.require(&service.required()).unwrap();
    let consumer_scope = Scope::child("ready-race.consumer", &root).unwrap();
    let revision =
        ComponentRevision::new("ready-race.revision", consumer_definition, consumer_scope);
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&candidate);
    let mut slot = ComponentSlot::new("ready-race.slot").unwrap();
    let desired = DesiredComponentState::enabled(slot.generation(1), revision, assignments);
    let epoch = match slot.reconcile(&registry, desired).unwrap() {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected dependency-ready start, got {outcome:?}"),
    };

    let gate = Gate::default();
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.ready-race", 'c'),
        [FakePlan {
            start: StartMode::Pending(gate.clone()),
            ..FakePlan::ready()
        }],
    ));
    let mut activation = prepare_activation(&mut slot, epoch, driver).unwrap();
    let mut waiter = Box::pin(activation.activate(&registry));
    assert!(poll_once(waiter.as_mut()).is_pending());
    drop(waiter);

    drop(provider_effects.close());
    gate.release();
    assert!(matches!(
        activation.activate(&registry).await,
        Err(PluginStartError::Superseded { .. })
    ));
    assert!(activation.cancellation().is_cancelled());
    assert!(provider_effects.close().await.is_clean());
    assert!(activation.finish_stop().await.unwrap().report().is_clean());
}

#[test]
fn rejected_preparation_contains_driver_and_prepared_control_destructors() {
    let registry = ServiceRegistry::new();
    for (index, plan) in [
        FakePlan {
            prepare: support::PrepareMode::Error("prepare rejected"),
            ..FakePlan::ready()
        },
        FakePlan {
            prepare: support::PrepareMode::WrongRevision(package_revision("acme.wrong", 'd')),
            prepared_drop_panics: true,
            ..FakePlan::ready()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let revision = component_revision(&format!("reject-drop-{index}"));
        let mut slot = ComponentSlot::new(format!("reject-drop-{index}.slot")).unwrap();
        let epoch = begin_start(&mut slot, &registry, &revision);
        let driver = Arc::new(
            FakeDriver::new(
                package_revision(&format!("acme.reject-drop-{index}"), 'e'),
                [plan],
            )
            .with_drop_panic(),
        );
        let error = prepare_activation(&mut slot, epoch, driver).unwrap_err();
        let HostPluginActivationError::DriverControlDropPanicked { summary } = error else {
            panic!("expected contained control destructor panic, got {error:?}");
        };
        assert!(summary.contains("plugin driver"));
        if index == 0 {
            assert!(summary.contains("prepare rejected"));
        } else {
            assert!(summary.contains("does not match requested"));
            assert!(summary.contains("prepared activation"));
        }
        assert_eq!(
            slot.live_state().unwrap().kind(),
            ComponentStateKind::Starting
        );
    }
}

#[tokio::test]
async fn compound_deactivation_panics_are_aggregated_before_control_disposal() {
    let cases = [
        (
            support::DeactivateMode::PanicPollDropPanic,
            false,
            &[
                "polling deactivation",
                "future destructor",
                "prepared activation",
            ][..],
        ),
        (
            support::DeactivateMode::ErrorDropPanic("deactivation error"),
            false,
            &[
                "deactivation error",
                "future destructor",
                "prepared activation",
            ][..],
        ),
        (
            support::DeactivateMode::PanicFactory,
            true,
            &[
                "constructing deactivation",
                "prepared activation",
                "plugin driver",
            ][..],
        ),
    ];

    for (index, (deactivate, driver_drop_panics, expected)) in cases.into_iter().enumerate() {
        let registry = ServiceRegistry::new();
        let revision = component_revision(&format!("compound-{index}"));
        let mut slot = ComponentSlot::new(format!("compound-{index}.slot")).unwrap();
        let epoch = begin_start(&mut slot, &registry, &revision);
        let driver = FakeDriver::new(
            package_revision(&format!("acme.compound-{index}"), 'f'),
            [FakePlan {
                deactivate,
                prepared_drop_panics: true,
                ..FakePlan::ready()
            }],
        );
        let driver = if driver_drop_panics {
            driver.with_drop_panic()
        } else {
            driver
        };
        let mut activation = prepare_activation(&mut slot, epoch, Arc::new(driver)).unwrap();
        activation.activate(&registry).await.unwrap();
        let (slot, _) = activation.release_active().unwrap();
        slot.reconcile(
            &registry,
            DesiredComponentState::removed(slot.generation(2)),
        )
        .unwrap();
        let record = slot.finish_stop(epoch).await.unwrap();
        assert_eq!(record.disposition(), StopDisposition::Blocked);
        let summary = record
            .report()
            .steps()
            .iter()
            .find_map(|step| match step {
                CloseStep::Cleanup(record) => record.outcome().failure(),
                CloseStep::Child { .. } => None,
            })
            .unwrap()
            .summary();
        for fragment in expected {
            assert!(
                summary.contains(fragment),
                "missing {fragment:?} in {summary:?}"
            );
        }
        slot.abandon_failed_cleanup(epoch).unwrap();
    }
}
