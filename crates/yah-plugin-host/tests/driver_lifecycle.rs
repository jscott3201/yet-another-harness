#[path = "driver_lifecycle/support.rs"]
mod support;

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use support::{
    DeactivateMode, FakeDriver, FakePlan, Gate, HealthMode, PrepareMode, StartMode,
    prepare_activation,
};
use yah_compose::{
    CleanupError, ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    ComponentStateKind, DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch,
    ReconcileOutcome, Scope, ServiceRegistry, StopDisposition, StopTarget,
};
use yah_plugin_host::{
    DriverActivationErrorKind, HostPluginActivationError, PackageDigest, PluginActivationHandle,
    PluginDriver, PluginHealth, PluginHealthError, PluginPackageId, PluginRevisionId,
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

fn enabled(
    slot: &ComponentSlot,
    sequence: u64,
    revision: &ComponentRevision,
) -> DesiredComponentState {
    DesiredComponentState::enabled(
        slot.generation(sequence),
        revision.clone(),
        ProviderAssignments::new(),
    )
}

fn begin_start(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    revision: &ComponentRevision,
    sequence: u64,
) -> ProviderSelectionEpoch {
    match slot
        .reconcile(registry, enabled(slot, sequence, revision))
        .unwrap()
    {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        }
        | ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected fresh start, got {outcome:?}"),
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn assert_send<T: Send>(_: &T) {}

#[tokio::test]
async fn dyn_driver_happy_path_registers_deactivation_first_and_fences_health() {
    let registry = ServiceRegistry::new();
    let revision = component_revision("happy");
    let mut slot = ComponentSlot::new("happy.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.happy", 'a'),
        [FakePlan::ready()],
    ));
    let erased: Arc<dyn PluginDriver> = driver.clone();
    let mut activation = prepare_activation(&mut slot, epoch, erased).unwrap();
    let id = activation.id().clone();
    let probe = driver.probe(&id);

    assert_eq!(probe.start_constructs(), 0);
    let waiter = activation.activate(&registry);
    assert_send(&waiter);
    let handle = waiter.await.unwrap();
    assert_eq!(handle.health().unwrap(), PluginHealth::Healthy);
    let (slot, released_handle) = activation.release_active().unwrap();
    assert_eq!(released_handle.id(), &id);

    let cleanup_driver = Arc::clone(&driver);
    slot.defer_sync(epoch, "later plugin resource", move || {
        cleanup_driver.record("effect:later");
        Ok(())
    })
    .unwrap();
    assert!(matches!(
        slot.reconcile(
            &registry,
            DesiredComponentState::removed(slot.generation(2))
        )
        .unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Removed,
            ..
        }
    ));
    assert!(matches!(
        handle.health(),
        Err(PluginHealthError::Inactive { .. })
    ));

    let record = slot.finish_stop(epoch).await.unwrap();
    assert_eq!(record.disposition(), StopDisposition::Completed);
    assert_eq!(record.report().cleanup_count(), 2);
    assert_eq!(probe.deactivate_constructs(), 1);
    assert!(probe.deactivate_saw_cancellation());
    assert!(!probe.resource_is_open());

    let trace = driver.trace();
    let later = trace
        .iter()
        .position(|entry| entry == "effect:later")
        .unwrap();
    let deactivate = trace
        .iter()
        .position(|entry| entry.starts_with("deactivate:construct:"))
        .unwrap();
    assert!(later < deactivate, "{trace:?}");
}

#[tokio::test]
async fn dropped_start_and_finish_waiters_resume_after_cancel_before_future_drop() {
    let start_gate = Gate::default();
    let stop_gate = Gate::default();
    let plan = FakePlan {
        start: StartMode::Pending(start_gate),
        deactivate: DeactivateMode::Pending(stop_gate.clone()),
        ..FakePlan::ready()
    };
    let registry = ServiceRegistry::new();
    let revision = component_revision("resume");
    let mut slot = ComponentSlot::new("resume.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let removed = DesiredComponentState::removed(slot.generation(2));
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.resume", 'b'),
        [plan],
    ));
    let mut activation = prepare_activation(&mut slot, epoch, driver.clone()).unwrap();
    let probe = driver.probe(activation.id());

    let mut first_waiter = Box::pin(activation.activate(&registry));
    assert!(poll_once(first_waiter.as_mut()).is_pending());
    drop(first_waiter);
    assert_eq!(probe.start_polls(), 1);
    assert_eq!(probe.start_drops(), 0);
    assert!(probe.resource_is_open());

    assert!(matches!(
        activation.reconcile(&registry, removed).unwrap(),
        ComponentSlotOutcome::StopBegun { .. }
    ));
    assert!(activation.cancellation().is_cancelled());
    assert_eq!(probe.start_drops(), 1);
    assert!(probe.start_drop_saw_cancellation());

    let mut first_finish = Box::pin(activation.finish_stop());
    assert!(poll_once(first_finish.as_mut()).is_pending());
    drop(first_finish);
    assert_eq!(probe.deactivate_constructs(), 1);
    assert_eq!(probe.deactivate_polls(), 1);
    stop_gate.release();
    let record = activation.finish_stop().await.unwrap();
    assert_eq!(record.disposition(), StopDisposition::Completed);
    assert_eq!(probe.deactivate_constructs(), 1);
    assert!(!probe.resource_is_open());
}

#[tokio::test]
async fn activation_errors_and_unwind_panics_seal_and_roll_back_without_escaping() {
    let cases = [
        (
            StartMode::Error("returned failure"),
            DriverActivationErrorKind::Failed,
        ),
        (StartMode::PanicFactory, DriverActivationErrorKind::Panicked),
        (StartMode::PanicPoll, DriverActivationErrorKind::Panicked),
        (
            StartMode::ReadyDropPanic,
            DriverActivationErrorKind::Panicked,
        ),
    ];

    for (index, (start, expected_kind)) in cases.into_iter().enumerate() {
        let registry = ServiceRegistry::new();
        let revision = component_revision(&format!("failure-{index}"));
        let mut slot = ComponentSlot::new(format!("failure-{index}.slot")).unwrap();
        let epoch = begin_start(&mut slot, &registry, &revision, 1);
        let driver = Arc::new(FakeDriver::new(
            package_revision(&format!("acme.failure-{index}"), 'c'),
            [FakePlan {
                start,
                ..FakePlan::ready()
            }],
        ));
        let mut activation = prepare_activation(&mut slot, epoch, driver.clone()).unwrap();
        let probe = driver.probe(activation.id());

        let error = activation.activate(&registry).await.unwrap_err();
        let PluginStartError::Driver { failure, .. } = error else {
            panic!("expected driver failure, got {error:?}");
        };
        assert_eq!(failure.kind(), expected_kind);
        assert!(activation.cancellation().is_cancelled());
        let record = activation.finish_stop().await.unwrap();
        assert_eq!(record.disposition(), StopDisposition::Completed);
        assert_eq!(probe.deactivate_constructs(), 1);
        drop(activation);
        assert_eq!(
            slot.live_state().unwrap().kind(),
            ComponentStateKind::Pending
        );
    }
}

#[tokio::test]
async fn cancellation_contains_a_pending_start_future_destructor_panic() {
    let gate = Gate::default();
    let registry = ServiceRegistry::new();
    let revision = component_revision("drop-panic");
    let mut slot = ComponentSlot::new("drop-panic.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.drop-panic", 'd'),
        [FakePlan {
            start: StartMode::PendingDropPanic(gate),
            ..FakePlan::ready()
        }],
    ));
    let mut activation = prepare_activation(&mut slot, epoch, driver.clone()).unwrap();
    let probe = driver.probe(activation.id());
    let mut waiter = Box::pin(activation.activate(&registry));
    assert!(poll_once(waiter.as_mut()).is_pending());
    drop(waiter);

    assert!(matches!(
        activation.fail_activation("host cancelled pending activation"),
        Ok(ReconcileOutcome::StopBegun { .. })
    ));
    assert_eq!(probe.start_drops(), 1);
    assert!(probe.start_drop_saw_cancellation());
    assert!(matches!(
        activation.activate(&registry).await,
        Err(PluginStartError::Driver { .. })
    ));
    let record = activation.finish_stop().await.unwrap();
    assert!(record.report().is_clean());
}

#[tokio::test]
async fn deactivation_failures_aggregate_cache_and_require_explicit_abandonment() {
    for (index, deactivate) in [
        DeactivateMode::Error("deactivation returned failure"),
        DeactivateMode::PanicPoll,
        DeactivateMode::DropPanic,
    ]
    .into_iter()
    .enumerate()
    {
        let registry = ServiceRegistry::new();
        let revision = component_revision(&format!("cleanup-{index}"));
        let mut slot = ComponentSlot::new(format!("cleanup-{index}.slot")).unwrap();
        let epoch = begin_start(&mut slot, &registry, &revision, 1);
        let driver = Arc::new(FakeDriver::new(
            package_revision(&format!("acme.cleanup-{index}"), 'e'),
            [FakePlan {
                deactivate,
                ..FakePlan::ready()
            }],
        ));
        let mut activation = prepare_activation(&mut slot, epoch, driver.clone()).unwrap();
        let probe = driver.probe(activation.id());
        activation.activate(&registry).await.unwrap();
        let (slot, _) = activation.release_active().unwrap();
        slot.defer_sync(epoch, "independent failing cleanup", || {
            Err(CleanupError::new("independent failure"))
        })
        .unwrap();
        slot.reconcile(
            &registry,
            DesiredComponentState::removed(slot.generation(2)),
        )
        .unwrap();

        let first = slot.finish_stop(epoch).await.unwrap();
        assert_eq!(first.disposition(), StopDisposition::Blocked);
        assert_eq!(first.report().failure_count(), 2);
        assert_eq!(probe.deactivate_constructs(), 1);
        let repeated = slot.finish_stop(epoch).await.unwrap();
        assert_eq!(repeated, first);
        assert_eq!(probe.deactivate_constructs(), 1);
        let abandoned = slot.abandon_failed_cleanup(epoch).unwrap();
        assert_eq!(abandoned.disposition(), StopDisposition::Abandoned);
        assert!(slot.live_state().is_none());
    }
}

#[tokio::test]
async fn one_multiplexing_driver_keeps_activation_and_health_state_exact() {
    let registry = ServiceRegistry::new();
    let gate_a = Gate::default();
    let gate_b = Gate::default();
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.multiplex", 'f'),
        [
            FakePlan {
                start: StartMode::Pending(gate_a.clone()),
                ..FakePlan::ready()
            },
            FakePlan {
                start: StartMode::Pending(gate_b.clone()),
                ..FakePlan::ready()
            },
        ],
    ));
    let mut slot_a = ComponentSlot::new("multiplex.a").unwrap();
    let mut slot_b = ComponentSlot::new("multiplex.b").unwrap();
    let revision_a = component_revision("multiplex-a");
    let revision_b = component_revision("multiplex-b");
    let epoch_a = begin_start(&mut slot_a, &registry, &revision_a, 1);
    let epoch_b = begin_start(&mut slot_b, &registry, &revision_b, 1);

    let mut activation_a = prepare_activation(&mut slot_a, epoch_a, driver.clone()).unwrap();
    let mut activation_b = prepare_activation(&mut slot_b, epoch_b, driver.clone()).unwrap();
    let mut waiter_a = Box::pin(activation_a.activate(&registry));
    let mut waiter_b = Box::pin(activation_b.activate(&registry));
    assert!(poll_once(waiter_a.as_mut()).is_pending());
    assert!(poll_once(waiter_b.as_mut()).is_pending());
    drop(waiter_a);
    drop(waiter_b);
    gate_a.release();
    gate_b.release();
    let handle_a = activation_a.activate(&registry).await.unwrap();
    let handle_b = activation_b.activate(&registry).await.unwrap();
    let id_a = handle_a.id().clone();
    let id_b = handle_b.id().clone();
    assert_ne!(id_a, id_b);
    let probe_a = driver.probe(&id_a);
    let probe_b = driver.probe(&id_b);
    let (slot_a, _) = activation_a.release_active().unwrap();
    let (slot_b, _) = activation_b.release_active().unwrap();

    slot_a
        .reconcile(
            &registry,
            DesiredComponentState::removed(slot_a.generation(2)),
        )
        .unwrap();
    slot_a.finish_stop(epoch_a).await.unwrap();
    assert_eq!(probe_a.deactivate_constructs(), 1);
    assert_eq!(probe_b.deactivate_constructs(), 0);
    assert!(probe_b.resource_is_open());
    assert!(matches!(
        handle_a.health(),
        Err(PluginHealthError::Inactive { .. })
    ));
    assert_eq!(handle_b.health().unwrap(), PluginHealth::Healthy);

    probe_b.set_health(HealthMode::Value(PluginHealth::degraded("warming")));
    assert_eq!(
        handle_b.health().unwrap(),
        PluginHealth::degraded("warming")
    );
    probe_b.set_health(HealthMode::Value(PluginHealth::unhealthy("backend lost")));
    assert_eq!(
        handle_b.health().unwrap(),
        PluginHealth::unhealthy("backend lost")
    );
    assert_eq!(
        slot_b.live_state().unwrap().kind(),
        ComponentStateKind::Active
    );
    probe_b.set_health(HealthMode::Error("probe unavailable"));
    assert!(matches!(
        handle_b.health(),
        Err(PluginHealthError::Driver { .. })
    ));
    assert_eq!(
        slot_b.live_state().unwrap().kind(),
        ComponentStateKind::Active
    );
    probe_b.set_health(HealthMode::Panic);
    assert!(matches!(
        handle_b.health(),
        Err(PluginHealthError::DriverPanicked { .. })
    ));
    assert_eq!(
        slot_b.live_state().unwrap().kind(),
        ComponentStateKind::Active
    );
}

#[tokio::test]
async fn whole_owner_drop_seals_but_does_not_claim_asynchronous_cleanup() {
    let registry = ServiceRegistry::new();

    let never_revision = component_revision("owner-never-started");
    let mut never_slot = ComponentSlot::new("owner-never-started.slot").unwrap();
    let never_epoch = begin_start(&mut never_slot, &registry, &never_revision, 1);
    let never_driver = Arc::new(FakeDriver::new(
        package_revision("acme.owner-never-started", '6'),
        [FakePlan::ready()],
    ));
    let never_cancellation;
    let never_probe;
    {
        let activation =
            prepare_activation(&mut never_slot, never_epoch, never_driver.clone()).unwrap();
        never_cancellation = activation.cancellation().clone();
        never_probe = never_driver.probe(activation.id());
        assert_eq!(never_probe.start_constructs(), 0);
    }
    assert!(never_cancellation.is_cancelled());
    assert_eq!(never_probe.deactivate_constructs(), 0);
    assert_eq!(
        never_slot.live_state().unwrap().kind(),
        ComponentStateKind::Stopping
    );
    let never_record = never_slot.finish_stop(never_epoch).await.unwrap();
    assert!(never_record.report().is_clean());
    assert_eq!(never_probe.deactivate_constructs(), 1);

    let gate = Gate::default();
    let pending_revision = component_revision("owner-pending");
    let mut pending_slot = ComponentSlot::new("owner-pending.slot").unwrap();
    let pending_epoch = begin_start(&mut pending_slot, &registry, &pending_revision, 1);
    let pending_driver = Arc::new(FakeDriver::new(
        package_revision("acme.owner-pending", '7'),
        [FakePlan {
            start: StartMode::Pending(gate),
            ..FakePlan::ready()
        }],
    ));
    let pending_cancellation;
    let pending_probe;
    {
        let mut activation =
            prepare_activation(&mut pending_slot, pending_epoch, pending_driver.clone()).unwrap();
        pending_cancellation = activation.cancellation().clone();
        pending_probe = pending_driver.probe(activation.id());
        let mut waiter = Box::pin(activation.activate(&registry));
        assert!(poll_once(waiter.as_mut()).is_pending());
        drop(waiter);
    }
    assert!(pending_cancellation.is_cancelled());
    assert!(pending_probe.start_drop_saw_cancellation());
    assert_eq!(pending_probe.deactivate_constructs(), 0);
    pending_slot.finish_stop(pending_epoch).await.unwrap();
    assert_eq!(pending_probe.deactivate_constructs(), 1);
}

#[tokio::test]
async fn late_success_cannot_activate_a_replaced_component_incarnation() {
    let start_gate = Gate::default();
    let registry = ServiceRegistry::new();
    let revision_a = component_revision("replace-a");
    let revision_b = component_revision("replace-b");
    let mut slot = ComponentSlot::new("replace.slot").unwrap();
    let desired_b = enabled(&slot, 2, &revision_b);
    let epoch_a = begin_start(&mut slot, &registry, &revision_a, 1);
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.replace", '1'),
        [
            FakePlan {
                start: StartMode::Pending(start_gate.clone()),
                ..FakePlan::ready()
            },
            FakePlan::ready(),
        ],
    ));
    let old_id;
    {
        let mut old = prepare_activation(&mut slot, epoch_a, driver.clone()).unwrap();
        old_id = old.id().clone();
        let mut waiter = Box::pin(old.activate(&registry));
        assert!(poll_once(waiter.as_mut()).is_pending());
        drop(waiter);
        assert!(matches!(
            old.reconcile(&registry, desired_b.clone()).unwrap(),
            ComponentSlotOutcome::StopBegun {
                target: StopTarget::Removed,
                ..
            }
        ));
        start_gate.release();
        assert!(matches!(
            old.activate(&registry).await,
            Err(PluginStartError::Superseded { .. })
        ));
        old.finish_stop().await.unwrap();
    }

    let epoch_b = begin_start(&mut slot, &registry, &revision_b, 2);
    assert_ne!(epoch_a, epoch_b);
    let mut replacement = prepare_activation(&mut slot, epoch_b, driver.clone()).unwrap();
    let new_handle = replacement.activate(&registry).await.unwrap();
    assert_ne!(new_handle.id(), &old_id);
    let (slot, _) = replacement.release_active().unwrap();
    assert_eq!(
        slot.live_state().unwrap().kind(),
        ComponentStateKind::Active
    );
}

#[test]
fn rejected_preparation_never_constructs_or_polls_driver_activation() {
    let registry = ServiceRegistry::new();
    let revision = component_revision("prepare-reject");
    let mut slot = ComponentSlot::new("prepare-reject.slot").unwrap();
    let epoch = begin_start(&mut slot, &registry, &revision, 1);
    slot.complete_start(epoch, &registry).unwrap();
    let driver = Arc::new(FakeDriver::new(
        package_revision("acme.prepare-reject", '2'),
        [FakePlan::ready()],
    ));
    assert!(matches!(
        prepare_activation(&mut slot, epoch, driver.clone()),
        Err(HostPluginActivationError::Composition(_))
    ));
    assert_eq!(driver.prepare_calls(), 0);

    let mut new_slot = ComponentSlot::new("prepare-mismatch.slot").unwrap();
    let new_revision = component_revision("prepare-mismatch");
    let new_epoch = begin_start(&mut new_slot, &registry, &new_revision, 1);
    let mismatch = Arc::new(FakeDriver::new(
        package_revision("acme.prepare-mismatch", '3'),
        [FakePlan {
            prepare: PrepareMode::WrongRevision(package_revision("acme.wrong", '4')),
            ..FakePlan::ready()
        }],
    ));
    assert!(matches!(
        prepare_activation(&mut new_slot, new_epoch, mismatch.clone()),
        Err(HostPluginActivationError::PreparedIdentityMismatch { .. })
    ));
    assert!(
        mismatch
            .trace()
            .iter()
            .all(|entry| !entry.starts_with("start:") && !entry.starts_with("deactivate:"))
    );

    for (index, prepare) in [
        PrepareMode::Error("inert preparation rejected"),
        PrepareMode::Panic,
    ]
    .into_iter()
    .enumerate()
    {
        let mut rejected_slot = ComponentSlot::new(format!("prepare-driver-{index}.slot")).unwrap();
        let rejected_revision = component_revision(&format!("prepare-driver-{index}"));
        let rejected_epoch = begin_start(&mut rejected_slot, &registry, &rejected_revision, 1);
        let rejected = Arc::new(FakeDriver::new(
            package_revision(&format!("acme.prepare-driver-{index}"), '5'),
            [FakePlan {
                prepare,
                ..FakePlan::ready()
            }],
        ));
        assert!(matches!(
            prepare_activation(&mut rejected_slot, rejected_epoch, rejected),
            Err(HostPluginActivationError::Driver(_)
                | HostPluginActivationError::DriverPanicked { .. })
        ));
        assert_eq!(
            rejected_slot.live_state().unwrap().kind(),
            ComponentStateKind::Starting
        );
    }
}

#[allow(dead_code)]
fn _assert_handle_is_send_sync(handle: &PluginActivationHandle) {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}
    assert_send_sync(handle);
}
