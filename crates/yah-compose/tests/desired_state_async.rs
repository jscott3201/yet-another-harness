use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::Notify;
use yah_compose::{
    CleanupError, ComponentDefinition, ComponentInstance, ComponentRevision, ComponentSlot,
    ComponentSlotError, ComponentSlotOutcome, ComponentStopReason, DependencyStopReason,
    DesiredComponentState, DesiredStopReason, EffectScope, ProviderAssignments, ProviderCandidate,
    ProviderSelectionEpoch, ReconcileError, ReconcileOutcome, Scope, ServiceDefinition,
    ServiceHandle, ServiceHandleError, ServiceRegistry, StopDisposition, StopTarget,
};

#[derive(Debug)]
struct Service(&'static str);

#[derive(Debug)]
struct OtherService;

struct Published {
    effects: EffectScope,
    candidate: ProviderCandidate,
}

fn publish(
    registry: &mut ServiceRegistry,
    service: &ServiceDefinition<Service>,
    label: &str,
    value: &'static str,
) -> Published {
    let definition = ComponentDefinition::new(format!("{label}.provider"));
    let scope = Scope::root(format!("{label}.scope"));
    let mut owner =
        ComponentInstance::new(format!("{label}.instance"), &definition, &scope).unwrap();
    let activation = owner.begin_start().unwrap();
    let mut effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    owner.complete_start(activation).unwrap();
    let candidate = registry
        .provide(&owner, &mut effects, service.provider(Service(value)))
        .unwrap();
    Published { effects, candidate }
}

fn revision(id: &str, service: Option<&ServiceDefinition<Service>>) -> ComponentRevision {
    let mut definition = ComponentDefinition::new(format!("{id}.component"));
    if let Some(service) = service {
        definition.require(&service.required()).unwrap();
    }
    ComponentRevision::new(id, definition, Scope::root("test.scope"))
}

fn enabled(
    slot: &ComponentSlot,
    generation_value: u64,
    revision: &ComponentRevision,
    assignments: &ProviderAssignments,
) -> DesiredComponentState {
    DesiredComponentState::enabled(
        slot.generation(generation_value),
        revision.clone(),
        assignments.clone(),
    )
}

fn start_epoch(outcome: ComponentSlotOutcome) -> ProviderSelectionEpoch {
    match outcome {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        }
        | ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected a start, got {outcome:?}"),
    }
}

fn start_with_service(
    slot: &mut ComponentSlot,
    registry: &ServiceRegistry,
    revision: &ComponentRevision,
    assignments: &ProviderAssignments,
    service: &ServiceDefinition<Service>,
    generation_value: u64,
) -> (ProviderSelectionEpoch, ServiceHandle<Service>) {
    let epoch = start_epoch(
        slot.reconcile(
            registry,
            enabled(slot, generation_value, revision, assignments),
        )
        .unwrap(),
    );
    let handle = slot.bind(epoch, registry, &service.required()).unwrap();
    slot.complete_start(epoch, registry).unwrap();
    (epoch, handle)
}

#[tokio::test]
async fn active_disable_revokes_immediately_and_reenable_uses_a_fresh_incarnation() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let published = publish(&mut registry, &service, "provider", "value");
    let revision = revision("test.revision-a", Some(&service));
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let (first_epoch, handle) =
        start_with_service(&mut slot, &registry, &revision, &assignments, &service, 1);

    assert!(matches!(
        slot.reconcile(
            &registry,
            DesiredComponentState::disabled(slot.generation(2), revision.clone())
        )
        .unwrap(),
        ComponentSlotOutcome::StopBegun {
            selection_epoch,
            target: StopTarget::Removed,
            reason: ComponentStopReason::Desired(DesiredStopReason::Disabled),
            ..
        } if selection_epoch == first_epoch
    ));
    assert!(matches!(
        handle.try_with(|service| service.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    let record = slot.finish_stop(first_epoch).await.unwrap();
    assert_eq!(record.target(), StopTarget::Removed);
    assert_eq!(
        record.reason(),
        &ComponentStopReason::Desired(DesiredStopReason::Disabled)
    );
    assert_eq!(record.disposition(), StopDisposition::Completed);
    assert!(slot.live_state().is_none());
    assert_eq!(slot.last_stop(), Some(&record));

    let second_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 3, &revision, &assignments))
            .unwrap(),
    );
    assert_ne!(second_epoch, first_epoch);
    assert_eq!(first_epoch.activation().sequence(), 1);
    assert_eq!(second_epoch.activation().sequence(), 1);
    assert!(matches!(
        slot.bind(first_epoch, &registry, &service.required()),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::StaleSelection { .. }
        ))
    ));
}

#[tokio::test]
async fn starting_disable_seals_before_delayed_bind_or_completion() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let published = publish(&mut registry, &service, "provider", "value");
    let revision = revision("test.revision-a", Some(&service));
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision, &assignments))
            .unwrap(),
    );
    let handle = slot.bind(epoch, &registry, &service.required()).unwrap();
    let cancellation = slot.cancellation(epoch).unwrap();

    assert!(matches!(
        slot.reconcile(
            &registry,
            DesiredComponentState::disabled(slot.generation(2), revision.clone())
        )
        .unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Removed,
            reason: ComponentStopReason::Desired(DesiredStopReason::Disabled),
            ..
        }
    ));
    assert!(cancellation.is_cancelled());
    assert!(matches!(
        handle.try_with(|service| service.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert!(matches!(
        slot.complete_start(epoch, &registry),
        Err(ComponentSlotError::MountedRevisionNotEnabled { .. })
    ));
    assert!(matches!(
        slot.bind(epoch, &registry, &service.required()),
        Err(ComponentSlotError::MountedRevisionNotEnabled { .. })
    ));
    slot.finish_stop(epoch).await.unwrap();
}

#[tokio::test]
async fn assignments_reactivate_one_instance_while_revision_change_replaces_it() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish(&mut registry, &service, "first", "first");
    let second = publish(&mut registry, &service, "second", "second");
    let revision_a = revision("test.revision-a", Some(&service));
    let revision_b = revision("test.revision-b", Some(&service));
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&first.candidate);
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let (first_epoch, _) =
        start_with_service(&mut slot, &registry, &revision_a, &assignments, &service, 1);

    assignments.assign(&second.candidate);
    assert!(matches!(
        slot.reconcile(&registry, enabled(&slot, 2, &revision_a, &assignments))
            .unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Pending,
            reason: ComponentStopReason::Dependency(DependencyStopReason::AssignmentChanged(_)),
            ..
        }
    ));
    slot.finish_stop(first_epoch).await.unwrap();
    let second_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 2, &revision_a, &assignments))
            .unwrap(),
    );
    assert_eq!(second_epoch.activation().sequence(), 2);
    slot.complete_start(second_epoch, &registry).unwrap();

    assert!(matches!(
        slot.reconcile(&registry, enabled(&slot, 3, &revision_b, &assignments))
            .unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Removed,
            reason: ComponentStopReason::Desired(DesiredStopReason::RevisionChanged { .. }),
            ..
        }
    ));
    slot.finish_stop(second_epoch).await.unwrap();
    let replacement_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 3, &revision_b, &assignments))
            .unwrap(),
    );
    assert_eq!(replacement_epoch.activation().sequence(), 1);
    assert_ne!(replacement_epoch, first_epoch);
    assert_eq!(slot.applied_revision(), Some(revision_b.id()));
}

#[tokio::test]
async fn explicit_abandonment_of_a_pending_stop_retains_the_revision_then_restarts() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish(&mut registry, &service, "first", "first");
    let second = publish(&mut registry, &service, "second", "second");
    let revision = revision("test.revision-a", Some(&service));
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&first.candidate);
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let (first_epoch, _) =
        start_with_service(&mut slot, &registry, &revision, &assignments, &service, 1);
    let calls = Arc::new(Mutex::new(0));
    let cleanup_calls = Arc::clone(&calls);
    slot.defer_sync(first_epoch, "fails", move || {
        *cleanup_calls.lock().unwrap() += 1;
        Err(CleanupError::new("old activation may still own a resource"))
    })
    .unwrap();

    assignments.assign(&second.candidate);
    slot.reconcile(&registry, enabled(&slot, 2, &revision, &assignments))
        .unwrap();
    let blocked = slot.finish_stop(first_epoch).await.unwrap();
    assert_eq!(blocked.target(), StopTarget::Pending);
    assert_eq!(blocked.disposition(), StopDisposition::Blocked);
    assert!(matches!(
        blocked.reason(),
        ComponentStopReason::Dependency(DependencyStopReason::AssignmentChanged(_))
    ));

    let abandoned = slot.abandon_failed_cleanup(first_epoch).unwrap();
    assert_eq!(abandoned.target(), StopTarget::Pending);
    assert_eq!(abandoned.disposition(), StopDisposition::Abandoned);
    assert_eq!(slot.applied_revision(), Some(revision.id()));
    assert_eq!(
        slot.live_state().unwrap().kind(),
        yah_compose::ComponentStateKind::Pending
    );
    assert_eq!(*calls.lock().unwrap(), 1);

    let second_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 2, &revision, &assignments))
            .unwrap(),
    );
    assert_eq!(second_epoch.activation().sequence(), 2);
    assert_ne!(second_epoch, first_epoch);
}

#[tokio::test]
async fn desired_removal_does_not_retarget_an_in_flight_dependency_stop() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish(&mut registry, &service, "first", "first");
    let second = publish(&mut registry, &service, "second", "second");
    let revision = revision("test.revision-a", Some(&service));
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&first.candidate);
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let (epoch, _) = start_with_service(&mut slot, &registry, &revision, &assignments, &service, 1);

    assignments.assign(&second.candidate);
    slot.reconcile(&registry, enabled(&slot, 2, &revision, &assignments))
        .unwrap();
    let removed = DesiredComponentState::removed(slot.generation(3));
    assert!(matches!(
        slot.reconcile(&registry, removed.clone()).unwrap(),
        ComponentSlotOutcome::Stopping {
            target: StopTarget::Pending,
            reason: ComponentStopReason::Dependency(DependencyStopReason::AssignmentChanged(_)),
            ..
        }
    ));

    let record = slot.finish_stop(epoch).await.unwrap();
    assert_eq!(record.target(), StopTarget::Pending);
    assert_eq!(slot.applied_revision(), Some(revision.id()));
    assert_eq!(
        slot.reconcile(&registry, removed).unwrap(),
        ComponentSlotOutcome::Unmounted {
            generation: slot.generation(3),
            applied_revision: revision.id().clone(),
            reason: DesiredStopReason::Removed,
        }
    );
    assert!(slot.applied_revision().is_none());
}

#[tokio::test]
async fn desired_churn_during_a_dropped_stop_uses_only_the_latest_revision() {
    let registry = ServiceRegistry::new();
    let revision_a = revision("test.revision-a", None);
    let revision_b = revision("test.revision-b", None);
    let revision_c = revision("test.revision-c", None);
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let first_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision_a, &assignments))
            .unwrap(),
    );
    slot.complete_start(first_epoch, &registry).unwrap();

    let release = Arc::new(Notify::new());
    let calls = Arc::new(Mutex::new(0));
    let cleanup_release = Arc::clone(&release);
    let cleanup_calls = Arc::clone(&calls);
    slot.defer_async(first_epoch, "wait", move || async move {
        *cleanup_calls.lock().unwrap() += 1;
        cleanup_release.notified().await;
        Ok(())
    })
    .unwrap();

    slot.reconcile(&registry, enabled(&slot, 2, &revision_b, &assignments))
        .unwrap();
    assert!(matches!(
        slot.reconcile(&registry, enabled(&slot, 3, &revision_c, &assignments))
            .unwrap(),
        ComponentSlotOutcome::Stopping {
            reason: ComponentStopReason::Desired(DesiredStopReason::RevisionChanged {
                ref desired,
                ..
            }),
            ..
        } if desired == revision_b.id()
    ));

    assert!(
        tokio::time::timeout(Duration::from_millis(20), slot.finish_stop(first_epoch))
            .await
            .is_err()
    );
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(slot.applied_revision(), Some(revision_a.id()));

    assert!(matches!(
        slot.reconcile(&registry, enabled(&slot, 4, &revision_a, &assignments))
            .unwrap(),
        ComponentSlotOutcome::Stopping { .. }
    ));
    release.notify_one();
    slot.finish_stop(first_epoch).await.unwrap();
    assert!(slot.applied_revision().is_none());

    let replacement_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 4, &revision_a, &assignments))
            .unwrap(),
    );
    assert_ne!(replacement_epoch, first_epoch);
    assert_eq!(replacement_epoch.activation().sequence(), 1);
    assert_eq!(slot.applied_revision(), Some(revision_a.id()));
    assert_eq!(*calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn cleanup_failure_blocks_latest_desired_state_until_explicit_abandonment() {
    let registry = ServiceRegistry::new();
    let revision_a = revision("test.revision-a", None);
    let replacement = revision("test.revision-b", None);
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision_a, &assignments))
            .unwrap(),
    );
    slot.complete_start(epoch, &registry).unwrap();
    let calls = Arc::new(Mutex::new(0));
    let cleanup_calls = Arc::clone(&calls);
    slot.defer_sync(epoch, "fails", move || {
        *cleanup_calls.lock().unwrap() += 1;
        Err(CleanupError::new("resource may still be live"))
    })
    .unwrap();

    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    assert!(matches!(
        slot.abandon_failed_cleanup(epoch),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::CleanupNotBlocked
        ))
    ));
    let blocked = slot.finish_stop(epoch).await.unwrap();
    assert_eq!(blocked.disposition(), StopDisposition::Blocked);
    assert_eq!(blocked.report().failure_count(), 1);
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(slot.applied_revision(), Some(revision_a.id()));
    assert!(matches!(
        slot.reconcile(&registry, enabled(&slot, 3, &replacement, &assignments))
            .unwrap(),
        ComponentSlotOutcome::Stopping {
            cleanup_blocked: true,
            ..
        }
    ));
    assert_eq!(slot.desired_revision(), Some(replacement.id()));
    assert_eq!(slot.applied_revision(), Some(revision_a.id()));

    let abandoned = slot.abandon_failed_cleanup(epoch).unwrap();
    assert_eq!(abandoned.disposition(), StopDisposition::Abandoned);
    assert_eq!(abandoned.report(), blocked.report());
    assert_eq!(
        abandoned.reason(),
        &ComponentStopReason::Desired(DesiredStopReason::Removed)
    );
    assert_eq!(slot.last_stop(), Some(&abandoned));
    assert!(slot.applied_revision().is_none());
    assert_eq!(*calls.lock().unwrap(), 1);
    let replacement_epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 3, &replacement, &assignments))
            .unwrap(),
    );
    assert_eq!(replacement_epoch.activation().sequence(), 1);
    assert_eq!(slot.applied_revision(), Some(replacement.id()));
}

#[tokio::test]
async fn revision_invalidation_precedes_validation_of_the_new_assignments() {
    let mut registry = ServiceRegistry::new();
    let revision_a = revision("test.revision-a", None);
    let revision_b = revision("test.revision-b", None);
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision_a, &assignments))
            .unwrap(),
    );
    slot.complete_start(epoch, &registry).unwrap();

    let other = ServiceDefinition::<OtherService>::new("test.other/v1");
    let provider_definition = ComponentDefinition::new("test.other-provider");
    let provider_scope = Scope::root("test.other-scope");
    let mut provider =
        ComponentInstance::new("test.other-instance", &provider_definition, &provider_scope)
            .unwrap();
    let provider_epoch = provider.begin_start().unwrap();
    let mut provider_effects = EffectScope::new("test.other-effects", provider_epoch).unwrap();
    provider.complete_start(provider_epoch).unwrap();
    let candidate = registry
        .provide(
            &provider,
            &mut provider_effects,
            other.provider(OtherService),
        )
        .unwrap();
    let mut invalid = ProviderAssignments::new();
    invalid.assign(&candidate);
    let desired_b = enabled(&slot, 2, &revision_b, &invalid);

    assert!(matches!(
        slot.reconcile(&registry, desired_b.clone()).unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Removed,
            ..
        }
    ));
    slot.finish_stop(epoch).await.unwrap();
    assert!(slot.applied_revision().is_none());
    assert!(matches!(
        slot.reconcile(&registry, desired_b).unwrap(),
        ComponentSlotOutcome::ConvergenceBlocked {
            generation,
            desired_revision: Some(desired),
            applied_revision: None,
            error: ReconcileError::UndeclaredAssignment { .. },
        } if generation == slot.generation(2) && desired == *revision_b.id()
    ));
    assert!(slot.applied_revision().is_none());
    assert_eq!(slot.desired_revision(), Some(revision_b.id()));
}

#[tokio::test]
async fn invalid_same_revision_assignments_are_rejected_before_desired_or_live_state_changes() {
    let mut registry = ServiceRegistry::new();
    let revision = revision("test.revision-a", None);
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision, &assignments))
            .unwrap(),
    );
    let cancellation = slot.cancellation(epoch).unwrap();

    let other = ServiceDefinition::<OtherService>::new("test.other/v1");
    let provider_definition = ComponentDefinition::new("test.other-provider");
    let provider_scope = Scope::root("test.other-scope");
    let mut provider =
        ComponentInstance::new("test.other-instance", &provider_definition, &provider_scope)
            .unwrap();
    let provider_epoch = provider.begin_start().unwrap();
    let mut provider_effects = EffectScope::new("test.other-effects", provider_epoch).unwrap();
    provider.complete_start(provider_epoch).unwrap();
    let candidate = registry
        .provide(
            &provider,
            &mut provider_effects,
            other.provider(OtherService),
        )
        .unwrap();
    let mut invalid = ProviderAssignments::new();
    invalid.assign(&candidate);
    let invalid_desired = enabled(&slot, 2, &revision, &invalid);

    assert!(matches!(
        slot.reconcile(&registry, invalid_desired.clone()),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::UndeclaredAssignment { .. }
        ))
    ));
    assert_eq!(slot.desired_generation(), Some(slot.generation(1)));
    assert_eq!(
        slot.live_state().unwrap().kind(),
        yah_compose::ComponentStateKind::Starting
    );
    assert!(!cancellation.is_cancelled());

    slot.complete_start(epoch, &registry).unwrap();
    assert!(matches!(
        slot.reconcile(&registry, invalid_desired),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::UndeclaredAssignment { .. }
        ))
    ));
    assert_eq!(slot.desired_generation(), Some(slot.generation(1)));
    assert_eq!(
        slot.live_state().unwrap().kind(),
        yah_compose::ComponentStateKind::Active
    );
    assert!(!cancellation.is_cancelled());
}

#[tokio::test]
async fn identical_desired_generation_still_detects_provider_withdrawal() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let mut published = publish(&mut registry, &service, "provider", "value");
    let revision = revision("test.revision-a", Some(&service));
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let desired = enabled(&slot, 1, &revision, &assignments);
    let (epoch, handle) =
        start_with_service(&mut slot, &registry, &revision, &assignments, &service, 1);

    drop(published.effects.close());
    assert!(matches!(
        slot.reconcile(&registry, desired).unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Pending,
            reason: ComponentStopReason::Dependency(DependencyStopReason::ProviderUnavailable(_)),
            ..
        }
    ));
    assert!(matches!(
        handle.try_with(|service| service.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert_eq!(
        slot.finish_stop(epoch).await.unwrap().target(),
        StopTarget::Pending
    );
    assert_eq!(slot.applied_revision(), Some(revision.id()));
}

#[tokio::test]
async fn removing_an_active_provider_hides_its_service_before_cleanup_is_polled() {
    let service = ServiceDefinition::<Service>::new("test.provided/v1");
    let mut registry = ServiceRegistry::new();
    let revision = revision("test.revision-a", None);
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision, &assignments))
            .unwrap(),
    );
    slot.complete_start(epoch, &registry).unwrap();
    let candidate = slot
        .provide(epoch, &mut registry, service.provider(Service("slot")))
        .unwrap();
    assert_eq!(
        registry.candidates(&service.required()).unwrap(),
        &[candidate]
    );

    slot.reconcile(
        &registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .unwrap();
    assert!(registry.candidates(&service.required()).unwrap().is_empty());
    slot.finish_stop(epoch).await.unwrap();
}
