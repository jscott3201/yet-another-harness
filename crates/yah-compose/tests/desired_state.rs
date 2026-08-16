use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotError,
    ComponentSlotOutcome, ComponentState, DesiredComponentState, ProviderAssignments,
    ReconcileOutcome, Scope, ServiceDefinition, ServiceRegistry,
};

#[derive(Debug)]
struct Greeting;

#[derive(Debug)]
struct Count;

fn revision(id: &str, definition: ComponentDefinition, scope: &str) -> ComponentRevision {
    ComponentRevision::new(id, definition, Scope::root(scope))
}

fn empty_revision(id: &str) -> ComponentRevision {
    revision(id, ComponentDefinition::new("test.component"), "test.scope")
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

fn start_epoch(outcome: ComponentSlotOutcome) -> yah_compose::ProviderSelectionEpoch {
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

#[test]
fn enabled_create_and_identical_generation_are_level_triggered_and_idempotent() {
    let registry = ServiceRegistry::new();
    let revision = empty_revision("test.revision-a");
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let desired = enabled(&slot, 1, &revision, &ProviderAssignments::new());

    let epoch = start_epoch(slot.reconcile(&registry, desired.clone()).unwrap());
    assert_eq!(slot.desired_generation(), Some(slot.generation(1)));
    assert_eq!(slot.desired_revision(), Some(revision.id()));
    assert_eq!(slot.applied_revision(), Some(revision.id()));

    assert_eq!(
        slot.reconcile(&registry, desired.clone()).unwrap(),
        ComponentSlotOutcome::Reconciled {
            generation: slot.generation(1),
            applied_revision: revision.id().clone(),
            component: ReconcileOutcome::AwaitingStart {
                selection_epoch: epoch,
            },
        }
    );
    assert_eq!(
        slot.complete_start(epoch, &registry).unwrap(),
        ReconcileOutcome::Active {
            selection_epoch: epoch,
        }
    );
    assert_eq!(
        slot.reconcile(&registry, desired).unwrap(),
        ComponentSlotOutcome::Reconciled {
            generation: slot.generation(1),
            applied_revision: revision.id().clone(),
            component: ReconcileOutcome::Active {
                selection_epoch: epoch,
            },
        }
    );
    assert_eq!(epoch.activation().sequence(), 1);
}

#[test]
fn stale_generation_and_generation_conflict_do_not_touch_live_state() {
    let registry = ServiceRegistry::new();
    let revision = empty_revision("test.revision-a");
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let desired = enabled(&slot, 2, &revision, &ProviderAssignments::new());
    let epoch = start_epoch(slot.reconcile(&registry, desired.clone()).unwrap());
    slot.complete_start(epoch, &registry).unwrap();
    let cancellation = slot.cancellation(epoch).unwrap();

    assert_eq!(
        slot.reconcile(
            &registry,
            DesiredComponentState::removed(slot.generation(1))
        ),
        Err(ComponentSlotError::StaleDesired {
            current: slot.generation(2),
            received: slot.generation(1),
        })
    );
    assert_eq!(
        slot.reconcile(
            &registry,
            DesiredComponentState::disabled(slot.generation(2), revision.clone())
        ),
        Err(ComponentSlotError::DesiredGenerationConflict {
            generation: slot.generation(2),
        })
    );
    assert_eq!(
        slot.live_state().unwrap().kind(),
        yah_compose::ComponentStateKind::Active
    );
    assert!(!cancellation.is_cancelled());
    assert_eq!(slot.desired(), Some(&desired));
}

#[test]
fn a_revision_identity_cannot_be_reused_with_different_content() {
    let registry = ServiceRegistry::new();
    let original = empty_revision("test.revision-a");
    let mut conflicting_definition = ComponentDefinition::new("test.component");
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    conflicting_definition.require(&service.required()).unwrap();
    let conflicting = revision(
        "test.revision-a",
        conflicting_definition,
        "test.other-scope",
    );
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(
            &registry,
            enabled(&slot, 1, &original, &ProviderAssignments::new()),
        )
        .unwrap(),
    );
    slot.complete_start(epoch, &registry).unwrap();
    let cancellation = slot.cancellation(epoch).unwrap();

    assert_eq!(
        slot.reconcile(
            &registry,
            enabled(&slot, 2, &conflicting, &ProviderAssignments::new())
        ),
        Err(ComponentSlotError::RevisionIdentityConflict {
            revision: original.id().clone(),
        })
    );
    assert_eq!(slot.desired_generation(), Some(slot.generation(1)));
    assert_eq!(slot.applied_revision(), Some(original.id()));
    assert!(!cancellation.is_cancelled());
}

#[test]
fn a_revision_identity_cannot_reuse_a_fresh_scope_with_the_same_label() {
    let registry = ServiceRegistry::new();
    let definition = ComponentDefinition::new("test.component");
    let original = ComponentRevision::new(
        "test.revision-a",
        definition.clone(),
        Scope::root("same.scope"),
    );
    let conflicting =
        ComponentRevision::new("test.revision-a", definition, Scope::root("same.scope"));
    let mut slot = ComponentSlot::new("test.slot").unwrap();
    let epoch = start_epoch(
        slot.reconcile(
            &registry,
            enabled(&slot, 1, &original, &ProviderAssignments::new()),
        )
        .unwrap(),
    );
    slot.complete_start(epoch, &registry).unwrap();
    let cancellation = slot.cancellation(epoch).unwrap();

    assert_eq!(
        slot.reconcile(
            &registry,
            enabled(&slot, 2, &conflicting, &ProviderAssignments::new()),
        ),
        Err(ComponentSlotError::RevisionIdentityConflict {
            revision: original.id().clone(),
        })
    );
    assert!(!cancellation.is_cancelled());
}

#[test]
fn disabled_intent_has_no_mount_and_pending_disable_unmounts_immediately() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut definition = ComponentDefinition::new("test.consumer");
    definition.require(&service.required()).unwrap();
    let revision = revision("test.revision-a", definition, "test.scope");
    let registry = ServiceRegistry::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();

    assert_eq!(
        slot.reconcile(
            &registry,
            DesiredComponentState::disabled(slot.generation(1), revision.clone())
        )
        .unwrap(),
        ComponentSlotOutcome::Disabled {
            generation: slot.generation(1),
            desired_revision: revision.id().clone(),
        }
    );
    assert!(slot.live_state().is_none());

    assert!(matches!(
        slot.reconcile(
            &registry,
            enabled(&slot, 2, &revision, &ProviderAssignments::new())
        )
        .unwrap(),
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::Pending { .. },
            ..
        }
    ));
    assert_eq!(slot.live_state(), Some(&ComponentState::Pending));

    assert_eq!(
        slot.reconcile(
            &registry,
            DesiredComponentState::disabled(slot.generation(3), revision.clone())
        )
        .unwrap(),
        ComponentSlotOutcome::Unmounted {
            generation: slot.generation(3),
            applied_revision: revision.id().clone(),
            reason: yah_compose::DesiredStopReason::Disabled,
        }
    );
    assert!(slot.live_state().is_none());
}

#[test]
fn pending_revision_replacement_and_removal_are_immediate_and_observable() {
    let greeting = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut first_definition = ComponentDefinition::new("test.consumer-a");
    first_definition.require(&greeting.required()).unwrap();
    let first = revision("test.revision-a", first_definition, "test.scope");
    let count = ServiceDefinition::<Count>::new("test.count/v1");
    let mut second_definition = ComponentDefinition::new("test.consumer-b");
    second_definition.require(&count.required()).unwrap();
    let second = revision("test.revision-b", second_definition, "test.scope");
    let registry = ServiceRegistry::new();
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();

    assert!(matches!(
        slot.reconcile(&registry, enabled(&slot, 1, &first, &assignments))
            .unwrap(),
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::Pending { .. },
            ..
        }
    ));
    let desired_second = enabled(&slot, 2, &second, &assignments);
    assert_eq!(
        slot.reconcile(&registry, desired_second.clone()).unwrap(),
        ComponentSlotOutcome::Unmounted {
            generation: slot.generation(2),
            applied_revision: first.id().clone(),
            reason: yah_compose::DesiredStopReason::RevisionChanged {
                previous: first.id().clone(),
                desired: second.id().clone(),
            },
        }
    );
    assert!(slot.applied_revision().is_none());
    assert!(matches!(
        slot.reconcile(&registry, desired_second).unwrap(),
        ComponentSlotOutcome::Mounted {
            applied_revision,
            component: ReconcileOutcome::Pending { .. },
            ..
        } if applied_revision == *second.id()
    ));
    assert_eq!(slot.applied_revision(), Some(second.id()));

    assert_eq!(
        slot.reconcile(
            &registry,
            DesiredComponentState::removed(slot.generation(3))
        )
        .unwrap(),
        ComponentSlotOutcome::Unmounted {
            generation: slot.generation(3),
            applied_revision: second.id().clone(),
            reason: yah_compose::DesiredStopReason::Removed,
        }
    );
    assert_eq!(
        slot.reconcile(
            &registry,
            DesiredComponentState::removed(slot.generation(3))
        )
        .unwrap(),
        ComponentSlotOutcome::Removed {
            generation: slot.generation(3),
        }
    );
}

#[test]
fn recreated_semantic_slot_rejects_a_generation_from_the_old_incarnation() {
    let original = ComponentSlot::new("test.slot").unwrap();
    let stale = DesiredComponentState::removed(original.generation(7));
    drop(original);

    let mut replacement = ComponentSlot::new("test.slot").unwrap();
    assert_eq!(
        replacement.reconcile(&ServiceRegistry::new(), stale.clone()),
        Err(ComponentSlotError::ForeignDesiredGeneration {
            expected: replacement.generation(7),
            received: stale.generation(),
        })
    );
    assert!(replacement.desired().is_none());
    assert!(replacement.live_state().is_none());
}

#[test]
fn historical_revision_identity_remains_immutable_after_switching_away() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut definition_a = ComponentDefinition::new("test.consumer-a");
    definition_a.require(&service.required()).unwrap();
    let revision_a = revision("test.revision-a", definition_a, "test.scope");
    let mut definition_b = ComponentDefinition::new("test.consumer-b");
    definition_b.require(&service.required()).unwrap();
    let revision_b = revision("test.revision-b", definition_b, "test.scope");
    let conflicting_a = empty_revision("test.revision-a");
    let registry = ServiceRegistry::new();
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("test.slot").unwrap();

    slot.reconcile(&registry, enabled(&slot, 1, &revision_a, &assignments))
        .unwrap();
    let desired_b = enabled(&slot, 2, &revision_b, &assignments);
    slot.reconcile(&registry, desired_b.clone()).unwrap();
    slot.reconcile(&registry, desired_b).unwrap();
    assert_eq!(slot.applied_revision(), Some(revision_b.id()));

    assert_eq!(
        slot.reconcile(&registry, enabled(&slot, 3, &conflicting_a, &assignments)),
        Err(ComponentSlotError::RevisionIdentityConflict {
            revision: revision_a.id().clone(),
        })
    );
    assert_eq!(slot.desired_generation(), Some(slot.generation(2)));
    assert_eq!(slot.applied_revision(), Some(revision_b.id()));
    assert_eq!(slot.live_state(), Some(&ComponentState::Pending));
}
