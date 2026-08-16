use yah_compose::{
    ComponentDefinition, ComponentInstance, ComponentState, DependencyIssue, DependencyReadiness,
    DependencyStopReason, EffectScope, ProviderAssignments, ProviderCandidate,
    ProviderSelectionEpoch, ReconcileError, ReconcileOutcome, ReconciledComponent, Scope,
    ServiceDefinition, ServiceHandleError, ServiceRegistry, StopCompletion,
};

#[derive(Debug)]
struct Greeting(&'static str);

#[derive(Debug)]
struct Count;

struct PublishedGreeting {
    effects: EffectScope,
    candidate: ProviderCandidate,
}

fn publish_greeting(
    registry: &mut ServiceRegistry,
    service: &ServiceDefinition<Greeting>,
    label: &str,
    value: &'static str,
) -> PublishedGreeting {
    let definition = ComponentDefinition::new(format!("{label}.component"));
    let scope = Scope::root(format!("{label}.scope"));
    let mut owner =
        ComponentInstance::new(format!("{label}.instance"), &definition, &scope).unwrap();
    let activation = owner.begin_start().unwrap();
    let mut effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    owner.complete_start(activation).unwrap();
    let candidate = registry
        .provide(&owner, &mut effects, service.provider(Greeting(value)))
        .unwrap();
    PublishedGreeting { effects, candidate }
}

fn greeting_consumer(
    service: &ServiceDefinition<Greeting>,
    instance_id: &str,
) -> ReconciledComponent {
    let mut definition = ComponentDefinition::new("test.greeting-consumer");
    definition.require(&service.required()).unwrap();
    let scope = Scope::root("test.consumer-scope");
    ReconciledComponent::mount(instance_id, definition, &scope).unwrap()
}

fn start_greeting_consumer(
    consumer: &mut ReconciledComponent,
    registry: &ServiceRegistry,
    service: &ServiceDefinition<Greeting>,
    assignments: &ProviderAssignments,
) -> (ProviderSelectionEpoch, yah_compose::ServiceHandle<Greeting>) {
    let selection_epoch = match consumer.reconcile(registry, assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected a fresh start, got {outcome:?}"),
    };
    let handle = consumer
        .bind(selection_epoch, registry, &service.required())
        .unwrap();
    assert_eq!(
        consumer
            .complete_start(selection_epoch, registry, assignments)
            .unwrap(),
        ReconcileOutcome::Active { selection_epoch }
    );
    (selection_epoch, handle)
}

#[test]
fn pending_diagnostics_distinguish_missing_unassigned_and_ambiguous() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let mut consumer = greeting_consumer(&service, "consumer.instance");
    let assignments = ProviderAssignments::new();

    assert_eq!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Pending {
            readiness: DependencyReadiness::Pending(vec![DependencyIssue::MissingProvider {
                service_id: service.id().clone(),
            }]),
        }
    );
    assert_eq!(consumer.state(), &ComponentState::Pending);

    let first = publish_greeting(&mut registry, &service, "first", "hello");
    assert_eq!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Pending {
            readiness: DependencyReadiness::Pending(vec![DependencyIssue::Unassigned {
                service_id: service.id().clone(),
                candidate: first.candidate.id(),
            }]),
        }
    );

    let second = publish_greeting(&mut registry, &service, "second", "bonjour");
    assert_eq!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Pending {
            readiness: DependencyReadiness::Pending(vec![DependencyIssue::Ambiguous {
                service_id: service.id().clone(),
                candidates: vec![first.candidate.id(), second.candidate.id()],
            }]),
        }
    );
    assert_eq!(consumer.state(), &ComponentState::Pending);

    let mut selected = assignments;
    selected.assign(&second.candidate);
    let selection = match consumer.reconcile(&registry, &selected).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection,
        outcome => panic!("expected explicit selection to start, got {outcome:?}"),
    };
    assert_eq!(selection.providers(), &[second.candidate]);
}

#[test]
fn multiple_requirements_resolve_in_declaration_not_publication_order() {
    let first_service = ServiceDefinition::<Greeting>::new("test.first/v1");
    let second_service = ServiceDefinition::<Greeting>::new("test.second/v1");
    let mut registry = ServiceRegistry::new();
    let second = publish_greeting(&mut registry, &second_service, "second", "second");
    let mut definition = ComponentDefinition::new("test.multi-consumer");
    definition.require(&first_service.required()).unwrap();
    definition.require(&second_service.required()).unwrap();
    let scope = Scope::root("test.multi-scope");
    let mut consumer =
        ReconciledComponent::mount("test.multi-instance", definition, &scope).unwrap();
    let assignments = ProviderAssignments::new();

    assert_eq!(
        consumer.readiness(&registry, &assignments).unwrap(),
        DependencyReadiness::Pending(vec![
            DependencyIssue::MissingProvider {
                service_id: first_service.id().clone(),
            },
            DependencyIssue::Unassigned {
                service_id: second_service.id().clone(),
                candidate: second.candidate.id(),
            },
        ])
    );

    let first = publish_greeting(&mut registry, &first_service, "first", "first");
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&first.candidate);
    assignments.assign(&second.candidate);
    let selection = match consumer.reconcile(&registry, &assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection,
        outcome => panic!("expected complete assignment to start, got {outcome:?}"),
    };
    assert!(second.candidate.id() < first.candidate.id());
    assert_eq!(selection.providers(), &[first.candidate, second.candidate]);
}

#[test]
fn exact_assignment_starts_binds_and_remains_stable() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let published = publish_greeting(&mut registry, &service, "provider", "hello");
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut consumer = greeting_consumer(&service, "consumer.instance");

    assert_eq!(
        consumer.readiness(&registry, &assignments).unwrap(),
        DependencyReadiness::Ready
    );

    let (selection_epoch, handle) =
        start_greeting_consumer(&mut consumer, &registry, &service, &assignments);

    assert_eq!(handle.try_with(|value| value.0).unwrap(), "hello");
    assert_eq!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Active { selection_epoch }
    );
    assert_eq!(selection_epoch.activation().sequence(), 1);
    assert_eq!(
        consumer.selection().unwrap().providers(),
        &[published.candidate]
    );
}

#[test]
fn an_empty_requirement_set_is_immediately_eligible() {
    let registry = ServiceRegistry::new();
    let definition = ComponentDefinition::new("test.no-dependencies");
    let scope = Scope::root("test.no-dependencies-scope");
    let mut component =
        ReconciledComponent::mount("test.no-dependencies-instance", definition, &scope).unwrap();
    let assignments = ProviderAssignments::new();

    let selection = match component.reconcile(&registry, &assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection,
        outcome => panic!("expected start, got {outcome:?}"),
    };
    assert!(selection.providers().is_empty());
    assert_eq!(
        component
            .complete_start(selection.epoch(), &registry, &assignments)
            .unwrap(),
        ReconcileOutcome::Active {
            selection_epoch: selection.epoch(),
        }
    );
}

#[test]
fn an_extra_candidate_does_not_override_an_explicit_assignment() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish_greeting(&mut registry, &service, "first", "first");
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&first.candidate);
    let mut consumer = greeting_consumer(&service, "consumer.instance");
    let (selection_epoch, handle) =
        start_greeting_consumer(&mut consumer, &registry, &service, &assignments);

    let _second = publish_greeting(&mut registry, &service, "second", "second");

    assert_eq!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Active { selection_epoch }
    );
    assert_eq!(handle.try_with(|value| value.0).unwrap(), "first");
}

#[tokio::test]
async fn assignment_change_cancels_old_handles_before_replacement_starts() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish_greeting(&mut registry, &service, "first", "first");
    let second = publish_greeting(&mut registry, &service, "second", "second");
    let mut desired = ProviderAssignments::new();
    desired.assign(&first.candidate);
    let mut consumer = greeting_consumer(&service, "consumer.instance");
    let (first_epoch, first_handle) =
        start_greeting_consumer(&mut consumer, &registry, &service, &desired);

    desired.assign(&second.candidate);
    assert!(matches!(
        consumer.reconcile(&registry, &desired).unwrap(),
        ReconcileOutcome::StopBegun {
            selection_epoch,
            reason: DependencyStopReason::AssignmentChanged(_),
        } if selection_epoch == first_epoch
    ));
    assert!(matches!(
        first_handle.try_with(|value| value.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert_eq!(
        consumer.state().kind(),
        yah_compose::ComponentStateKind::Stopping
    );

    assert!(matches!(
        consumer.finish_stop(first_epoch).await.unwrap(),
        StopCompletion::Completed { selection_epoch, .. } if selection_epoch == first_epoch
    ));
    assert_eq!(consumer.state(), &ComponentState::Pending);

    let (second_epoch, second_handle) =
        start_greeting_consumer(&mut consumer, &registry, &service, &desired);
    assert_ne!(second_epoch, first_epoch);
    assert_eq!(second_epoch.activation().sequence(), 2);
    assert_eq!(second_handle.try_with(|value| value.0).unwrap(), "second");
    assert!(matches!(
        consumer.bind(first_epoch, &registry, &service.required()),
        Err(ReconcileError::InvalidState { .. }) | Err(ReconcileError::StaleSelection { .. })
    ));
}

#[tokio::test]
async fn recomposition_withdraws_services_owned_by_the_old_activation() {
    let upstream = ServiceDefinition::<Greeting>::new("test.upstream/v1");
    let downstream = ServiceDefinition::<Greeting>::new("test.downstream/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish_greeting(&mut registry, &upstream, "first", "first");
    let second = publish_greeting(&mut registry, &upstream, "second", "second");
    let mut desired = ProviderAssignments::new();
    desired.assign(&first.candidate);
    let mut middle = greeting_consumer(&upstream, "middle.instance");
    let (middle_epoch, _upstream_handle) =
        start_greeting_consumer(&mut middle, &registry, &upstream, &desired);
    let downstream_candidate = middle
        .provide(
            middle_epoch,
            &mut registry,
            downstream.provider(Greeting("middle")),
        )
        .unwrap();

    let mut downstream_assignments = ProviderAssignments::new();
    downstream_assignments.assign(&downstream_candidate);
    let mut leaf = greeting_consumer(&downstream, "leaf.instance");
    let (_leaf_epoch, downstream_handle) =
        start_greeting_consumer(&mut leaf, &registry, &downstream, &downstream_assignments);
    assert_eq!(
        downstream_handle.try_with(|value| value.0).unwrap(),
        "middle"
    );

    desired.assign(&second.candidate);
    middle.reconcile(&registry, &desired).unwrap();

    assert!(matches!(
        downstream_handle.try_with(|value| value.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert!(
        registry
            .candidates(&downstream.required())
            .unwrap()
            .is_empty()
    );
    middle.finish_stop(middle_epoch).await.unwrap();
}

#[tokio::test]
async fn provider_withdrawal_revokes_immediately_then_reconciles_to_pending() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let mut published = publish_greeting(&mut registry, &service, "provider", "hello");
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut consumer = greeting_consumer(&service, "consumer.instance");
    let (selection_epoch, handle) =
        start_greeting_consumer(&mut consumer, &registry, &service, &assignments);

    drop(published.effects.close());
    assert!(matches!(
        handle.try_with(|value| value.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert!(matches!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::StopBegun {
            reason: DependencyStopReason::ProviderUnavailable(_),
            ..
        }
    ));
    assert!(
        consumer
            .cancellation(selection_epoch)
            .unwrap()
            .is_cancelled()
    );
    assert!(
        consumer
            .finish_stop(selection_epoch)
            .await
            .unwrap()
            .is_completed()
    );

    assert!(matches!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Pending {
            readiness: DependencyReadiness::Pending(_),
        }
    ));
}

#[tokio::test]
async fn provider_loss_during_start_prevents_active_publication() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let mut published = publish_greeting(&mut registry, &service, "provider", "hello");
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut consumer = greeting_consumer(&service, "consumer.instance");
    let selection_epoch = match consumer.reconcile(&registry, &assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };

    drop(published.effects.close());
    assert!(matches!(
        consumer.bind(selection_epoch, &registry, &service.required()),
        Err(ReconcileError::Registry(_))
    ));
    assert!(matches!(
        consumer
            .complete_start(selection_epoch, &registry, &assignments)
            .unwrap(),
        ReconcileOutcome::StopBegun {
            reason: DependencyStopReason::ProviderUnavailable(_),
            ..
        }
    ));
    assert_eq!(
        consumer.state().kind(),
        yah_compose::ComponentStateKind::Stopping
    );
    assert!(
        consumer
            .finish_stop(selection_epoch)
            .await
            .unwrap()
            .is_completed()
    );
    assert_eq!(consumer.state(), &ComponentState::Pending);
}

#[tokio::test]
async fn selection_change_during_start_rejects_delayed_completion() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let first = publish_greeting(&mut registry, &service, "first", "first");
    let second = publish_greeting(&mut registry, &service, "second", "second");
    let mut first_assignment = ProviderAssignments::new();
    first_assignment.assign(&first.candidate);
    let mut second_assignment = ProviderAssignments::new();
    second_assignment.assign(&second.candidate);
    let mut consumer = greeting_consumer(&service, "consumer.instance");
    let selection_epoch = match consumer.reconcile(&registry, &first_assignment).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };

    assert!(matches!(
        consumer
            .complete_start(selection_epoch, &registry, &second_assignment)
            .unwrap(),
        ReconcileOutcome::StopBegun {
            reason: DependencyStopReason::AssignmentChanged(_),
            ..
        }
    ));
    assert!(matches!(
        consumer.complete_start(selection_epoch, &registry, &first_assignment),
        Err(ReconcileError::InvalidState { .. })
    ));
    consumer.finish_stop(selection_epoch).await.unwrap();
}

#[test]
fn invalid_assignment_and_requirement_inputs_do_not_mutate_lifecycle() {
    let greeting = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let count = ServiceDefinition::<Count>::new("test.count/v1");
    let conflicting = ServiceDefinition::<Count>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let greeting_provider = publish_greeting(&mut registry, &greeting, "greeting", "hello");
    let count_definition = ComponentDefinition::new("count.provider");
    let count_scope = Scope::root("count.scope");
    let mut count_owner =
        ComponentInstance::new("count.instance", &count_definition, &count_scope).unwrap();
    let count_activation = count_owner.begin_start().unwrap();
    let mut count_effects = EffectScope::new("count.effects", count_activation).unwrap();
    count_owner.complete_start(count_activation).unwrap();
    let count_candidate = registry
        .provide(&count_owner, &mut count_effects, count.provider(Count))
        .unwrap();
    let mut desired = ProviderAssignments::new();
    desired.assign(&greeting_provider.candidate);
    let mut consumer = greeting_consumer(&greeting, "consumer.instance");
    let selection_epoch = match consumer.reconcile(&registry, &desired).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };

    assert!(matches!(
        consumer.bind(selection_epoch, &registry, &count.required()),
        Err(ReconcileError::UndeclaredRequirement { .. })
    ));
    assert!(matches!(
        consumer.bind(selection_epoch, &registry, &conflicting.required()),
        Err(ReconcileError::RequirementContractMismatch { .. })
    ));
    let greeting_handle = consumer
        .bind(selection_epoch, &registry, &greeting.required())
        .unwrap();
    consumer
        .complete_start(selection_epoch, &registry, &desired)
        .unwrap();

    let mut invalid = desired.clone();
    invalid.assign(&count_candidate);
    assert!(matches!(
        consumer.reconcile(&registry, &invalid),
        Err(ReconcileError::UndeclaredAssignment { .. })
    ));
    assert_eq!(
        consumer.state().kind(),
        yah_compose::ComponentStateKind::Active
    );
    assert_eq!(greeting_handle.try_with(|value| value.0).unwrap(), "hello");
}

#[tokio::test]
async fn reused_semantic_instance_id_cannot_reuse_an_old_selection_epoch() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let mut registry = ServiceRegistry::new();
    let published = publish_greeting(&mut registry, &service, "provider", "hello");
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&published.candidate);
    let mut first = greeting_consumer(&service, "reused.instance");
    let first_epoch = match first.reconcile(&registry, &assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };
    drop(first);

    let mut replacement = greeting_consumer(&service, "reused.instance");
    let replacement_epoch = match replacement.reconcile(&registry, &assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected replacement start, got {outcome:?}"),
    };
    assert_ne!(first_epoch, replacement_epoch);
    assert!(matches!(
        replacement.bind(first_epoch, &registry, &service.required()),
        Err(ReconcileError::StaleSelection { .. })
    ));
}
