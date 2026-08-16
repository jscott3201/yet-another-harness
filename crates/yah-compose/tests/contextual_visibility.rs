use yah_compose::{
    ComponentDefinition, ComponentInstance, DependencyIssue, DependencyReadiness, EffectScope,
    ProviderAssignments, ReconcileOutcome, ReconciledComponent, Scope, ServiceDefinition,
    ServiceRegistry, ServiceRegistryError,
};

#[derive(Debug)]
struct Greeting(#[allow(dead_code)] &'static str);

fn active(label: &str, scope: &Scope) -> (ComponentInstance, EffectScope) {
    let def = ComponentDefinition::new(format!("{label}.component"));
    let mut instance = ComponentInstance::new(format!("{label}.instance"), &def, scope).unwrap();
    let epoch = instance.begin_start().unwrap();
    let effects = EffectScope::new(format!("{label}.effects"), epoch).unwrap();
    instance.complete_start(epoch).unwrap();
    (instance, effects)
}

#[test]
fn reconciliation_uses_the_same_visibility_for_inventory_binding_and_withdrawal() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let root = Scope::root("root");
    let hidden_branch = Scope::child("hidden", &root).unwrap();
    let consumer_scope = Scope::child("consumer", &root).unwrap();
    let (visible_owner, mut visible_effects) = active("visible", &root);
    let (hidden_owner, mut hidden_effects) = active("hidden", &hidden_branch);
    let mut registry = ServiceRegistry::new();
    let visible = registry
        .provide(
            &visible_owner,
            &mut visible_effects,
            service.provider(Greeting("visible")),
        )
        .unwrap();
    let hidden = registry
        .provide(
            &hidden_owner,
            &mut hidden_effects,
            service.provider(Greeting("hidden")),
        )
        .unwrap();
    let mut definition = ComponentDefinition::new("consumer");
    definition.require(&service.required()).unwrap();
    let mut consumer =
        ReconciledComponent::mount("consumer.instance", definition, &consumer_scope).unwrap();
    let mut assignments = ProviderAssignments::new();
    assignments.assign(&hidden);

    assert_eq!(
        consumer.readiness(&registry, &assignments).unwrap(),
        DependencyReadiness::Pending(vec![DependencyIssue::AssignedProviderUnavailable {
            service_id: service.id().clone(),
            assigned: hidden.id(),
            available: vec![visible.id()],
        }])
    );
    assert!(matches!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::Pending { .. }
    ));

    assignments.assign(&visible);
    let selection_epoch = match consumer.reconcile(&registry, &assignments).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected visible provider to start, got {outcome:?}"),
    };
    let handle = consumer
        .bind(selection_epoch, &registry, &service.required())
        .unwrap();
    consumer
        .complete_start(selection_epoch, &registry, &assignments)
        .unwrap();
    assert_eq!(handle.try_with(|value| value.0).unwrap(), "visible");

    drop(visible_effects.close());
    assert!(matches!(
        consumer.reconcile(&registry, &assignments).unwrap(),
        ReconcileOutcome::StopBegun { .. }
    ));
}

#[test]
fn providers_flow_to_descendants_but_not_ancestors_siblings_or_other_roots() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let root = Scope::root("root");
    let child = Scope::child("child", &root).unwrap();
    let grandchild = Scope::child("grandchild", &child).unwrap();
    let sibling = Scope::child("sibling", &root).unwrap();
    let other = Scope::root("root");
    let (owner, mut effects) = active("owner", &child);
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(&owner, &mut effects, service.provider(Greeting("hi")))
        .unwrap();
    assert_eq!(
        registry.candidates(&service.required(), &child).unwrap(),
        vec![candidate.clone()]
    );
    assert_eq!(
        registry
            .candidates(&service.required(), &grandchild)
            .unwrap(),
        vec![candidate.clone()]
    );
    assert_eq!(
        registry.candidates(&service.required(), &root).unwrap(),
        Vec::<_>::new()
    );
    assert_eq!(
        registry.candidates(&service.required(), &sibling).unwrap(),
        Vec::<_>::new()
    );
    assert_eq!(
        registry.candidates(&service.required(), &other).unwrap(),
        Vec::<_>::new()
    );
}

#[test]
fn exact_invisible_provider_is_rejected_without_an_information_oracle() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let other = ServiceDefinition::<Greeting>::new("test.other/v1");
    let root = Scope::root("root");
    let child = Scope::child("child", &root).unwrap();
    let (owner, mut provider_effects) = active("owner", &child);
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(Greeting("hi")),
        )
        .unwrap();
    let definition = {
        let mut d = ComponentDefinition::new("consumer");
        d.require(&service.required()).unwrap();
        d
    };
    let mut consumer = ComponentInstance::new("consumer", &definition, &root).unwrap();
    let epoch = consumer.begin_start().unwrap();
    let effects = EffectScope::new("consumer.effects", epoch).unwrap();
    assert!(matches!(
        registry.bind(&consumer, &effects, &service.required(), candidate.id()),
        Err(ServiceRegistryError::ProviderUnavailable { .. })
    ));
    assert!(matches!(
        registry.bind(&consumer, &effects, &other.required(), candidate.id()),
        Err(ServiceRegistryError::ProviderUnavailable { .. })
    ));
}
