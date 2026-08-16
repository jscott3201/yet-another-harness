use std::sync::OnceLock;

use yah_compose::{
    ComponentDefinition, ComponentDefinitionError, ComponentInstance, ComponentState, EffectScope,
    EffectScopeError, EffectScopeState, RequirementStatus, Scope, ServiceDefinition,
    ServiceRegistry, ServiceRegistryError,
};

#[derive(Debug)]
struct Greeting(&'static str);

#[derive(Debug)]
struct Count(u64);

fn composition_scope() -> &'static Scope {
    static SCOPE: OnceLock<Scope> = OnceLock::new();
    SCOPE.get_or_init(|| Scope::root("service-registry.tests"))
}

fn active_instance(label: &str) -> (ComponentInstance, EffectScope) {
    let definition = ComponentDefinition::new(format!("{label}.component"));
    let mut instance = ComponentInstance::new(
        format!("{label}.instance"),
        &definition,
        composition_scope(),
    )
    .unwrap();
    let activation = instance.begin_start().unwrap();
    let effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    instance.complete_start(activation).unwrap();
    (instance, effects)
}

fn starting_instance(
    label: &str,
    definition: &ComponentDefinition,
) -> (ComponentInstance, EffectScope) {
    let mut instance =
        ComponentInstance::new(format!("{label}.instance"), definition, composition_scope())
            .unwrap();
    let activation = instance.begin_start().unwrap();
    let effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    (instance, effects)
}

#[test]
fn component_definitions_record_each_required_service_once() {
    let greeting = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let count = ServiceDefinition::<Count>::new("test.count/v1");
    let mut definition = ComponentDefinition::new("test.consumer");

    definition.require(&greeting.required()).unwrap();
    definition.require(&count.required()).unwrap();

    assert_eq!(definition.requirements().len(), 2);
    assert_eq!(definition.requirements()[0].service_id(), greeting.id());
    assert_eq!(definition.requirements()[1].service_id(), count.id());
    assert_eq!(
        definition.require(&greeting.required()),
        Err(ComponentDefinitionError::DuplicateRequirement {
            component_id: definition.id().clone(),
            service_id: greeting.id().clone(),
        })
    );

    let conflicting = ServiceDefinition::<u64>::new("test.greeting/v1");
    assert!(matches!(
        definition.require(&conflicting.required()),
        Err(ComponentDefinitionError::DuplicateRequirement { service_id, .. })
            if service_id == *greeting.id()
    ));
}

#[test]
fn deterministic_missing_requirements_leave_the_consumer_pending() {
    let greeting = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let count = ServiceDefinition::<Count>::new("test.count/v1");
    let mut consumer_definition = ComponentDefinition::new("test.consumer");
    consumer_definition.require(&greeting.required()).unwrap();
    consumer_definition.require(&count.required()).unwrap();
    let consumer_scope = composition_scope().clone();
    let consumer =
        ComponentInstance::new("consumer.instance", &consumer_definition, &consumer_scope).unwrap();
    let mut registry = ServiceRegistry::new();

    assert_eq!(
        registry
            .requirement_status(&consumer_definition, &consumer_scope)
            .unwrap(),
        RequirementStatus::Missing(vec![greeting.id().clone(), count.id().clone()])
    );
    assert_eq!(consumer.state(), &ComponentState::Pending);

    let (greeting_owner, mut greeting_effects) = active_instance("greeting");
    registry
        .provide(
            &greeting_owner,
            &mut greeting_effects,
            greeting.provider(Greeting("hello")),
        )
        .unwrap();
    assert_eq!(
        registry
            .requirement_status(&consumer_definition, &consumer_scope)
            .unwrap(),
        RequirementStatus::Missing(vec![count.id().clone()])
    );
    assert_eq!(consumer.state(), &ComponentState::Pending);

    let (count_owner, mut count_effects) = active_instance("count");
    registry
        .provide(&count_owner, &mut count_effects, count.provider(Count(7)))
        .unwrap();
    assert_eq!(
        registry
            .requirement_status(&consumer_definition, &consumer_scope)
            .unwrap(),
        RequirementStatus::Ready
    );
}

#[test]
fn semantic_ids_and_rust_contract_types_both_participate_in_matching() {
    let greeting = ServiceDefinition::<Greeting>::new("test.service/v1");
    let wrong_type = ServiceDefinition::<Count>::new("test.service/v1");
    let other_id = ServiceDefinition::<Greeting>::new("test.other/v1");
    let (owner, mut effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let published = registry
        .provide(&owner, &mut effects, greeting.provider(Greeting("hello")))
        .unwrap();

    assert_eq!(
        registry.candidates(&wrong_type.required(), owner.scope()),
        Err(ServiceRegistryError::ContractTypeMismatch {
            service_id: greeting.id().clone(),
            expected: std::any::type_name::<Greeting>(),
            received: std::any::type_name::<Count>(),
        })
    );
    assert!(
        registry
            .candidates(&other_id.required(), owner.scope())
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        registry
            .candidates(&greeting.required(), owner.scope())
            .unwrap(),
        vec![published]
    );
}

#[test]
fn publication_requires_an_active_owner_and_its_exact_activation_scope() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let pending_definition = ComponentDefinition::new("pending.component");
    let pending_scope = Scope::root("pending.scope");
    let pending =
        ComponentInstance::new("pending.instance", &pending_definition, &pending_scope).unwrap();
    let (_, mut unrelated_effects) = active_instance("unrelated");
    let mut registry = ServiceRegistry::new();

    assert!(matches!(
        registry.provide(
            &pending,
            &mut unrelated_effects,
            service.provider(Greeting("never visible")),
        ),
        Err(ServiceRegistryError::ProviderOwnerNotActive { .. })
    ));

    let (owner, _) = active_instance("owner");
    assert!(matches!(
        registry.provide(
            &owner,
            &mut unrelated_effects,
            service.provider(Greeting("wrong activation")),
        ),
        Err(ServiceRegistryError::ActivationMismatch { .. })
    ));
    assert!(
        registry
            .candidates(&service.required(), pending.scope())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_sealed_effect_scope_rejects_publication_without_visibility() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let (owner, mut effects) = active_instance("provider");
    let scope_id = effects.id();
    drop(effects.close());
    let mut registry = ServiceRegistry::new();

    assert_eq!(
        registry.provide(
            &owner,
            &mut effects,
            service.provider(Greeting("never visible")),
        ),
        Err(ServiceRegistryError::EffectScope(
            EffectScopeError::NotOpen {
                scope_id,
                state: EffectScopeState::Closing,
            }
        ))
    );
    assert!(
        registry
            .candidates(&service.required(), owner.scope())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn multiple_candidates_use_id_order_not_publication_order_or_implicit_selection() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let requirement = service.required();
    let (first_owner, mut first_effects) = active_instance("first");
    let (second_owner, mut second_effects) = active_instance("second");
    let mut registry = ServiceRegistry::new();
    let second = registry
        .provide(
            &second_owner,
            &mut second_effects,
            service.provider(Greeting("second")),
        )
        .unwrap();
    let first = registry
        .provide(
            &first_owner,
            &mut first_effects,
            service.provider(Greeting("first")),
        )
        .unwrap();

    assert!(first.id() < second.id());
    assert_eq!(
        registry
            .candidates(&requirement, first_owner.scope())
            .unwrap(),
        vec![first.clone(), second.clone()]
    );

    let mut consumer_definition = ComponentDefinition::new("consumer.component");
    consumer_definition.require(&requirement).unwrap();
    let (first_consumer, first_consumer_effects) =
        starting_instance("first-consumer", &consumer_definition);
    let (second_consumer, second_consumer_effects) =
        starting_instance("second-consumer", &consumer_definition);
    let first_handle = registry
        .bind(
            &first_consumer,
            &first_consumer_effects,
            &requirement,
            first.id(),
        )
        .unwrap();
    let second_handle = registry
        .bind(
            &second_consumer,
            &second_consumer_effects,
            &requirement,
            second.id(),
        )
        .unwrap();

    assert_eq!(
        first_handle.try_with(|greeting| greeting.0).unwrap(),
        "first"
    );
    assert_eq!(
        second_handle.try_with(|greeting| greeting.0).unwrap(),
        "second"
    );
}

#[test]
fn exact_binding_rejects_a_provider_for_another_service() {
    let greeting = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let other = ServiceDefinition::<Greeting>::new("test.other/v1");
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            other.provider(Greeting("other")),
        )
        .unwrap();
    let mut consumer_definition = ComponentDefinition::new("consumer.component");
    consumer_definition.require(&greeting.required()).unwrap();
    let (consumer, consumer_effects) = starting_instance("consumer", &consumer_definition);
    let provider_id = candidate.id();

    assert!(matches!(
        registry.bind(
            &consumer,
            &consumer_effects,
            &greeting.required(),
            candidate.id(),
        ),
        Err(ServiceRegistryError::ProviderDoesNotSatisfy {
            provider_id: received_id,
            required,
            provided,
        }) if received_id == provider_id
            && required == *greeting.id()
            && provided == *other.id()
    ));
}

#[test]
fn sized_provider_payloads_remain_shared_without_requiring_clone() {
    let count = ServiceDefinition::<Count>::new("test.count/v1");
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(&owner, &mut provider_effects, count.provider(Count(42)))
        .unwrap();
    let mut consumer_definition = ComponentDefinition::new("consumer.component");
    consumer_definition.require(&count.required()).unwrap();
    let (consumer, consumer_effects) = starting_instance("consumer", &consumer_definition);
    let handle = registry
        .bind(
            &consumer,
            &consumer_effects,
            &count.required(),
            candidate.id(),
        )
        .unwrap();

    assert_eq!(handle.try_with(|value| value.0).unwrap(), 42);
}
