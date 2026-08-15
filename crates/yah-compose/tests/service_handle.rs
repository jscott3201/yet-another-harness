use std::sync::{
    Arc, Barrier,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use yah_compose::{
    CleanupFailureKind, ComponentDefinition, ComponentInstance, EffectScope, Scope,
    ServiceDefinition, ServiceHandle, ServiceHandleError, ServiceRegistry, ServiceRegistryError,
};

#[derive(Debug)]
struct Greeting(&'static str);

fn active_instance(label: &str) -> (ComponentInstance, EffectScope) {
    let definition = ComponentDefinition::new(format!("{label}.component"));
    let scope = Scope::root(format!("{label}.scope"));
    let mut instance =
        ComponentInstance::new(format!("{label}.instance"), &definition, &scope).unwrap();
    let activation = instance.begin_start().unwrap();
    let effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    instance.complete_start(activation).unwrap();
    (instance, effects)
}

fn consumer(
    label: &str,
    service: &ServiceDefinition<Greeting>,
) -> (ComponentInstance, EffectScope) {
    let mut definition = ComponentDefinition::new(format!("{label}.component"));
    definition.require(&service.required()).unwrap();
    let scope = Scope::root(format!("{label}.scope"));
    let mut instance =
        ComponentInstance::new(format!("{label}.instance"), &definition, &scope).unwrap();
    let activation = instance.begin_start().unwrap();
    let effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    (instance, effects)
}

fn assert_revoked<T: ?Sized + Send + Sync + 'static>(handle: &ServiceHandle<T>) {
    assert!(matches!(
        handle.try_with(|_| ()),
        Err(ServiceHandleError::Revoked { .. })
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn unpolled_close_revokes_before_cleanup_and_cannot_remove_replacement() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let requirement = service.required();
    let (old_owner, mut old_effects) = active_instance("old-provider");
    let mut registry = ServiceRegistry::new();
    let old_candidate = registry
        .provide(
            &old_owner,
            &mut old_effects,
            service.provider(Greeting("old")),
        )
        .unwrap();
    let (old_consumer, old_consumer_effects) = consumer("old-consumer", &service);
    let old_handle = registry
        .bind(
            &old_consumer,
            &old_consumer_effects,
            &requirement,
            old_candidate.id(),
        )
        .unwrap();
    assert_eq!(old_handle.try_with(|value| value.0).unwrap(), "old");

    drop(old_effects.close());
    assert_revoked(&old_handle);
    assert!(registry.candidates(&requirement).unwrap().is_empty());
    let (stale_consumer, stale_consumer_effects) = consumer("stale-consumer", &service);
    assert!(matches!(
        registry.bind(
            &stale_consumer,
            &stale_consumer_effects,
            &requirement,
            old_candidate.id(),
        ),
        Err(ServiceRegistryError::ProviderUnavailable { provider_id })
            if provider_id == old_candidate.id()
    ));

    let (new_owner, mut new_effects) = active_instance("new-provider");
    let new_candidate = registry
        .provide(
            &new_owner,
            &mut new_effects,
            service.provider(Greeting("new")),
        )
        .unwrap();
    assert_ne!(old_candidate.id(), new_candidate.id());
    let (new_consumer, new_consumer_effects) = consumer("new-consumer", &service);
    let new_handle = registry
        .bind(
            &new_consumer,
            &new_consumer_effects,
            &requirement,
            new_candidate.id(),
        )
        .unwrap();

    let first_report = old_effects.close().await;
    let repeated_report = old_effects.close().await;
    assert_eq!(first_report, repeated_report);
    assert_eq!(first_report.cleanup_count(), 1);
    assert_eq!(
        registry.candidates(&requirement).unwrap(),
        vec![new_candidate]
    );
    assert_revoked(&old_handle);
    assert_eq!(new_handle.try_with(|value| value.0).unwrap(), "new");
}

#[test]
fn dropping_provider_or_consumer_scopes_fails_closed() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let requirement = service.required();
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(Greeting("hello")),
        )
        .unwrap();
    let (first_consumer, first_effects) = consumer("first-consumer", &service);
    let (second_consumer, second_effects) = consumer("second-consumer", &service);
    let first_handle = registry
        .bind(
            &first_consumer,
            &first_effects,
            &requirement,
            candidate.id(),
        )
        .unwrap();
    let second_handle = registry
        .bind(
            &second_consumer,
            &second_effects,
            &requirement,
            candidate.id(),
        )
        .unwrap();

    drop(first_effects);
    assert_revoked(&first_handle);
    assert_eq!(second_handle.try_with(|value| value.0).unwrap(), "hello");
    assert_eq!(registry.candidates(&requirement).unwrap().len(), 1);

    drop(provider_effects);
    assert_revoked(&second_handle);
    assert!(registry.candidates(&requirement).unwrap().is_empty());
}

#[test]
fn registry_drop_revokes_handles_even_while_the_effect_scope_survives() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let requirement = service.required();
    let (owner, mut provider_effects) = active_instance("provider");
    let (consumer, consumer_effects) = consumer("consumer", &service);
    let handle = {
        let mut registry = ServiceRegistry::new();
        let candidate = registry
            .provide(
                &owner,
                &mut provider_effects,
                service.provider(Greeting("hello")),
            )
            .unwrap();
        registry
            .bind(&consumer, &consumer_effects, &requirement, candidate.id())
            .unwrap()
    };

    assert_revoked(&handle);
}

#[test]
fn a_call_admitted_before_revocation_may_finish_but_later_calls_fail() {
    struct BlockingService {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let service = ServiceDefinition::<BlockingService>::new("test.blocking/v1");
    let requirement = service.required();
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(BlockingService {
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            }),
        )
        .unwrap();
    let mut definition = ComponentDefinition::new("consumer.component");
    definition.require(&requirement).unwrap();
    let scope = Scope::root("consumer.scope");
    let mut consumer = ComponentInstance::new("consumer.instance", &definition, &scope).unwrap();
    let activation = consumer.begin_start().unwrap();
    let effects = EffectScope::new("consumer.effects", activation).unwrap();
    let handle = registry
        .bind(&consumer, &effects, &requirement, candidate.id())
        .unwrap();
    let in_flight = handle.clone();

    let call = std::thread::spawn(move || {
        in_flight.try_with(|service| {
            service.entered.wait();
            service.release.wait();
            7
        })
    });
    entered.wait();
    drop(provider_effects.close());
    assert_revoked(&handle);
    release.wait();

    assert_eq!(call.join().unwrap().unwrap(), 7);
}

#[test]
fn binding_requires_a_starting_consumer_with_the_matching_open_scope() {
    let service = ServiceDefinition::<Greeting>::new("test.greeting/v1");
    let requirement = service.required();
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(Greeting("hello")),
        )
        .unwrap();
    let (mut active_consumer, consumer_effects) = consumer("consumer", &service);
    let activation = active_consumer.state().activation().unwrap();
    active_consumer.complete_start(activation).unwrap();

    assert!(matches!(
        registry.bind(
            &active_consumer,
            &consumer_effects,
            &requirement,
            candidate.id(),
        ),
        Err(ServiceRegistryError::ConsumerNotStarting { .. })
    ));

    let (starting, mut starting_effects) = consumer("starting", &service);
    drop(starting_effects.close());
    assert!(matches!(
        registry.bind(&starting, &starting_effects, &requirement, candidate.id(),),
        Err(ServiceRegistryError::ConsumerScopeNotOpen { .. })
    ));

    let (other, other_effects) = consumer("other", &service);
    assert!(matches!(
        registry.bind(&starting, &other_effects, &requirement, candidate.id(),),
        Err(ServiceRegistryError::ActivationMismatch { .. })
    ));
    drop(other);
}

trait Greeter: Send + Sync {
    fn greet(&self) -> &'static str;
}

struct English;

impl Greeter for English {
    fn greet(&self) -> &'static str {
        "hello"
    }
}

#[test]
fn unsized_trait_contracts_and_public_registry_values_are_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ServiceRegistry>();
    assert_send_sync::<ServiceHandle<dyn Greeter>>();

    let service = ServiceDefinition::<dyn Greeter>::new("test.greeter/v1");
    let requirement = service.required();
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let implementation: Arc<dyn Greeter> = Arc::new(English);
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider_arc(implementation),
        )
        .unwrap();
    let mut definition = ComponentDefinition::new("consumer.component");
    definition.require(&requirement).unwrap();
    let scope = Scope::root("consumer.scope");
    let mut consumer = ComponentInstance::new("consumer.instance", &definition, &scope).unwrap();
    let activation = consumer.begin_start().unwrap();
    let effects = EffectScope::new("consumer.effects", activation).unwrap();
    let handle = registry
        .bind(&consumer, &effects, &requirement, candidate.id())
        .unwrap();

    assert_eq!(handle.try_with(Greeter::greet).unwrap(), "hello");
}

struct DropSignal(Arc<AtomicUsize>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stale_handle_clones_do_not_keep_a_provider_value_alive() {
    let service = ServiceDefinition::<DropSignal>::new("test.drop-signal/v1");
    let requirement = service.required();
    let dropped = Arc::new(AtomicUsize::new(0));
    let (owner, mut provider_effects) = active_instance("provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(DropSignal(Arc::clone(&dropped))),
        )
        .unwrap();
    let mut definition = ComponentDefinition::new("consumer.component");
    definition.require(&requirement).unwrap();
    let scope = Scope::root("consumer.scope");
    let mut consumer = ComponentInstance::new("consumer.instance", &definition, &scope).unwrap();
    let activation = consumer.begin_start().unwrap();
    let effects = EffectScope::new("consumer.effects", activation).unwrap();
    let handle = registry
        .bind(&consumer, &effects, &requirement, candidate.id())
        .unwrap();
    let clone = handle.clone();

    let report = provider_effects.close().await;

    assert!(report.is_clean());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_revoked(&handle);
    assert_revoked(&clone);
}

struct PanicOnDrop(Arc<AtomicBool>);

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
        panic!("provider destructor panicked");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn provider_destructor_panic_is_reported_and_older_cleanup_continues() {
    let service = ServiceDefinition::<PanicOnDrop>::new("test.panic/v1");
    let dropped = Arc::new(AtomicBool::new(false));
    let older_cleanup_ran = Arc::new(AtomicBool::new(false));
    let older_cleanup_observation = Arc::clone(&older_cleanup_ran);
    let (owner, mut provider_effects) = active_instance("provider");
    provider_effects
        .defer_sync("older cleanup", move || {
            older_cleanup_observation.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let mut registry = ServiceRegistry::new();
    registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(PanicOnDrop(Arc::clone(&dropped))),
        )
        .unwrap();

    let report = provider_effects.close().await;

    assert!(dropped.load(Ordering::SeqCst));
    assert!(older_cleanup_ran.load(Ordering::SeqCst));
    assert_eq!(report.cleanup_count(), 2);
    assert_eq!(report.failure_count(), 1);
    let failure = report.steps()[0]
        .clone()
        .into_cleanup_failure()
        .expect("provider cleanup should fail");
    assert_eq!(failure.kind(), CleanupFailureKind::Panicked);
    assert!(registry.candidates(&service.required()).unwrap().is_empty());
}

trait CloseStepExt {
    fn into_cleanup_failure(self) -> Option<yah_compose::CleanupFailure>;
}

impl CloseStepExt for yah_compose::CloseStep {
    fn into_cleanup_failure(self) -> Option<yah_compose::CleanupFailure> {
        match self {
            yah_compose::CloseStep::Cleanup(record) => match record.outcome() {
                yah_compose::CleanupOutcome::Failed(failure) => Some(failure.clone()),
                yah_compose::CleanupOutcome::Succeeded => None,
            },
            yah_compose::CloseStep::Child { .. } => None,
        }
    }
}
