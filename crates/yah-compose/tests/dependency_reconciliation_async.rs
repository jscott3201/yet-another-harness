use std::{
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use tokio::sync::Notify;
use yah_compose::{
    CleanupError, ComponentDefinition, ComponentInstance, ComponentStateKind, ComponentStopReason,
    DependencyStopReason, EffectScope, ProviderAssignments, ProviderCandidate, ReconcileOutcome,
    ReconciledComponent, Scope, ServiceDefinition, ServiceRegistry, StopCompletion,
};

#[derive(Debug)]
struct Service(&'static str);

fn composition_scope() -> &'static Scope {
    static SCOPE: OnceLock<Scope> = OnceLock::new();
    SCOPE.get_or_init(|| Scope::root("dependency-reconciliation-async.tests"))
}

fn publish(
    registry: &mut ServiceRegistry,
    service: &ServiceDefinition<Service>,
    label: &str,
    value: &'static str,
) -> (EffectScope, ProviderCandidate) {
    let definition = ComponentDefinition::new(format!("{label}.provider"));
    let mut owner = ComponentInstance::new(
        format!("{label}.instance"),
        &definition,
        composition_scope(),
    )
    .unwrap();
    let activation = owner.begin_start().unwrap();
    let mut effects = EffectScope::new(format!("{label}.effects"), activation).unwrap();
    owner.complete_start(activation).unwrap();
    let candidate = registry
        .provide(&owner, &mut effects, service.provider(Service(value)))
        .unwrap();
    (effects, candidate)
}

fn consumer(service: &ServiceDefinition<Service>) -> ReconciledComponent {
    let mut definition = ComponentDefinition::new("test.consumer");
    definition.require(&service.required()).unwrap();
    ReconciledComponent::mount("test.consumer-instance", definition, composition_scope()).unwrap()
}

#[tokio::test]
async fn dropped_pending_finish_resumes_the_same_cleanup_once() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let (_first_effects, first) = publish(&mut registry, &service, "first", "first");
    let (_second_effects, second) = publish(&mut registry, &service, "second", "second");
    let mut desired = ProviderAssignments::new();
    desired.assign(&first);
    let mut consumer = consumer(&service);
    let selection_epoch = match consumer.reconcile(&registry, &desired).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };
    consumer
        .complete_start(selection_epoch, &registry, &desired)
        .unwrap();

    let release = Arc::new(Notify::new());
    let calls = Arc::new(Mutex::new(0));
    let cleanup_release = Arc::clone(&release);
    let cleanup_calls = Arc::clone(&calls);
    consumer
        .defer_async(selection_epoch, "wait to clean", move || async move {
            *cleanup_calls.lock().unwrap() += 1;
            cleanup_release.notified().await;
            Ok(())
        })
        .unwrap();

    desired.assign(&second);
    assert!(matches!(
        consumer.reconcile(&registry, &desired).unwrap(),
        ReconcileOutcome::StopBegun {
            reason: ComponentStopReason::Dependency(DependencyStopReason::AssignmentChanged(_)),
            ..
        }
    ));

    {
        let pending = consumer.finish_stop(selection_epoch);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), pending)
                .await
                .is_err()
        );
    }
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(consumer.state().kind(), ComponentStateKind::Stopping);

    release.notify_one();
    assert!(
        consumer
            .finish_stop(selection_epoch)
            .await
            .unwrap()
            .is_completed()
    );
    assert_eq!(*calls.lock().unwrap(), 1);
    assert_eq!(consumer.state().kind(), ComponentStateKind::Pending);
}

#[tokio::test]
async fn cleanup_failure_runs_older_cleanup_and_blocks_replacement() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let (_first_effects, first) = publish(&mut registry, &service, "first", "first");
    let (_second_effects, second) = publish(&mut registry, &service, "second", "second");
    let mut desired = ProviderAssignments::new();
    desired.assign(&first);
    let mut consumer = consumer(&service);
    let selection_epoch = match consumer.reconcile(&registry, &desired).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };
    consumer
        .complete_start(selection_epoch, &registry, &desired)
        .unwrap();

    let cleanup_order = Arc::new(Mutex::new(Vec::new()));
    let older_order = Arc::clone(&cleanup_order);
    consumer
        .defer_sync(selection_epoch, "older succeeds", move || {
            older_order.lock().unwrap().push("older");
            Ok(())
        })
        .unwrap();
    let newer_order = Arc::clone(&cleanup_order);
    consumer
        .defer_sync(selection_epoch, "newer fails", move || {
            newer_order.lock().unwrap().push("newer");
            Err(CleanupError::new("still live"))
        })
        .unwrap();

    desired.assign(&second);
    consumer.reconcile(&registry, &desired).unwrap();
    let completion = consumer.finish_stop(selection_epoch).await.unwrap();
    let StopCompletion::Blocked { report, .. } = completion else {
        panic!("cleanup failure must block replacement");
    };
    assert_eq!(report.cleanup_count(), 2);
    assert_eq!(report.failure_count(), 1);
    assert_eq!(*cleanup_order.lock().unwrap(), vec!["newer", "older"]);
    assert_eq!(consumer.state().kind(), ComponentStateKind::Stopping);
    assert_eq!(consumer.last_close_report(), Some(&report));

    assert!(matches!(
        consumer.reconcile(&registry, &desired).unwrap(),
        ReconcileOutcome::Stopping {
            cleanup_blocked: true,
            ..
        }
    ));
    assert!(matches!(
        consumer.finish_stop(selection_epoch).await.unwrap(),
        StopCompletion::Blocked { .. }
    ));
    assert_eq!(*cleanup_order.lock().unwrap(), vec!["newer", "older"]);
}

#[tokio::test]
async fn assignment_updates_during_teardown_are_recomputed_after_stop() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let (_first_effects, first) = publish(&mut registry, &service, "first", "first");
    let (_second_effects, second) = publish(&mut registry, &service, "second", "second");
    let mut desired = ProviderAssignments::new();
    desired.assign(&first);
    let mut consumer = consumer(&service);
    let first_epoch = match consumer.reconcile(&registry, &desired).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };
    consumer
        .complete_start(first_epoch, &registry, &desired)
        .unwrap();

    desired.assign(&second);
    consumer.reconcile(&registry, &desired).unwrap();
    desired.assign(&first);
    assert!(matches!(
        consumer.reconcile(&registry, &desired).unwrap(),
        ReconcileOutcome::Stopping { .. }
    ));
    consumer.finish_stop(first_epoch).await.unwrap();

    let second_epoch = match consumer.reconcile(&registry, &desired).unwrap() {
        ReconcileOutcome::StartBegun { selection } => {
            assert_eq!(
                selection.provider_for(service.id()).unwrap().id(),
                first.id()
            );
            selection.epoch()
        }
        outcome => panic!("expected a fresh start, got {outcome:?}"),
    };
    assert_ne!(second_epoch, first_epoch);
}

#[test]
fn service_payload_is_used_so_the_fixture_contract_stays_real() {
    let service = ServiceDefinition::<Service>::new("test.service/v1");
    let mut registry = ServiceRegistry::new();
    let (_effects, candidate) = publish(&mut registry, &service, "provider", "value");
    let mut desired = ProviderAssignments::new();
    desired.assign(&candidate);
    let mut consumer = consumer(&service);
    let epoch = match consumer.reconcile(&registry, &desired).unwrap() {
        ReconcileOutcome::StartBegun { selection } => selection.epoch(),
        outcome => panic!("expected start, got {outcome:?}"),
    };
    let handle = consumer
        .bind(epoch, &registry, &service.required())
        .unwrap();
    assert_eq!(handle.try_with(|service| service.0).unwrap(), "value");
}
