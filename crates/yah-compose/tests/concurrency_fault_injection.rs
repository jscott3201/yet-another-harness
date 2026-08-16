use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use yah_compose::{
    CleanupFailureKind, CleanupOutcome, CloseStep, ComponentDefinition, ComponentInstance,
    ComponentRevision, ComponentSlot, ComponentSlotOutcome, ComponentState, ComponentStopReason,
    DesiredComponentState, EffectScope, FailurePhase, ProviderAssignments, ProviderSelectionEpoch,
    ReconcileOutcome, Scope, ServiceDefinition, ServiceHandle, ServiceHandleError, ServiceRegistry,
    StopDisposition, StopTarget,
};

#[derive(Debug)]
struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct PanicOnDrop(Arc<AtomicBool>);

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
        panic!("provider destructor panicked");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cmp007_parent_close_drains_child_provider_calls_before_any_cleanup() {
    const CALLS: usize = 3;

    let service = ServiceDefinition::<DropProbe>::new("fault.provider-child/v1");
    let requirement = service.required();
    let dropped = Arc::new(AtomicUsize::new(0));
    let resource_closed = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Barrier::new(CALLS + 1));
    let release = Arc::new(Barrier::new(CALLS + 1));
    let (owner, mut provider_effects) = active_instance("fault.provider-child");
    let mut registry = ServiceRegistry::new();
    let candidate = {
        let child = provider_effects.child("published child").unwrap();
        registry
            .provide(
                &owner,
                child,
                service.provider(DropProbe(Arc::clone(&dropped))),
            )
            .unwrap()
    };
    let cleanup_observation = Arc::clone(&resource_closed);
    provider_effects
        .defer_sync("newer parent resource", move || {
            cleanup_observation.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let (consumer, mut consumer_effects) = consumer("fault.consumer", &service);
    let handle = registry
        .bind(&consumer, &consumer_effects, &requirement, candidate.id())
        .unwrap();

    let calls = (0..CALLS)
        .map(|_| {
            let handle = handle.clone();
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            let resource_closed = Arc::clone(&resource_closed);
            std::thread::spawn(move || {
                handle.try_with(|_| {
                    entered.wait();
                    release.wait();
                    assert!(!resource_closed.load(Ordering::SeqCst));
                })
            })
        })
        .collect::<Vec<_>>();
    entered.wait();

    let mut close = Box::pin(provider_effects.close());
    assert!(matches!(poll_once(close.as_mut()), Poll::Pending));
    assert!(registry.candidates(&requirement).unwrap().is_empty());
    assert_revoked(&handle);
    assert!(!resource_closed.load(Ordering::SeqCst));
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(close);

    release.wait();
    for call in calls {
        call.join().unwrap().unwrap();
    }
    let report = provider_effects.close().await;

    assert!(report.is_clean());
    assert!(resource_closed.load(Ordering::SeqCst));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(consumer_effects.close().await.is_clean());
}

#[tokio::test(flavor = "current_thread")]
async fn cmp007_child_consumer_close_is_isolated_but_parent_close_drains_its_call() {
    let service = ServiceDefinition::<DropProbe>::new("fault.consumer-child/v1");
    let requirement = service.required();
    let dropped = Arc::new(AtomicUsize::new(0));
    let (owner, mut provider_effects) = active_instance("fault.provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(DropProbe(Arc::clone(&dropped))),
        )
        .unwrap();
    let (consumer, mut consumer_effects) = consumer("fault.consumer-tree", &service);
    let bound_child = consumer_effects.child("bound child").unwrap().id();
    let sibling = consumer_effects.child("independent sibling").unwrap().id();
    let handle = registry
        .bind(
            &consumer,
            consumer_effects.scope_mut(bound_child).unwrap(),
            &requirement,
            candidate.id(),
        )
        .unwrap();

    assert!(
        consumer_effects
            .scope_mut(sibling)
            .unwrap()
            .close()
            .await
            .is_clean()
    );
    assert!(handle.try_with(|_| ()).is_ok());

    let resource_closed = Arc::new(AtomicBool::new(false));
    let cleanup_observation = Arc::clone(&resource_closed);
    consumer_effects
        .defer_sync("consumer parent resource", move || {
            cleanup_observation.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let in_flight = handle.clone();
    let call_entered = Arc::clone(&entered);
    let call_release = Arc::clone(&release);
    let call_resource = Arc::clone(&resource_closed);
    let call = std::thread::spawn(move || {
        in_flight.try_with(|_| {
            call_entered.wait();
            call_release.wait();
            assert!(!call_resource.load(Ordering::SeqCst));
        })
    });
    entered.wait();

    let mut close = Box::pin(consumer_effects.close());
    assert!(matches!(poll_once(close.as_mut()), Poll::Pending));
    assert_revoked(&handle);
    assert!(!resource_closed.load(Ordering::SeqCst));
    assert_eq!(registry.candidates(&requirement).unwrap(), vec![candidate]);

    release.wait();
    call.join().unwrap().unwrap();
    assert!(close.await.is_clean());
    assert!(resource_closed.load(Ordering::SeqCst));
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(provider_effects.close().await.is_clean());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn cmp007_callback_unwind_releases_provider_and_consumer_activity() {
    let service = ServiceDefinition::<DropProbe>::new("fault.callback-panic/v1");
    let requirement = service.required();
    let dropped = Arc::new(AtomicUsize::new(0));
    let provider_cleaned = Arc::new(AtomicBool::new(false));
    let consumer_cleaned = Arc::new(AtomicBool::new(false));
    let (owner, mut provider_effects) = active_instance("fault.panic-provider");
    let mut registry = ServiceRegistry::new();
    let candidate = registry
        .provide(
            &owner,
            &mut provider_effects,
            service.provider(DropProbe(Arc::clone(&dropped))),
        )
        .unwrap();
    let provider_cleanup = Arc::clone(&provider_cleaned);
    provider_effects
        .defer_sync("provider resource", move || {
            provider_cleanup.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let (consumer, mut consumer_effects) = consumer("fault.panic-consumer", &service);
    let handle = registry
        .bind(&consumer, &consumer_effects, &requirement, candidate.id())
        .unwrap();
    let consumer_cleanup = Arc::clone(&consumer_cleaned);
    consumer_effects
        .defer_sync("consumer resource", move || {
            consumer_cleanup.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let in_flight = handle.clone();
    let call_entered = Arc::clone(&entered);
    let call_release = Arc::clone(&release);
    let provider_state = Arc::clone(&provider_cleaned);
    let consumer_state = Arc::clone(&consumer_cleaned);
    let call = std::thread::spawn(move || {
        catch_unwind(AssertUnwindSafe(|| {
            let _ = in_flight.try_with(|_| {
                call_entered.wait();
                call_release.wait();
                assert!(!provider_state.load(Ordering::SeqCst));
                assert!(!consumer_state.load(Ordering::SeqCst));
                panic!("service callback panicked");
            });
        }))
    });
    entered.wait();

    let mut provider_close = Box::pin(provider_effects.close());
    let mut consumer_close = Box::pin(consumer_effects.close());
    assert!(matches!(poll_once(provider_close.as_mut()), Poll::Pending));
    assert!(matches!(poll_once(consumer_close.as_mut()), Poll::Pending));
    assert_revoked(&handle);

    release.wait();
    assert!(call.join().unwrap().is_err());
    assert!(provider_close.await.is_clean());
    assert!(consumer_close.await.is_clean());
    assert!(provider_cleaned.load(Ordering::SeqCst));
    assert!(consumer_cleaned.load(Ordering::SeqCst));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn cmp007_active_failure_drains_call_and_contains_final_provider_destructor_panic() {
    let service = ServiceDefinition::<PanicOnDrop>::new("fault.destructor-panic/v1");
    let requirement = service.required();
    let dropped = Arc::new(AtomicBool::new(false));
    let older_cleanup_ran = Arc::new(AtomicBool::new(false));
    let newer_cleanup_ran = Arc::new(AtomicBool::new(false));
    let mut registry = ServiceRegistry::new();
    let (mut provider, revision, epoch) = active_slot("fault.destructor-provider", &registry);
    let older_cleanup = Arc::clone(&older_cleanup_ran);
    provider
        .defer_sync(epoch, "older cleanup", move || {
            older_cleanup.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let candidate = provider
        .provide(
            epoch,
            &mut registry,
            service.provider(PanicOnDrop(Arc::clone(&dropped))),
        )
        .unwrap();
    let newer_cleanup = Arc::clone(&newer_cleanup_ran);
    provider
        .defer_sync(epoch, "newer cleanup", move || {
            newer_cleanup.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let (consumer, mut consumer_effects) = consumer("fault.destructor-consumer", &service);
    let handle = registry
        .bind(&consumer, &consumer_effects, &requirement, candidate.id())
        .unwrap();
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let in_flight = handle.clone();
    let call_entered = Arc::clone(&entered);
    let call_release = Arc::clone(&release);
    let call = std::thread::spawn(move || {
        in_flight.try_with(|_| {
            call_entered.wait();
            call_release.wait();
            7
        })
    });
    entered.wait();

    assert!(matches!(
        provider.fail_activation(epoch, "provider task failed").unwrap(),
        ReconcileOutcome::StopBegun {
            target: StopTarget::Pending,
            reason: ComponentStopReason::ActivationFailed(ref failure),
            ..
        } if failure.phase() == FailurePhase::Active
            && failure.summary() == "provider task failed"
    ));
    let mut finish = Box::pin(provider.finish_stop(epoch));
    assert!(matches!(poll_once(finish.as_mut()), Poll::Pending));
    assert_revoked(&handle);
    assert!(registry.candidates(&requirement).unwrap().is_empty());
    assert!(!older_cleanup_ran.load(Ordering::SeqCst));
    assert!(!newer_cleanup_ran.load(Ordering::SeqCst));
    drop(finish);

    release.wait();
    assert_eq!(call.join().unwrap().unwrap(), 7);
    let record = provider.finish_stop(epoch).await.unwrap();

    assert_eq!(record.disposition(), StopDisposition::Blocked);
    assert_eq!(record.target(), StopTarget::Pending);
    assert!(matches!(
        record.reason(),
        ComponentStopReason::ActivationFailed(failure)
            if failure.phase() == FailurePhase::Active
                && failure.summary() == "provider task failed"
    ));
    assert_eq!(record.report().failure_count(), 1);
    assert!(record.report().steps().iter().any(|step| {
        matches!(
            step,
            CloseStep::Cleanup(cleanup)
                if matches!(cleanup.outcome(), CleanupOutcome::Failed(failure)
                    if failure.kind() == CleanupFailureKind::Panicked)
        )
    }));
    assert!(dropped.load(Ordering::SeqCst));
    assert!(older_cleanup_ran.load(Ordering::SeqCst));
    assert!(newer_cleanup_ran.load(Ordering::SeqCst));
    assert!(matches!(
        provider.live_state(),
        Some(ComponentState::Stopping { .. })
    ));

    let abandoned = provider.abandon_failed_cleanup(epoch).unwrap();
    assert_eq!(abandoned.disposition(), StopDisposition::Abandoned);
    assert_eq!(provider.live_state(), Some(&ComponentState::Pending));
    assert!(consumer_effects.close().await.is_clean());
    assert_eq!(revision.id(), provider.applied_revision().unwrap());
}

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

fn consumer<T: ?Sized + Send + Sync + 'static>(
    label: &str,
    service: &ServiceDefinition<T>,
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

fn active_slot(
    label: &str,
    registry: &ServiceRegistry,
) -> (ComponentSlot, ComponentRevision, ProviderSelectionEpoch) {
    let definition = ComponentDefinition::new(format!("{label}.component"));
    let revision = ComponentRevision::new(
        format!("{label}.revision"),
        definition,
        Scope::root(format!("{label}.scope")),
    );
    let mut slot = ComponentSlot::new(format!("{label}.slot")).unwrap();
    let desired = DesiredComponentState::enabled(
        slot.generation(1),
        revision.clone(),
        ProviderAssignments::new(),
    );
    let epoch = match slot.reconcile(registry, desired).unwrap() {
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::StartBegun { selection },
            ..
        } => selection.epoch(),
        outcome => panic!("expected provider start, got {outcome:?}"),
    };
    slot.complete_start(epoch, registry).unwrap();
    (slot, revision, epoch)
}

fn assert_revoked<T: ?Sized + Send + Sync + 'static>(handle: &ServiceHandle<T>) {
    assert!(matches!(
        handle.try_with(|_| ()),
        Err(ServiceHandleError::Revoked { .. })
    ));
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
