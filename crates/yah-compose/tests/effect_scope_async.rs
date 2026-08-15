use std::{
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use tokio::sync::oneshot;
use yah_compose::{
    CleanupError, CleanupFailureKind, CloseStep, ComponentDefinition, ComponentInstance,
    ComponentState, EffectScope, EffectScopeError, EffectScopeState, Scope, StopTarget,
};

fn instance_and_activation() -> (ComponentInstance, yah_compose::ActivationEpoch) {
    let definition = ComponentDefinition::new("test.component");
    let scope = Scope::root("root");
    let mut instance = ComponentInstance::new("instance", &definition, &scope).unwrap();
    let activation = instance.begin_start().unwrap();
    (instance, activation)
}

fn record(order: &Arc<Mutex<Vec<&'static str>>>, value: &'static str) {
    order.lock().unwrap().push(value);
}

fn poll_once<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

struct ManualGate {
    open: Arc<AtomicBool>,
}

impl Future for ManualGate {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.open.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

struct ReadyWithPanickingDrop {
    order: Arc<Mutex<Vec<&'static str>>>,
    result: Option<Result<(), CleanupError>>,
}

impl Future for ReadyWithPanickingDrop {
    type Output = Result<(), CleanupError>;

    fn poll(mut self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        let result = self
            .result
            .take()
            .expect("test future must be polled only once");
        record(&self.order, "future-poll");
        Poll::Ready(result)
    }
}

impl Drop for ReadyWithPanickingDrop {
    fn drop(&mut self) {
        panic!("future destructor panicked");
    }
}

struct PanickingPanicPayload(&'static str);

impl Drop for PanickingPanicPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor panicked: {}", self.0);
    }
}

struct PollPanicsWithPayload {
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl Future for PollPanicsWithPayload {
    type Output = Result<(), CleanupError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        record(&self.order, "poll-panic");
        std::panic::panic_any(PanickingPanicPayload("poll"));
    }
}

struct ReadyWhoseDropPanicsWithPayload {
    order: Arc<Mutex<Vec<&'static str>>>,
}

impl Future for ReadyWhoseDropPanicsWithPayload {
    type Output = Result<(), CleanupError>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        record(&self.order, "drop-future-poll");
        Poll::Ready(Ok(()))
    }
}

impl Drop for ReadyWhoseDropPanicsWithPayload {
    fn drop(&mut self) {
        std::panic::panic_any(PanickingPanicPayload("future-drop"));
    }
}

#[tokio::test]
async fn sync_and_async_cleanup_share_one_lifo_order() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation).unwrap();

    let first_order = order.clone();
    scope
        .defer_sync("sync-1", move || {
            record(&first_order, "sync-1");
            Ok(())
        })
        .unwrap();
    let second_order = order.clone();
    scope
        .defer_async("async-2", move || async move {
            record(&second_order, "async-2");
            Ok(())
        })
        .unwrap();
    let third_order = order.clone();
    scope
        .defer_sync("sync-3", move || {
            record(&third_order, "sync-3");
            Ok(())
        })
        .unwrap();

    let report = scope.close().await;

    assert_eq!(*order.lock().unwrap(), ["sync-3", "async-2", "sync-1"]);
    assert!(report.is_clean());
}

#[tokio::test]
async fn subtree_cancellation_is_visible_before_cleanup_begins() {
    let (_, activation) = instance_and_activation();
    let mut root = EffectScope::new("root-effects", activation).unwrap();
    let root_signal = root.cancellation();
    let child_signal = {
        let child = root.child("child-effects").unwrap();
        child.cancellation()
    };
    let observed = Arc::new(AtomicBool::new(false));
    let observed_during_cleanup = observed.clone();
    root.defer_sync("observe-cancellation", move || {
        observed_during_cleanup.store(
            root_signal.is_cancelled() && child_signal.is_cancelled(),
            Ordering::SeqCst,
        );
        Ok(())
    })
    .unwrap();

    let _report = root.close().await;

    assert!(observed.load(Ordering::SeqCst));
}

#[tokio::test]
async fn child_close_does_not_cancel_parent_or_sibling() {
    let (_, activation) = instance_and_activation();
    let mut root = EffectScope::new("root-effects", activation).unwrap();
    let root_signal = root.cancellation();
    let sibling_signal = root.child("sibling-effects").unwrap().cancellation();
    let child_signal = {
        let child = root.child("child-effects").unwrap();
        let signal = child.cancellation();
        let _report = child.close().await;
        signal
    };

    assert!(child_signal.is_cancelled());
    assert!(!root_signal.is_cancelled());
    assert!(!sibling_signal.is_cancelled());

    let _report = root.close().await;
    assert!(root_signal.is_cancelled());
    assert!(sibling_signal.is_cancelled());
}

#[tokio::test]
async fn cooperative_task_observes_cancellation_and_is_joined_by_cleanup() {
    let (_, activation) = instance_and_activation();
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let cancellation = scope.cancellation();
    let task = tokio::spawn(async move {
        cancellation.cancelled().await;
        "stopped"
    });
    let joined = Arc::new(AtomicBool::new(false));
    let joined_during_cleanup = joined.clone();
    scope
        .defer_async("join-task", move || async move {
            let outcome = task
                .await
                .map_err(|error| CleanupError::new(error.to_string()))?;
            joined_during_cleanup.store(outcome == "stopped", Ordering::SeqCst);
            Ok(())
        })
        .unwrap();

    let report = tokio::time::timeout(Duration::from_secs(1), scope.close())
        .await
        .expect("cooperative cleanup should not hit the watchdog");

    assert!(report.is_clean());
    assert!(joined.load(Ordering::SeqCst));
}

#[tokio::test]
async fn dropped_pending_close_resumes_without_losing_or_repeating_cleanup() {
    let (_, activation) = instance_and_activation();
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let scope_id = scope.id();
    let (release_sender, release_receiver) = oneshot::channel::<()>();
    let invocations = Arc::new(Mutex::new(0_u8));
    let invocations_during_cleanup = invocations.clone();
    scope
        .defer_async("gated-cleanup", move || async move {
            *invocations_during_cleanup.lock().unwrap() += 1;
            release_receiver
                .await
                .map_err(|error| CleanupError::new(error.to_string()))?;
            Ok(())
        })
        .unwrap();

    let mut first_close = Box::pin(scope.close());
    let first_poll = poll_fn(|cx| Poll::Ready(first_close.as_mut().poll(cx))).await;
    assert!(first_poll.is_pending());
    drop(first_close);

    assert_eq!(scope.state(), EffectScopeState::Closing);
    assert_eq!(*invocations.lock().unwrap(), 1);
    assert_eq!(
        scope.defer_sync("too-late", || Ok(())),
        Err(EffectScopeError::NotOpen {
            scope_id,
            state: EffectScopeState::Closing,
        })
    );

    release_sender.send(()).unwrap();
    let report = scope.close().await;

    assert!(report.is_clean());
    assert_eq!(*invocations.lock().unwrap(), 1);
}

#[test]
fn unpolled_close_still_seals_then_resumes_cleanup_without_an_executor() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let cancellation = scope.cancellation();
    let child_id = scope.child("child").unwrap().id();
    for label in ["older", "newer"] {
        let order_during_cleanup = order.clone();
        scope
            .defer_sync(label, move || {
                record(&order_during_cleanup, label);
                Ok(())
            })
            .unwrap();
    }

    drop(scope.close());
    assert_eq!(scope.state(), EffectScopeState::Closing);
    assert!(cancellation.is_cancelled());
    assert!(order.lock().unwrap().is_empty());
    assert_eq!(
        scope.scope_mut(child_id).unwrap_err(),
        EffectScopeError::NotOpen {
            scope_id: scope.id(),
            state: EffectScopeState::Closing,
        }
    );

    let mut resumed = Box::pin(scope.close());
    let Poll::Ready(report) = poll_once(resumed.as_mut()) else {
        panic!("synchronous cleanup should complete in one manual poll");
    };
    drop(resumed);

    assert!(report.is_clean());
    assert_eq!(*order.lock().unwrap(), ["newer", "older"]);
}

#[test]
fn nested_pending_child_close_resumes_once_without_an_executor() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(AtomicBool::new(false));
    let invocations = Arc::new(Mutex::new(0_u8));
    let mut root = EffectScope::new("root", activation).unwrap();

    let parent_order = order.clone();
    root.defer_sync("parent", move || {
        record(&parent_order, "parent");
        Ok(())
    })
    .unwrap();
    {
        let child = root.child("child").unwrap();
        let child_order = order.clone();
        child
            .defer_sync("child-sync", move || {
                record(&child_order, "child-sync");
                Ok(())
            })
            .unwrap();
        let async_order = order.clone();
        let async_gate = gate.clone();
        let async_invocations = invocations.clone();
        child
            .defer_async("child-async", move || async move {
                *async_invocations.lock().unwrap() += 1;
                ManualGate { open: async_gate }.await;
                record(&async_order, "child-async");
                Ok(())
            })
            .unwrap();
    }

    let mut first_close = Box::pin(root.close());
    assert!(poll_once(first_close.as_mut()).is_pending());
    drop(first_close);
    assert_eq!(*invocations.lock().unwrap(), 1);
    assert!(order.lock().unwrap().is_empty());

    gate.store(true, Ordering::SeqCst);
    let mut resumed = Box::pin(root.close());
    let Poll::Ready(report) = poll_once(resumed.as_mut()) else {
        panic!("opened manual gate should let the nested close finish");
    };
    drop(resumed);

    assert!(report.is_clean());
    assert_eq!(*invocations.lock().unwrap(), 1);
    assert_eq!(
        *order.lock().unwrap(),
        ["child-async", "child-sync", "parent"]
    );
}

#[tokio::test]
async fn future_destructor_panic_is_reported_without_skipping_older_cleanup() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let older_order = order.clone();
    scope
        .defer_sync("older", move || {
            record(&older_order, "older");
            Ok(())
        })
        .unwrap();
    let future_order = order.clone();
    scope
        .defer_async("drop-panics", move || ReadyWithPanickingDrop {
            order: future_order,
            result: Some(Ok(())),
        })
        .unwrap();

    let report = scope.close().await;

    assert_eq!(*order.lock().unwrap(), ["future-poll", "older"]);
    assert_eq!(report.failure_count(), 1);
    let CloseStep::Cleanup(record) = &report.steps()[0] else {
        panic!("first close step should describe the async cleanup");
    };
    let failure = record.outcome().failure().unwrap();
    assert_eq!(failure.kind(), CleanupFailureKind::Panicked);
    assert_eq!(
        failure.summary(),
        "cleanup future destructor panicked: future destructor panicked"
    );
}

#[tokio::test]
async fn factory_and_prior_failure_drop_panics_are_aggregated_and_cleanup_continues() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let older_order = order.clone();
    scope
        .defer_sync("older", move || {
            record(&older_order, "older");
            Ok(())
        })
        .unwrap();
    let drop_order = order.clone();
    scope
        .defer_async("error-then-drop-panics", move || ReadyWithPanickingDrop {
            order: drop_order,
            result: Some(Err(CleanupError::new("returned failure"))),
        })
        .unwrap();
    let factory_order = order.clone();
    scope
        .defer_async(
            "factory-panics",
            move || -> std::future::Ready<Result<(), CleanupError>> {
                record(&factory_order, "factory-panic");
                panic!("factory panicked");
            },
        )
        .unwrap();

    let report = scope.close().await;

    assert_eq!(
        *order.lock().unwrap(),
        ["factory-panic", "future-poll", "older"]
    );
    assert_eq!(report.failure_count(), 2);
    let failures: Vec<_> = report
        .steps()
        .iter()
        .filter_map(|step| match step {
            CloseStep::Cleanup(record) => record.outcome().failure(),
            CloseStep::Child { .. } => None,
        })
        .collect();
    assert_eq!(failures[0].kind(), CleanupFailureKind::Panicked);
    assert_eq!(failures[0].summary(), "factory panicked");
    assert_eq!(failures[1].kind(), CleanupFailureKind::Panicked);
    assert_eq!(
        failures[1].summary(),
        "cleanup failed before its future destructor panicked: returned failure; destructor panic: future destructor panicked"
    );
}

#[test]
fn panicking_panic_payload_destructors_cannot_break_cleanup_reporting() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let older_order = order.clone();
    scope
        .defer_sync("older", move || {
            record(&older_order, "older");
            Ok(())
        })
        .unwrap();
    let sync_order = order.clone();
    scope
        .defer_sync("sync-payload", move || {
            record(&sync_order, "sync-panic");
            std::panic::panic_any(PanickingPanicPayload("sync"));
        })
        .unwrap();
    let factory_order = order.clone();
    scope
        .defer_async(
            "factory-payload",
            move || -> std::future::Ready<Result<(), CleanupError>> {
                record(&factory_order, "factory-panic");
                std::panic::panic_any(PanickingPanicPayload("factory"));
            },
        )
        .unwrap();
    let poll_order = order.clone();
    scope
        .defer_async("poll-payload", move || PollPanicsWithPayload {
            order: poll_order,
        })
        .unwrap();
    let drop_order = order.clone();
    scope
        .defer_async("drop-payload", move || ReadyWhoseDropPanicsWithPayload {
            order: drop_order,
        })
        .unwrap();

    let mut close = Box::pin(scope.close());
    let Poll::Ready(report) = poll_once(close.as_mut()) else {
        panic!("all payload-panic cleanup paths should finish in one poll");
    };
    drop(close);

    assert_eq!(
        *order.lock().unwrap(),
        [
            "drop-future-poll",
            "poll-panic",
            "factory-panic",
            "sync-panic",
            "older"
        ]
    );
    assert_eq!(report.cleanup_count(), 5);
    assert_eq!(report.failure_count(), 4);
    let summaries: Vec<_> = report
        .steps()
        .iter()
        .filter_map(|step| match step {
            CloseStep::Cleanup(record) => {
                record.outcome().failure().map(|failure| failure.summary())
            }
            CloseStep::Child { .. } => None,
        })
        .collect();
    assert!(summaries[0].contains("future-drop"));
    assert!(summaries[1].contains("poll"));
    assert!(summaries[2].contains("factory"));
    assert!(summaries[3].contains("sync"));
    assert!(
        summaries
            .iter()
            .all(|summary| summary.contains("panic payload destructor panicked"))
    );
}

#[tokio::test]
async fn async_error_and_panic_do_not_skip_older_cleanup() {
    let (_, activation) = instance_and_activation();
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let oldest_order = order.clone();
    scope
        .defer_async("oldest-success", move || async move {
            record(&oldest_order, "oldest-success");
            Ok(())
        })
        .unwrap();
    let error_order = order.clone();
    scope
        .defer_async("async-error", move || async move {
            record(&error_order, "async-error");
            Err(CleanupError::new("async failed"))
        })
        .unwrap();
    let panic_order = order.clone();
    scope
        .defer_async("async-panic", move || async move {
            record(&panic_order, "async-panic");
            panic!("async panicked")
        })
        .unwrap();

    let report = scope.close().await;

    assert_eq!(
        *order.lock().unwrap(),
        ["async-panic", "async-error", "oldest-success"]
    );
    assert_eq!(report.failure_count(), 2);
    let kinds: Vec<_> = report
        .steps()
        .iter()
        .filter_map(|step| match step {
            CloseStep::Cleanup(record) => record.outcome().failure().map(|failure| failure.kind()),
            CloseStep::Child { .. } => None,
        })
        .collect();
    assert_eq!(
        kinds,
        [
            CleanupFailureKind::Panicked,
            CleanupFailureKind::ReturnedError
        ]
    );
}

#[tokio::test]
async fn caller_keeps_failed_activation_stopping_until_effect_cleanup_finishes() {
    let (mut instance, activation) = instance_and_activation();
    let cleaned = Arc::new(AtomicBool::new(false));
    let cleaned_during_close = cleaned.clone();
    let mut effects = EffectScope::new("activation-effects", activation).unwrap();
    effects
        .defer_async("activation-registration", move || async move {
            cleaned_during_close.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();

    instance
        .mark_failed(activation, "activation callback failed")
        .unwrap();
    instance
        .begin_stop(activation, StopTarget::Pending)
        .unwrap();
    assert!(matches!(instance.state(), ComponentState::Stopping { .. }));

    let report = effects.close().await;
    assert!(report.is_clean());
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(matches!(instance.state(), ComponentState::Stopping { .. }));

    instance.complete_stop(activation).unwrap();
    assert_eq!(instance.state(), &ComponentState::Pending);
    assert_eq!(
        instance.last_failure().map(|failure| failure.summary()),
        Some("activation callback failed")
    );
}
