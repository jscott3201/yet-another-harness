use std::sync::{Arc, Mutex};

use yah_compose::{
    ActivationEpoch, CleanupError, CleanupFailureKind, CleanupOutcome, CloseReport, CloseStep,
    ComponentDefinition, ComponentInstance, EffectScope, EffectScopeError, EffectScopeState, Scope,
};

fn activation() -> ActivationEpoch {
    let definition = ComponentDefinition::new("test.component");
    let scope = Scope::root("root");
    let mut instance = ComponentInstance::new("instance", &definition, &scope).unwrap();
    instance.begin_start().unwrap()
}

fn record(order: &Arc<Mutex<Vec<&'static str>>>, value: &'static str) {
    order.lock().unwrap().push(value);
}

fn cleanup_labels(report: &CloseReport) -> Vec<&str> {
    fn visit<'a>(report: &'a CloseReport, labels: &mut Vec<&'a str>) {
        for step in report.steps() {
            match step {
                CloseStep::Cleanup(cleanup) => labels.push(cleanup.label()),
                CloseStep::Child { report, .. } => visit(report, labels),
            }
        }
    }

    let mut labels = Vec::new();
    visit(report, &mut labels);
    labels
}

#[tokio::test]
async fn empty_close_is_cached_and_idempotent() {
    let activation = activation();
    let mut scope = EffectScope::new("effects", activation).unwrap();
    let scope_id = scope.id();

    let first = scope.close().await;
    let second = scope.close().await;

    assert_eq!(first, second);
    assert_eq!(first.scope_id(), scope_id);
    assert_eq!(first.scope_label(), "effects");
    assert_eq!(first.activation(), activation);
    assert_eq!(first.cleanup_count(), 0);
    assert!(first.is_clean());
    assert_eq!(scope.state(), EffectScopeState::Closed);
    assert_eq!(scope.closed_report(), Some(&first));
}

#[tokio::test]
async fn nested_scopes_unwind_at_their_tree_aware_lifo_position() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut root = EffectScope::new("root-effects", activation()).unwrap();

    let order_for_parent_one = order.clone();
    root.defer_sync("parent-1", move || {
        record(&order_for_parent_one, "parent-1");
        Ok(())
    })
    .unwrap();
    {
        let child = root.child("child-effects").unwrap();
        let order_for_child_one = order.clone();
        child
            .defer_sync("child-1", move || {
                record(&order_for_child_one, "child-1");
                Ok(())
            })
            .unwrap();
        let order_for_child_two = order.clone();
        child
            .defer_sync("child-2", move || {
                record(&order_for_child_two, "child-2");
                Ok(())
            })
            .unwrap();
    }
    let order_for_parent_two = order.clone();
    root.defer_sync("parent-2", move || {
        record(&order_for_parent_two, "parent-2");
        Ok(())
    })
    .unwrap();

    let report = root.close().await;

    assert_eq!(
        *order.lock().unwrap(),
        ["parent-2", "child-2", "child-1", "parent-1"]
    );
    assert_eq!(
        cleanup_labels(&report),
        ["parent-2", "child-2", "child-1", "parent-1"]
    );
    assert_eq!(report.cleanup_count(), 4);
    assert!(report.is_clean());
}

#[tokio::test]
async fn early_child_close_is_observed_without_rerunning_cleanup() {
    let invocations = Arc::new(Mutex::new(0_u8));
    let mut root = EffectScope::new("root-effects", activation()).unwrap();
    let child_report = {
        let child = root.child("child-effects").unwrap();
        let invocations = invocations.clone();
        child
            .defer_sync("child-cleanup", move || {
                *invocations.lock().unwrap() += 1;
                Err(CleanupError::new("child cleanup failed"))
            })
            .unwrap();
        child.close().await
    };

    let parent_report = root.close().await;
    let repeated_parent_report = root.close().await;

    assert_eq!(*invocations.lock().unwrap(), 1);
    assert_eq!(child_report.failure_count(), 1);
    assert_eq!(parent_report.failure_count(), 1);
    assert_eq!(repeated_parent_report, parent_report);
    assert!(matches!(
        parent_report.steps(),
        [CloseStep::Child {
            report,
            already_closed: true,
        }] if report.as_ref() == &child_report
    ));
}

#[tokio::test]
async fn returned_errors_and_panics_are_aggregated_without_short_circuiting() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut scope = EffectScope::new("effects", activation()).unwrap();

    let order_for_oldest = order.clone();
    scope
        .defer_sync("oldest-success", move || {
            record(&order_for_oldest, "oldest-success");
            Ok(())
        })
        .unwrap();
    let order_for_error = order.clone();
    scope
        .defer_sync("returned-error", move || {
            record(&order_for_error, "returned-error");
            Err(CleanupError::new("expected failure"))
        })
        .unwrap();
    let order_for_panic = order.clone();
    scope
        .defer_sync("panicked", move || {
            record(&order_for_panic, "panicked");
            panic!("expected panic")
        })
        .unwrap();
    let order_for_newest = order.clone();
    scope
        .defer_sync("newest-success", move || {
            record(&order_for_newest, "newest-success");
            Ok(())
        })
        .unwrap();

    let report = scope.close().await;

    assert_eq!(
        *order.lock().unwrap(),
        [
            "newest-success",
            "panicked",
            "returned-error",
            "oldest-success"
        ]
    );
    assert_eq!(report.failure_count(), 2);
    assert_eq!(cleanup_labels(&report), *order.lock().unwrap());
    let failures: Vec<_> = report
        .steps()
        .iter()
        .filter_map(|step| match step {
            CloseStep::Cleanup(record) => record.outcome().failure(),
            CloseStep::Child { .. } => None,
        })
        .collect();
    assert_eq!(failures[0].kind(), CleanupFailureKind::Panicked);
    assert_eq!(failures[0].summary(), "expected panic");
    assert_eq!(failures[1].kind(), CleanupFailureKind::ReturnedError);
    assert_eq!(failures[1].summary(), "expected failure");
}

#[tokio::test]
async fn completed_scope_rejects_new_effects_and_children() {
    let mut scope = EffectScope::new("effects", activation()).unwrap();
    let scope_id = scope.id();
    let _report = scope.close().await;

    assert_eq!(
        scope.defer_sync("too-late", || Ok(())),
        Err(EffectScopeError::NotOpen {
            scope_id,
            state: EffectScopeState::Closed,
        })
    );
    assert_eq!(
        scope.child("too-late-child").unwrap_err(),
        EffectScopeError::NotOpen {
            scope_id,
            state: EffectScopeState::Closed,
        }
    );
}

#[tokio::test]
async fn local_cleanup_does_not_claim_to_reverse_an_escaped_action() {
    let local_registration = Arc::new(Mutex::new(true));
    let escaped_action = Arc::new(Mutex::new(true));
    let mut scope = EffectScope::new("effects", activation()).unwrap();
    let local_registration_for_cleanup = local_registration.clone();
    scope
        .defer_sync("local-registration", move || {
            *local_registration_for_cleanup.lock().unwrap() = false;
            Ok(())
        })
        .unwrap();

    let report = scope.close().await;

    assert!(report.is_clean());
    assert!(!*local_registration.lock().unwrap());
    assert!(*escaped_action.lock().unwrap());
}

#[test]
fn public_handles_have_the_intended_thread_traits() {
    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send::<EffectScope>();
    assert_send_sync::<yah_compose::ScopeCancellation>();
}

#[test]
fn cleanup_outcome_exposes_success_without_a_failure() {
    assert_eq!(CleanupOutcome::Succeeded.failure(), None);
}

#[test]
fn generated_scope_ids_keep_duplicate_labels_and_registrations_distinct() {
    let activation = activation();
    let mut first_root = EffectScope::new("duplicate", activation).unwrap();
    let mut second_root = EffectScope::new("duplicate", activation).unwrap();
    let first_root_registration = first_root.defer_sync("cleanup", || Ok(())).unwrap();
    let second_root_registration = second_root.defer_sync("cleanup", || Ok(())).unwrap();

    assert_ne!(first_root.id(), second_root.id());
    assert_ne!(first_root_registration, second_root_registration);

    let (first_child_id, first_child_registration) = {
        let child = first_root.child("duplicate-child").unwrap();
        (child.id(), child.defer_sync("cleanup", || Ok(())).unwrap())
    };
    let (second_child_id, second_child_registration) = {
        let child = first_root.child("duplicate-child").unwrap();
        (child.id(), child.defer_sync("cleanup", || Ok(())).unwrap())
    };

    assert_ne!(first_child_id, second_child_id);
    assert_ne!(first_child_registration, second_child_registration);
    assert_eq!(first_root.label(), second_root.label());
    assert_eq!(
        first_root.scope_mut(first_child_id).unwrap().label(),
        "duplicate-child"
    );
}

#[test]
fn subtree_lookup_is_activation_fenced_and_rejects_unknown_scopes() {
    let root_activation = activation();
    let mut root = EffectScope::new("root", root_activation).unwrap();
    let child_id = root.child("child").unwrap().id();
    assert_eq!(root.scope_mut(child_id).unwrap().id(), child_id);

    let unrelated = EffectScope::new("unrelated", root_activation).unwrap();
    let unrelated_id = unrelated.id();
    assert_eq!(
        root.scope_mut(unrelated_id).unwrap_err(),
        EffectScopeError::UnknownScope {
            scope_id: unrelated_id,
        }
    );

    let other_activation = activation();
    let other = EffectScope::new("other-activation", other_activation).unwrap();
    assert_eq!(
        root.scope_mut(other.id()).unwrap_err(),
        EffectScopeError::WrongActivation {
            expected: root_activation,
            received: other_activation,
        }
    );
}

#[test]
fn dropping_an_open_scope_requests_cancellation_but_abandons_cleanup() {
    let cleanup_ran = Arc::new(Mutex::new(false));
    let mut scope = EffectScope::new("effects", activation()).unwrap();
    let cancellation = scope.cancellation();
    let cleanup_ran_during_close = cleanup_ran.clone();
    scope
        .defer_sync("cleanup", move || {
            *cleanup_ran_during_close.lock().unwrap() = true;
            Ok(())
        })
        .unwrap();

    drop(scope);

    assert!(cancellation.is_cancelled());
    assert!(!*cleanup_ran.lock().unwrap());
}
