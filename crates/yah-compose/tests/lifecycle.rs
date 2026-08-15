use yah_compose::{
    ComponentDefinition, ComponentInstance, ComponentState, FailurePhase, LifecycleAction,
    LifecycleError, Scope, StopTarget,
};

fn instance() -> ComponentInstance {
    let definition = ComponentDefinition::new("test.component");
    let scope = Scope::root("test.scope");
    ComponentInstance::new("test.instance", &definition, &scope).unwrap()
}

#[test]
fn new_instance_preserves_identity_and_begins_pending() {
    let definition = ComponentDefinition::new("test.component");
    let scope = Scope::root("test.scope");
    let instance = ComponentInstance::new("test.instance", &definition, &scope).unwrap();

    assert_eq!(instance.id().as_str(), "test.instance");
    assert_eq!(instance.definition_id(), definition.id());
    assert_eq!(instance.scope_id(), scope.id());
    assert_eq!(instance.state(), &ComponentState::Pending);
}

#[test]
fn activation_stop_and_reactivation_advance_the_epoch() {
    let mut instance = instance();

    let first = instance.begin_start().unwrap();
    assert_eq!(first.sequence(), 1);
    assert_eq!(
        instance.state(),
        &ComponentState::Starting { activation: first }
    );
    instance.complete_start(first).unwrap();
    assert_eq!(
        instance.state(),
        &ComponentState::Active { activation: first }
    );

    instance.begin_stop(first, StopTarget::Pending).unwrap();
    assert!(matches!(
        instance.state(),
        ComponentState::Stopping {
            activation,
            target: StopTarget::Pending,
            prior_failure: None,
        } if *activation == first
    ));
    instance.complete_stop(first).unwrap();
    assert_eq!(instance.state(), &ComponentState::Pending);

    let second = instance.begin_start().unwrap();
    assert_eq!(second.sequence(), 2);
    assert!(second > first);
}

#[test]
fn stale_start_completion_cannot_activate_a_new_attempt() {
    let mut instance = instance();
    let first = instance.begin_start().unwrap();
    instance.begin_stop(first, StopTarget::Pending).unwrap();
    instance.complete_stop(first).unwrap();
    let second = instance.begin_start().unwrap();
    let before = instance.state().clone();

    assert_eq!(
        instance.complete_start(first),
        Err(LifecycleError::StaleActivation {
            expected: second,
            received: first,
        })
    );
    assert_eq!(instance.state(), &before);
}

#[test]
fn stop_during_starting_blocks_late_activation() {
    let mut instance = instance();
    let activation = instance.begin_start().unwrap();

    instance
        .begin_stop(activation, StopTarget::Pending)
        .unwrap();
    let stopping = instance.state().clone();
    assert_eq!(
        instance.complete_start(activation),
        Err(LifecycleError::InvalidTransition {
            from: yah_compose::ComponentStateKind::Stopping,
            action: LifecycleAction::CompleteStart,
        })
    );
    assert_eq!(instance.state(), &stopping);
    instance.complete_stop(activation).unwrap();
    assert_eq!(instance.state(), &ComponentState::Pending);
}

#[test]
fn failed_start_preserves_diagnostics_through_cleanup_then_retries() {
    let mut instance = instance();
    let first = instance.begin_start().unwrap();
    instance
        .mark_failed(first, "factory refused config")
        .unwrap();

    let expected_failure = ComponentFailureView {
        phase: FailurePhase::Starting,
        summary: "factory refused config",
    };
    assert_failure(instance.state(), first, expected_failure);

    instance.begin_stop(first, StopTarget::Pending).unwrap();
    match instance.state() {
        ComponentState::Stopping {
            activation,
            target,
            prior_failure: Some(failure),
        } => {
            assert_eq!(*activation, first);
            assert_eq!(*target, StopTarget::Pending);
            assert_eq!(failure.phase(), FailurePhase::Starting);
            assert_eq!(failure.summary(), "factory refused config");
        }
        other => panic!("expected failed cleanup state, got {other:?}"),
    }
    instance.complete_stop(first).unwrap();
    assert_eq!(
        instance.last_failure().map(|failure| failure.summary()),
        Some("factory refused config")
    );
    let second = instance.begin_start().unwrap();
    assert!(second > first);
    assert_eq!(
        instance.last_failure().map(|failure| failure.summary()),
        Some("factory refused config")
    );
}

#[test]
fn active_failure_records_the_runtime_phase() {
    let mut instance = instance();
    let activation = instance.begin_start().unwrap();
    instance.complete_start(activation).unwrap();
    instance
        .mark_failed(activation, "background task exited")
        .unwrap();

    assert_failure(
        instance.state(),
        activation,
        ComponentFailureView {
            phase: FailurePhase::Active,
            summary: "background task exited",
        },
    );
}

#[test]
fn pending_removes_directly_while_live_instance_stops_first() {
    let mut pending = instance();
    pending.remove_pending().unwrap();
    assert_eq!(pending.state(), &ComponentState::Removed);

    let mut active = instance();
    let activation = active.begin_start().unwrap();
    active.complete_start(activation).unwrap();
    assert_eq!(
        active.remove_pending(),
        Err(LifecycleError::InvalidTransition {
            from: yah_compose::ComponentStateKind::Active,
            action: LifecycleAction::RemovePending,
        })
    );
    active.begin_stop(activation, StopTarget::Removed).unwrap();
    active.complete_stop(activation).unwrap();
    assert_eq!(active.state(), &ComponentState::Removed);
}

#[test]
fn removed_is_terminal_and_rejections_do_not_mutate_it() {
    let mut instance = instance();
    let activation = instance.begin_start().unwrap();
    instance
        .begin_stop(activation, StopTarget::Removed)
        .unwrap();
    instance.complete_stop(activation).unwrap();
    let removed = instance.state().clone();

    assert_eq!(
        instance.begin_start(),
        Err(LifecycleError::InvalidTransition {
            from: yah_compose::ComponentStateKind::Removed,
            action: LifecycleAction::BeginStart,
        })
    );
    assert_eq!(
        instance.begin_stop(activation, StopTarget::Pending),
        Err(LifecycleError::InvalidTransition {
            from: yah_compose::ComponentStateKind::Removed,
            action: LifecycleAction::BeginStop,
        })
    );
    assert_eq!(instance.state(), &removed);
}

#[test]
fn wrong_epoch_cannot_fail_or_finish_stop() {
    let mut instance = instance();
    let first = instance.begin_start().unwrap();
    instance.begin_stop(first, StopTarget::Pending).unwrap();
    instance.complete_stop(first).unwrap();
    let second = instance.begin_start().unwrap();
    let before = instance.state().clone();

    assert_eq!(
        instance.mark_failed(first, "late failure"),
        Err(LifecycleError::StaleActivation {
            expected: second,
            received: first,
        })
    );
    assert_eq!(instance.state(), &before);

    instance.complete_start(second).unwrap();
    instance.begin_stop(second, StopTarget::Pending).unwrap();
    let stopping = instance.state().clone();
    assert_eq!(
        instance.complete_stop(first),
        Err(LifecycleError::StaleActivation {
            expected: second,
            received: first,
        })
    );
    assert_eq!(instance.state(), &stopping);
}

#[test]
fn replacement_instance_rejects_the_old_instances_first_activation() {
    let mut original = instance();
    let old_activation = original.begin_start().unwrap();

    let mut replacement = instance();
    let new_activation = replacement.begin_start().unwrap();
    assert_eq!(old_activation.sequence(), 1);
    assert_eq!(new_activation.sequence(), 1);
    assert_ne!(old_activation, new_activation);
    let before = replacement.state().clone();

    assert_eq!(
        replacement.complete_start(old_activation),
        Err(LifecycleError::StaleActivation {
            expected: new_activation,
            received: old_activation,
        })
    );
    assert_eq!(replacement.state(), &before);
}

#[test]
fn delayed_stop_intent_cannot_stop_a_newer_activation() {
    let mut instance = instance();
    let first = instance.begin_start().unwrap();
    instance.begin_stop(first, StopTarget::Pending).unwrap();
    instance.complete_stop(first).unwrap();
    let second = instance.begin_start().unwrap();
    instance.complete_start(second).unwrap();
    let before = instance.state().clone();

    assert_eq!(
        instance.begin_stop(first, StopTarget::Pending),
        Err(LifecycleError::StaleActivation {
            expected: second,
            received: first,
        })
    );
    assert_eq!(instance.state(), &before);
}

struct ComponentFailureView<'a> {
    phase: FailurePhase,
    summary: &'a str,
}

fn assert_failure(
    state: &ComponentState,
    expected_activation: yah_compose::ActivationEpoch,
    expected: ComponentFailureView<'_>,
) {
    match state {
        ComponentState::Failed {
            activation,
            failure,
        } => {
            assert_eq!(*activation, expected_activation);
            assert_eq!(failure.phase(), expected.phase);
            assert_eq!(failure.summary(), expected.summary);
        }
        other => panic!("expected failed state, got {other:?}"),
    }
}
