//! Independently authored black-box composition scenarios informed by the
//! pinned Cordis behavior sources in the project attribution.
//!
//! These cases exercise YAH's public Rust API and deliberate semantics. They
//! are not translations of Cordis tests: dependency selection is explicit,
//! registry instances are explicit visibility domains, and desired churn is
//! caller-driven rather than filesystem HMR.

use std::{sync::Arc, task::Poll};

use tokio::sync::Notify;
use yah_compose::{
    CleanupError, ComponentDefinition, ComponentInstance, ComponentSlot, ComponentSlotError,
    ComponentSlotOutcome, ComponentState, ComponentStopReason, DependencyIssue,
    DependencyReadiness, DesiredComponentState, DesiredStopReason, EffectScope, FailurePhase,
    ProviderAssignments, ReconcileError, ReconcileOutcome, Scope, ServiceDefinition,
    ServiceHandleError, ServiceRegistry, StopDisposition, StopTarget,
};

#[path = "semantic_conformance/support.rs"]
mod support;

use support::{
    Message, Trace, assignment, enabled, poll_once, revision, start_consumer, start_epoch,
    start_provider,
};

#[tokio::test]
async fn cmp006_cleanup_is_tree_lifo_idempotent_and_failure_tolerant() {
    let definition = ComponentDefinition::new("conformance.cleanup.component");
    let scope = Scope::root("conformance.cleanup.scope");
    let mut instance =
        ComponentInstance::new("conformance.cleanup.instance", &definition, &scope).unwrap();
    let activation = instance.begin_start().unwrap();
    let mut effects = EffectScope::new("conformance cleanup", activation).unwrap();
    let cancellation = effects.cancellation();
    let trace = Trace::default();

    let oldest = trace.clone();
    effects
        .defer_sync("outer oldest", move || {
            oldest.push("outer-oldest");
            Ok(())
        })
        .unwrap();
    {
        let child = effects.child("nested").unwrap();
        let child_sync = trace.clone();
        child
            .defer_sync("child sync", move || {
                child_sync.push("child-sync");
                Ok(())
            })
            .unwrap();
        let child_async = trace.clone();
        child
            .defer_async("child async failure", move || async move {
                child_async.push("child-async");
                Err(CleanupError::new("expected conformance failure"))
            })
            .unwrap();
    }
    let newest = trace.clone();
    effects
        .defer_sync("outer newest", move || {
            newest.push("outer-newest");
            Ok(())
        })
        .unwrap();

    drop(effects.close());
    assert!(cancellation.is_cancelled());
    assert!(trace.entries().is_empty());

    let report = effects.close().await;
    assert_eq!(
        trace.entries(),
        ["outer-newest", "child-async", "child-sync", "outer-oldest"]
    );
    assert_eq!(report.failure_count(), 1);
    assert_eq!(effects.close().await, report);
    assert_eq!(
        trace.entries(),
        ["outer-newest", "child-async", "child-sync", "outer-oldest"]
    );
}

#[tokio::test]
async fn cmp006_pending_dependency_waits_for_publication_and_exact_assignment() {
    let service = ServiceDefinition::<Message>::new("conformance.message/v1");
    let mut registry = ServiceRegistry::new();
    let consumer_revision = revision(
        "conformance.consumer.revision",
        "conformance.consumer.scope",
        Some(&service),
    );
    let mut consumer = ComponentSlot::new("conformance.consumer.slot").unwrap();
    let no_assignments = ProviderAssignments::new();

    assert!(matches!(
        consumer
            .reconcile(
                &registry,
                enabled(&consumer, 1, &consumer_revision, &no_assignments)
            )
            .unwrap(),
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::Pending {
                readiness: DependencyReadiness::Pending(ref issues),
            },
            ..
        } if matches!(issues.as_slice(), [DependencyIssue::MissingProvider { .. }])
    ));

    let mut provider = ComponentSlot::new("conformance.provider.slot").unwrap();
    let provider_revision = revision(
        "conformance.provider.revision",
        "conformance.provider.scope",
        None,
    );
    let provider_epoch = start_epoch(
        provider
            .reconcile(
                &registry,
                enabled(
                    &provider,
                    1,
                    &provider_revision,
                    &ProviderAssignments::new(),
                ),
            )
            .unwrap(),
    );
    assert!(matches!(
        provider.provide(
            provider_epoch,
            &mut registry,
            service.provider(Message("too early")),
        ),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::InvalidState { .. }
        ))
    ));
    assert!(matches!(
        consumer
            .reconcile(
                &registry,
                enabled(&consumer, 1, &consumer_revision, &no_assignments)
            )
            .unwrap(),
        ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::Pending {
                readiness: DependencyReadiness::Pending(ref issues),
            },
            ..
        } if matches!(issues.as_slice(), [DependencyIssue::MissingProvider { .. }])
    ));

    provider.complete_start(provider_epoch, &registry).unwrap();
    let candidate = provider
        .provide(
            provider_epoch,
            &mut registry,
            service.provider(Message("ready")),
        )
        .unwrap();
    assert!(matches!(
        consumer
            .reconcile(
                &registry,
                enabled(&consumer, 1, &consumer_revision, &no_assignments)
            )
            .unwrap(),
        ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::Pending {
                readiness: DependencyReadiness::Pending(ref issues),
            },
            ..
        } if matches!(issues.as_slice(), [DependencyIssue::Unassigned { candidate: id, .. }] if *id == candidate.id())
    ));

    let assignments = assignment(&candidate);
    let (epoch, handle) = start_consumer(
        &mut consumer,
        &registry,
        &service,
        &consumer_revision,
        &assignments,
        2,
    );
    assert_eq!(handle.try_with(|message| message.0).unwrap(), "ready");
    assert_eq!(handle.consumer_activation(), epoch.activation());
}

#[tokio::test]
async fn cmp006_service_replacement_revokes_before_fresh_binding() {
    let service = ServiceDefinition::<Message>::new("conformance.message/v1");
    let mut registry = ServiceRegistry::new();
    let first = start_provider(&mut registry, &service, "first", "first");
    let second = start_provider(&mut registry, &service, "second", "second");
    let consumer_revision = revision(
        "conformance.consumer.revision",
        "conformance.consumer.scope",
        Some(&service),
    );
    let first_assignment = assignment(&first.candidate);
    let second_assignment = assignment(&second.candidate);
    let mut consumer = ComponentSlot::new("conformance.consumer.slot").unwrap();
    let (first_epoch, first_handle) = start_consumer(
        &mut consumer,
        &registry,
        &service,
        &consumer_revision,
        &first_assignment,
        1,
    );

    assert!(matches!(
        consumer
            .reconcile(
                &registry,
                enabled(&consumer, 1, &consumer_revision, &first_assignment)
            )
            .unwrap(),
        ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::Active { .. },
            ..
        }
    ));
    assert_eq!(first_handle.try_with(|message| message.0).unwrap(), "first");
    assert_eq!(first_handle.provider().id(), first.candidate.id());
    assert!(matches!(
        consumer
            .reconcile(
                &registry,
                enabled(&consumer, 2, &consumer_revision, &second_assignment)
            )
            .unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Pending,
            ..
        }
    ));
    assert!(matches!(
        first_handle.try_with(|message| message.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert_eq!(
        consumer.finish_stop(first_epoch).await.unwrap().target(),
        StopTarget::Pending
    );

    let (second_epoch, second_handle) = start_consumer(
        &mut consumer,
        &registry,
        &service,
        &consumer_revision,
        &second_assignment,
        2,
    );
    assert_ne!(second_epoch, first_epoch);
    assert_eq!(
        second_handle.try_with(|message| message.0).unwrap(),
        "second"
    );
    assert!(matches!(
        first_handle.try_with(|message| message.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
}

#[tokio::test]
async fn cmp006_registry_domains_isolate_candidates_bindings_and_revocation() {
    let service = ServiceDefinition::<Message>::new("conformance.message/v1");
    let mut left_registry = ServiceRegistry::new();
    let mut right_registry = ServiceRegistry::new();
    let mut left_provider = start_provider(&mut left_registry, &service, "left", "left");
    let right_provider = start_provider(&mut right_registry, &service, "right", "right");
    let consumer_revision = revision(
        "conformance.consumer.revision",
        "conformance.consumer.scope",
        Some(&service),
    );

    let mut left_consumer = ComponentSlot::new("conformance.left-consumer.slot").unwrap();
    let (_, left_handle) = start_consumer(
        &mut left_consumer,
        &left_registry,
        &service,
        &consumer_revision,
        &assignment(&left_provider.candidate),
        1,
    );

    let mut right_consumer = ComponentSlot::new("conformance.right-consumer.slot").unwrap();
    let foreign_assignment = assignment(&left_provider.candidate);
    assert!(matches!(
        right_consumer
            .reconcile(
                &right_registry,
                enabled(
                    &right_consumer,
                    1,
                    &consumer_revision,
                    &foreign_assignment,
                )
            )
            .unwrap(),
        ComponentSlotOutcome::Mounted {
            component: ReconcileOutcome::Pending {
                readiness: DependencyReadiness::Pending(ref issues),
            },
            ..
        } if matches!(issues.as_slice(), [DependencyIssue::AssignedProviderUnavailable { assigned, available, .. }]
            if *assigned == left_provider.candidate.id() && available == &[right_provider.candidate.id()])
    ));
    let (right_epoch, right_handle) = start_consumer(
        &mut right_consumer,
        &right_registry,
        &service,
        &consumer_revision,
        &assignment(&right_provider.candidate),
        2,
    );

    let disable_left = DesiredComponentState::disabled(
        left_provider.slot.generation(2),
        left_provider.revision.clone(),
    );
    assert!(matches!(
        left_provider
            .slot
            .reconcile(&left_registry, disable_left)
            .unwrap(),
        ComponentSlotOutcome::StopBegun { .. }
    ));
    assert!(matches!(
        left_handle.try_with(|message| message.0),
        Err(ServiceHandleError::Revoked { .. })
    ));
    assert_eq!(right_handle.try_with(|message| message.0).unwrap(), "right");
    left_provider
        .slot
        .finish_stop(left_provider.epoch)
        .await
        .unwrap();
    assert_eq!(right_handle.try_with(|message| message.0).unwrap(), "right");
    assert!(matches!(
        right_consumer
            .reconcile(
                &right_registry,
                enabled(
                    &right_consumer,
                    2,
                    &consumer_revision,
                    &assignment(&right_provider.candidate),
                )
            )
            .unwrap(),
        ComponentSlotOutcome::Reconciled {
            component: ReconcileOutcome::Active { selection_epoch },
            ..
        } if selection_epoch == right_epoch
    ));
}

#[tokio::test]
async fn cmp006_activation_failure_rolls_back_then_retries_fresh() {
    let registry = ServiceRegistry::new();
    let failed_revision = revision(
        "conformance.failed.revision",
        "conformance.failed.scope",
        None,
    );
    let healthy_revision = revision(
        "conformance.healthy.revision",
        "conformance.healthy.scope",
        None,
    );
    let assignments = ProviderAssignments::new();
    let mut failed = ComponentSlot::new("conformance.failed.slot").unwrap();
    let mut healthy = ComponentSlot::new("conformance.healthy.slot").unwrap();
    let failed_epoch = start_epoch(
        failed
            .reconcile(
                &registry,
                enabled(&failed, 1, &failed_revision, &assignments),
            )
            .unwrap(),
    );
    let healthy_epoch = start_epoch(
        healthy
            .reconcile(
                &registry,
                enabled(&healthy, 1, &healthy_revision, &assignments),
            )
            .unwrap(),
    );
    healthy.complete_start(healthy_epoch, &registry).unwrap();
    let failed_cancellation = failed.cancellation(failed_epoch).unwrap();
    let healthy_cancellation = healthy.cancellation(healthy_epoch).unwrap();
    let trace = Trace::default();
    let cleanup = trace.clone();
    failed
        .defer_sync(failed_epoch, "failed activation cleanup", move || {
            cleanup.push("failed-cleanup");
            Ok(())
        })
        .unwrap();

    assert!(matches!(
        failed
            .fail_activation(failed_epoch, "factory rejected config")
            .unwrap(),
        ReconcileOutcome::StopBegun {
            target: StopTarget::Pending,
            reason: ComponentStopReason::ActivationFailed(ref failure),
            ..
        } if failure.phase() == FailurePhase::Starting
            && failure.summary() == "factory rejected config"
    ));
    assert!(failed_cancellation.is_cancelled());
    assert!(!healthy_cancellation.is_cancelled());
    assert!(matches!(
        healthy.live_state(),
        Some(ComponentState::Active { .. })
    ));
    assert!(matches!(
        failed.complete_start(failed_epoch, &registry),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::InvalidState { .. }
        ))
    ));

    let record = failed.finish_stop(failed_epoch).await.unwrap();
    assert_eq!(record.target(), StopTarget::Pending);
    assert_eq!(record.disposition(), StopDisposition::Completed);
    assert!(matches!(
        record.reason(),
        ComponentStopReason::ActivationFailed(failure)
            if failure.phase() == FailurePhase::Starting
                && failure.summary() == "factory rejected config"
    ));
    assert_eq!(trace.entries(), ["failed-cleanup"]);
    assert_eq!(failed.live_state(), Some(&ComponentState::Pending));

    let retry_epoch = start_epoch(
        failed
            .reconcile(
                &registry,
                enabled(&failed, 1, &failed_revision, &assignments),
            )
            .unwrap(),
    );
    assert_ne!(retry_epoch, failed_epoch);
    let retry_cancellation = failed.cancellation(retry_epoch).unwrap();
    assert!(matches!(
        failed.fail_activation(failed_epoch, "late failure"),
        Err(ComponentSlotError::Reconcile(
            ReconcileError::StaleSelection { .. }
        ))
    ));
    assert!(!retry_cancellation.is_cancelled());
    let retry_cleanup = trace.clone();
    failed
        .defer_sync(retry_epoch, "retry cleanup", move || {
            retry_cleanup.push("retry-cleanup");
            Ok(())
        })
        .unwrap();
    failed.complete_start(retry_epoch, &registry).unwrap();
    assert!(matches!(
        failed
            .fail_activation(retry_epoch, "runtime task exited")
            .unwrap(),
        ReconcileOutcome::StopBegun {
            reason: ComponentStopReason::ActivationFailed(ref failure),
            ..
        } if failure.phase() == FailurePhase::Active
            && failure.summary() == "runtime task exited"
    ));
    assert!(retry_cancellation.is_cancelled());
    let active_record = failed.finish_stop(retry_epoch).await.unwrap();
    assert!(matches!(
        active_record.reason(),
        ComponentStopReason::ActivationFailed(failure)
            if failure.phase() == FailurePhase::Active
                && failure.summary() == "runtime task exited"
    ));
    assert!(!healthy_cancellation.is_cancelled());
    assert!(matches!(
        healthy.live_state(),
        Some(ComponentState::Active { .. })
    ));
    assert_eq!(trace.entries(), ["failed-cleanup", "retry-cleanup"]);
}

#[tokio::test]
async fn cmp006_rapid_desired_churn_applies_only_the_latest_revision() {
    let registry = ServiceRegistry::new();
    let revision_a = revision("conformance.revision-a", "conformance.scope", None);
    let revision_b = revision("conformance.revision-b", "conformance.scope", None);
    let revision_c = revision("conformance.revision-c", "conformance.scope", None);
    let assignments = ProviderAssignments::new();
    let mut slot = ComponentSlot::new("conformance.reload.slot").unwrap();
    let epoch_a = start_epoch(
        slot.reconcile(&registry, enabled(&slot, 1, &revision_a, &assignments))
            .unwrap(),
    );
    slot.complete_start(epoch_a, &registry).unwrap();

    let trace = Trace::default();
    let cleanup_trace = trace.clone();
    let release = Arc::new(Notify::new());
    let cleanup_release = Arc::clone(&release);
    slot.defer_async(epoch_a, "suspended cleanup", move || async move {
        cleanup_trace.push("cleanup-started");
        cleanup_release.notified().await;
        cleanup_trace.push("cleanup-finished");
        Ok(())
    })
    .unwrap();

    assert!(matches!(
        slot.reconcile(
            &registry,
            enabled(&slot, 2, &revision_b, &assignments),
        )
        .unwrap(),
        ComponentSlotOutcome::StopBegun {
            target: StopTarget::Removed,
            reason: ComponentStopReason::Desired(DesiredStopReason::RevisionChanged {
                ref previous,
                ref desired,
            }),
            ..
        } if previous == revision_a.id() && desired == revision_b.id()
    ));
    {
        let mut finish = Box::pin(slot.finish_stop(epoch_a));
        assert!(matches!(poll_once(finish.as_mut()), Poll::Pending));
    }
    assert_eq!(trace.entries(), ["cleanup-started"]);
    assert_eq!(slot.applied_revision(), Some(revision_a.id()));

    let disabled_b = DesiredComponentState::disabled(slot.generation(3), revision_b.clone());
    assert!(matches!(
        slot.reconcile(&registry, disabled_b).unwrap(),
        ComponentSlotOutcome::Stopping {
            target: StopTarget::Removed,
            reason: ComponentStopReason::Desired(DesiredStopReason::RevisionChanged {
                ref previous,
                ref desired,
            }),
            ..
        } if previous == revision_a.id() && desired == revision_b.id()
    ));
    let latest = enabled(&slot, 4, &revision_c, &assignments);
    assert!(matches!(
        slot.reconcile(&registry, latest.clone()).unwrap(),
        ComponentSlotOutcome::Stopping {
            target: StopTarget::Removed,
            reason: ComponentStopReason::Desired(DesiredStopReason::RevisionChanged {
                ref previous,
                ref desired,
            }),
            ..
        } if previous == revision_a.id() && desired == revision_b.id()
    ));
    assert_eq!(slot.applied_revision(), Some(revision_a.id()));

    release.notify_one();
    let record = slot.finish_stop(epoch_a).await.unwrap();
    assert_eq!(record.target(), StopTarget::Removed);
    assert!(matches!(
        record.reason(),
        ComponentStopReason::Desired(DesiredStopReason::RevisionChanged {
            previous,
            desired,
        }) if previous == revision_a.id() && desired == revision_b.id()
    ));
    assert_eq!(trace.entries(), ["cleanup-started", "cleanup-finished"]);
    assert!(slot.applied_revision().is_none());

    let epoch_c = start_epoch(slot.reconcile(&registry, latest).unwrap());
    slot.complete_start(epoch_c, &registry).unwrap();
    assert_eq!(slot.applied_revision(), Some(revision_c.id()));
    assert_ne!(epoch_c, epoch_a);
    assert_eq!(trace.entries(), ["cleanup-started", "cleanup-finished"]);
}
