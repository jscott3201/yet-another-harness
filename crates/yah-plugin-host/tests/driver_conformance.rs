#[path = "driver_conformance/support.rs"]
mod support;

use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

use support::ReferenceTarget;
use yah_plugin_host::{
    DriverConformanceCase, DriverConformanceCaseResult, DriverConformanceFailure,
    DriverConformancePhase, DriverConformanceResourceState, DriverConformanceTarget,
    DriverConformanceTeardown, DriverConformanceTerminal, run_driver_conformance,
    run_driver_conformance_case,
};

#[tokio::test]
async fn scripted_reference_passes_the_ordered_portable_suite() {
    let report = run_driver_conformance(&ReferenceTarget::new()).await;
    assert!(report.is_conformant(), "{report}");
    assert_eq!(report.cases().len(), DriverConformanceCase::ALL.len());
    assert_eq!(
        report
            .cases()
            .iter()
            .map(DriverConformanceCaseResult::case)
            .collect::<Vec<_>>(),
        DriverConformanceCase::ALL
    );
}

#[tokio::test]
async fn ready_lifecycle_is_independently_runnable() {
    assert_case(DriverConformanceCase::ReadyLifecycle).await;
}

#[tokio::test]
async fn pending_start_cancellation_is_independently_runnable() {
    assert_case(DriverConformanceCase::PendingStartCancellation).await;
}

#[tokio::test]
async fn returned_start_failure_is_independently_runnable() {
    assert_case(DriverConformanceCase::ReturnedStartFailure).await;
}

#[tokio::test]
async fn returned_deactivation_failure_is_independently_runnable() {
    assert_case(DriverConformanceCase::ReturnedDeactivationFailure).await;
}

#[tokio::test]
async fn shared_driver_isolation_is_independently_runnable() {
    assert_case(DriverConformanceCase::SharedDriverIsolation).await;
}

#[tokio::test]
async fn independent_revision_oracle_reports_metadata_mismatch_without_starting() {
    let result = run_driver_conformance_case(
        &ReferenceTarget::with_mismatched_revision(),
        DriverConformanceCase::ReadyLifecycle,
    )
    .await;
    let DriverConformanceCaseResult::Failed(failure) = result else {
        panic!("mismatched driver metadata unexpectedly passed");
    };
    assert_eq!(failure.phase(), DriverConformancePhase::Preparation);
    assert!(
        failure
            .observations()
            .iter()
            .all(|observation| observation.start_factory_calls() == 0)
    );
}

#[tokio::test]
async fn aggregate_runner_retains_every_independent_failure() {
    let report = run_driver_conformance(&ReferenceTarget::with_mismatched_revision()).await;
    assert!(!report.is_conformant());
    assert_eq!(report.cases().len(), DriverConformanceCase::ALL.len());
    assert!(
        report
            .failures()
            .all(|failure| failure.phase() == DriverConformancePhase::Preparation)
    );
}

#[tokio::test]
async fn unexpected_pending_start_completion_fails_after_clean_release() {
    let failure = case_failure(
        &ReferenceTarget::with_pending_start_completion(),
        DriverConformanceCase::PendingStartCancellation,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Start);
    assert_clean_release(&failure, 1);
}

#[tokio::test]
async fn unhealthy_ready_subject_fails_after_clean_release() {
    let failure = case_failure(
        &ReferenceTarget::with_unhealthy_ready_activation(),
        DriverConformanceCase::ReadyLifecycle,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Health);
    assert_clean_release(&failure, 1);
}

#[tokio::test]
async fn probe_failure_after_acquisition_is_preserved_and_cleanup_still_runs() {
    let failure = case_failure(
        &ReferenceTarget::with_probe_failure_after_acquisition(),
        DriverConformanceCase::PendingStartCancellation,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Probe);
    assert_eq!(failure.teardown(), DriverConformanceTeardown::Clean);
    let observation = &failure.observations()[0];
    assert_eq!(observation.resource_state(), None);
    assert_eq!(observation.deactivation_factory_calls(), 1);
    assert!(
        observation
            .resource_probe_error()
            .is_some_and(|error| error.contains("injected probe failure"))
    );
}

#[tokio::test]
async fn transient_probe_failure_remains_in_final_structured_evidence() {
    let failure = case_failure(
        &ReferenceTarget::with_transient_probe_failure_after_acquisition(),
        DriverConformanceCase::PendingStartCancellation,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Probe);
    assert_eq!(failure.teardown(), DriverConformanceTeardown::Clean);
    let observation = &failure.observations()[0];
    assert_eq!(observation.resource_state(), None);
    assert_eq!(observation.deactivation_factory_calls(), 1);
    assert!(
        observation
            .resource_probe_error()
            .is_some_and(|error| error.contains("injected transient probe failure"))
    );
}

#[tokio::test]
async fn probe_failure_on_a_blocked_report_records_terminal_abandonment() {
    let failure = case_failure(
        &ReferenceTarget::with_probe_failure_after_acquisition(),
        DriverConformanceCase::ReturnedDeactivationFailure,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Probe);
    assert_eq!(
        failure.teardown(),
        DriverConformanceTeardown::BlockedThenAbandoned
    );
    let observation = &failure.observations()[0];
    assert_eq!(observation.deactivation_factory_calls(), 1);
    assert_eq!(observation.resource_state(), None);
    assert!(observation.resource_probe_error().is_some());
}

#[tokio::test]
async fn unexpectedly_clean_deactivation_is_rejected_without_repeating_cleanup() {
    let failure = case_failure(
        &ReferenceTarget::with_clean_deactivation(),
        DriverConformanceCase::ReturnedDeactivationFailure,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Teardown);
    assert_clean_release(&failure, 1);
    assert_eq!(
        failure.observations()[0].deactivation_terminal(),
        Some(DriverConformanceTerminal::Succeeded)
    );
}

#[tokio::test]
async fn second_shared_start_failure_cleans_both_exact_activations() {
    let failure = case_failure(
        &ReferenceTarget::with_second_shared_start_failure(),
        DriverConformanceCase::SharedDriverIsolation,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Start);
    assert_clean_release(&failure, 2);
}

#[tokio::test]
async fn shared_driver_crosstalk_is_rejected_after_both_activations_stop() {
    let failure = case_failure(
        &ReferenceTarget::with_shared_driver_crosstalk(),
        DriverConformanceCase::SharedDriverIsolation,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Isolation);
    assert_clean_release(&failure, 2);
}

#[tokio::test]
async fn reverse_shared_driver_crosstalk_is_rejected_after_both_activations_stop() {
    let failure = case_failure(
        &ReferenceTarget::with_reverse_shared_driver_crosstalk(),
        DriverConformanceCase::SharedDriverIsolation,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Isolation);
    assert_eq!(failure.teardown(), DriverConformanceTeardown::Clean);
    assert_eq!(failure.observations().len(), 2);
    assert_eq!(
        failure.observations()[0].resource_state(),
        Some(DriverConformanceResourceState::Live)
    );
    assert_eq!(
        failure.observations()[1].resource_state(),
        Some(DriverConformanceResourceState::Released)
    );
}

#[test]
fn paired_failure_seals_both_owners_before_awaiting_pending_cleanup() {
    let target = ReferenceTarget::with_pending_first_cleanup_after_second_start_failure();
    let mut waiter = Box::pin(run_driver_conformance_case(
        &target,
        DriverConformanceCase::SharedDriverIsolation,
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(
        Future::poll(waiter.as_mut(), &mut context),
        Poll::Pending
    ));

    let states = target.activation_states();
    assert_eq!(states.len(), 2);
    assert!(states.iter().all(|(cancelled, _)| *cancelled));
    assert_eq!(
        states
            .iter()
            .filter(|(_, state)| *state == DriverConformanceResourceState::Live)
            .count(),
        1
    );
    assert_eq!(
        states
            .iter()
            .filter(|(_, state)| *state == DriverConformanceResourceState::Released)
            .count(),
        1
    );
}

#[tokio::test]
async fn decorated_driver_final_drop_remains_inside_the_host_cleanup_report() {
    let failure = case_failure(
        &ReferenceTarget::with_driver_drop_panic(),
        DriverConformanceCase::ReadyLifecycle,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Teardown);
    assert_eq!(
        failure.teardown(),
        DriverConformanceTeardown::BlockedThenAbandoned
    );
    let observation = &failure.observations()[0];
    assert_eq!(observation.deactivation_factory_calls(), 1);
    assert_eq!(
        observation.resource_state(),
        Some(DriverConformanceResourceState::Released)
    );
    assert!(failure.summary().contains("cleanup reported 1 failure"));
}

#[tokio::test]
async fn untransferred_driver_and_panic_payload_drops_are_contained() {
    let failure = case_failure(
        &ReferenceTarget::with_setup_kind_mismatch_and_drop_payload_panic(),
        DriverConformanceCase::ReadyLifecycle,
    )
    .await;
    assert_eq!(failure.phase(), DriverConformancePhase::Setup);
    assert_eq!(failure.teardown(), DriverConformanceTeardown::NotStarted);
    assert!(failure.observations().is_empty());
    assert!(
        failure
            .summary()
            .contains("untransferred driver destructor panicked")
    );
    assert!(
        failure
            .summary()
            .contains("panic payload destructor panicked")
    );
    assert!(
        failure
            .summary()
            .contains("reference panic payload destructor panic")
    );
}

#[tokio::test]
async fn trusted_probe_rejects_an_activation_from_another_subject() {
    let target = ReferenceTarget::new();
    let result = run_driver_conformance_case(&target, DriverConformanceCase::ReadyLifecycle).await;
    let DriverConformanceCaseResult::Passed(report) = result else {
        panic!("reference case did not produce an activation identity");
    };
    let foreign_id = report.activations()[0].activation();
    let (_, _driver, probe) = target
        .subject(DriverConformanceCase::ReadyLifecycle)
        .expect("reference subject should be available")
        .into_parts();
    assert!(probe.resource_state(foreign_id).is_err());
}

#[test]
fn conformance_waiters_are_send_without_choosing_an_executor() {
    fn assert_send<T: Send>(_: &T) {}
    let target = ReferenceTarget::new();
    let waiter = run_driver_conformance_case(&target, DriverConformanceCase::ReadyLifecycle);
    assert_send(&waiter);
}

async fn assert_case(case: DriverConformanceCase) {
    let result = run_driver_conformance_case(&ReferenceTarget::new(), case).await;
    assert!(result.passed(), "{:#?}", result.failure());
}

async fn case_failure(
    target: &ReferenceTarget,
    case: DriverConformanceCase,
) -> DriverConformanceFailure {
    match run_driver_conformance_case(target, case).await {
        DriverConformanceCaseResult::Failed(failure) => failure,
        DriverConformanceCaseResult::Passed(report) => {
            panic!("nonconforming target unexpectedly passed: {report:#?}")
        }
    }
}

fn assert_clean_release(failure: &DriverConformanceFailure, activation_count: usize) {
    assert_eq!(failure.teardown(), DriverConformanceTeardown::Clean);
    assert_eq!(failure.observations().len(), activation_count);
    for observation in failure.observations() {
        assert_eq!(observation.deactivation_factory_calls(), 1);
        assert_eq!(
            observation.resource_state(),
            Some(DriverConformanceResourceState::Released)
        );
    }
}
