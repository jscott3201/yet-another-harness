//! The acceptance triad — restart, disconnect, cancel — plus the bootstrap
//! hygiene the fd-3 design promises and the supervision guarantees a
//! misbehaving worker forces: a worker that stops reading, a worker that
//! never speaks, a worker whose descendant outlives it, and an activation
//! its composition simply abandons.
//!
//! Every case drives a real child through the host's own activation guard,
//! not through driver internals: the same path a composition uses. Restart
//! means what the protocol allows it to mean: a dead worker poisons its
//! activation permanently (there is no resume), health says so, cleanup
//! releases the process, and recovery is a fresh activation with a fresh
//! process — proved here on the same shared driver.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::time::Duration;

use fixtures::{lifecycle_revision, worker_program};
use serde_json::json;
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, DriverKind, EffectiveCapabilityGrants, HostPluginActivation,
    PluginActivationHandle, PluginActivationId, PluginHealth, PluginRevision, PluginStartError,
};
use yah_plugin_ipc::types::{CancelReason, CancelTarget, Outcome, WireErrorKind};
use yah_plugin_proc::{
    CallEnd, DiagnosticStream, PendingCall, ProcActivationPlan, ProcLimits, ProcObserver,
    ProcessPluginDriver, ResourceState,
};

struct Rig {
    slot: ComponentSlot,
    registry: ServiceRegistry,
    broker: CapabilityBroker,
    grants: EffectiveCapabilityGrants,
    epoch: ProviderSelectionEpoch,
}

impl Rig {
    /// Mount a fresh component so the slot yields a real selection epoch.
    fn new(label: &str, revision: &PluginRevision) -> Self {
        let registry = ServiceRegistry::new();
        let mut slot = ComponentSlot::new(label).expect("slot label is canonical");
        let desired = DesiredComponentState::enabled(
            slot.generation(1),
            ComponentRevision::new(
                format!("{label}.revision"),
                ComponentDefinition::new(format!("{label}.component")),
                Scope::root(format!("{label}.scope")),
            ),
            ProviderAssignments::new(),
        );
        let epoch = match slot
            .reconcile(&registry, desired)
            .expect("fresh component begins start")
        {
            ComponentSlotOutcome::Mounted {
                component: ReconcileOutcome::StartBegun { selection },
                ..
            } => selection.epoch(),
            other => panic!("fresh component did not begin start: {other:?}"),
        };
        let grants = EffectiveCapabilityGrants::empty(revision);
        Self {
            slot,
            registry,
            broker: CapabilityBroker::new().expect("broker is constructible"),
            grants,
            epoch,
        }
    }
}

fn scripted(
    revision: &PluginRevision,
    plans: Vec<ProcActivationPlan>,
) -> (
    std::sync::Arc<dyn yah_plugin_host::PluginDriver>,
    ProcObserver,
) {
    ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        plans,
    )
}

/// Stop a successfully activated component the way a composition does:
/// release the guard back to its slot, reconcile to removed, drive the stop.
async fn stop_active(
    activation: HostPluginActivation<'_>,
    registry: &ServiceRegistry,
    epoch: ProviderSelectionEpoch,
) {
    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(registry, removed).expect("begins stopping");
    slot.finish_stop(epoch).await.expect("cleanup completes");
}

/// Poll health until the predicate holds; a bounded wait, not a sleep.
async fn health_becomes(
    activation: &PluginActivationHandle,
    what: &'static str,
    accept: impl Fn(&PluginHealth) -> bool,
) -> PluginHealth {
    for _ in 0..500 {
        if let Ok(health) = activation.health()
            && accept(&health)
        {
            return health;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("health never became {what}");
}

/// A settle that regresses into silence must fail the test, not wedge CI.
async fn settled_within(call: PendingCall) -> CallEnd {
    tokio::time::timeout(Duration::from_secs(5), call.settled())
        .await
        .expect("the call settles within its bound")
        .expect("the call settles")
}

/// Wait for the pid to vanish; `kill(pid, 0)` failing is the proof.
async fn process_gone(pid: i32) {
    for _ in 0..500 {
        if unsafe { libc::kill(pid, 0) } == -1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("worker process {pid} survived its reclaim");
}

/// The two lines the `bootstrap-report` worker prints, once both arrive.
async fn bootstrap_report(observer: &ProcObserver, id: &PluginActivationId) -> (String, String) {
    for _ in 0..500 {
        if let Some(tail) = observer.diagnostics_tail(id, DiagnosticStream::Stdout)
            && tail.matches('\n').count() >= 2
        {
            let mut lines = tail.lines();
            return (
                lines.next().unwrap_or_default().to_owned(),
                lines.next().unwrap_or_default().to_owned(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the worker never reported its bootstrap state");
}

#[tokio::test]
async fn a_dead_worker_poisons_its_activation_and_a_fresh_one_serves() {
    let revision = lifecycle_revision("restart", '6');
    // One driver, two scripted activations: the crash and the recovery.
    let (driver, observer) = scripted(
        &revision,
        vec![
            ProcActivationPlan::worker("crash-after-hello"),
            ProcActivationPlan::ready(),
        ],
    );

    let mut rig = Rig::new("proc.restart.crash", &revision);
    let mut crashed = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        std::sync::Arc::clone(&driver),
    )
    .expect("preparation is inert and succeeds");
    let crashed_id = crashed.id().clone();
    let crashed_handle = crashed
        .activate(&rig.registry)
        .await
        .expect("the handshake completed before the scripted crash");
    // The worker exits right after negotiation, without a goodbye: a bare
    // disconnect, never proof its external actions failed.
    let health = health_becomes(&crashed_handle, "unhealthy", |health| {
        matches!(health, PluginHealth::Unhealthy { .. })
    })
    .await;
    let PluginHealth::Unhealthy { summary } = health else {
        unreachable!()
    };
    assert!(
        summary.contains("disconnected"),
        "the health summary names the disconnect: {summary}"
    );
    // No resume exists to try: recovery starts with releasing this one.
    stop_active(crashed, &rig.registry, rig.epoch).await;
    assert_eq!(observer.deactivation_calls(&crashed_id), 1);

    // The fresh activation on the same driver is the restart.
    let mut rig = Rig::new("proc.restart.fresh", &revision);
    let mut fresh =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let fresh_id = fresh.id().clone();
    let fresh_handle = fresh
        .activate(&rig.registry)
        .await
        .expect("a fresh process serves after the crash");
    assert!(matches!(fresh_handle.health(), Ok(PluginHealth::Healthy)));
    let call = observer
        .begin_call(&fresh_id, "tool.echo", json!({"alive": true}), None)
        .await
        .expect("the call opens");
    match settled_within(call).await {
        CallEnd::Settled(Outcome::Ok { result }) => assert_eq!(result, json!({"alive": true})),
        other => panic!("expected the echo, got {other:?}"),
    }
    stop_active(fresh, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn a_disconnect_settles_in_flight_work_outcome_unknown() {
    let revision = lifecycle_revision("disconnect", '7');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("exit-mid-call")]);
    let mut rig = Rig::new("proc.disconnect", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    let call = observer
        .begin_call(&id, "tool.slow", json!(null), None)
        .await
        .expect("the call opens");
    // The worker exits on receiving the call, without answering: the one
    // outcome the host must not invent is success or failure.
    match settled_within(call).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::OutcomeUnknown);
            assert!(reconcile, "a disconnect always requires reconciliation");
        }
        other => panic!("expected outcome-unknown, got {other:?}"),
    }
    health_becomes(&handle, "unhealthy", |health| {
        matches!(health, PluginHealth::Unhealthy { .. })
    })
    .await;
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn a_cancelled_call_still_terminates_with_the_cancelled_outcome() {
    let revision = lifecycle_revision("cancel", '8');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("cancel-ack")]);
    let mut rig = Rig::new("proc.cancel", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    let call = observer
        .begin_call(&id, "tool.hold", json!(null), None)
        .await
        .expect("the call opens");
    observer
        .cancel(&id, call.call_id(), CancelTarget::Call)
        .await
        .expect("the cancel goes out");
    // The worker acknowledges by answering — silence would be
    // indistinguishable from a lost worker.
    match settled_within(call).await {
        CallEnd::Settled(Outcome::Cancelled { reason }) => {
            assert_eq!(reason, CancelReason::Requested);
        }
        other => panic!("expected the cancelled outcome, got {other:?}"),
    }
    assert!(matches!(handle.health(), Ok(PluginHealth::Healthy)));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn an_expired_deadline_settles_locally_and_tolerates_the_late_answer() {
    let revision = lifecycle_revision("deadline", '9');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("cancel-ack")]);
    let mut rig = Rig::new("proc.deadline", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    // The worker holds the call until cancelled; the budget expires first,
    // the host enforces it, and the worker's cancelled answer arrives as
    // the tolerated late terminal.
    let call = observer
        .begin_call(&id, "tool.hold", json!(null), Some(50))
        .await
        .expect("the call opens");
    match settled_within(call).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::DeadlineExceeded);
            assert!(reconcile, "the worker may have acted before the budget");
        }
        other => panic!("expected deadline-exceeded, got {other:?}"),
    }
    // The late terminal is observed, not assumed. The wire is ordered and
    // the worker sequential, so the deadline's own cancel reaches it before
    // this follow-up does: by the time the follow-up settles, the first
    // call's late cancelled answer was already fed — and tolerated. A pump
    // that stops emitting or flushing cancels fails here instead of
    // passing vacuously.
    let follow_up = observer
        .begin_call(&id, "tool.hold", json!(null), None)
        .await
        .expect("the session still serves after the late terminal");
    observer
        .cancel(&id, follow_up.call_id(), CancelTarget::Call)
        .await
        .expect("the cancel goes out");
    match settled_within(follow_up).await {
        CallEnd::Settled(Outcome::Cancelled { reason }) => {
            assert_eq!(reason, CancelReason::Requested);
        }
        other => panic!("expected the cancelled follow-up, got {other:?}"),
    }
    assert!(matches!(handle.health(), Ok(PluginHealth::Healthy)));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn the_bootstrap_leaks_nothing_and_diagnostics_stay_diagnostics() {
    let revision = lifecycle_revision("bootstrap", 'a');
    let (driver, observer) = scripted(
        &revision,
        vec![
            ProcActivationPlan::worker("bootstrap-report"),
            ProcActivationPlan::worker("bootstrap-report"),
        ],
    );
    let mut first_rig = Rig::new("proc.bootstrap.first", &revision);
    let mut first = HostPluginActivation::prepare(
        &mut first_rig.slot,
        first_rig.epoch,
        &first_rig.broker,
        &first_rig.grants,
        std::sync::Arc::clone(&driver),
    )
    .expect("preparation is inert and succeeds");
    let first_id = first.id().clone();
    let _first_handle = first.activate(&first_rig.registry).await.expect("starts");

    // The sibling spawns while the first worker — and the host's end of its
    // channel — is live, which is exactly when a descriptor could cross.
    let mut second_rig = Rig::new("proc.bootstrap.second", &revision);
    let mut second = HostPluginActivation::prepare(
        &mut second_rig.slot,
        second_rig.epoch,
        &second_rig.broker,
        &second_rig.grants,
        driver,
    )
    .expect("preparation is inert and succeeds");
    let second_id = second.id().clone();
    let _second_handle = second.activate(&second_rig.registry).await.expect("starts");

    // The handshake completing IS the fd-3 evidence: the worker found its
    // channel with no path, port, or token to name it by. What remains is
    // the negative space — the environment carries only the allowlist, and
    // the descriptor table carries only stdio and the channel, even with a
    // sibling activation's socketpair live in the host.
    for id in [&first_id, &second_id] {
        let (env, fds) = bootstrap_report(&observer, id).await;
        assert_eq!(
            env, "env:PATH",
            "the worker environment is the allowlist and nothing else"
        );
        assert_eq!(
            fds, "fds:0,1,2,3",
            "the worker holds stdio and its channel and nothing else"
        );
    }
    // Diagnostics ride stdout as text; the protocol never does.
    let call = observer
        .begin_call(&second_id, "tool.echo", json!("still-serving"), None)
        .await
        .expect("the call opens");
    assert!(matches!(
        settled_within(call).await,
        CallEnd::Settled(Outcome::Ok { .. })
    ));
    stop_active(second, &second_rig.registry, second_rig.epoch).await;
    stop_active(first, &first_rig.registry, first_rig.epoch).await;
}

#[tokio::test]
async fn a_worker_that_stops_reading_stalls_neither_the_clock_nor_the_kill() {
    let revision = lifecycle_revision("deaf", 'b');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("deaf")],
        ProcLimits {
            kill_grace_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.deaf", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let pid = observer.worker_pid(&id).expect("a live worker has a pid");

    // A payload far past the socketpair buffer, toward a worker that will
    // never read it: only a pump whose writes are one select arm among
    // many keeps running the deadline clock through the back-pressure.
    let flood = json!("x".repeat(200 * 1024));
    let call = observer
        .begin_call(&id, "tool.flood", flood, Some(50))
        .await
        .expect("the call opens");
    match settled_within(call).await {
        CallEnd::Lost { error, .. } => assert_eq!(error, WireErrorKind::DeadlineExceeded),
        other => panic!("expected the deadline, got {other:?}"),
    }
    // No goodbye can even be written, so this deactivation is the forced
    // path — grace, group SIGKILL, reap — with the process provably gone.
    stop_active(activation, &rig.registry, rig.epoch).await;
    process_gone(pid).await;
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

#[tokio::test]
async fn a_mute_worker_fails_start_at_the_handshake_deadline() {
    let revision = lifecycle_revision("mute", 'c');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::handshake_timeout()],
        ProcLimits {
            handshake_deadline_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.mute", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    // A worker that connects and says nothing is invisible to the
    // protocol; the driver's clock is the only defence, and it must fail
    // the start rather than let it pend forever.
    let failure = match activation.activate(&rig.registry).await {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a mute worker must fail its start: {other:?}"),
    };
    assert!(
        failure.summary().contains("did not complete within 100ms"),
        "the failure names the exhausted budget: {}",
        failure.summary()
    );
    // The child was spawned before the clock ran out; cleanup releases it.
    activation.finish_stop().await.expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

#[tokio::test]
async fn deactivation_settles_work_the_worker_still_holds() {
    let revision = lifecycle_revision("handover", 'd');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("cancel-ack")]);
    let mut rig = Rig::new("proc.handover", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    let call = observer
        .begin_call(&id, "tool.hold", json!(null), None)
        .await
        .expect("the call opens");
    // Deactivate while the worker holds the call. The work was handed
    // over, so its fate is unknowable: no outcome may be invented, no
    // waiter may be dropped in silence.
    stop_active(activation, &rig.registry, rig.epoch).await;
    match settled_within(call).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::OutcomeUnknown);
            assert!(reconcile, "handed-over work always requires reconciliation");
        }
        other => panic!("expected outcome-unknown, got {other:?}"),
    }
    // The recorded cause is the host's own goodbye, not a synthetic loss.
    let summary = observer.close_summary(&id).expect("the session ended");
    assert!(
        summary.contains("host goodbye"),
        "the close names the goodbye as first cause: {summary}"
    );
}

#[tokio::test]
async fn an_abandoned_activation_reaps_its_worker() {
    let revision = lifecycle_revision("abandon", 'e');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::ready()]);
    let mut rig = Rig::new("proc.abandon", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        std::sync::Arc::clone(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");
    let pid = observer.worker_pid(&id).expect("a live worker has a pid");

    // No deactivation, no release: the guard, handle, and slot just
    // vanish, the way a buggy composition loses them. The driver and
    // observer both outlive the abandonment, and neither may pin the
    // worker alive — the pump treats its last command sender dropping as
    // the abandonment signal and reclaims the process itself.
    drop(handle);
    drop(activation);
    drop(rig);
    process_gone(pid).await;
    assert_eq!(observer.deactivation_calls(&id), 0);
}

#[tokio::test]
async fn a_dead_worker_is_seen_even_when_a_descendant_holds_its_channel() {
    let revision = lifecycle_revision("descendant", 'f');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("spawn-helper")]);
    let mut rig = Rig::new("proc.descendant", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    // The worker exits immediately, but its helper inherited fd 3, so the
    // socket never reaches end-of-file: the process's own exit is the only
    // signal, and health must see it anyway.
    health_becomes(&handle, "unhealthy", |health| {
        matches!(health, PluginHealth::Unhealthy { .. })
    })
    .await;
    // The dead session refuses new work instead of queueing it toward a
    // socket only the helper still holds.
    let refused = observer
        .begin_call(&id, "tool.echo", json!(null), None)
        .await;
    assert!(
        refused.is_err(),
        "a dead session must refuse new work: {:?}",
        refused.err()
    );
    // Reclaim sweeps the worker's process group: the helper dies with the
    // activation rather than orphaning with the host's ambient authority.
    // (The leaked-child gate would fail this test if it survived.)
    stop_active(activation, &rig.registry, rig.epoch).await;
}
