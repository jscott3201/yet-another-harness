//! The acceptance triad — restart, disconnect, cancel — plus deadline
//! behaviour and the bootstrap hygiene the fd-3 design promises. The
//! misbehaving-worker corpus lives in `process_supervision.rs`.
//!
//! Every case drives a real child through the host's own activation guard,
//! not through driver internals: the same path a composition uses. Restart
//! means what the protocol allows it to mean: a dead worker poisons its
//! activation permanently (there is no resume), health says so, cleanup
//! releases the process, and recovery is a fresh activation with a fresh
//! process — proved here on the same shared driver.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use fixtures::lifecycle_revision;
use rig::{Rig, diagnostics_show, health_becomes, scripted, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{HostPluginActivation, PluginHealth};
use yah_plugin_ipc::types::{CancelReason, CancelTarget, Outcome, WireErrorKind};
use yah_plugin_proc::{CallEnd, ProcActivationPlan, WORKER_CHANNEL_FD};

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
    let expired_id = call.call_id();
    match settled_within(call).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::DeadlineExceeded);
            assert!(reconcile, "the worker may have acted before the budget");
        }
        other => panic!("expected deadline-exceeded, got {other:?}"),
    }
    // The late terminal is observed, not assumed: the worker only answers
    // a call whose cancel it received, and it reports each answer on the
    // diagnostics lane — so this line existing means the expiry's cancel
    // went out and the retired call was answered late. The wire is
    // ordered, so by the time the follow-up below settles, that late
    // answer was already fed to the session — and tolerated.
    diagnostics_show(&observer, &id, &format!("answered:{}", expired_id.0)).await;
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
    // Arm the hazard the descriptor sweep exists for: a descriptor this
    // process holds WITHOUT close-on-exec, the way one inherited from a
    // shell or a git hook arrives. `F_DUPFD` (unlike Rust's own opens)
    // leaves the flag clear, and the floor keeps it clear of the channel.
    let ambient = unsafe { libc::fcntl(0, libc::F_DUPFD, WORKER_CHANNEL_FD + 7) };
    // The worker's probe enumerates fds 0..64, so the hazard must sit
    // inside that window or the assertion below pins nothing.
    assert!(
        ambient > WORKER_CHANNEL_FD && ambient < 64,
        "the ambient hazard sits inside the worker's probe range: {ambient}"
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
    // the descriptor table carries only stdio and the channel, with both a
    // sibling activation's socketpair and the armed ambient descriptor
    // live in the host.
    for id in [&first_id, &second_id] {
        let report = rig::bootstrap_report(&observer, id).await;
        assert_eq!(
            report.0, "env:PATH",
            "the worker environment is the allowlist and nothing else"
        );
        assert_eq!(
            report.1, "fds:0,1,2,3",
            "the worker holds stdio and its channel and nothing else"
        );
    }
    unsafe {
        libc::close(ambient);
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
