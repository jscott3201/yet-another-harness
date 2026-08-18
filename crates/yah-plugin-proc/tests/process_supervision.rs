//! The misbehaving-worker corpus: every guarantee the supervisor keeps
//! when a worker does the wrong thing. A worker that stops reading, a
//! worker past the outbound cap, a worker that never speaks, a program
//! that does not exist, work still in the worker's hands at deactivation,
//! an activation its composition abandons, a death only the process — not
//! the socket — reveals, and a polite goodbye racing its own exit.
//!
//! The cooperative corpus (restart, disconnect, cancel, deadline,
//! bootstrap hygiene) lives in `process_lifecycle.rs`; the shared harness
//! in `support/rig.rs` drives everything through the host's own guard.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use fixtures::lifecycle_revision;
use rig::{
    Rig, health_becomes, process_gone, reported_helper_pid, scripted, settled_within, stop_active,
};
use serde_json::json;
use yah_plugin_host::{DriverKind, HostPluginActivation, PluginHealth, PluginStartError};
use yah_plugin_ipc::types::{Outcome, WireErrorKind};
use yah_plugin_proc::{
    CallEnd, ProcActivationPlan, ProcLimits, ProcessPluginDriver, ResourceState,
};

#[tokio::test]
async fn a_worker_that_stops_reading_stalls_neither_the_clock_nor_the_kill() {
    let revision = lifecycle_revision("deaf", 'b');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
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

    // Enough payload to back up any platform's socketpair buffer (Linux's
    // default takes ~208 KiB in one write; macOS's ~8 KiB), toward a
    // worker that will never read it: only a pump whose writes are one
    // select arm among many keeps running the deadline clock through the
    // back-pressure.
    let mut calls = Vec::new();
    for _ in 0..6 {
        calls.push(
            observer
                .begin_call(&id, "tool.flood", json!("x".repeat(200 * 1024)), Some(50))
                .await
                .expect("the call opens"),
        );
    }
    for call in calls {
        match settled_within(call).await {
            CallEnd::Lost { error, .. } => assert_eq!(error, WireErrorKind::DeadlineExceeded),
            other => panic!("expected the deadline, got {other:?}"),
        }
    }
    // No goodbye can even be written, so this deactivation is the forced
    // path — grace, group SIGKILL, reap — with the process provably gone.
    stop_active(activation, &rig.registry, rig.epoch).await;
    process_gone(pid).await;
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

#[tokio::test]
async fn a_worker_past_the_outbound_cap_is_declared_dead_not_buffered() {
    let revision = lifecycle_revision("outcap", '1');
    // A cap of one byte: deliberately below any admitted frame, so the
    // start-time clamp to the session's own frame ceiling is what actually
    // governs — a conformant worker must never be accused over a single
    // frame it was not given the chance to read.
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("deaf")],
        ProcLimits {
            outbound_buffer_cap_bytes: 1,
            kill_grace_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.outcap", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let pid = observer.worker_pid(&id).expect("a live worker has a pid");

    // Flood past the clamped floor (~1 MiB) against a worker that will
    // never drain it; later opens may find the session already ended.
    let mut calls = Vec::new();
    for _ in 0..8 {
        match observer
            .begin_call(&id, "tool.flood", json!("x".repeat(200 * 1024)), None)
            .await
        {
            Ok(call) => calls.push(call),
            Err(_) => break,
        }
    }
    // At least two opens prove the clamp: an unclamped one-byte cap would
    // end the session at the first frame, before a second call could open.
    assert!(
        calls.len() >= 2,
        "the cap floor admits at least one full frame: {} opened",
        calls.len()
    );
    for call in calls {
        match settled_within(call).await {
            CallEnd::Lost { reconcile, .. } => {
                assert!(reconcile, "the worker may hold the flooded calls");
            }
            other => panic!("expected the calls lost at the cap, got {other:?}"),
        }
    }
    let summary = observer.close_summary(&id).expect("the session ended");
    assert!(
        summary.contains("stopped draining"),
        "the close names the cap, not a generic loss: {summary}"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
    process_gone(pid).await;
}

#[tokio::test]
async fn a_terminal_arriving_during_deactivation_still_settles_as_itself() {
    let revision = lifecycle_revision("latereply", '4');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("late-reply")]);
    let mut rig = Rig::new("proc.latereply", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    let call = observer
        .begin_call(&id, "tool.echo", json!("handed-over"), None)
        .await
        .expect("the call opens");
    // The worker read the call and went deaf; its answer lands a beat
    // later, while the host is already deactivating. The exit path drains
    // the receive buffer before declaring input over, so the caller gets
    // the real outcome — not a synthetic outcome-unknown for work that
    // was in fact answered.
    stop_active(activation, &rig.registry, rig.epoch).await;
    match settled_within(call).await {
        CallEnd::Settled(Outcome::Ok { result }) => assert_eq!(result, json!("handed-over")),
        other => panic!("expected the late terminal to settle the call, got {other:?}"),
    }
}

#[tokio::test]
async fn a_mute_worker_fails_start_at_the_handshake_deadline() {
    let revision = lifecycle_revision("mute", 'c');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
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
async fn a_missing_worker_program_fails_the_spawn_not_the_protocol() {
    let revision = lifecycle_revision("nospawn", '2');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        "/nonexistent/yah-worker-binary",
        vec![ProcActivationPlan::ready()],
    );
    let mut rig = Rig::new("proc.nospawn", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    // The most common misconfiguration — a wrong worker path — must fail
    // as exactly that, not masquerade as a worker that ran and
    // disconnected mid-protocol. (The descriptor sweep marks rather than
    // closes precisely so std's exec-status pipe survives to report this.)
    let failure = match activation.activate(&rig.registry).await {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a missing program must fail its start: {other:?}"),
    };
    assert!(
        failure.summary().contains("worker did not spawn"),
        "the failure names the spawn, not the protocol: {}",
        failure.summary()
    );
    // Nothing was spawned, so cleanup releases nothing.
    activation.finish_stop().await.expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::NotAcquired));
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
async fn a_polite_goodbye_with_work_in_hand_is_not_a_bare_disconnect() {
    let revision = lifecycle_revision("polite", '3');
    let (driver, observer) = scripted(
        &revision,
        vec![ProcActivationPlan::worker("goodbye-mid-call")],
    );
    let mut rig = Rig::new("proc.polite", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    // The worker answers the call with a goodbye and exits while a helper
    // holds its channel open, so no end-of-file ever arrives: only the
    // buffered goodbye — drained when the child's exit is detected —
    // distinguishes this from a bare disconnect. Loss is classified, not
    // collapsed: a goodbye settles cancelled, without reconciliation.
    let call = observer
        .begin_call(&id, "tool.hold", json!(null), None)
        .await
        .expect("the call opens");
    match settled_within(call).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::Cancelled);
            assert!(!reconcile, "a goodbye settles without reconciliation");
        }
        other => panic!("expected the goodbye's cancellation, got {other:?}"),
    }
    let summary = observer.close_summary(&id).expect("the session ended");
    assert!(
        summary.contains("worker goodbye"),
        "the close names the worker's goodbye: {summary}"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
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
    let helper_pid = reported_helper_pid(&observer, &id).await;

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
    // Reclaim sweeps the worker's process group: the helper provably dies
    // with the activation rather than orphaning with the host's ambient
    // authority.
    stop_active(activation, &rig.registry, rig.epoch).await;
    process_gone(helper_pid).await;
}
