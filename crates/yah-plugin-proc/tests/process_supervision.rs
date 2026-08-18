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
    CallEnd, DiagnosticStream, ProcActivationPlan, ProcLimits, ProcessPluginDriver, ResourceState,
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
    // Retirement keeps terminal facts only: the live pump link — and with
    // it the pid and the diagnostics buffers — does not survive release.
    assert!(
        observer.worker_pid(&id).is_none(),
        "the pid does not survive release"
    );
    assert!(
        observer
            .diagnostics_tail(&id, DiagnosticStream::Stdout)
            .is_none(),
        "diagnostics do not survive release"
    );
}

#[tokio::test]
async fn a_goodbye_behind_a_failing_write_is_still_a_goodbye() {
    let revision = lifecycle_revision("goodbyewrite", '5');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("goodbye-then-linger")],
        ProcLimits {
            kill_grace_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.goodbyewrite", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let pid = observer.worker_pid(&id).expect("a live worker has a pid");

    // One held call, then a flood the worker will never read. The worker
    // shuts its read half before its goodbye exists, so on Linux the
    // host's write provably fails first — and a failed write must not end
    // input: the goodbye that follows still decides the exit (cancelled,
    // no reconciliation — never a bare disconnect). On macOS the kernel
    // hides a peer's read-shutdown from the writer entirely, so there the
    // goodbye simply arrives on the read arm and the same classification
    // must hold; the write-failure half of the pin is exercised on the
    // platform CI runs.
    let held = observer
        .begin_call(&id, "tool.hold", json!(null), None)
        .await
        .expect("the call opens");
    let mut floods = Vec::new();
    for _ in 0..6 {
        match observer
            .begin_call(&id, "tool.flood", json!("x".repeat(200 * 1024)), None)
            .await
        {
            Ok(call) => floods.push(call),
            Err(_) => break,
        }
    }
    match settled_within(held).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::Cancelled);
            assert!(!reconcile, "a goodbye settles without reconciliation");
        }
        other => panic!("expected the goodbye's cancellation, got {other:?}"),
    }
    let summary = observer.close_summary(&id).expect("the session ended");
    assert!(
        summary.contains("worker goodbye"),
        "the close names the goodbye, not the failed write: {summary}"
    );
    for call in floods {
        let _ = settled_within(call).await;
    }
    // The lingering worker only dies by the kill.
    stop_active(activation, &rig.registry, rig.epoch).await;
    process_gone(pid).await;
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

#[tokio::test]
async fn deactivation_reclaims_everything_even_at_a_generous_grace() {
    let revision = lifecycle_revision("slowgrace", '7');
    // A grace past the point where the exit path's two windows exceed the
    // old single-grace join bound: the worst case a real composition can
    // configure, against the worst worker — deaf, jammed, holding a
    // descendant. Both processes must still die, and the full exit path
    // (not the abort fallback) must be what runs.
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("deaf-with-helper")],
        ProcLimits {
            kill_grace_ms: 2_200,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.slowgrace", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let pid = observer.worker_pid(&id).expect("a live worker has a pid");
    let helper_pid = reported_helper_pid(&observer, &id).await;

    // Jam the socket so the goodbye flush burns its whole window.
    let call = observer
        .begin_call(&id, "tool.flood", json!("x".repeat(200 * 1024)), Some(50))
        .await
        .expect("the call opens");
    match settled_within(call).await {
        CallEnd::Lost { error, .. } => assert_eq!(error, WireErrorKind::DeadlineExceeded),
        other => panic!("expected the deadline, got {other:?}"),
    }
    let began = std::time::Instant::now();
    stop_active(activation, &rig.registry, rig.epoch).await;
    // The floor proves the pump's own exit ran to its sweep instead of
    // being aborted at the old single-grace bound (which sits below it).
    assert!(
        began.elapsed() >= std::time::Duration::from_millis(4_400),
        "the exit path ran both grace windows: {:?}",
        began.elapsed()
    );
    process_gone(pid).await;
    process_gone(helper_pid).await;
}

#[tokio::test]
async fn an_abandoned_worker_that_left_its_group_is_still_reclaimed() {
    let revision = lifecycle_revision("nogroup", '8');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("leave-group")],
        ProcLimits {
            kill_grace_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.nogroup", &revision);
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
    // The worker provably left the group the bootstrap made it lead, so
    // the group sweep signals an empty group and only the direct leader
    // kill reclaims anything.
    rig::diagnostics_show(&observer, &id, "left-group:ok").await;
    let pid = observer.worker_pid(&id).expect("a live worker has a pid");

    drop(handle);
    drop(activation);
    drop(rig);
    process_gone(pid).await;
}

#[tokio::test]
async fn a_zero_tick_interval_is_clamped_not_a_panic() {
    let revision = lifecycle_revision("zerotick", '9');
    // A zero interval would panic tokio's timer inside the pump task,
    // failing every activation with a spurious handshake error; the clamp
    // to the clock's floor is what makes this succeed.
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::ready()],
        ProcLimits {
            tick_interval_ms: 0,
            handshake_deadline_ms: 1_000,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.zerotick", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let call = observer
        .begin_call(&id, "tool.echo", json!("ticking"), None)
        .await
        .expect("the call opens");
    assert!(matches!(
        settled_within(call).await,
        CallEnd::Settled(Outcome::Ok { .. })
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// Only Linux surfaces a peer's read-shutdown to writers (macOS accepts
/// the writes outright, measured), so this pin runs where CI does.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_worker_that_stops_reading_without_goodbye_is_reported_not_healthy() {
    let revision = lifecycle_revision("halfdead", '0');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("read-shut-linger")],
        ProcLimits {
            kill_grace_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.halfdead", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    // One call the worker reads before shutting its read half, then a
    // flood whose write failure is what kills the outbound direction.
    let _held = observer
        .begin_call(&id, "tool.hold", json!(null), None)
        .await
        .expect("the call opens");
    let _jam = observer
        .begin_call(&id, "tool.flood", json!("x".repeat(200 * 1024)), None)
        .await
        .expect("the call opens");
    // No goodbye, no end-of-file, no exit: health is the only signal, and
    // it must name the half-death rather than report Healthy forever.
    let health = health_becomes(&handle, "unhealthy", |health| {
        matches!(health, PluginHealth::Unhealthy { .. })
    })
    .await;
    let PluginHealth::Unhealthy { summary } = health else {
        unreachable!()
    };
    assert!(
        summary.contains("stopped reading"),
        "health names the half-death: {summary}"
    );
    // A call opened after the outbound death provably never reached the
    // worker: it settles immediately, cancelled, without reconciliation.
    let late = observer
        .begin_call(&id, "tool.late", json!(null), None)
        .await
        .expect("the session still admits the call");
    match settled_within(late).await {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::Cancelled);
            assert!(
                !reconcile,
                "an untransmitted call demands no reconciliation"
            );
        }
        other => panic!("expected the never-delivered settlement, got {other:?}"),
    }
    stop_active(activation, &rig.registry, rig.epoch).await;
}
