//! Hostile transport pressure against the real supervised process: floods,
//! starvation, partial input, trickles, races, and deactivation under
//! load — the claims that involve IO or scheduling, driven through the
//! same activation guard a composition uses.
//!
//! The session-level model corpus (in `yah-plugin-ipc`) pins the state
//! rules; this file pins what only a real socketpair and a real child can:
//! that a hostile peer can poison only its own activation — never wedge
//! the pump, starve the deadline clock or the child-exit watch, leak a
//! waiter, or grow host memory past a named bound.
//!
//! Handle release/reclaim races over real transport are absent here on
//! purpose: the pump exposes no handle surface yet (no application sits
//! above the driver — that is YAH-M4-03), so those races are pinned at
//! the session level by the model corpus and the resource-handle
//! fixtures.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use fixtures::lifecycle_revision;
use rig::{Rig, diagnostics_show, health_becomes, process_gone, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{DriverKind, HostPluginActivation, PluginHealth};
use yah_plugin_ipc::types::WireErrorKind;
use yah_plugin_proc::{
    CallEnd, ProcActivationPlan, ProcLimits, ProcessPluginDriver, ResourceState,
};

fn pressure_limits() -> ProcLimits {
    ProcLimits {
        kill_grace_ms: 100,
        handshake_deadline_ms: 2_000,
        ..ProcLimits::default()
    }
}

/// A worker that floods 400 unsolicited calls without ever reading: the
/// in-flight ceiling must refuse every one past the bound — spending an
/// id per refusal, never queueing — while the deadline clock keeps
/// running and a host call still settles by deadline.
#[tokio::test]
async fn an_inbound_call_flood_dies_at_the_ceiling_without_wedging_the_pump() {
    let revision = lifecycle_revision("flood", 'a');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("flood")],
        pressure_limits(),
    );
    let mut rig = Rig::new("proc.flood", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();

    // A host call with a real deadline, issued into the flood: if the
    // pump were wedged processing the flood, this deadline could not
    // fire.
    let call = observer
        .begin_call(&id, "tool.probe", json!("probe"), Some(60))
        .await
        .expect("the host side still admits calls during the flood");
    match settled_within(call).await {
        CallEnd::Lost {
            error: WireErrorKind::DeadlineExceeded,
            reconcile: true,
        } => {}
        other => panic!("the probe call should die at its deadline under the flood, got {other:?}"),
    }
    diagnostics_show(&observer, &id, "flooded:400").await;

    // Deactivation ends it cleanly; the flood worker never reads, so the
    // kill path does the reclaim.
    stop_active(activation, &rig.registry, rig.epoch).await;
    assert!(observer.resource_state(&id) == Ok(ResourceState::Released));
}

/// A caller-side command flood against a worker that never reads: the
/// bounded command channel rejects what it cannot hold (observable, with
/// nothing admitted behind the rejection), the session's in-flight
/// ceiling refuses the rest, every admitted call still settles exactly
/// once by deadline, and the channel drains back to its full capacity.
#[tokio::test]
async fn a_command_flood_meets_bounded_admission_and_loses_no_admitted_call() {
    let revision = lifecycle_revision("cmdflood", 'b');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("deaf")],
        pressure_limits(),
    );
    let mut rig = Rig::new("proc.cmdflood", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    // No deadlines: an admitted call's slot never frees on its own, so
    // the rejections below are deterministic rather than a race between
    // the flood and the expiry clock.
    let mut calls = Vec::new();
    for _ in 0..64 {
        calls.push(observer.begin_call(&id, "tool.flood", json!("x".repeat(64 * 1024)), None));
    }
    let mut admitted = Vec::new();
    let mut rejected = 0usize;
    for call in calls {
        match call.await {
            Ok(pending) => admitted.push(pending),
            Err(message) => {
                assert!(
                    message.contains("at capacity") || message.contains("refused the call"),
                    "rejection must be observable and named: {message}"
                );
                rejected += 1;
            }
        }
    }
    assert!(
        rejected > 0,
        "a 64-call flood must be rejected: the deaf worker never frees a slot"
    );
    assert!(
        admitted.len() <= 16,
        "admissions are bounded by the session's in-flight ceiling, got {}",
        admitted.len()
    );
    // The channel drained back to its full capacity: rejection pressure
    // left no residue, and every admitted call holds its waiter while
    // the activation is live.
    let gauges = observer.gauges(&id).expect("live gauges");
    assert_eq!(
        gauges.command_channel_available,
        gauges.command_channel_capacity
    );
    assert_eq!(gauges.pending_calls, admitted.len());
    // Deactivation settles every admitted call exactly once —
    // outcome-unknown with reconciliation required, since the deaf
    // worker may have acted on work whose outcome was never learned.
    stop_active(activation, &rig.registry, rig.epoch).await;
    assert!(observer.resource_state(&id) == Ok(ResourceState::Released));
    let expected = gauges.pending_calls;
    let mut settled = 0usize;
    for call in admitted {
        match tokio::time::timeout(std::time::Duration::from_secs(5), call.settled()).await {
            Ok(Ok(CallEnd::Lost {
                error: WireErrorKind::OutcomeUnknown,
                reconcile: true,
            })) => settled += 1,
            other => panic!("outcome-unknown settlement expected, got {other:?}"),
        }
    }
    assert_eq!(settled, expected, "no admitted call is lost or doubled");
}

/// Partial input at end-of-input: a worker that dies mid-prefix and one
/// that dies mid-payload both close as protocol faults — never as clean
/// disconnects, never as hangs.
#[tokio::test]
async fn partial_input_at_eof_is_a_fault_not_a_hang() {
    for mode in ["half-prefix", "half-payload"] {
        let revision = lifecycle_revision(mode, 'c');
        let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
            revision.id().clone(),
            DriverKind::NodeProcess,
            fixtures::worker_program(),
            vec![ProcActivationPlan::worker(mode)],
            pressure_limits(),
        );
        let mut rig = Rig::new(&format!("proc.{mode}"), &revision);
        let mut activation = HostPluginActivation::prepare(
            &mut rig.slot,
            rig.epoch,
            &rig.broker,
            &rig.grants,
            driver,
        )
        .expect("preparation is inert and succeeds");
        let handle = activation.activate(&rig.registry).await.expect("starts");
        let id = activation.id().clone();
        let pid = observer.worker_pid(&id).expect("live pid");
        health_becomes(&handle, "the mid-frame fault", |health| {
            matches!(health, PluginHealth::Unhealthy { .. } if matches!(&health, PluginHealth::Unhealthy { summary } if summary.contains("invalid-frame")))
        })
        .await;
        process_gone(pid).await;
        assert!(
            observer
                .close_summary(&id)
                .expect("a closed session has a summary")
                .contains("invalid-frame"),
            "{mode}: the truncated input must be the named cause"
        );
    }
}

/// A goodbye that arrives slower than any single read — one byte per
/// millisecond — is still a goodbye, not a disconnect: the bounded drain
/// must reassemble it.
#[tokio::test]
async fn a_trickled_goodbye_is_not_lost_to_the_drain_bound() {
    let revision = lifecycle_revision("trickle", 'd');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("trickle-goodbye")],
        ProcLimits {
            kill_grace_ms: 100,
            handshake_deadline_ms: 2_000,
            // A coarse tick keeps the test honest: the goodbye must be
            // classified by the drain, not by a lucky tick.
            tick_interval_ms: 50,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.trickle", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    let pid = observer.worker_pid(&id).expect("live pid");
    health_becomes(&handle, "the trickled goodbye", |health| {
        matches!(&health, PluginHealth::Unhealthy { summary } if summary.contains("worker goodbye"))
    })
    .await;
    process_gone(pid).await;
    assert!(
        observer
            .close_summary(&id)
            .expect("summary")
            .contains("worker goodbye"),
        "a trickled goodbye classifies as a goodbye"
    );
}

/// Diagnostics a worker writes immediately before dying are retained:
/// the exit path drains the pipes after the reap.
#[tokio::test]
async fn diagnostics_written_just_before_death_are_retained() {
    let revision = lifecycle_revision("diagdie", 'e');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("diag-then-die")],
        pressure_limits(),
    );
    let mut rig = Rig::new("proc.diagdie", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    let pid = observer.worker_pid(&id).expect("live pid");
    diagnostics_show(&observer, &id, "last-words:retained").await;
    process_gone(pid).await;
    assert!(
        observer
            .close_summary(&id)
            .expect("summary")
            .contains("outcome-unknown"),
        "a death without goodbye is a disconnect"
    );
}

/// Everything at once: an inbound call flood, a diagnostic line, a worker
/// that then goes deaf, host calls with deadlines riding the flood, and a
/// deactivation in the middle of it. Every admitted call settles exactly
/// once, the clock and the kill path stay live, and the process group is
/// reclaimed.
#[tokio::test]
async fn deactivation_under_combined_pressure_reclaims_everything() {
    let revision = lifecycle_revision("chaos", 'f');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("chaos")],
        ProcLimits {
            kill_grace_ms: 100,
            ..ProcLimits::default()
        },
    );
    let mut rig = Rig::new("proc.chaos", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    let pid = observer.worker_pid(&id).expect("live pid");

    diagnostics_show(&observer, &id, "chaos:begin").await;
    let mut calls = Vec::new();
    for _ in 0..8 {
        if let Ok(call) = observer
            .begin_call(&id, "tool.probe", json!("x".repeat(32 * 1024)), Some(120))
            .await
        {
            calls.push(call);
        }
    }
    assert!(
        !calls.is_empty(),
        "the host side admits calls during the flood"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
    assert!(observer.resource_state(&id) == Ok(ResourceState::Released));
    process_gone(pid).await;
    for call in calls {
        // Deactivation settles held work outcome-unknown — the flood may
        // have kept the worker busy, so reconciliation is required.
        match tokio::time::timeout(std::time::Duration::from_secs(5), call.settled()).await {
            Ok(Ok(CallEnd::Lost {
                error: WireErrorKind::OutcomeUnknown,
                reconcile: true,
            })) => {}
            Ok(Ok(CallEnd::Settled(_))) => {}
            other => panic!("every admitted call settles exactly once, got {other:?}"),
        }
    }
    assert!(
        observer.gauges(&id).is_none(),
        "gauges do not outlive the pump"
    );
}

/// The configured command-channel bound is the bound the channel actually
/// has: the gauge reads it back from the channel, so a pump that ignored
/// its configuration could not hide behind the default.
#[tokio::test]
async fn the_command_channel_reports_its_configured_bound() {
    let revision = lifecycle_revision("chanbound", '7');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("conformant")],
        ProcLimits {
            command_channel_capacity: 2,
            ..pressure_limits()
        },
    );
    let mut rig = Rig::new("proc.chanbound", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    let gauges = observer.gauges(&id).expect("live gauges");
    assert_eq!(gauges.command_channel_capacity, 2);
    assert!(gauges.command_channel_available <= 2);
    // The accept frame may still sit in the outbound buffer at read
    // time; what must hold is the frame bound, not a race with the
    // handshake.
    assert!(
        gauges.outbound_buffer_bytes <= yah_plugin_ipc::MAX_FRAME_BYTES + 64,
        "a fresh pump buffers at most its handshake traffic"
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
    assert!(observer.gauges(&id).is_none(), "gauges end with the pump");
}
