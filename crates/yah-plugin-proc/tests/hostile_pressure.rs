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
//! Handle release/reclaim races over real transport are pinned where the
//! surface lives: the session-level model corpus covers the wire law, and
//! the endpoint tests drive the dispatcher's capability table through the
//! real transport.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use fixtures::lifecycle_revision;
use rig::{
    Rig, diagnostics_show, endpoint, health_becomes, process_gone, settled_within, stop_active,
};
use serde_json::json;
use yah_plugin_host::{DriverKind, HostPluginActivation, PluginHealth};
use yah_plugin_proc::{
    CallTerminal, EndpointError, ProcActivationPlan, ProcLimits, ProcessPluginDriver, Refusal,
    ResourceState,
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
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.flood", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();

    // A host call with a real deadline, issued into the flood: if the
    // pump were wedged processing the flood, this deadline could not
    // fire.
    let call = endpoint(&endpoint_driver, &id)
        .call("tool.probe", json!("probe"), Some(60))
        .await
        .expect("the host side still admits calls during the flood");
    match settled_within(call).await {
        CallTerminal::DeadlineExceeded => {}
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
/// once at deactivation, and the channel drains back to its full
/// capacity.
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
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.cmdflood", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    // No deadlines: an admitted call's slot never frees on its own, so
    // the rejections below are deterministic rather than a race between
    // the flood and the expiry clock.
    let activation_endpoint = endpoint(&endpoint_driver, &id);
    let mut calls = Vec::new();
    for _ in 0..64 {
        calls.push(activation_endpoint.call("tool.flood", json!("x".repeat(64 * 1024)), None));
    }
    let mut admitted = Vec::new();
    let mut rejected = 0usize;
    for call in calls {
        match call.await {
            Ok(pending) => admitted.push(pending),
            Err(error) => {
                // Rejection must be observable and named: pre-admission
                // pressure or a session ceiling, never a silent queue.
                assert!(
                    matches!(
                        error,
                        EndpointError::AtCapacity | EndpointError::Refused(Refusal::CallCeiling)
                    ),
                    "rejection must be observable and named: {error:?}"
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
        match tokio::time::timeout(std::time::Duration::from_secs(5), call.terminal()).await {
            Ok(Ok(CallTerminal::LostOutcomeUnknown)) => settled += 1,
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
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.chaos", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    let pid = observer.worker_pid(&id).expect("live pid");

    diagnostics_show(&observer, &id, "chaos:begin").await;
    let mut calls = Vec::new();
    let activation_endpoint = endpoint(&endpoint_driver, &id);
    for _ in 0..8 {
        if let Ok(call) = activation_endpoint
            .call("tool.probe", json!("x".repeat(32 * 1024)), Some(120))
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
        match tokio::time::timeout(std::time::Duration::from_secs(5), call.terminal()).await {
            Ok(Ok(CallTerminal::LostOutcomeUnknown)) => {}
            Ok(Ok(CallTerminal::Completed(_))) => {}
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

/// The queue itself, not just the session ceiling: submissions are polled
/// one at a time without ever yielding to the runtime, so the pump — a
/// task on this same current-thread runtime — cannot drain between them.
/// Every channel slot fills through the production submission function
/// and the next submission executes the pre-admission `try_send`
/// rejection — the queue's `Full` branch, distinct from a session
/// refusal. Deactivation then ends the activation within a named bound
/// regardless of the backlog, and every submission resolves exactly
/// once: whatever the pump dequeued first is admitted work settled as
/// outcome-unknown, and whatever it never dequeued ends observably with
/// the pump — no lost waiter either way.
#[tokio::test]
async fn a_saturated_command_queue_rejects_pre_admission_and_deactivation_resolves_every_submission()
 {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    let revision = lifecycle_revision("queuesat", 'c');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("deaf")],
        pressure_limits(),
    );
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.queuesat", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    let capacity = observer
        .gauges(&id)
        .expect("live gauges")
        .command_channel_capacity;

    // Single-poll each submission: the endpoint call's first poll runs
    // its synchronous head — admission gate through `try_send` — and
    // parks on the `opened` oneshot. Polling with a no-op waker never
    // yields, so the pump cannot interleave and the queue genuinely
    // fills.
    let activation_endpoint = endpoint(&endpoint_driver, &id);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut submissions: Vec<_> = (0..=capacity)
        .map(|_| {
            Some(Box::pin(activation_endpoint.call(
                "tool.queue",
                json!("q"),
                None,
            )))
        })
        .collect();
    let mut queued = 0usize;
    let mut queue_rejected = Vec::new();
    for slot in submissions.iter_mut() {
        let submission = slot.as_mut().expect("filled below");
        match submission.as_mut().poll(&mut cx) {
            Poll::Pending => queued += 1,
            Poll::Ready(result) => {
                // Completed futures are consumed here: polling one twice
                // is a bug, and the resolution phase must only see the
                // still-pending queue.
                let _ = slot.take();
                match result {
                    Err(error) => queue_rejected.push(error),
                    Ok(_) => {
                        panic!("a queued-slot submission cannot open before any pump tick")
                    }
                }
            }
        }
    }
    assert_eq!(queued, capacity, "every slot fills before the rejection");
    assert_eq!(
        queue_rejected,
        vec![EndpointError::AtCapacity],
        "the one-past submission is the queue's Full branch, not a session refusal"
    );
    // Nothing was dequeued: no id minted, no session admission, no
    // waiter in the pending table.
    let gauges = observer.gauges(&id).expect("live gauges");
    assert_eq!(gauges.command_channel_available, 0);
    assert_eq!(gauges.pending_calls, 0);

    // Deactivation with the backlog in place: the watch signal
    // preempts at the pump's next loop-top — it must never wait out an
    // unbounded drain — and the whole activation ends within a named
    // bound.
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stop_active(activation, &rig.registry, rig.epoch),
    )
    .await
    .expect("deactivation does not wait behind a full command queue");

    // Every submission resolves exactly once, and no waiter is lost:
    // dequeued-before-shutdown submissions were admitted and settle as
    // outcome-unknown with reconciliation required; the rest ended with
    // the pump, their `opened` oneshots gone — observable, never
    // silent.
    let mut admitted = Vec::new();
    let mut ended_with_pump = 0usize;
    for slot in submissions.iter_mut().flatten() {
        let resolved = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if let Poll::Ready(result) = slot.as_mut().poll(&mut cx) {
                    return result;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("no queued submission is left hanging");
        match resolved {
            Ok(pending) => admitted.push(pending),
            Err(error) => {
                assert!(
                    matches!(error, EndpointError::Closed { .. }),
                    "an undelivered call ends observably: {error:?}"
                );
                ended_with_pump += 1;
            }
        }
    }
    assert_eq!(
        admitted.len() + ended_with_pump + queue_rejected.len(),
        capacity + 1,
        "every submission is accounted for exactly once"
    );
    for pending in admitted {
        let settled = tokio::time::timeout(std::time::Duration::from_secs(5), pending.terminal())
            .await
            .expect("admitted work settles despite the backlog");
        assert!(
            matches!(settled, Ok(CallTerminal::LostOutcomeUnknown)),
            "outcome-unknown settlement expected, got {settled:?}"
        );
    }
    assert!(
        observer.gauges(&id).is_none(),
        "gauges do not outlive the pump"
    );
    assert!(observer.resource_state(&id) == Ok(ResourceState::Released));
}

/// A worker that hoards every frame, then drains and echoes, then shuts
/// its read half while staying alive: the host's outbound buffer grows
/// to a real high-water allocation, is observed still allocated after it
/// empties (occupancy is not allocation), and is observed released —
/// capacity zero, truthfully — once the read half's EPIPE half-closes
/// the output direction. A worker that never reads cannot reach the
/// drain, and a worker that dies takes its gauges with it, so the
/// hoard-drain-shut script is the one that can pin the policy live.
///
/// Linux-only because the drain needs the kernel to wake a full
/// socketpair's writer as the peer reads — Linux epoll does, macOS
/// kqueue does not, so there the drain would stall by platform
/// semantics rather than by any property of this driver. The canonical
/// Linux lane runs this in hosted CI and in the pinned-image stress.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_half_closed_output_releases_its_buffer_and_reports_zero_capacity() {
    let revision = lifecycle_revision("halfclose", 'd');
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        fixtures::worker_program(),
        vec![ProcActivationPlan::worker("hoard-drain-shut:400:300:30000")],
        pressure_limits(),
    );
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.halfclose", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let id = activation.id().clone();
    // Sixteen near-maximal calls (~4 MiB of frames, well past what the
    // channel absorbs, well under the outbound cap that would reclaim
    // the session) land while the worker is hoarding. Sixteen, not
    // more: nothing settles during the hoard, so the session's 16-call
    // in-flight ceiling is the whole allowance.
    let activation_endpoint = endpoint(&endpoint_driver, &id);
    for _ in 0..16 {
        activation_endpoint
            .call("tool.hoard", json!("x".repeat(250_000)), None)
            .await
            .expect("the pump admits while its buffer grows");
    }
    // The worker drains and echoes: occupancy falls to zero, but the
    // allocation must still be there — a live pump keeps its high-water
    // buffer for reuse, and the gauge must not pretend otherwise.
    let mut high_water = 0usize;
    let mut drained = false;
    for _ in 0..500 {
        if let Some(gauges) = observer.gauges(&id) {
            high_water = high_water.max(gauges.outbound_buffer_capacity);
            if gauges.outbound_buffer_bytes == 0 && high_water > 1024 * 1024 {
                assert_eq!(
                    gauges.outbound_buffer_capacity, high_water,
                    "an emptied buffer stays allocated while the pump is live"
                );
                drained = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(drained, "the hoard must drain (high water {high_water})");
    // The worker's idle clock is running; give it past the idle bound so
    // the read half is certainly shut, then write one more frame: the
    // failing write half-closes the output direction, and the zero the
    // gauge reports from then on must be the truth about retained
    // memory — the buffer released, not merely cleared.
    tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
    let probe_a = activation_endpoint
        .call("tool.probe", json!("a"), None)
        .await
        .expect("the probe is admitted");
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let probe_b = activation_endpoint
        .call("tool.probe", json!("b"), None)
        .await
        .expect("the second probe is admitted");
    let mut released = false;
    for _ in 0..500 {
        if let Some(gauges) = observer.gauges(&id)
            && gauges.outbound_buffer_capacity == 0
        {
            assert_eq!(gauges.outbound_buffer_bytes, 0);
            released = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(released, "the half-close must release the grown buffer");
    // The lingering worker holds the session open; deactivation ends it,
    // and both probes resolve — no lost waiter, whichever path each took.
    stop_active(activation, &rig.registry, rig.epoch).await;
    for pending in [probe_a, probe_b] {
        let settled = tokio::time::timeout(std::time::Duration::from_secs(5), pending.terminal())
            .await
            .expect("every probe settles");
        assert!(settled.is_ok(), "a probe settles exactly once: {settled:?}");
    }
    assert!(observer.resource_state(&id) == Ok(ResourceState::Released));
}
