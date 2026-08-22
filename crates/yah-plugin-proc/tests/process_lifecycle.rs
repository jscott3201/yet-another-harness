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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use fixtures::{lifecycle_revision, worker_program};
use rig::{Rig, diagnostics_show, endpoint, health_becomes, scripted, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{DriverKind, HostPluginActivation, PluginHealth};
use yah_plugin_ipc::types::{CancelReason, CancelTarget, Outcome};
use yah_plugin_proc::{
    Availability, CallTerminal, EndpointError, ProcActivationPlan, ProcLimits, ProcessPluginDriver,
    ResourceState, WORKER_CHANNEL_FD, WorkerMethod, WorkerMethodRegistry, WorkerMethodRequest,
    WorkerMethodResult,
};

struct BlockingMethod {
    entered: AtomicBool,
    cancelled: AtomicBool,
    open: AtomicBool,
}

impl WorkerMethod for BlockingMethod {
    fn invoke(
        &self,
        request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, yah_plugin_proc::WorkerMethodFailure> {
        self.entered.store(true, Ordering::Release);
        while !self.open.load(Ordering::Acquire) {
            if request.is_cancelled() {
                self.cancelled.store(true, Ordering::Release);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        WorkerMethodResult::new(json!({ "returned": true }))
            .map_err(|_| yah_plugin_proc::WorkerMethodFailure::failed("fixture result rejected"))
    }
}

async fn provider_enters(provider: &BlockingMethod) {
    for _ in 0..500 {
        if provider.entered.load(Ordering::Acquire) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the scripted worker never reached the blocked provider");
}

async fn process_gone_within(pid: i32, bound: Duration) -> bool {
    tokio::time::timeout(bound, async {
        loop {
            if unsafe { libc::kill(pid, 0) } == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok()
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
        rig::as_trait(&driver),
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

    // The fresh activation on the same driver is the restart. A clone
    // survives the move into prepare, so the endpoint stays reachable.
    let endpoint_driver = std::sync::Arc::clone(&driver);
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
    let call = endpoint(&endpoint_driver, &fresh_id)
        .call("tool.echo", json!({"alive": true}), None)
        .await
        .expect("the call opens");
    match settled_within(call).await {
        CallTerminal::Completed(Outcome::Ok { result }) => {
            assert_eq!(result, json!({"alive": true}))
        }
        other => panic!("expected the echo, got {other:?}"),
    }
    stop_active(fresh, &rig.registry, rig.epoch).await;
}

/// A retained endpoint is bound to its own activation. A later activation on
/// the same driver must neither admit it nor receive a call it attempted.
#[tokio::test]
async fn a_stopped_endpoint_cannot_retarget_a_replacement_activation() {
    let revision = lifecycle_revision("stale-endpoint", '4');
    let (driver, observer) = scripted(
        &revision,
        vec![
            ProcActivationPlan::worker("audit-echo"),
            ProcActivationPlan::worker("audit-echo"),
        ],
    );
    let mut first_rig = Rig::new("proc.stale-endpoint.first", &revision);
    let mut first = HostPluginActivation::prepare(
        &mut first_rig.slot,
        first_rig.epoch,
        &first_rig.broker,
        &first_rig.grants,
        rig::as_trait(&driver),
    )
    .expect("first preparation succeeds");
    let first_id = first.id().clone();
    let _first_handle = first
        .activate(&first_rig.registry)
        .await
        .expect("first starts");
    let retained = endpoint(&driver, &first_id);
    stop_active(first, &first_rig.registry, first_rig.epoch).await;

    assert!(
        matches!(retained.availability(), Availability::Closed { .. }),
        "a retained endpoint remains typed closed after its activation stops"
    );
    let retained_refusal = retained
        .call("tool.echo", json!({ "source": "A" }), None)
        .await;

    let mut second_rig = Rig::new("proc.stale-endpoint.second", &revision);
    let mut second = HostPluginActivation::prepare(
        &mut second_rig.slot,
        second_rig.epoch,
        &second_rig.broker,
        &second_rig.grants,
        rig::as_trait(&driver),
    )
    .expect("replacement preparation succeeds");
    let second_id = second.id().clone();
    let _second_handle = second
        .activate(&second_rig.registry)
        .await
        .expect("replacement starts");

    // This branch is unreachable in the real implementation. It is kept so
    // the stale-resolution mutation can prove that an A lookup would send a
    // real call to B before the test records its failure after cleanup.
    let stale_lookup_closed = match driver.endpoint(&first_id) {
        Err(EndpointError::Closed { .. } | EndpointError::Closing) => true,
        Ok(retargeted) => {
            let leaked = retargeted
                .call("tool.echo", json!({ "source": "A" }), None)
                .await
                .expect("a retargeted endpoint would admit against B");
            assert!(
                matches!(settled_within(leaked).await, CallTerminal::Completed(_)),
                "the mutation must reach B rather than merely manufacture an endpoint"
            );
            false
        }
        Err(other) => panic!("stale lookup must be closed, got {other:?}"),
    };
    let fresh = endpoint(&driver, &second_id)
        .call("tool.echo", json!({ "source": "B" }), None)
        .await
        .expect("the replacement endpoint serves");
    assert_eq!(
        settled_within(fresh).await,
        CallTerminal::Completed(Outcome::Ok {
            result: json!({ "source": "B" }),
        })
    );
    diagnostics_show(&observer, &second_id, "audit:{\"source\":\"B\"}").await;
    let second_trace = observer
        .diagnostics_tail(&second_id, yah_plugin_proc::DiagnosticStream::Stdout)
        .unwrap_or_default();
    stop_active(second, &second_rig.registry, second_rig.epoch).await;

    assert!(
        matches!(
            retained_refusal,
            Err(EndpointError::Closed { .. } | EndpointError::Closing)
        ),
        "the retained endpoint admits nothing after A stops"
    );
    assert!(
        stale_lookup_closed,
        "an A lookup must not resolve to B or admit a call there"
    );
    assert!(
        !second_trace.contains("\"source\":\"A\"") && second_trace.contains("\"source\":\"B\""),
        "B receives only its own endpoint call: {second_trace}"
    );
}

/// Scope cancellation is visible to an already-running registered method, but
/// a synchronous callback cannot be interrupted. It still cannot delay
/// endpoint withdrawal, worker reap, or activation cleanup.
#[tokio::test]
async fn scope_cancellation_reaps_without_waiting_for_a_sync_method() {
    let revision = lifecycle_revision("blocked-close", 'b');
    let provider = Arc::new(BlockingMethod {
        entered: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        open: AtomicBool::new(false),
    });
    let mut methods = WorkerMethodRegistry::new();
    methods
        .register(
            "application.cancel",
            Arc::clone(&provider) as Arc<dyn WorkerMethod>,
        )
        .expect("method registers before activation");
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits_and_methods(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("registered-cancel-during")],
        ProcLimits {
            kill_grace_ms: 20,
            ..ProcLimits::default()
        },
        methods,
    );
    let mut rig = Rig::new("proc.blocked-close", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        rig::as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let retained = endpoint(&driver, &id);
    provider_enters(&provider).await;
    let pid = observer.worker_pid(&id).expect("the worker is live");

    let (slot, _activation_handle) = activation.release_active().expect("releases active");
    slot.reconcile(
        &rig.registry,
        yah_compose::DesiredComponentState::removed(slot.generation(2)),
    )
    .expect("starts closing and cancels the scope");

    let fresh_admission = retained.call("tool.echo", json!("after-close"), None).await;
    let reaped_before_provider_returns = process_gone_within(pid, Duration::from_secs(3)).await;
    let cancellation_observed = tokio::time::timeout(Duration::from_secs(3), async {
        while !provider.cancelled.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    tokio::time::timeout(Duration::from_secs(5), slot.finish_stop(rig.epoch))
        .await
        .expect("cleanup is not blocked by the synchronous method")
        .expect("cleanup succeeds while the method remains blocked");
    assert_eq!(
        observer.resource_state(&id),
        Ok(ResourceState::Released),
        "deactivation retires the already-reaped pump"
    );
    assert!(
        !provider.open.load(Ordering::Acquire),
        "the callback was not forcibly interrupted"
    );

    assert!(
        matches!(
            fresh_admission,
            Err(EndpointError::Closing | EndpointError::Closed { .. })
        ) && reaped_before_provider_returns
            && cancellation_observed,
        "scope cancellation must withdraw admission, notify the callback, and reap before it returns; \
         admission_withdrawn={}, reaped_before_return={reaped_before_provider_returns}, \
         cancellation_observed={cancellation_observed}",
        fresh_admission.is_err(),
    );
    provider.open.store(true, Ordering::Release);
}

#[tokio::test]
async fn a_disconnect_settles_in_flight_work_outcome_unknown() {
    let revision = lifecycle_revision("disconnect", '7');
    let (driver, _observer) =
        scripted(&revision, vec![ProcActivationPlan::worker("exit-mid-call")]);
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.disconnect", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    let call = endpoint(&endpoint_driver, &id)
        .call("tool.slow", json!(null), None)
        .await
        .expect("the call opens");
    // The worker exits on receiving the call, without answering: the one
    // outcome the host must not invent is success or failure.
    match settled_within(call).await {
        CallTerminal::LostOutcomeUnknown => {
            // A disconnect always requires reconciliation; the terminal
            // says so by being this variant and no other.
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
async fn a_protocol_fatal_retains_its_kind_separately_from_disconnect() {
    let revision = lifecycle_revision("fatal", 'c');
    let (driver, _observer) = scripted(
        &revision,
        vec![ProcActivationPlan::worker("fatal-mid-call")],
    );
    let endpoint_driver = Arc::clone(&driver);
    let mut rig = Rig::new("proc.fatal", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let call = endpoint(&endpoint_driver, &id)
        .call("application.fatal", json!(null), None)
        .await
        .expect("call admits before the fault");
    assert_eq!(
        settled_within(call).await,
        CallTerminal::LostProtocolFault {
            kind: yah_plugin_ipc::types::WireErrorKind::UnknownHandle,
        }
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn a_cancelled_call_still_terminates_with_the_cancelled_outcome() {
    let revision = lifecycle_revision("cancel", '8');
    let (driver, _observer) = scripted(&revision, vec![ProcActivationPlan::worker("cancel-ack")]);
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.cancel", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    let activation_endpoint = endpoint(&endpoint_driver, &id);
    let call = activation_endpoint
        .call("tool.hold", json!(null), None)
        .await
        .expect("the call opens");
    activation_endpoint
        .cancel(call.call_id(), CancelTarget::Call)
        .await
        .expect("the cancel goes out");
    // The worker acknowledges by answering — silence would be
    // indistinguishable from a lost worker.
    match settled_within(call).await {
        CallTerminal::Completed(Outcome::Cancelled { reason }) => {
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
    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut rig = Rig::new("proc.deadline", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");

    // The worker holds the call until cancelled; the budget expires first,
    // the host enforces it, and the worker's cancelled answer arrives as
    // the tolerated late terminal.
    let activation_endpoint = endpoint(&endpoint_driver, &id);
    let call = activation_endpoint
        .call("tool.hold", json!(null), Some(50))
        .await
        .expect("the call opens");
    let expired_id = call.call_id();
    match settled_within(call).await {
        // The worker may have acted before the budget; the terminal says
        // reconcile-first by being DeadlineExceeded and no other variant.
        CallTerminal::DeadlineExceeded => {}
        other => panic!("expected deadline-exceeded, got {other:?}"),
    }
    // The late terminal is observed, not assumed: the worker only answers
    // a call whose cancel it received, and it reports each answer on the
    // diagnostics lane — so this line existing means the expiry's cancel
    // went out and the retired call was answered late. The wire is
    // ordered, so by the time the follow-up below settles, that late
    // answer was already fed to the session — and tolerated.
    diagnostics_show(&observer, &id, &format!("answered:{}", expired_id.0)).await;
    let follow_up = activation_endpoint
        .call("tool.hold", json!(null), None)
        .await
        .expect("the session still serves after the late terminal");
    activation_endpoint
        .cancel(follow_up.call_id(), CancelTarget::Call)
        .await
        .expect("the cancel goes out");
    match settled_within(follow_up).await {
        CallTerminal::Completed(Outcome::Cancelled { reason }) => {
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

    let endpoint_driver = std::sync::Arc::clone(&driver);
    let mut first_rig = Rig::new("proc.bootstrap.first", &revision);
    let mut first = HostPluginActivation::prepare(
        &mut first_rig.slot,
        first_rig.epoch,
        &first_rig.broker,
        &first_rig.grants,
        rig::as_trait(&driver),
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
    let call = endpoint(&endpoint_driver, &second_id)
        .call("tool.echo", json!("still-serving"), None)
        .await
        .expect("the call opens");
    assert!(matches!(
        settled_within(call).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    stop_active(second, &second_rig.registry, second_rig.epoch).await;
    stop_active(first, &first_rig.registry, first_rig.epoch).await;
}
