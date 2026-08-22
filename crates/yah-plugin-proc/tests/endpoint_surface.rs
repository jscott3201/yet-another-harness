//! Production endpoint behavior through real host activations and workers.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use fixtures::{lifecycle_revision, worker_program};
use rig::{Rig, as_trait, diagnostics_show, endpoint, process_gone, settled_within, stop_active};
use serde_json::json;
use yah_compose::DesiredComponentState;
use yah_plugin_host::{DriverKind, HostPluginActivation};
use yah_plugin_ipc::types::{Outcome, StreamClass};
use yah_plugin_proc::{
    ArtifactReader, Availability, CallTerminal, EndpointError, ProcActivationPlan,
    ProcessPluginDriver, Refusal, ResourceState,
};

#[tokio::test]
async fn endpoint_is_unavailable_while_hello_accept_is_pending() {
    let revision = lifecycle_revision("endpoint-pending", '9');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::pending_start()],
    );
    let mut rig = Rig::new("proc.endpoint-pending", &revision);
    let removed = DesiredComponentState::removed(rig.slot.generation(2));
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    {
        let start = activation.activate(&rig.registry);
        tokio::pin!(start);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), start.as_mut())
                .await
                .is_err(),
            "the silent worker remains before readiness"
        );
    }
    assert!(matches!(
        driver.endpoint(&id),
        Err(EndpointError::NotNegotiated)
    ));
    let pid = observer
        .worker_pid(&id)
        .expect("pending start owns a worker");
    activation
        .reconcile(&rig.registry, removed)
        .expect("pending start begins removal");
    activation
        .finish_stop()
        .await
        .expect("pending worker cleanup completes");
    process_gone(pid).await;
}

#[tokio::test]
async fn endpoint_publication_and_withdrawal_follow_the_exact_activation() {
    let revision = lifecycle_revision("endpoint", 'a');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::ready()],
    );
    let mut rig = Rig::new("proc.endpoint", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    assert!(matches!(
        driver.endpoint(&id),
        Err(EndpointError::NotStarted)
    ));

    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let retained = endpoint(&driver, &id);
    assert_eq!(retained.activation_id(), &id);
    assert_eq!(retained.availability(), Availability::Active);
    let pid = observer.worker_pid(&id).expect("worker is live");

    stop_active(activation, &rig.registry, rig.epoch).await;
    assert!(matches!(
        retained.availability(),
        Availability::Closed { .. }
    ));
    assert!(matches!(
        retained.call("application.echo", json!(null), None).await,
        Err(EndpointError::Closed { .. } | EndpointError::Closing)
    ));
    assert!(matches!(
        driver.endpoint(&id),
        Err(EndpointError::Closed { .. })
    ));
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
    process_gone(pid).await;
}

#[tokio::test]
async fn endpoint_bounds_precede_admission_and_a_real_call_settles() {
    let revision = lifecycle_revision("endpoint-bounds", 'b');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::ready()],
    );
    let mut rig = Rig::new("proc.endpoint-bounds", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let endpoint = endpoint(&driver, &id);
    let before = observer.gauges(&id).expect("live gauges");

    assert!(matches!(
        endpoint
            .call(
                "application.echo",
                json!("x".repeat(yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES + 1)),
                None,
            )
            .await,
        Err(EndpointError::Refused(Refusal::PayloadTooLarge { .. }))
    ));
    assert!(matches!(
        endpoint.call("", json!(null), None).await,
        Err(EndpointError::Refused(Refusal::InvalidField(_)))
    ));
    assert_eq!(
        before.command_channel_available,
        observer
            .gauges(&id)
            .expect("live gauges")
            .command_channel_available,
        "pre-admission refusals spend no command slot"
    );

    let call = endpoint
        .call("application.echo", json!({ "value": 7 }), None)
        .await
        .expect("bounded call admits");
    assert_eq!(
        settled_within(call).await,
        CallTerminal::Completed(Outcome::Ok {
            result: json!({ "value": 7 }),
        })
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn stream_delivery_conserves_credit_and_keeps_its_terminal() {
    let revision = lifecycle_revision("stream", 'c');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("stream-items:20")],
    );
    let mut rig = Rig::new("proc.stream", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let mut stream = endpoint(&driver, &id)
        .call_stream("application.stream", json!(null), None)
        .await
        .expect("stream admits");
    let mut seq = Vec::new();
    while let Some(frame) = stream.next_item().await {
        assert_eq!(frame.class, StreamClass::Lossless);
        assert_eq!(frame.dropped, 0);
        seq.push(frame.seq);
    }
    assert_eq!(seq, (0..20).collect::<Vec<_>>());
    assert_eq!(stream.local_drops(), 0);
    assert_eq!(
        stream.terminal().await.expect("terminal lands"),
        CallTerminal::Completed(Outcome::Ok {
            result: json!({ "streamed": 20 }),
        })
    );
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn stream_credit_is_earned_by_drainage_not_empty_capacity() {
    let revision = lifecycle_revision("stream-credit", 'f');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("stream-stall:16")],
    );
    let mut rig = Rig::new("proc.stream-credit", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let endpoint = endpoint(&driver, &id);
    let mut stream = endpoint
        .call_stream("application.stream", json!(null), None)
        .await
        .expect("stream admits");
    diagnostics_show(&observer, &id, "stall:sent:16").await;

    let probe = endpoint
        .call("control.probe", json!(null), None)
        .await
        .expect("probe admits");
    assert_eq!(
        settled_within(probe).await,
        CallTerminal::Completed(Outcome::Ok {
            result: json!({ "grants": 0 }),
        }),
        "unused channel capacity grants no credit"
    );
    assert_eq!(stream.next_item().await.expect("one queued item").seq, 0);
    diagnostics_show(&observer, &id, "stall:credit:1").await;
    let probe = endpoint
        .call("control.probe", json!(null), None)
        .await
        .expect("second probe admits");
    assert_eq!(
        settled_within(probe).await,
        CallTerminal::Completed(Outcome::Ok {
            result: json!({ "grants": 1 }),
        }),
        "one drained frame funds one replacement grant"
    );
    drop(stream);
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn lossy_stream_flood_cannot_displace_credited_lossless_data() {
    let revision = lifecycle_revision("stream-lossy", '1');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("stream-lossy-flood:3000")],
    );
    let mut rig = Rig::new("proc.stream-lossy", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let mut stream = endpoint(&driver, &id)
        .call_stream("application.stream", json!(null), None)
        .await
        .expect("stream admits");
    let mut last = None;
    let mut previous_drops = 0;
    while let Some(frame) = stream.next_item().await {
        assert!(frame.dropped >= previous_drops, "drop count is monotonic");
        previous_drops = frame.dropped;
        last = Some(frame);
    }
    let last = last.expect("the final lossless frame is delivered");
    assert_eq!(last.class, StreamClass::Lossless);
    assert!(!last.more);
    assert!(last.dropped > 0 && stream.local_drops() > 0);
    assert!(matches!(
        stream.terminal().await,
        Ok(CallTerminal::Completed(_))
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn dropped_call_and_stream_waiters_retire_without_blocking_reap() {
    let revision = lifecycle_revision("drop-waiters", 'd');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("cancel-ack")],
    );
    let mut rig = Rig::new("proc.drop-waiters", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let endpoint = endpoint(&driver, &id);
    let call = endpoint
        .call("application.hold", json!(null), None)
        .await
        .expect("call admits");
    drop(call);
    let stream = endpoint
        .call_stream("application.hold-stream", json!(null), None)
        .await
        .expect("stream admits");
    drop(stream);

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            if observer
                .gauges(&id)
                .is_some_and(|gauges| gauges.pending_calls == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped receivers retire their pump waiters");
    let pid = observer.worker_pid(&id).expect("worker remains owned");
    stop_active(activation, &rig.registry, rig.epoch).await;
    process_gone(pid).await;
}

#[tokio::test]
async fn spilled_result_reads_verifies_releases_and_reaches_zero() {
    let revision = lifecycle_revision("spill", 'e');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("spill:50000")],
    );
    let mut rig = Rig::new("proc.spill", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let endpoint = endpoint(&driver, &id);
    let offer = match settled_within(
        endpoint
            .call("application.make", json!(null), None)
            .await
            .expect("call admits"),
    )
    .await
    {
        CallTerminal::Completed(Outcome::Spilled { artifact }) => artifact,
        other => panic!("expected spill, got {other:?}"),
    };
    let mut reader = ArtifactReader::new(&endpoint, offer.clone(), 1 << 20).expect("within limit");
    let mut bytes = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.expect("chunk verifies") {
        bytes.extend_from_slice(&chunk);
    }
    assert_eq!(bytes.len(), 50000);
    reader.verify().expect("whole-object digest matches");
    endpoint
        .release_worker_handle(offer.handle)
        .await
        .expect("worker acknowledges release");
    assert_eq!(observer.gauges(&id).expect("live").pending_releases, 0);
    stop_active(activation, &rig.registry, rig.epoch).await;
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}
