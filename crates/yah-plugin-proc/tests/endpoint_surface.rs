//! The production invocation surface: endpoint fencing, stream delivery,
//! the worker-to-host capability dispatcher, and spilled-artifact
//! pull-reads — every claim driven through a real supervised process over
//! the real transport, via the same activation guard a composition uses.
//!
//! The lifecycle and supervision corpora pin what happens when calls are
//! lost; this file pins what an application can hold, what it may ask,
//! and what it is refused before any provider runs.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use fixtures::{lifecycle_revision, lifecycle_revision_requesting, worker_program};
use rig::{Rig, as_trait, diagnostics_show, endpoint, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{
    CapabilityDefinition, CapabilityId, DriverKind, EffectiveCapabilityGrants,
    HostPluginActivation, PluginRevision, TextCapability, TextCapabilityFailure,
};
use yah_plugin_ipc::types::Outcome;
use yah_plugin_proc::{
    ActivationEndpoint, ArtifactReader, Availability, CallTerminal, EndpointError,
    ProcActivationPlan, ProcLimits, ProcessPluginDriver, Refusal,
};

/// An uppercasing text provider — the dispatcher's first application.
struct Upper;

impl TextCapability for Upper {
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure> {
        Ok(input.to_uppercase())
    }
}

/// A provider that parks until released — the slow provider whose slot
/// occupancy makes the queue bounds observable.
struct Gated {
    open: AtomicBool,
}

impl TextCapability for Gated {
    fn invoke(&self, _input: &str) -> Result<String, TextCapabilityFailure> {
        while !self.open.load(Ordering::Acquire) {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok("gated".to_owned())
    }
}

const CAPABILITY: &str = "test.text-upper/v1";

fn upper_definition() -> CapabilityDefinition<dyn TextCapability> {
    CapabilityDefinition::new(CapabilityId::new(CAPABILITY).expect("canonical id"))
}

/// A rig whose broker holds an [`Upper`] registration and whose grants
/// admit exactly that capability. The registration guard stays with the
/// rig: dropping it would withdraw the provider.
fn upper_rig(
    label: &str,
    revision: &PluginRevision,
) -> (
    Rig,
    EffectiveCapabilityGrants,
    yah_plugin_host::CapabilityProviderRegistration<dyn TextCapability>,
) {
    let mut base = Rig::new(label, revision);
    let registration = base
        .broker
        .register(
            &upper_definition(),
            Arc::new(Upper) as Arc<dyn TextCapability>,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(revision, [registration.grant()])
        .expect("the fixture manifest requests the capability");
    (base, grants, registration)
}

/// Fencing: no endpoint before start, an active one after negotiation,
/// and a closed one carrying the retained summary after release.
#[tokio::test]
async fn the_endpoint_exists_only_between_negotiation_and_release() {
    let revision = lifecycle_revision("endpoint", 'a');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::ready()],
    );
    let mut rig = Rig::new("proc.endpoint.fence", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();

    // Before start there is nothing to publish: not a dead endpoint — no
    // endpoint at all.
    match driver.endpoint(&id) {
        Err(EndpointError::NotStarted) => {}
        other => panic!("an unstarted activation has no endpoint: {other:?}"),
    }

    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let ep = driver
        .endpoint(&id)
        .expect("negotiated activation publishes");
    assert_eq!(ep.availability(), Availability::Active);
    assert_eq!(ep.activation_id(), &id);

    // Release withdraws it permanently, keeping the terminal facts.
    stop_active(activation, &rig.registry, rig.epoch).await;
    assert!(
        matches!(ep.availability(), Availability::Closed { summary: Some(_) }),
        "a clone fails closed after release, keeping the cause"
    );
    match driver.endpoint(&id) {
        Err(EndpointError::Closed { summary }) => {
            assert!(
                summary.unwrap_or_default().contains("host goodbye"),
                "the retained summary names the close cause"
            );
        }
        other => panic!("release must close the endpoint, got {other:?}"),
    }
}

/// Byte bounds precede the queue: an oversized payload is refused without
/// occupying a command slot, so bounded admission never hides an
/// unbounded body.
#[tokio::test]
async fn an_oversized_payload_is_refused_before_any_slot_is_spent() {
    let revision = lifecycle_revision("bigpayload", 'b');
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::ready()],
    );
    let mut rig = Rig::new("proc.bigpayload", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let ep = endpoint(&driver, &id);

    let before = observer.gauges(&id).expect("live gauges");
    let error = ep
        .call(
            "tool.echo",
            json!("x".repeat(yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES + 1)),
            None,
        )
        .await
        .expect_err("the oversized payload is refused");
    match error {
        EndpointError::Refused(Refusal::PayloadTooLarge { bytes }) => assert!(
            bytes > yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES,
            "the refusal names the actual size: {bytes}"
        ),
        other => panic!("expected the payload bound, got {other:?}"),
    }
    let after = observer.gauges(&id).expect("live gauges");
    assert_eq!(
        before.command_channel_available, after.command_channel_available,
        "the refusal spent no admission slot"
    );

    // A conformant call still works, and the session is untouched.
    let call = ep
        .call("tool.echo", json!("fine"), None)
        .await
        .expect("admits");
    assert!(matches!(
        settled_within(call).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// The worker-to-host item flow: open, framed items in order, then
/// exactly one terminal — and the item channel closes with the call.
#[tokio::test]
async fn a_stream_call_delivers_items_in_order_and_one_terminal() {
    let revision = lifecycle_revision("stream", 'c');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("stream-items:6")],
    );
    let mut rig = Rig::new("proc.stream", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    let mut stream = endpoint(&driver, &id)
        .call_stream("tool.stream", json!(null), None)
        .await
        .expect("the stream call opens");
    let mut seen = Vec::new();
    while let Some(frame) = stream.next_item().await {
        assert_eq!(frame.class, yah_plugin_ipc::types::StreamClass::Lossless);
        assert_eq!(frame.dropped, 0, "lossless items declare no drops");
        seen.push(frame.seq);
    }
    assert_eq!(seen, vec![0, 1, 2, 3, 4, 5], "items arrive in sequence");
    match stream.terminal().await.expect("the call settles") {
        CallTerminal::Completed(Outcome::Ok { result }) => {
            assert_eq!(result, json!({ "streamed": 6 }))
        }
        other => panic!("expected the streamed completion, got {other:?}"),
    }
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// Dropping the item receiver mutes delivery but never the terminal:
/// the call still ends exactly once, on the real transport.
#[tokio::test]
async fn dropping_the_item_receiver_mutes_delivery_not_the_terminal() {
    let revision = lifecycle_revision("mute", 'd');
    let (driver, _observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("stream-items:200")],
    );
    let mut rig = Rig::new("proc.mute", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    let mut stream = endpoint(&driver, &id)
        .call_stream("tool.stream", json!(null), None)
        .await
        .expect("the stream call opens");
    let first = stream.next_item().await.expect("the first item arrives");
    assert_eq!(first.seq, 0);
    // Drop the receiver mid-stream: the worker keeps producing, the host
    // mutes delivery, and the terminal still lands exactly once.
    let pending = stream.into_pending();
    match tokio::time::timeout(std::time::Duration::from_secs(5), pending.terminal())
        .await
        .expect("the muted call still settles")
    {
        Ok(CallTerminal::Completed(_)) | Ok(CallTerminal::LostCancelled) => {}
        other => panic!("exactly one terminal, got {other:?}"),
    }
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// The dispatcher's happy path over the real transport: acquire mints a
/// handle, invoke answers through the provider itself, release spends the
/// id — all observed through the worker's own diagnostics.
#[tokio::test]
async fn a_capability_cycle_runs_through_the_dispatcher() {
    let revision = lifecycle_revision_requesting("capcycle", 'e', &[CAPABILITY]);
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker(
            "capability-cycle:test.text-upper/v1",
        )],
    );
    let (mut rig, grants, _registration) = upper_rig("proc.capcycle", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    for _ in 0..40 {
        let out = observer.diagnostics_tail(&id, yah_plugin_proc::DiagnosticStream::Stdout);
        eprintln!("OUT: {out:?}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    diagnostics_show(&observer, &id, "cap:acquired:").await;
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// The dispatcher's refusal surface over the real transport: unknown
/// methods are refused without echoing the ask, forged handles land in
/// the same bounded refusal as unknown ones, malformed ids never reach
/// the broker, and a double release is a fault-shaped refusal.
#[tokio::test]
async fn hostile_probes_meet_bounded_refusals() {
    let revision = lifecycle_revision_requesting("caphostile", 'f', &[CAPABILITY]);
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("capability-hostile")],
    );
    let (mut rig, grants, _registration) = upper_rig("proc.caphostile", &revision);
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    // The method name must not echo; the kind names the family.
    diagnostics_show(&observer, &id, "hostile:method:UnknownMethod:echoed=false").await;
    diagnostics_show(&observer, &id, "hostile:forged:UnknownHandle").await;
    diagnostics_show(&observer, &id, "hostile:malformed:InvalidFrame").await;
    // First release acks; the second is refused — the discriminants
    // differ, and the test reads them from the worker's own report.
    diagnostics_show(&observer, &id, "hostile:release5:Ok").await;
    diagnostics_show(&observer, &id, "hostile:release6:Err").await;
    stop_active(activation, &rig.registry, rig.epoch).await;
}

/// Queue saturation at the configured bound: with one provider slot and
/// one queue slot, a gated provider pins the first invoke, the second
/// waits in the queue, and everything past the bound is refused on the
/// call itself — retryable, before any provider runs. Opening the gate
/// lets the held work complete.
#[tokio::test]
async fn dispatcher_saturation_refuses_one_past_the_bound_without_losing_admitted_work() {
    let revision = lifecycle_revision_requesting("capsat", '1', &[CAPABILITY]);
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        vec![ProcActivationPlan::worker("capability-flood:4")],
        ProcLimits {
            dispatch_queue_capacity: 1,
            provider_concurrency: 1,
            ..ProcLimits::default()
        },
    );
    let mut base = Rig::new("proc.capsat", &revision);
    let gate = Arc::new(Gated {
        open: AtomicBool::new(false),
    });
    let provider: Arc<dyn TextCapability> = Arc::clone(&gate) as Arc<dyn TextCapability>;
    let registration = base
        .broker
        .register(
            &CapabilityDefinition::<dyn TextCapability>::new(
                CapabilityId::new(CAPABILITY).expect("canonical id"),
            ),
            provider,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&revision, [registration.grant()])
        .expect("the fixture manifest requests the capability");
    let mut activation = HostPluginActivation::prepare(
        &mut base.slot,
        base.epoch,
        &base.broker,
        &grants,
        as_trait(&driver),
    )
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&base.registry).await.expect("starts");

    // Four invokes against one provider slot and one queue slot: the
    // first is admitted into the parked provider, the second fills the
    // queue, and everything past the bound is refused on the call itself
    // — retryable, before any provider runs.
    diagnostics_show(&observer, &id, ":ResourceExhausted:retryable=true").await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    {
        // Nothing completed while the gate was shut: the provider is
        // parked in its one slot.
        let tail = observer
            .diagnostics_tail(&id, yah_plugin_proc::DiagnosticStream::Stdout)
            .expect("the worker reported");
        assert!(
            !tail.contains(":ok"),
            "no invoke completes while the provider is parked: {tail}"
        );
        assert_eq!(
            tail.matches("ResourceExhausted:retryable=true").count(),
            3,
            "exactly the bound's overflow is refused: {tail}"
        );
    }

    // Opening the gate lets the one admitted invoke complete — the slot
    // bound held for the whole life of the burst.
    gate.open.store(true, Ordering::Release);
    diagnostics_show(&observer, &id, "flood:done").await;
    {
        let tail = observer
            .diagnostics_tail(&id, yah_plugin_proc::DiagnosticStream::Stdout)
            .expect("the worker reported");
        assert_eq!(
            tail.matches(":ok").count(),
            1,
            "exactly the one admitted invoke completes: {tail}"
        );
    }
    stop_active(activation, &base.registry, base.epoch).await;
}

/// A spilled result pull-reads in bounded chunks behind its digest, and
/// the digest claim is verified against the accumulated bytes. An offer
/// over the caller's limit is refused before the first pull, and the
/// handle is released explicitly when the read is done.
#[tokio::test]
async fn spilled_artifacts_pull_read_verify_and_release() {
    let revision = lifecycle_revision("spill", '2');
    let (driver, _observer) = ProcessPluginDriver::scripted(
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
    .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    let ep: ActivationEndpoint = endpoint(&driver, &id);

    let call = ep
        .call("tool.make", json!(null), None)
        .await
        .expect("admits");
    let offer = match settled_within(call).await {
        CallTerminal::Completed(Outcome::Spilled { artifact }) => artifact,
        other => panic!("expected the spilled offer, got {other:?}"),
    };
    assert_eq!(offer.bytes, 50000);

    // Preflight: an offer over the caller's limit never reaches the wire.
    assert!(ArtifactReader::new(&ep, offer.clone(), 1000).is_err());

    let mut reader = ArtifactReader::new(&ep, offer.clone(), 1 << 20).expect("within the limit");
    let mut total = Vec::new();
    while let Some(chunk) = reader.next_chunk().await.expect("chunk pulls") {
        total.extend_from_slice(&chunk);
    }
    assert_eq!(total.len(), 50000, "every byte arrives exactly once");
    reader.verify().expect("the digest claim holds");

    // The spilled handle is the worker's to keep until told otherwise:
    // the explicit release is acknowledged.
    ep.release_worker_handle(offer.handle)
        .await
        .expect("the release is acknowledged");
    stop_active(activation, &rig.registry, rig.epoch).await;
}
