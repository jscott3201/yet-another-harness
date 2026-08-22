//! Capability cancellation and reclamation across process termination paths.

#[path = "support/fixtures.rs"]
mod fixtures;
#[path = "support/rig.rs"]
mod rig;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;

use fixtures::{capability_revision, worker_program};
use rig::{
    Rig, as_trait, diagnostics_show, endpoint, health_becomes, process_gone, settled_within,
    stop_active,
};
use serde_json::json;
use yah_compose::DesiredComponentState;
use yah_plugin_host::{
    CapabilityDefinition, CapabilityId, CapabilityProviderRegistration, DriverKind,
    EffectiveCapabilityGrants, HostPluginActivation, PluginHealth, TextCapability,
    TextCapabilityFailure,
};
use yah_plugin_ipc::types::Outcome;
use yah_plugin_proc::{
    CallTerminal, EndpointError, ProcActivationPlan, ProcLimits, ProcObserver, ProcessPluginDriver,
    ResourceState,
};

const CAPABILITY_ID: &str = "yah.test.text/v1";

fn definition() -> CapabilityDefinition<dyn TextCapability> {
    CapabilityDefinition::new(CapabilityId::new(CAPABILITY_ID).expect("fixture id is valid"))
}

fn grant(
    rig: &mut Rig,
    revision: &yah_plugin_host::PluginRevision,
    provider: Arc<dyn TextCapability>,
) -> CapabilityProviderRegistration<dyn TextCapability> {
    let registration = rig
        .broker
        .register(&definition(), provider)
        .expect("provider registers");
    rig.grants = EffectiveCapabilityGrants::new(revision, [registration.grant()])
        .expect("requested capability is granted");
    registration
}

async fn handles_zero(observer: &ProcObserver, id: &yah_plugin_host::PluginActivationId) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if observer.capability_handle_gauges(id).is_some_and(|gauges| {
                gauges.session_live_handles == 0 && gauges.process_capability_entries == 0
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both capability handle counts reach zero");
}

struct BlockingText {
    entered: AtomicUsize,
    open: AtomicBool,
}

impl TextCapability for BlockingText {
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        while !self.open.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(input.to_owned())
    }
}

#[tokio::test]
async fn in_flight_cancel_and_scope_close_reap_without_waiting_for_sync_provider() {
    let revision = capability_revision("capability-close", '9', CAPABILITY_ID);
    let provider = Arc::new(BlockingText {
        entered: AtomicUsize::new(0),
        open: AtomicBool::new(false),
    });
    let mut rig = Rig::new("proc.capability-close", &revision);
    let _registration = grant(
        &mut rig,
        &revision,
        Arc::clone(&provider) as Arc<dyn TextCapability>,
    );
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-cancel-during")],
        ProcLimits {
            provider_concurrency: 1,
            dispatch_queue_capacity: 2,
            kill_grace_ms: 20,
            ..ProcLimits::default()
        },
    );
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let _health = activation.activate(&rig.registry).await.expect("starts");
    let retained = endpoint(&driver, &id);
    tokio::time::timeout(Duration::from_secs(5), async {
        while provider.entered.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("provider enters");
    let pid = observer.worker_pid(&id).expect("worker is live");
    let control = retained
        .call("control.cancel", json!(null), None)
        .await
        .expect("worker receives the cancellation barrier");
    assert!(matches!(
        settled_within(control).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    diagnostics_show(&observer, &id, "capability:during:cancelled").await;
    diagnostics_show(&observer, &id, "capability:during:stale-queued").await;

    let (slot, _handle) = activation.release_active().expect("active releases");
    slot.reconcile(
        &rig.registry,
        DesiredComponentState::removed(slot.generation(2)),
    )
    .expect("scope begins closing");
    assert!(matches!(
        retained.call("after.close", json!(null), None).await,
        Err(EndpointError::Closing | EndpointError::Closed { .. })
    ));
    process_gone(pid).await;
    handles_zero(&observer, &id).await;
    assert_eq!(
        provider.entered.load(Ordering::Acquire),
        1,
        "the cancelled invoke's callback alone ran; queued stale work was skipped"
    );
    assert!(!provider.open.load(Ordering::Acquire));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), slot.finish_stop(rig.epoch))
            .await
            .is_err(),
        "host cleanup preserves the admitted-call drain while process reap stays bounded"
    );
    provider.open.store(true, Ordering::Release);
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup resumes after the provider returns");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

struct EchoText;

impl TextCapability for EchoText {
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure> {
        Ok(input.to_owned())
    }
}

#[tokio::test]
async fn goodbye_disconnect_and_protocol_fatal_reclaim_exact_process_entries() {
    for (index, ending, summary) in [
        (0, "goodbye", "worker goodbye"),
        (1, "disconnect", "disconnected"),
        (2, "fatal", "protocol fault unknown-handle"),
    ] {
        let digest = char::from(b'a' + index);
        let revision = capability_revision(&format!("capability-{ending}"), digest, CAPABILITY_ID);
        let mut rig = Rig::new(&format!("proc.capability-{ending}"), &revision);
        let _registration = grant(&mut rig, &revision, Arc::new(EchoText));
        let mode = match ending {
            "goodbye" => "capability-reclaim:goodbye",
            "disconnect" => "capability-reclaim:disconnect",
            _ => "capability-reclaim:fatal",
        };
        let (driver, observer) = ProcessPluginDriver::scripted(
            revision.id().clone(),
            DriverKind::NodeProcess,
            worker_program(),
            [ProcActivationPlan::worker(mode)],
        );
        let mut activation = HostPluginActivation::prepare(
            &mut rig.slot,
            rig.epoch,
            &rig.broker,
            &rig.grants,
            as_trait(&driver),
        )
        .expect("preparation succeeds");
        let id = activation.id().clone();
        let health = activation.activate(&rig.registry).await.expect("starts");
        diagnostics_show(&observer, &id, "capability:reclaim:acquired=").await;
        health_becomes(&health, "closed", |health| {
            matches!(health, PluginHealth::Unhealthy { .. })
        })
        .await;
        handles_zero(&observer, &id).await;
        let close = observer
            .close_summary(&id)
            .expect("close cause is retained");
        assert!(
            close.contains(summary),
            "{ending} close is classified: {close}"
        );
        stop_active(activation, &rig.registry, rig.epoch).await;
        handles_zero(&observer, &id).await;
    }
}

#[tokio::test]
async fn host_deactivation_reclaims_one_activation_while_a_sibling_keeps_serving() {
    let revision = capability_revision("capability-siblings", 'd', CAPABILITY_ID);
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [
            ProcActivationPlan::worker("capability-reclaim:deactivate"),
            ProcActivationPlan::worker("capability-basic"),
        ],
    );

    let mut first_rig = Rig::new("proc.capability-sibling-first", &revision);
    let _first_registration = grant(&mut first_rig, &revision, Arc::new(EchoText));
    let mut first = HostPluginActivation::prepare(
        &mut first_rig.slot,
        first_rig.epoch,
        &first_rig.broker,
        &first_rig.grants,
        as_trait(&driver),
    )
    .expect("first prepares");
    let first_id = first.id().clone();
    let _first_handle = first
        .activate(&first_rig.registry)
        .await
        .expect("first starts");
    diagnostics_show(&observer, &first_id, "capability:reclaim:acquired=").await;

    let mut second_rig = Rig::new("proc.capability-sibling-second", &revision);
    let _second_registration = grant(&mut second_rig, &revision, Arc::new(EchoText));
    let mut second = HostPluginActivation::prepare(
        &mut second_rig.slot,
        second_rig.epoch,
        &second_rig.broker,
        &second_rig.grants,
        as_trait(&driver),
    )
    .expect("second prepares");
    let second_id = second.id().clone();
    let _second_handle = second
        .activate(&second_rig.registry)
        .await
        .expect("second starts");
    diagnostics_show(&observer, &second_id, "capability:release:ack").await;

    stop_active(first, &first_rig.registry, first_rig.epoch).await;
    handles_zero(&observer, &first_id).await;
    assert_eq!(
        observer.resource_state(&first_id),
        Ok(ResourceState::Released)
    );
    let sibling = endpoint(&driver, &second_id)
        .call("application.echo", json!({ "sibling": "healthy" }), None)
        .await
        .expect("sibling endpoint remains active");
    assert!(matches!(
        settled_within(sibling).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    handles_zero(&observer, &second_id).await;
    stop_active(second, &second_rig.registry, second_rig.epoch).await;
}

#[tokio::test]
async fn abandoned_driver_ownership_reaps_and_reclaims_without_deactivation() {
    let revision = capability_revision("capability-abandon", 'e', CAPABILITY_ID);
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-reclaim:abandon")],
    );
    let mut rig = Rig::new("proc.capability-abandon", &revision);
    let registration = grant(&mut rig, &revision, Arc::new(EchoText));
    let mut activation = HostPluginActivation::prepare(
        &mut rig.slot,
        rig.epoch,
        &rig.broker,
        &rig.grants,
        as_trait(&driver),
    )
    .expect("preparation succeeds");
    let id = activation.id().clone();
    let handle = activation.activate(&rig.registry).await.expect("starts");
    diagnostics_show(&observer, &id, "capability:reclaim:acquired=").await;
    let pid = observer.worker_pid(&id).expect("worker is live");

    drop(handle);
    drop(activation);
    drop(registration);
    drop(rig);
    process_gone(pid).await;
    handles_zero(&observer, &id).await;
    assert_eq!(observer.deactivation_calls(&id), 0);
}
