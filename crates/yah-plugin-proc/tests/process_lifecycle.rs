//! The acceptance triad — restart, disconnect, cancel — plus the bootstrap
//! hygiene the fd-3 design promises.
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
    PluginActivationHandle, PluginHealth, PluginRevision,
};
use yah_plugin_ipc::types::{CancelReason, CancelTarget, Outcome, WireErrorKind};
use yah_plugin_proc::{
    CallEnd, DiagnosticStream, ProcActivationPlan, ProcObserver, ProcessPluginDriver,
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
    match call.settled().await.expect("the call settles") {
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
    match call.settled().await.expect("the loss settles the call") {
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
    match call.settled().await.expect("the call settles") {
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
    match call.settled().await.expect("the deadline settles the call") {
        CallEnd::Lost { error, reconcile } => {
            assert_eq!(error, WireErrorKind::DeadlineExceeded);
            assert!(reconcile, "the worker may have acted before the budget");
        }
        other => panic!("expected deadline-exceeded, got {other:?}"),
    }
    // The session survives the late answer; the worker stays healthy.
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(matches!(handle.health(), Ok(PluginHealth::Healthy)));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn the_bootstrap_leaks_nothing_and_diagnostics_stay_diagnostics() {
    let revision = lifecycle_revision("bootstrap", 'a');
    let (driver, observer) = scripted(&revision, vec![ProcActivationPlan::worker("env-report")]);
    let mut rig = Rig::new("proc.bootstrap", &revision);
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    // The handshake completing IS the fd-3 evidence: the worker found its
    // channel with no path, port, or token to name it by. What remains to
    // prove is the negative space — the environment carries nothing.
    let mut report = String::new();
    for _ in 0..500 {
        if let Some(tail) = observer.diagnostics_tail(&id, DiagnosticStream::Stdout)
            && tail.contains('\n')
        {
            report = tail;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        report.trim(),
        "env:PATH",
        "the worker environment is the allowlist and nothing else"
    );
    // Diagnostics ride stdout as text; the protocol never does.
    let call = observer
        .begin_call(&id, "tool.echo", json!("still-serving"), None)
        .await
        .expect("the call opens");
    assert!(matches!(
        call.settled().await.expect("the call settles"),
        CallEnd::Settled(Outcome::Ok { .. })
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}
