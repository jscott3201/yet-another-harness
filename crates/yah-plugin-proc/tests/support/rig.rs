//! Shared harness for the process driver's lifecycle and supervision tests.
//!
//! Each test binary uses a different subset, so unused items here are
//! expected. Everything drives the host's own activation guard — the same
//! path a composition uses — never driver internals.

#![allow(dead_code)]

use std::time::Duration;

use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, DriverKind, EffectiveCapabilityGrants, HostPluginActivation,
    PluginActivationHandle, PluginActivationId, PluginHealth, PluginRevision,
};
use yah_plugin_proc::{
    CallEnd, DiagnosticStream, PendingCall, ProcActivationPlan, ProcObserver, ProcessPluginDriver,
};

use super::fixtures::worker_program;

pub(crate) struct Rig {
    pub slot: ComponentSlot,
    pub registry: ServiceRegistry,
    pub broker: CapabilityBroker,
    pub grants: EffectiveCapabilityGrants,
    pub epoch: ProviderSelectionEpoch,
}

impl Rig {
    /// Mount a fresh component so the slot yields a real selection epoch.
    pub fn new(label: &str, revision: &PluginRevision) -> Self {
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

pub(crate) fn scripted(
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
pub(crate) async fn stop_active(
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
pub(crate) async fn health_becomes(
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

/// A settle that regresses into silence must fail the test, not wedge CI.
pub(crate) async fn settled_within(call: PendingCall) -> CallEnd {
    tokio::time::timeout(Duration::from_secs(5), call.settled())
        .await
        .expect("the call settles within its bound")
        .expect("the call settles")
}

/// Wait for the pid to vanish; `kill(pid, 0)` failing is the proof.
pub(crate) async fn process_gone(pid: i32) {
    for _ in 0..500 {
        if unsafe { libc::kill(pid, 0) } == -1 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("worker process {pid} survived its reclaim");
}

/// Poll the stdout diagnostics tail until it contains `needle`.
pub(crate) async fn diagnostics_show(
    observer: &ProcObserver,
    id: &PluginActivationId,
    needle: &str,
) {
    for _ in 0..500 {
        if let Some(tail) = observer.diagnostics_tail(id, DiagnosticStream::Stdout)
            && tail.contains(needle)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("diagnostics never showed {needle:?}");
}

/// The two lines the `bootstrap-report` worker prints, once both arrive.
pub(crate) async fn bootstrap_report(
    observer: &ProcObserver,
    id: &PluginActivationId,
) -> (String, String) {
    for _ in 0..500 {
        if let Some(tail) = observer.diagnostics_tail(id, DiagnosticStream::Stdout)
            && tail.matches('\n').count() >= 2
        {
            let mut lines = tail.lines();
            return (
                lines.next().unwrap_or_default().to_owned(),
                lines.next().unwrap_or_default().to_owned(),
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the worker never reported its bootstrap state");
}

/// The pid the `spawn-helper` worker printed, once it arrives.
pub(crate) async fn reported_helper_pid(observer: &ProcObserver, id: &PluginActivationId) -> i32 {
    for _ in 0..500 {
        if let Some(tail) = observer.diagnostics_tail(id, DiagnosticStream::Stdout)
            && let Some(pid) = tail
                .lines()
                .find_map(|line| line.trim().strip_prefix("helper:"))
        {
            return pid.parse().expect("the helper pid parses");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("the worker never reported its helper pid");
}
