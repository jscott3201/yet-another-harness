//! Portable text-capability semantics over one real supervised worker process.

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
use rig::{Rig, as_trait, diagnostics_show, endpoint, settled_within, stop_active};
use serde_json::json;
use yah_plugin_host::{
    CapabilityDefinition, CapabilityId, CapabilityProviderRegistration, DriverKind,
    EffectiveCapabilityGrants, HostPluginActivation, TextCapability, TextCapabilityFailure,
};
use yah_plugin_ipc::types::Outcome;
use yah_plugin_proc::{
    CallTerminal, ProcActivationPlan, ProcLimits, ProcObserver, ProcessPluginDriver, WorkerMethod,
    WorkerMethodFailure, WorkerMethodRegistry, WorkerMethodRequest, WorkerMethodResult,
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
        .expect("requested provider is granted");
    registration
}

async fn handles_become(
    observer: &ProcObserver,
    id: &yah_plugin_host::PluginActivationId,
    session: usize,
    process: usize,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if observer.capability_handle_gauges(id).is_some_and(|gauges| {
                gauges.session_live_handles == session
                    && gauges.process_capability_entries == process
            }) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("capability handle gauges reach the expected values");
}

struct EchoText {
    calls: AtomicUsize,
    prefix: &'static str,
}

impl EchoText {
    fn new(prefix: &'static str) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            prefix,
        }
    }
}

impl TextCapability for EchoText {
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Ok(format!("{}:{input}", self.prefix))
    }
}

#[tokio::test]
async fn granted_acquire_invoke_release_uses_the_control_frame_and_reaches_zero() {
    let revision = capability_revision("capability-basic", '1', CAPABILITY_ID);
    let provider = Arc::new(EchoText::new("echo"));
    let mut rig = Rig::new("proc.capability-basic", &revision);
    let _registration = grant(
        &mut rig,
        &revision,
        Arc::clone(&provider) as Arc<dyn TextCapability>,
    );
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-basic")],
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    diagnostics_show(&observer, &id, "capability:invoke:ok:echo:hello").await;
    diagnostics_show(&observer, &id, "capability:release:ack").await;
    diagnostics_show(
        &observer,
        &id,
        "capability:released:wire:UnknownHandle:echoed=false",
    )
    .await;
    handles_become(&observer, &id, 0, 0).await;
    assert_eq!(provider.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        Arc::strong_count(&provider),
        2,
        "the test and registration are the only strong provider owners"
    );

    let sibling = endpoint(&driver, &id)
        .call("application.echo", json!({ "alive": true }), None)
        .await
        .expect("the release does not poison the session");
    assert!(matches!(
        settled_within(sibling).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn invalid_ungranted_and_mismatched_acquires_are_domain_data() {
    let revision = capability_revision("capability-acquire-errors", '2', CAPABILITY_ID);

    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-acquire-probe")],
    );
    let mut denied_rig = Rig::new("proc.capability-denied", &revision);
    let mut denied = HostPluginActivation::prepare(
        &mut denied_rig.slot,
        denied_rig.epoch,
        &denied_rig.broker,
        &denied_rig.grants,
        as_trait(&driver),
    )
    .expect("denied activation prepares");
    let denied_id = denied.id().clone();
    let _handle = denied.activate(&denied_rig.registry).await.expect("starts");
    diagnostics_show(
        &observer,
        &denied_id,
        "capability:invalid:domain:invalid-id",
    )
    .await;
    diagnostics_show(
        &observer,
        &denied_id,
        "capability:target:domain:not-granted",
    )
    .await;
    stop_active(denied, &denied_rig.registry, denied_rig.epoch).await;

    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-acquire-probe")],
    );
    let mut mismatch_rig = Rig::new("proc.capability-mismatch", &revision);
    let wrong = mismatch_rig
        .broker
        .register(
            &CapabilityDefinition::<usize>::new(
                CapabilityId::new(CAPABILITY_ID).expect("fixture id is valid"),
            ),
            Arc::new(7usize),
        )
        .expect("non-text provider registers");
    mismatch_rig.grants = EffectiveCapabilityGrants::new(&revision, [wrong.grant()])
        .expect("wrong contract is still the exact admitted registration");
    let mut mismatch = HostPluginActivation::prepare(
        &mut mismatch_rig.slot,
        mismatch_rig.epoch,
        &mismatch_rig.broker,
        &mismatch_rig.grants,
        as_trait(&driver),
    )
    .expect("mismatched activation prepares");
    let mismatch_id = mismatch.id().clone();
    let _handle = mismatch
        .activate(&mismatch_rig.registry)
        .await
        .expect("starts");
    diagnostics_show(
        &observer,
        &mismatch_id,
        "capability:target:domain:mismatched",
    )
    .await;
    stop_active(mismatch, &mismatch_rig.registry, mismatch_rig.epoch).await;
}

#[tokio::test]
async fn withdrawal_revokes_held_handles_and_fresh_acquire_never_follows_replacement() {
    let revision = capability_revision("capability-replacement", '3', CAPABILITY_ID);
    let old = Arc::new(EchoText::new("old"));
    let replacement = Arc::new(EchoText::new("new"));
    let mut rig = Rig::new("proc.capability-replacement", &revision);
    let registration = grant(
        &mut rig,
        &revision,
        Arc::clone(&old) as Arc<dyn TextCapability>,
    );
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-replacement")],
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    diagnostics_show(&observer, &id, "capability:replacement:ready").await;

    drop(registration.withdraw());
    let _replacement_registration = rig
        .broker
        .register(
            &definition(),
            Arc::clone(&replacement) as Arc<dyn TextCapability>,
        )
        .expect("replacement registers");
    let control = endpoint(&driver, &id)
        .call("control.continue", json!(null), None)
        .await
        .expect("worker continues after replacement");
    assert!(matches!(
        settled_within(control).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    diagnostics_show(&observer, &id, "capability:held:domain:revoked").await;
    diagnostics_show(&observer, &id, "capability:fresh:domain:unavailable").await;
    assert_eq!(old.calls.load(Ordering::Acquire), 0);
    assert_eq!(replacement.calls.load(Ordering::Acquire), 0);
    stop_active(activation, &rig.registry, rig.epoch).await;
}

struct OutcomeText;

impl TextCapability for OutcomeText {
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure> {
        match input {
            "bad" => Err(TextCapabilityFailure::invalid_input("é".repeat(600))),
            "fail" => Err(TextCapabilityFailure::failed("é".repeat(600))),
            "oversize" => Ok("x".repeat(yah_plugin_ipc::MAX_INLINE_RESULT_BYTES + 1)),
            "panic" => panic!("provider-secret"),
            _ => Ok(input.to_owned()),
        }
    }
}

#[tokio::test]
async fn provider_failures_panics_and_oversize_output_are_bounded_domain_results() {
    let revision = capability_revision("capability-outcomes", '4', CAPABILITY_ID);
    let mut rig = Rig::new("proc.capability-outcomes", &revision);
    let _registration = grant(&mut rig, &revision, Arc::new(OutcomeText));
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-provider-outcomes")],
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");

    diagnostics_show(
        &observer,
        &id,
        "capability:bad:domain:invalid-input:chars=512",
    )
    .await;
    diagnostics_show(&observer, &id, "capability:fail:domain:failed:chars=512").await;
    diagnostics_show(&observer, &id, "capability:oversize:domain:exhausted").await;
    diagnostics_show(&observer, &id, "capability:panic:domain:failed").await;
    diagnostics_show(&observer, &id, "capability:ok:ok:ok").await;
    diagnostics_show(&observer, &id, "capability:provider-outcomes:done").await;
    handles_become(&observer, &id, 0, 0).await;
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn malformed_forged_and_released_handles_are_call_local() {
    let revision = capability_revision("capability-malformed", '5', CAPABILITY_ID);
    let mut rig = Rig::new("proc.capability-malformed", &revision);
    let _registration = grant(&mut rig, &revision, Arc::new(EchoText::new("echo")));
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-malformed")],
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    for expected in [
        "capability:malformed-acquire:wire:InvalidFrame:echoed=false",
        "capability:malformed-invoke:wire:InvalidFrame:echoed=false",
        "capability:forged:wire:UnknownHandle:echoed=false",
        "capability:released:wire:UnknownHandle:echoed=false",
        "capability:malformed:done",
    ] {
        diagnostics_show(&observer, &id, expected).await;
    }
    handles_become(&observer, &id, 0, 0).await;
    let sibling = endpoint(&driver, &id)
        .call("application.echo", json!("healthy"), None)
        .await
        .expect("malformed capability calls do not poison siblings");
    assert!(matches!(
        settled_within(sibling).await,
        CallTerminal::Completed(Outcome::Ok { .. })
    ));
    stop_active(activation, &rig.registry, rig.epoch).await;
}

#[tokio::test]
async fn shared_handle_ceiling_refuses_at_bound_and_release_admits_a_monotonic_name() {
    let revision = capability_revision("capability-limit", '6', CAPABILITY_ID);
    let mut rig = Rig::new("proc.capability-limit", &revision);
    let _registration = grant(&mut rig, &revision, Arc::new(EchoText::new("echo")));
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-handle-limit")],
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    diagnostics_show(&observer, &id, "capability:at-limit:domain:handle-limit").await;
    diagnostics_show(&observer, &id, "monotonic=true").await;
    diagnostics_show(&observer, &id, "capability:limit:done").await;
    handles_become(&observer, &id, 0, 0).await;
    stop_active(activation, &rig.registry, rig.epoch).await;
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
async fn invoke_ordered_before_release_finishes_but_one_after_ack_is_unknown() {
    let revision = capability_revision("capability-order", '7', CAPABILITY_ID);
    let provider = Arc::new(BlockingText {
        entered: AtomicUsize::new(0),
        open: AtomicBool::new(false),
    });
    let mut rig = Rig::new("proc.capability-order", &revision);
    let _registration = grant(
        &mut rig,
        &revision,
        Arc::clone(&provider) as Arc<dyn TextCapability>,
    );
    let (driver, observer) = ProcessPluginDriver::scripted(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-release-order")],
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    tokio::time::timeout(Duration::from_secs(5), async {
        while provider.entered.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the pre-release invoke reaches the provider");
    diagnostics_show(&observer, &id, "capability:ordered:release-ack").await;
    handles_become(&observer, &id, 0, 0).await;
    provider.open.store(true, Ordering::Release);
    diagnostics_show(&observer, &id, "capability:ordered-before:ok:blocked").await;
    diagnostics_show(
        &observer,
        &id,
        "capability:ordered-after:wire:UnknownHandle:echoed=false",
    )
    .await;
    assert_eq!(provider.entered.load(Ordering::Acquire), 1);
    stop_active(activation, &rig.registry, rig.epoch).await;
}

struct BlockingMethod {
    entered: AtomicBool,
    open: AtomicBool,
}

impl WorkerMethod for BlockingMethod {
    fn invoke(
        &self,
        _request: &WorkerMethodRequest,
    ) -> Result<WorkerMethodResult, WorkerMethodFailure> {
        self.entered.store(true, Ordering::Release);
        while !self.open.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(WorkerMethodResult::new(json!({ "held": true })).expect("inline result"))
    }
}

#[tokio::test]
async fn queued_capability_cancellation_skips_provider_and_leaks_no_mint() {
    let revision = capability_revision("capability-queued-cancel", '8', CAPABILITY_ID);
    let provider = Arc::new(EchoText::new("echo"));
    let method = Arc::new(BlockingMethod {
        entered: AtomicBool::new(false),
        open: AtomicBool::new(false),
    });
    let mut methods = WorkerMethodRegistry::new();
    methods
        .register(
            "application.hold",
            Arc::clone(&method) as Arc<dyn WorkerMethod>,
        )
        .expect("method registers");
    let (driver, observer) = ProcessPluginDriver::scripted_with_limits_and_methods(
        revision.id().clone(),
        DriverKind::NodeProcess,
        worker_program(),
        [ProcActivationPlan::worker("capability-cancel-queued")],
        ProcLimits {
            provider_concurrency: 1,
            dispatch_queue_capacity: 3,
            ..ProcLimits::default()
        },
        methods,
    );
    let mut rig = Rig::new("proc.capability-queued-cancel", &revision);
    let _registration = grant(
        &mut rig,
        &revision,
        Arc::clone(&provider) as Arc<dyn TextCapability>,
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
    let _handle = activation.activate(&rig.registry).await.expect("starts");
    diagnostics_show(&observer, &id, "capability:queued:cancelled=2").await;
    assert!(method.entered.load(Ordering::Acquire));
    assert_eq!(provider.calls.load(Ordering::Acquire), 0);
    handles_become(&observer, &id, 1, 1).await;
    method.open.store(true, Ordering::Release);
    diagnostics_show(&observer, &id, "capability:queued:done").await;
    handles_become(&observer, &id, 0, 0).await;
    stop_active(activation, &rig.registry, rig.epoch).await;
}
