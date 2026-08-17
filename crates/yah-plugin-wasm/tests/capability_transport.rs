//! A brokered capability crossing the Wasm ABI, proved from both sides.
//!
//! Every claim here is a pair, the same shape the resource ceilings use: a
//! host-side half that drives the generated import implementation directly
//! against a context the host lifecycle really admitted, and a component half
//! where the `capability-consumer` fixture observes the same outcome from
//! inside the sandbox and reports it as its tool answer. The fixture's codes
//! (`A<digit>` for acquire, `E<digit>` for call) are the WIT enum
//! discriminants, so an assertion on them is an assertion on what the guest
//! was actually told.
//!
//! What the pairs cover: a granted call answers through the activation-scoped
//! handle; an absent grant is refused; provider withdrawal revokes a held
//! handle while a fresh acquire reports `unavailable` - two codes, not one,
//! because the immutable grant snapshot never follows a replacement; a stale
//! activation is fenced in the deterministic window between stop beginning
//! and cleanup finishing; and the handle ceiling refuses at N, admits at 2N,
//! and frees on drop - on both release paths, guest `resource.drop` and store
//! teardown, which is the property that makes the live count a release count.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use fixtures::revision_requesting;
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, CapabilityDefinition, CapabilityId, DriverActivationError,
    DriverConformanceCase, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, EffectiveCapabilityGrants,
    HostPluginActivation, PluginActivationId, PluginActivationRequest, PluginDriver, PluginHealth,
    PluginRevision, PluginRevisionId, PluginStartContext, PreparedDriverActivation, TextCapability,
    TextCapabilityFailure,
};
use yah_plugin_wasm::{
    HostObserver, HostState, ResourceState, WasmActivationPlan, WasmComponentDriver, WasmLimits,
    WasmObserver,
    bindings::yah::plugin::capabilities::{AcquireErrorCode, CallErrorCode, Host, HostCapability},
};

/// The capability the fixture guest asks for, baked into its component text.
const CAPABILITY_ID: &str = "example.text-echo/v1";

fn definition() -> CapabilityDefinition<dyn TextCapability> {
    CapabilityDefinition::new(
        CapabilityId::new(CAPABILITY_ID).expect("the fixture capability ID is canonical"),
    )
}

/// Example-only provider: echoes, and refuses two magic inputs on purpose.
#[derive(Default)]
struct EchoText {
    calls: AtomicUsize,
}

impl TextCapability for EchoText {
    fn invoke(&self, input: &str) -> Result<String, TextCapabilityFailure> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        match input {
            "bad" => Err(TextCapabilityFailure::invalid_input("the input is refused")),
            "boom" => Err(TextCapabilityFailure::failed("the provider broke")),
            _ => Ok(format!("echo:{input}")),
        }
    }
}

/// A contract that is not the portable text contract, for the mismatch pair.
trait NotText: Send + Sync {}
struct NotTextProvider;
impl NotText for NotTextProvider {}

struct Rig {
    slot: ComponentSlot,
    registry: ServiceRegistry,
    broker: CapabilityBroker,
    epoch: ProviderSelectionEpoch,
    revision: PluginRevision,
}

impl Rig {
    fn new(label: &str, digest: char) -> Self {
        let revision = revision_requesting(
            DriverConformanceCase::ReadyLifecycle,
            digest,
            &[CAPABILITY_ID],
        )
        .expect("fixture revision is valid");
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
        Self {
            slot,
            registry,
            broker: CapabilityBroker::new().expect("broker is constructible"),
            epoch,
            revision,
        }
    }
}

fn consumer_driver(
    revision: &PluginRevision,
    limits: WasmLimits,
) -> (Arc<dyn PluginDriver>, WasmObserver) {
    WasmComponentDriver::scripted_with_limits(
        revision.id().clone(),
        [WasmActivationPlan::capability_consumer()],
        limits,
    )
    .expect("wasm driver builds")
}

/// Stop a live activation the way `example_guests` does, returning at the
/// point where cleanup has fully finished.
async fn stop(rig_epoch: ProviderSelectionEpoch, activation: HostPluginActivation<'_>) {
    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    let registry = ServiceRegistry::new();
    slot.reconcile(&registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig_epoch)
        .await
        .expect("cleanup completes");
}

// ---------------------------------------------------------------------------
// Component half: the fixture guest observes each outcome from inside.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_granted_capability_answers_through_the_guest() {
    let mut rig = Rig::new("wasm.capability.granted", '1');
    let provider = Arc::new(EchoText::default());
    let registration = rig
        .broker
        .register(&definition(), provider.clone() as Arc<dyn TextCapability>)
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("the fixture manifest requests the capability");
    let (driver, observer) = consumer_driver(&rig.revision, WasmLimits::default());

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();
    activation
        .activate(&rig.registry)
        .await
        .expect("the consumer activates");

    let answered = observer
        .call_fixture_tool(&id, "ping")
        .await
        .expect("a granted call answers");
    assert_eq!(answered, "echo:ping");
    assert_eq!(
        provider.calls.load(Ordering::Acquire),
        1,
        "the provider itself must have run, not a copy of its answer"
    );

    // A provider refusal is a code the guest reads, never a trap: the enum
    // discriminants are invalid-input = 2 and failed = 3.
    assert_eq!(
        observer
            .call_fixture_tool(&id, "bad")
            .await
            .expect("answers"),
        "E2"
    );
    assert_eq!(
        observer
            .call_fixture_tool(&id, "boom")
            .await
            .expect("answers"),
        "E3"
    );

    let host = observer
        .host_observer(&id)
        .expect("a live activation has a host observer");
    assert_eq!(host.capability_acquires(), 1);
    assert_eq!(host.capability_acquire_refusals(), 0);
    assert_eq!(host.capability_calls(), 3);
    assert_eq!(host.capability_call_refusals(), 2);
    assert_eq!(host.live_capability_handles(), 1);

    stop(rig.epoch, activation).await;
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
    // The guest never called resource.drop, so this is the store-teardown
    // release path: the table entry's own Drop is what counted the handle out.
    assert_eq!(host.live_capability_handles(), 0);
    drop(registration);
}

#[tokio::test]
async fn an_absent_grant_is_refused_as_not_granted() {
    let mut rig = Rig::new("wasm.capability.absent", '2');
    let grants = EffectiveCapabilityGrants::empty(&rig.revision);
    let (driver, observer) = consumer_driver(&rig.revision, WasmLimits::default());

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();
    activation
        .activate(&rig.registry)
        .await
        .expect("activation succeeds with nothing granted - refusal is the tool's answer");

    // not-granted = 1, observed at activate time and again on a fresh try.
    assert_eq!(
        observer
            .call_fixture_tool(&id, "ping")
            .await
            .expect("answers"),
        "A1"
    );
    assert_eq!(
        observer
            .call_fixture_tool(&id, "acquire")
            .await
            .expect("answers"),
        "A1"
    );

    let host = observer.host_observer(&id).expect("observer exists");
    assert_eq!(host.capability_acquires(), 2);
    assert_eq!(host.capability_acquire_refusals(), 2);
    assert_eq!(host.live_capability_handles(), 0);

    stop(rig.epoch, activation).await;
}

#[tokio::test]
async fn provider_replacement_revokes_held_handles_and_never_retargets() {
    let mut rig = Rig::new("wasm.capability.replacement", '3');
    let registration = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("grant is requested");
    let (driver, observer) = consumer_driver(&rig.revision, WasmLimits::default());

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();
    activation
        .activate(&rig.registry)
        .await
        .expect("the consumer activates");
    assert_eq!(
        observer
            .call_fixture_tool(&id, "ping")
            .await
            .expect("answers"),
        "echo:ping"
    );

    let _withdrawn = registration.withdraw();

    // The held handle is revoked (call enum: revoked = 0)...
    assert_eq!(
        observer
            .call_fixture_tool(&id, "ping")
            .await
            .expect("answers"),
        "E0"
    );
    // ...while a fresh acquire is a different finding: the registration the
    // snapshot names is gone (acquire enum: unavailable = 3).
    assert_eq!(
        observer
            .call_fixture_tool(&id, "acquire")
            .await
            .expect("answers"),
        "A3"
    );

    // A replacement provider must not be reachable through the old grant: the
    // snapshot binds the exact registration, and that identity is spent.
    let replacement = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("a replacement registration succeeds");
    assert_eq!(
        observer
            .call_fixture_tool(&id, "acquire")
            .await
            .expect("answers"),
        "A3"
    );

    stop(rig.epoch, activation).await;
    drop(replacement);
}

#[tokio::test]
async fn a_stale_activation_is_fenced_in_the_stop_window() {
    let mut rig = Rig::new("wasm.capability.stale", '4');
    let registration = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("grant is requested");
    let (driver, observer) = consumer_driver(&rig.revision, WasmLimits::default());

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();
    activation
        .activate(&rig.registry)
        .await
        .expect("the consumer activates");
    assert_eq!(
        observer
            .call_fixture_tool(&id, "ping")
            .await
            .expect("answers"),
        "echo:ping"
    );

    // Begin the stop but do not finish it. Closing the effect scope revokes
    // the activity gate eagerly, while the driver's deactivation is a deferred
    // cleanup that only `finish_stop` drives - so this window is deterministic,
    // the store is still live and callable, and the fence is the only thing
    // standing between a stale activation and a granted authority.
    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");

    // The held handle refuses (call enum: revoked = 0)...
    assert_eq!(
        observer
            .call_fixture_tool(&id, "ping")
            .await
            .expect("answers"),
        "E0"
    );
    // ...and so does a fresh acquire, as the activation itself (acquire enum:
    // revoked = 2), not as a provider problem - the provider is fine.
    assert_eq!(
        observer
            .call_fixture_tool(&id, "acquire")
            .await
            .expect("answers"),
        "A2"
    );

    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
    drop(registration);
}

#[tokio::test]
async fn the_handle_ceiling_refuses_at_the_bound_and_frees_on_drop() {
    // Refused under the tight bound, admitted under the generous one, one
    // variable between them - the same pair shape as the resource ceilings.
    let tight = WasmLimits {
        max_capability_handles: 2,
        ..WasmLimits::default()
    };
    let generous = WasmLimits {
        max_capability_handles: 4,
        ..WasmLimits::default()
    };

    {
        let mut rig = Rig::new("wasm.capability.ceiling.tight", '5');
        let registration = rig
            .broker
            .register(
                &definition(),
                Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
            )
            .expect("registration succeeds");
        let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
            .expect("grant is requested");
        let (driver, observer) = consumer_driver(&rig.revision, tight);
        let mut activation =
            HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
                .expect("preparation succeeds");
        let id = activation.id().clone();
        activation
            .activate(&rig.registry)
            .await
            .expect("the consumer activates");

        // activate already holds one handle; the second fills the ceiling.
        assert_eq!(
            observer
                .call_fixture_tool(&id, "grab")
                .await
                .expect("answers"),
            "grabbed"
        );
        // handle-limit = 5.
        assert_eq!(
            observer
                .call_fixture_tool(&id, "grab")
                .await
                .expect("answers"),
            "A5"
        );
        let host = observer.host_observer(&id).expect("observer exists");
        assert_eq!(host.live_capability_handles(), 2);

        // Guest-side resource.drop is the other release path: it frees a slot
        // the next grab can take.
        assert_eq!(
            observer
                .call_fixture_tool(&id, "drop")
                .await
                .expect("answers"),
            "dropped"
        );
        assert_eq!(host.live_capability_handles(), 1);
        assert_eq!(
            observer
                .call_fixture_tool(&id, "grab")
                .await
                .expect("answers"),
            "grabbed"
        );
        assert_eq!(host.live_capability_handles(), 2);

        stop(rig.epoch, activation).await;
        assert_eq!(host.live_capability_handles(), 0);
        drop(registration);
    }

    {
        let mut rig = Rig::new("wasm.capability.ceiling.generous", '6');
        let registration = rig
            .broker
            .register(
                &definition(),
                Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
            )
            .expect("registration succeeds");
        let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
            .expect("grant is requested");
        let (driver, observer) = consumer_driver(&rig.revision, generous);
        let mut activation =
            HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
                .expect("preparation succeeds");
        let id = activation.id().clone();
        activation
            .activate(&rig.registry)
            .await
            .expect("the consumer activates");

        for _ in 0..3 {
            assert_eq!(
                observer
                    .call_fixture_tool(&id, "grab")
                    .await
                    .expect("answers"),
                "grabbed",
                "the same sequence must be admitted under the generous bound"
            );
        }

        stop(rig.epoch, activation).await;
        drop(registration);
    }
}

#[tokio::test]
async fn a_grant_under_another_contract_is_refused_as_mismatched() {
    let mut rig = Rig::new("wasm.capability.mismatch", '7');
    let registration = rig
        .broker
        .register(
            &CapabilityDefinition::<dyn NotText>::new(
                CapabilityId::new(CAPABILITY_ID).expect("canonical"),
            ),
            Arc::new(NotTextProvider) as Arc<dyn NotText>,
        )
        .expect("registration under another contract succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("grant is requested");
    let (driver, observer) = consumer_driver(&rig.revision, WasmLimits::default());

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();
    activation
        .activate(&rig.registry)
        .await
        .expect("the consumer activates");

    // mismatched = 4: granted, but not carriable under the text contract.
    assert_eq!(
        observer
            .call_fixture_tool(&id, "ping")
            .await
            .expect("answers"),
        "A4"
    );

    stop(rig.epoch, activation).await;
    drop(registration);
}

// ---------------------------------------------------------------------------
// Host half: the same mapping driven directly, against an admitted context.
// ---------------------------------------------------------------------------

/// A driver that exists to catch the start permit's capability context.
///
/// The context constructor is private to the host crate on purpose, so even
/// the "unit" half of each pair goes through `HostPluginActivation`: what
/// these tests hold is a context the host really admitted, not a lookalike.
#[derive(Default)]
struct ContextProbe {
    context: Mutex<Option<PluginStartContext>>,
}

struct ProbeDriver {
    revision: PluginRevisionId,
    probe: Arc<ContextProbe>,
}

struct ProbePrepared {
    id: PluginActivationId,
    probe: Arc<ContextProbe>,
}

impl PluginDriver for ProbeDriver {
    fn kind(&self) -> DriverKind {
        DriverKind::WasmComponent
    }

    fn revision_id(&self) -> &PluginRevisionId {
        &self.revision
    }

    fn prepare(
        &self,
        request: PluginActivationRequest,
    ) -> Result<Arc<dyn PreparedDriverActivation>, DriverPrepareError> {
        Ok(Arc::new(ProbePrepared {
            id: request.id().clone(),
            probe: Arc::clone(&self.probe),
        }))
    }
}

impl PreparedDriverActivation for ProbePrepared {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        *self
            .probe
            .context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(permit.context().clone());
        Box::pin(async { Ok(()) })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        Ok(PluginHealth::Healthy)
    }

    fn deactivate(
        &self,
        _permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Drive the probe through the host lifecycle far enough to admit a context.
async fn admitted_context<'slot>(
    rig: &'slot mut Rig,
    grants: &EffectiveCapabilityGrants,
) -> (PluginStartContext, HostPluginActivation<'slot>) {
    let probe = Arc::new(ContextProbe::default());
    let driver: Arc<dyn PluginDriver> = Arc::new(ProbeDriver {
        revision: rig.revision.id().clone(),
        probe: Arc::clone(&probe),
    });
    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, grants, driver)
            .expect("preparation succeeds");
    activation
        .activate(&rig.registry)
        .await
        .expect("the probe activates");
    let context = probe
        .context
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .expect("start delivered the context");
    (context, activation)
}

fn acquire_code(state: &mut HostState, id: &str) -> Result<(), AcquireErrorCode> {
    match state.acquire(id.to_owned()) {
        Ok(resource) => {
            // Release at once so mapping probes do not consume the ceiling.
            HostCapability::drop(state, resource).expect("drop never errors");
            Ok(())
        }
        Err(error) => Err(error.code),
    }
}

#[tokio::test]
async fn the_acquire_mapping_is_whole_set_against_an_admitted_context() {
    let mut rig = Rig::new("wasm.capability.unit.acquire", '8');
    let registration = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("grant is requested");
    let epoch = rig.epoch;
    let (context, activation) = admitted_context(&mut rig, &grants).await;
    let mut state =
        HostState::with_grants(HostObserver::new(), WasmLimits::default(), context.clone());

    // A malformed name is refused as itself, before any grant is consulted.
    assert_eq!(
        acquire_code(&mut state, "not a capability id"),
        Err(AcquireErrorCode::InvalidId)
    );
    // A well-formed name outside the snapshot is simply not granted.
    assert_eq!(
        acquire_code(&mut state, "example.other/v1"),
        Err(AcquireErrorCode::NotGranted)
    );
    // The granted name resolves, and a held resource answers through the
    // provider.
    let resource = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("the granted capability resolves");
    assert_eq!(
        HostCapability::invoke(&mut state, resource, "ping".to_owned())
            .expect("a live handle answers"),
        "echo:ping"
    );

    // Withdrawal splits into two codes: the held handle is revoked, while a
    // fresh acquire reports the registration as unavailable.
    let held = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("still granted before withdrawal");
    let _withdrawn = registration.withdraw();
    assert_eq!(
        HostCapability::invoke(&mut state, held, "ping".to_owned())
            .expect_err("a withdrawn provider refuses")
            .code,
        CallErrorCode::Revoked
    );
    assert_eq!(
        acquire_code(&mut state, CAPABILITY_ID),
        Err(AcquireErrorCode::Unavailable)
    );

    // Closing the activation fences the context itself: the same acquire that
    // was `unavailable` a moment ago is now about the activation, not the
    // provider.
    stop(epoch, activation).await;
    assert_eq!(
        acquire_code(&mut state, CAPABILITY_ID),
        Err(AcquireErrorCode::Revoked)
    );

    // A state that never saw a start permit holds no grants at all.
    let mut bare = HostState::with_limits(HostObserver::new(), WasmLimits::default());
    assert_eq!(
        acquire_code(&mut bare, CAPABILITY_ID),
        Err(AcquireErrorCode::NotGranted)
    );

    // The four remaining broker variants - both ID exhaustions, duplicate
    // registration, and a foreign registration - cannot escape
    // `PluginStartContext::handle`: each is constructed only during broker
    // creation, provider registration, or grant validation at prepare, all
    // before a start permit exists. They map fail-closed to `unavailable` and
    // have no reachable case to assert here; this comment is their evidence.
}

#[tokio::test]
async fn the_call_mapping_covers_the_provider_and_the_mismatch() {
    let mut rig = Rig::new("wasm.capability.unit.call", '9');
    let registration = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("grant is requested");
    let epoch = rig.epoch;
    let (context, activation) = admitted_context(&mut rig, &grants).await;
    let mut state = HostState::with_grants(HostObserver::new(), WasmLimits::default(), context);

    // Provider refusals map by their own two codes and keep the grant live.
    let resource = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("the granted capability resolves");
    assert_eq!(
        HostCapability::invoke(&mut state, resource, "bad".to_owned())
            .expect_err("the provider refuses")
            .code,
        CallErrorCode::InvalidInput
    );
    let resource = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("a refusal does not revoke");
    assert_eq!(
        HostCapability::invoke(&mut state, resource, "boom".to_owned())
            .expect_err("the provider fails")
            .code,
        CallErrorCode::Failed
    );
    let observer = state.observer().clone();
    assert_eq!(observer.capability_calls(), 2);
    assert_eq!(observer.capability_call_refusals(), 2);

    stop(epoch, activation).await;

    // The mismatch pair's host half needs its own broker, because the first
    // registration fixes a capability ID's contract type for a broker's life.
    let mut mismatch_rig = Rig::new("wasm.capability.unit.mismatch", 'a');
    let mismatch_registration = mismatch_rig
        .broker
        .register(
            &CapabilityDefinition::<dyn NotText>::new(
                CapabilityId::new(CAPABILITY_ID).expect("canonical"),
            ),
            Arc::new(NotTextProvider) as Arc<dyn NotText>,
        )
        .expect("registration under another contract succeeds");
    let mismatch_grants =
        EffectiveCapabilityGrants::new(&mismatch_rig.revision, [mismatch_registration.grant()])
            .expect("grant is requested");
    let mismatch_epoch = mismatch_rig.epoch;
    let (context, activation) = admitted_context(&mut mismatch_rig, &mismatch_grants).await;
    let mut state = HostState::with_grants(HostObserver::new(), WasmLimits::default(), context);
    assert_eq!(
        acquire_code(&mut state, CAPABILITY_ID),
        Err(AcquireErrorCode::Mismatched)
    );
    stop(mismatch_epoch, activation).await;
    drop(mismatch_registration);
}

#[tokio::test]
async fn the_handle_ceiling_holds_at_the_host_seam_too() {
    let mut rig = Rig::new("wasm.capability.unit.ceiling", 'b');
    let registration = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("registration succeeds");
    let grants = EffectiveCapabilityGrants::new(&rig.revision, [registration.grant()])
        .expect("grant is requested");
    let epoch = rig.epoch;
    let (context, activation) = admitted_context(&mut rig, &grants).await;
    let limits = WasmLimits {
        max_capability_handles: 1,
        ..WasmLimits::default()
    };
    let mut state = HostState::with_grants(HostObserver::new(), limits, context);

    let held = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("the first handle is admitted");
    assert_eq!(
        state
            .acquire(CAPABILITY_ID.to_owned())
            .expect_err("the second is past the ceiling")
            .code,
        AcquireErrorCode::HandleLimit
    );
    assert_eq!(state.observer().live_capability_handles(), 1);

    // Dropping the held handle frees its slot for the next acquire.
    HostCapability::drop(&mut state, held).expect("drop never errors");
    assert_eq!(state.observer().live_capability_handles(), 0);
    let reacquired = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("a freed slot admits again");
    HostCapability::drop(&mut state, reacquired).expect("drop never errors");

    stop(epoch, activation).await;
    drop(registration);
}
