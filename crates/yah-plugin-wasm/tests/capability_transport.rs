//! A brokered capability crossing the Wasm ABI, proved from both sides.
//!
//! The claims here are pairs, the same shape the resource ceilings use: a
//! host-side half that drives the generated import implementation directly
//! against a context the host lifecycle really admitted, and a component half
//! where the `capability-consumer` fixture observes the same outcome from
//! inside the guest and reports it as its tool answer. The fixture's codes
//! (`A<digit>` for acquire, `E<digit>` for call) are the WIT enum
//! discriminants - frozen in order by `world_contract.rs` - so an assertion
//! on them is an assertion on what the guest was actually told. One claim is
//! deliberately single-sided: `invalid-id` has host evidence only, because
//! the fixture bakes a well-formed ID into its component text.
//!
//! What the pairs cover: a granted call answers through the activation-scoped
//! handle; an absent grant is refused; provider withdrawal revokes a held
//! handle while a fresh acquire reports `unavailable` - two codes, not one,
//! because the immutable grant snapshot never follows a replacement; a stale
//! activation is fenced in the deterministic window between stop beginning
//! and cleanup finishing; and the handle ceiling refuses at N, admits at 2N,
//! and frees on drop - on both release paths, guest `resource.drop` and store
//! teardown, which share the entry's `Drop`, so the decrement is a release
//! rather than a `drop`-call.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use fixtures::revision_requesting;
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, CapabilityDefinition, CapabilityId, DriverConformanceCase,
    EffectiveCapabilityGrants, HostPluginActivation, PluginDriver, PluginRevision, TextCapability,
    TextCapabilityFailure,
};
use yah_plugin_wasm::{
    ResourceState, WasmActivationPlan, WasmComponentDriver, WasmLimits, WasmObserver,
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
///
/// The caller's registry, not a throwaway: removal ignores it today, but a
/// removal that grows a deregistration step must run against the registry the
/// component was actually mounted in.
async fn stop(
    rig_epoch: ProviderSelectionEpoch,
    registry: &ServiceRegistry,
    activation: HostPluginActivation<'_>,
) {
    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(registry, removed)
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

    stop(rig.epoch, &rig.registry, activation).await;
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

    stop(rig.epoch, &rig.registry, activation).await;
}

#[tokio::test]
async fn provider_withdrawal_revokes_held_handles_and_replacement_never_retargets() {
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

    // The held resource is revoked (call enum: revoked = 0)...
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

    stop(rig.epoch, &rig.registry, activation).await;
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

    // The held resource refuses (call enum: revoked = 0)...
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
        // With nothing held and no refusal ever recorded, a plain call answers
        // "nohand" - not a rendered non-code that reads like a refusal.
        assert_eq!(
            observer.call_fixture_tool(&id, "x").await.expect("answers"),
            "nohand"
        );
        assert_eq!(
            observer
                .call_fixture_tool(&id, "grab")
                .await
                .expect("answers"),
            "grabbed"
        );
        assert_eq!(host.live_capability_handles(), 2);

        stop(rig.epoch, &rig.registry, activation).await;
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

        stop(rig.epoch, &rig.registry, activation).await;
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

    stop(rig.epoch, &rig.registry, activation).await;
    drop(registration);
}

// The host-seam half lives beside this file; see its header.
#[path = "capability_transport/host_seam.rs"]
mod host_seam;
