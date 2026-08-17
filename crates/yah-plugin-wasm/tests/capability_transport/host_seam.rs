//! The host half of the capability-transport pairs.
//!
//! These drive the generated import implementation directly - the same
//! `acquire`/`invoke`/`drop` the guest enters, minus the guest - against a
//! context the host lifecycle really admitted. The context constructor is
//! private to the host crate on purpose, so even this half goes through
//! `HostPluginActivation`, with a probe driver whose only job is to catch the
//! start permit's context. Split from the component half by the 700-line cap;
//! the pairing between the two files is the parent module's doc claim.

use std::sync::{Arc, Mutex};

use yah_compose::{ComponentSlot, DesiredComponentState, ProviderSelectionEpoch, ServiceRegistry};
use yah_plugin_host::{
    CapabilityBroker, CapabilityDefinition, CapabilityId, DriverActivationError,
    DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind, DriverPrepareError,
    DriverStartPermit, DriverStopPermit, EffectiveCapabilityGrants, HostPluginActivation,
    PluginActivationId, PluginActivationRequest, PluginDriver, PluginHealth, PluginRevision,
    PluginRevisionId, PluginStartContext, PreparedDriverActivation, TextCapability,
};
use yah_plugin_wasm::{
    HostObserver, HostState, WasmLimits,
    bindings::yah::plugin::capabilities::{AcquireErrorCode, CallErrorCode, Host, HostCapability},
};

use super::{CAPABILITY_ID, EchoText, NotText, NotTextProvider, Rig, definition, stop};

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
///
/// Borrows the rig by field so the caller keeps `registry` and `epoch` for
/// `stop` while the returned activation holds the slot.
async fn admitted_context<'slot>(
    slot: &'slot mut ComponentSlot,
    epoch: ProviderSelectionEpoch,
    broker: &CapabilityBroker,
    registry: &ServiceRegistry,
    revision: &PluginRevision,
    grants: &EffectiveCapabilityGrants,
) -> (PluginStartContext, HostPluginActivation<'slot>) {
    let probe = Arc::new(ContextProbe::default());
    let driver: Arc<dyn PluginDriver> = Arc::new(ProbeDriver {
        revision: revision.id().clone(),
        probe: Arc::clone(&probe),
    });
    let mut activation = HostPluginActivation::prepare(slot, epoch, broker, grants, driver)
        .expect("preparation succeeds");
    activation
        .activate(registry)
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
    let (context, activation) = admitted_context(
        &mut rig.slot,
        epoch,
        &rig.broker,
        &rig.registry,
        &rig.revision,
        &grants,
    )
    .await;
    let mut state =
        HostState::with_grants(HostObserver::new(), WasmLimits::default(), context.clone());

    // A malformed name is refused as itself, before any grant is consulted,
    // and the refusal reflects none of the guest's bytes back: the message is
    // host-authored static text, not the rejected value re-rendered.
    let invalid = state
        .acquire("not a capability id \u{7}".to_owned())
        .expect_err("a malformed name is refused");
    assert_eq!(invalid.code, AcquireErrorCode::InvalidId);
    assert!(
        !invalid.message.contains("not a capability") && !invalid.message.contains('\u{7}'),
        "the refusal must not echo guest text: {:?}",
        invalid.message
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

    // Withdrawal splits into two codes: the held resource is revoked, while a
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
    let unavailable = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect_err("the spent registration refuses");
    assert_eq!(unavailable.code, AcquireErrorCode::Unavailable);
    assert!(
        !unavailable.message.contains("capability-provider:"),
        "registration identities are host internals and must not cross: {:?}",
        unavailable.message
    );

    // A replacement registration is never reachable through the old grant:
    // the snapshot binds the exact registration, and that identity is spent.
    let replacement = rig
        .broker
        .register(
            &definition(),
            Arc::new(EchoText::default()) as Arc<dyn TextCapability>,
        )
        .expect("a replacement registration succeeds");
    assert_eq!(
        acquire_code(&mut state, CAPABILITY_ID),
        Err(AcquireErrorCode::Unavailable)
    );
    drop(replacement);

    // Beginning the stop fences the context itself, before cleanup runs: the
    // same acquire that was `unavailable` a moment ago is now about the
    // activation, not the provider - and this is the mid-window moment the
    // fixture's "A2" observes, not the settled state after cleanup.
    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    let revoked = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect_err("a closing activation refuses");
    assert_eq!(revoked.code, AcquireErrorCode::Revoked);
    assert!(
        !revoked.message.contains('@'),
        "activation identities are host internals and must not cross: {:?}",
        revoked.message
    );
    slot.finish_stop(epoch).await.expect("cleanup completes");
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
    let (context, activation) = admitted_context(
        &mut rig.slot,
        epoch,
        &rig.broker,
        &rig.registry,
        &rig.revision,
        &grants,
    )
    .await;
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

    // A resource held into the stop window refuses as revoked - the host
    // half of the fixture's stale-window "E0", invoked between stop beginning
    // and cleanup finishing, not after.
    let held_into_stop = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("still granted before the stop");
    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    assert_eq!(
        HostCapability::invoke(&mut state, held_into_stop, "ping".to_owned())
            .expect_err("a closing activation's resource refuses")
            .code,
        CallErrorCode::Revoked
    );
    slot.finish_stop(epoch).await.expect("cleanup completes");

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
    let (context, activation) = admitted_context(
        &mut mismatch_rig.slot,
        mismatch_epoch,
        &mismatch_rig.broker,
        &mismatch_rig.registry,
        &mismatch_rig.revision,
        &mismatch_grants,
    )
    .await;
    let mut state = HostState::with_grants(HostObserver::new(), WasmLimits::default(), context);
    let mismatched = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect_err("another contract's grant refuses");
    assert_eq!(mismatched.code, AcquireErrorCode::Mismatched);
    assert!(
        !mismatched.message.contains("::") && !mismatched.message.contains("dyn "),
        "Rust contract type paths are host internals and must not cross: {:?}",
        mismatched.message
    );
    stop(mismatch_epoch, &mismatch_rig.registry, activation).await;
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
    let (context, activation) = admitted_context(
        &mut rig.slot,
        epoch,
        &rig.broker,
        &rig.registry,
        &rig.revision,
        &grants,
    )
    .await;
    let tight = WasmLimits {
        max_capability_handles: 1,
        ..WasmLimits::default()
    };
    let generous = WasmLimits {
        max_capability_handles: 2,
        ..WasmLimits::default()
    };

    // The generous half of the pair, so the refusal below is attributable to
    // the bound rather than to the seam: the same two acquires both admit.
    let mut roomy = HostState::with_grants(HostObserver::new(), generous, context.clone());
    let first = roomy
        .acquire(CAPABILITY_ID.to_owned())
        .expect("the first handle is admitted");
    let second = roomy
        .acquire(CAPABILITY_ID.to_owned())
        .expect("the second is under the generous bound");
    HostCapability::drop(&mut roomy, first).expect("drop never errors");
    HostCapability::drop(&mut roomy, second).expect("drop never errors");

    let mut state = HostState::with_grants(HostObserver::new(), tight, context);

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

    // Dropping the held resource frees its slot for the next acquire.
    HostCapability::drop(&mut state, held).expect("drop never errors");
    assert_eq!(state.observer().live_capability_handles(), 0);
    let _reacquired = state
        .acquire(CAPABILITY_ID.to_owned())
        .expect("a freed slot admits again");

    // The other release path, host half: drop the state with the entry still
    // live - the table drops, the entry's `Drop` runs, and the observer the
    // state cloned counts the handle out with no `drop` call ever made.
    let observer = state.observer().clone();
    assert_eq!(observer.live_capability_handles(), 1);
    drop(state);
    assert_eq!(observer.live_capability_handles(), 0);

    stop(epoch, &rig.registry, activation).await;
    drop(registration);
}
