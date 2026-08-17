//! Evidence that a guest which will not stop is stopped anyway.
//!
//! These cases drive activations through the host's own activation guard rather
//! than the portable corpus. The portable corpus is what every driver must
//! pass; limits are this backend's mechanism, and a case only this driver could
//! satisfy does not belong in a shared suite.
//!
//! Every case that expects a limit to fire runs under a watchdog. The
//! conformance runner supplies no timeout of its own, and measuring elapsed
//! time *after* an await cannot fail when the limit never fires: the assertion
//! is never reached. See [`Watchdog`] for why the watchdog is a thread rather
//! than `tokio::time::timeout`.
//!
//! Two ceilings are proved by pairs, not by a single failing case. A test that
//! only ever sees a failure cannot tell "the ceiling refused" from "the fixture
//! was broken", so each ceiling also has a case where a generous ceiling admits
//! the same guest.

#[path = "support/fixtures.rs"]
mod fixtures;

use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use fixtures::revision;
use yah_compose::{
    ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
    DesiredComponentState, ProviderAssignments, ProviderSelectionEpoch, ReconcileOutcome, Scope,
    ServiceRegistry,
};
use yah_plugin_host::{
    CapabilityBroker, DriverActivationErrorKind, DriverConformanceCase, EffectiveCapabilityGrants,
    HostPluginActivation, PluginRevision, PluginStartError,
};
use yah_plugin_wasm::{
    ResourceState, WasmActivationPlan, WasmComponentDriver, WasmLimits, WasmObserver,
};

/// Bounds tight enough that a runaway guest dies quickly in a test.
///
/// The tick is short and the budget small so the deadline is reached in tens of
/// milliseconds rather than seconds. The memory ceiling is one page above the
/// fixture's initial page, so the hog reaches it immediately.
fn test_limits() -> WasmLimits {
    WasmLimits {
        memory_bytes: 2 * 64 * 1024,
        epoch_tick: Duration::from_millis(5),
        call_budget_ticks: 4,
        ..WasmLimits::default()
    }
}

/// A generous multiple of the configured deadline.
///
/// Wide enough that a loaded machine will not fail the assertion, narrow enough
/// that "the deadline never fired" cannot pass.
fn kill_bound() -> Duration {
    // 250x the deadline, which is itself 20ms, so about five seconds. The bound
    // only has to separate "terminated" from "never terminates", so it is set
    // well past anything scheduling noise can produce: a false positive here
    // aborts the whole test binary, which is far worse than a slow pass.
    test_limits().call_deadline() * 250
}

/// Bounds that admit the fixture corpus, for the control half of each pair.
fn generous_limits() -> WasmLimits {
    WasmLimits {
        memory_bytes: 64 * 64 * 1024,
        ..test_limits()
    }
}

/// Turns a hang into a reported failure.
///
/// A guest call now yields its thread at every epoch tick, so an in-runtime
/// timeout would usually work. The watchdog is a thread anyway, because the
/// thing these cases are checking is exactly the mechanism a cooperative
/// timeout would depend on: if the yield regresses, the guest holds its thread
/// again and a timer has nothing to wake. A guard that fails only when the
/// mechanism it guards still works is not a guard.
///
/// It also covers a hang in host code outside the fiber, which no amount of
/// guest cooperation reaches.
///
/// Being outside the runtime, it cannot cancel a blocked call - so it ends the
/// process instead. That takes the rest of the test binary down with it, which
/// is the deliberate trade: a crash names the case that overran within
/// seconds, where a hang says nothing until CI gives up on the whole job.
struct Watchdog {
    finished: Arc<AtomicBool>,
}

impl Watchdog {
    fn arm(what: &'static str) -> Self {
        let finished = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&finished);
        let bound = kill_bound();
        thread::spawn(move || {
            let deadline = Instant::now() + bound;
            while Instant::now() < deadline {
                if flag.load(Ordering::Acquire) {
                    return;
                }
                thread::sleep(Duration::from_millis(2));
            }
            if flag.load(Ordering::Acquire) {
                return;
            }
            // `stderr()`, not `eprintln!`. libtest captures the latter through
            // `io::set_output_capture`, and `abort` discards the captured
            // buffer - so the message would be one nobody ever reads and the
            // crash would name nothing. Writing to the handle bypasses capture.
            let message = format!("watchdog: {what} did not finish within {bound:?}\n");
            let _ = std::io::Write::write_all(&mut std::io::stderr(), message.as_bytes());
            std::process::abort();
        });
        Self { finished }
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
    }
}

/// Await `future` under a watchdog, so a limit that never fires is reported.
async fn within_kill_bound<T>(what: &'static str, future: impl Future<Output = T>) -> T {
    let _watchdog = Watchdog::arm(what);
    future.await
}

struct Rig {
    slot: ComponentSlot,
    registry: ServiceRegistry,
    broker: CapabilityBroker,
    grants: EffectiveCapabilityGrants,
    epoch: ProviderSelectionEpoch,
    revision: PluginRevision,
}

impl Rig {
    /// Mount a fresh component so the slot yields a real selection epoch.
    ///
    /// The driver is only reachable through an activation the composition layer
    /// admitted, so the limits are proved on the same path the host uses.
    fn new(label: &str, digest: char) -> Self {
        let revision = revision(DriverConformanceCase::ReadyLifecycle, digest)
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
        let grants = EffectiveCapabilityGrants::empty(&revision);
        Self {
            slot,
            registry,
            broker: CapabilityBroker::new().expect("broker is constructible"),
            grants,
            epoch,
            revision,
        }
    }
}

fn driver(
    revision: &PluginRevision,
    plans: Vec<WasmActivationPlan>,
) -> (Arc<dyn yah_plugin_host::PluginDriver>, WasmObserver) {
    driver_with(revision, plans, test_limits())
}

fn driver_with(
    revision: &PluginRevision,
    plans: Vec<WasmActivationPlan>,
    limits: WasmLimits,
) -> (Arc<dyn yah_plugin_host::PluginDriver>, WasmObserver) {
    WasmComponentDriver::scripted_with_limits(revision.id().clone(), plans, limits)
        .expect("wasm driver builds")
}

#[tokio::test]
async fn a_guest_that_never_returns_is_killed_by_the_call_deadline() {
    let mut rig = Rig::new("wasm.limits.runaway", '1');
    let (driver, observer) = driver(&rig.revision, vec![WasmActivationPlan::runaway()]);

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation is inert and succeeds");
    let id = activation.id().clone();

    let outcome = within_kill_bound("the call deadline", activation.activate(&rig.registry)).await;

    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a runaway guest must fail activation, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    assert!(
        failure.summary().contains("call deadline"),
        "the failure must name the limit that stopped it: {}",
        failure.summary()
    );
    assert!(
        observer.is_faulted(&id),
        "a killed activation must be recorded as unable to run again"
    );
    // The kill must not release the store: the host contract requires a failed
    // start to keep its partial resource for cleanup to find.
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    activation.finish_stop().await.expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
    assert_eq!(observer.deactivation_calls(&id), 1);
}

#[tokio::test]
async fn a_runaway_guest_does_not_stop_a_sibling_on_the_same_driver() {
    let mut runaway_rig = Rig::new("wasm.limits.sibling.a", '2');
    let (driver, observer) = driver(
        &runaway_rig.revision,
        vec![WasmActivationPlan::runaway(), WasmActivationPlan::ready()],
    );

    let mut runaway = HostPluginActivation::prepare(
        &mut runaway_rig.slot,
        runaway_rig.epoch,
        &runaway_rig.broker,
        &runaway_rig.grants,
        Arc::clone(&driver),
    )
    .expect("first preparation succeeds");
    let runaway_id = runaway.id().clone();
    assert!(
        within_kill_bound(
            "the runaway sibling",
            runaway.activate(&runaway_rig.registry)
        )
        .await
        .is_err()
    );

    // Tear the runaway down *before* the sibling runs any guest code. Teardown
    // is what raises the kill flag, and a kill flag is sticky in a way a spent
    // tick budget is not: a fresh call resets its budget, but nothing resets a
    // kill. So this order is what distinguishes a per-activation interrupt from
    // a driver-wide one, and the driver reads that flag on the way *into* a
    // call - not only at an epoch deadline, which a call this short would never
    // reach. Share the interrupt and the sibling below is refused entry.
    runaway
        .finish_stop()
        .await
        .expect("runaway cleanup completes");

    // The sibling shares the driver, and therefore the engine and the one
    // ticker advancing its epoch. It must be untouched by both the tick that
    // killed its neighbour and the kill that tore the neighbour down.
    let mut ready_rig = Rig::new("wasm.limits.sibling.b", '2');
    let ready_epoch = ready_rig.epoch;
    let mut ready = HostPluginActivation::prepare(
        &mut ready_rig.slot,
        ready_rig.epoch,
        &ready_rig.broker,
        &ready_rig.grants,
        driver,
    )
    .expect("second preparation succeeds");
    let ready_id = ready.id().clone();
    let handle = within_kill_bound("the healthy sibling", ready.activate(&ready_rig.registry))
        .await
        .expect("a well-behaved sibling still activates");

    assert!(matches!(
        handle.health(),
        Ok(yah_plugin_host::PluginHealth::Healthy)
    ));
    assert_eq!(observer.resource_state(&ready_id), Ok(ResourceState::Live));
    assert!(observer.is_faulted(&runaway_id));
    assert!(!observer.is_faulted(&ready_id));

    // Surviving is not enough: the sibling must still stop cleanly afterwards.
    let (slot, _handle) = ready.release_active().expect("active sibling releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&ready_rig.registry, removed)
        .expect("sibling begins stopping");
    slot.finish_stop(ready_epoch)
        .await
        .expect("sibling cleanup completes");
    assert_eq!(
        observer.resource_state(&ready_id),
        Ok(ResourceState::Released)
    );
}

#[tokio::test]
async fn a_guest_that_grows_past_the_memory_ceiling_is_refused() {
    let mut rig = Rig::new("wasm.limits.memory", '3');
    let (driver, observer) = driver(&rig.revision, vec![WasmActivationPlan::memory_hog()]);

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    let outcome = within_kill_bound("the memory ceiling", activation.activate(&rig.registry)).await;

    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a memory-hungry guest must fail activation, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    // A ceiling refuses; it does not interrupt. `memory.grow` answers -1, the
    // guest sees it and reaches its own `unreachable`. Asserting the guest's
    // trap - rather than merely that the deadline was not involved - is what
    // separates "the ceiling refused" from any other way the call could fail.
    assert!(
        failure.summary().contains("trapped"),
        "the guest must fail on its own terms: {}",
        failure.summary()
    );
    assert!(
        !failure.summary().contains("call deadline"),
        "growth is refused, not interrupted: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

#[tokio::test]
async fn the_same_growing_guest_activates_under_a_ceiling_that_can_afford_it() {
    // The control for the case above. Without it, that test cannot tell a
    // ceiling that refused from a fixture that would have failed regardless:
    // the same guest, the same driver, only the ceiling differs.
    let mut rig = Rig::new("wasm.limits.memory.ok", '4');
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::memory_hog()],
        generous_limits(),
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    within_kill_bound("the growing guest", activation.activate(&rig.registry))
        .await
        .expect("growth within the ceiling is granted");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

#[tokio::test]
async fn the_memory_ceiling_is_a_total_across_every_memory_a_guest_declares() {
    // Wasmtime's own `StoreLimits` applies its ceiling to each memory
    // separately. This fixture declares two memories of two pages against a
    // two-page ceiling: per-memory accounting admits both, for four pages under
    // a ceiling of two. Only a running total refuses it.
    let mut rig = Rig::new("wasm.limits.memory.split", '5');
    let (driver, _observer) = driver(&rig.revision, vec![WasmActivationPlan::multi_memory()]);

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");

    let outcome = within_kill_bound(
        "the aggregate memory ceiling",
        activation.activate(&rig.registry),
    )
    .await;
    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a guest over the total ceiling must fail, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    // Naming the ceiling, not just the phase. "instantiation" alone also
    // matches a count refusal and a kill on entry, so it would not distinguish
    // this case from its neighbours.
    assert!(
        failure.summary().contains("memory") && failure.summary().contains("limit"),
        "the byte ceiling must say it was the thing that refused: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
}

#[tokio::test]
async fn several_memories_are_admitted_when_their_total_fits() {
    // The control for the case above: the same two memories, a ceiling that can
    // afford their sum. Without this, "refused" could mean the driver simply
    // cannot instantiate a component with more than one memory.
    let mut rig = Rig::new("wasm.limits.memory.split.ok", '6');
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::multi_memory()],
        generous_limits(),
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    within_kill_bound("the multi-memory guest", activation.activate(&rig.registry))
        .await
        .expect("a total within the ceiling is granted");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
}

#[tokio::test]
async fn a_guest_may_not_hold_more_memories_than_the_host_allows() {
    // The escape a byte ceiling cannot see. Every extra memory in this fixture
    // is empty, so it adds nothing to the total `memory_bytes` charges - and
    // yet each one costs the host an address-space reservation. Bounding bytes
    // without bounding counts bounds the wrong resource.
    let mut rig = Rig::new("wasm.limits.memory.count", '7');
    let (driver, _observer) = driver(&rig.revision, vec![WasmActivationPlan::many_memories()]);

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");

    let outcome = within_kill_bound(
        "the memory count ceiling",
        activation.activate(&rig.registry),
    )
    .await;
    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a guest over the memory count must fail, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    assert!(
        failure.summary().contains("memory count"),
        "the count ceiling must say it was the count that refused: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
}

#[tokio::test]
async fn the_same_empty_memories_are_admitted_when_the_count_allows_them() {
    // The control: the byte total never changes between this case and the one
    // above, so only the count ceiling can explain the difference.
    let mut rig = Rig::new("wasm.limits.memory.count.ok", '8');
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::many_memories()],
        WasmLimits {
            max_memories: 16,
            ..generous_limits()
        },
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    within_kill_bound("the many-memory guest", activation.activate(&rig.registry))
        .await
        .expect("a count within the ceiling is granted");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
}

#[tokio::test]
async fn a_guest_that_recurses_too_deep_is_stopped_by_the_depth_bound() {
    // The bound under test is `guest_stack_bytes`, not the size of the stack
    // the call runs on. Only a recursing guest can tell them apart: it reaches
    // a depth limit while the fiber it runs on is far from exhausted.
    let mut rig = Rig::new("wasm.limits.recursion", '9');
    let (driver, _observer) = driver(&rig.revision, vec![WasmActivationPlan::deep_recursion()]);

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");

    let outcome = within_kill_bound("the depth bound", activation.activate(&rig.registry)).await;
    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a too-deep guest must fail, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    // Naming the exhaustion matters: a depth bound and a call deadline are
    // different limits, and a guest that recursed is not a guest that spun.
    assert!(
        failure.summary().contains("stack exhausted"),
        "the depth bound must name itself: {}",
        failure.summary()
    );
    assert!(
        !failure.summary().contains("call deadline"),
        "recursion is refused by depth, not by time: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
}

#[tokio::test]
async fn the_same_recursion_completes_under_a_bound_that_can_afford_it() {
    // The control, and the only thing that makes the case above attributable.
    // Same guest, same depth, only the host's number differs. It also pins
    // that the driver sets the bound at all: left unset, Wasmtime's 512 KiB
    // default applies and this guest cannot finish.
    let mut rig = Rig::new("wasm.limits.recursion.ok", 'a');
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::deep_recursion()],
        WasmLimits {
            guest_stack_bytes: 8 * 1024 * 1024,
            call_stack_bytes: 16 * 1024 * 1024,
            ..generous_limits()
        },
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    within_kill_bound("the deep guest", activation.activate(&rig.registry))
        .await
        .expect("recursion within the bound is granted");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
}

/// A guest may not spend more host bytes on one call than the budget allows.
///
/// The memory ceiling cannot stand in for this. The fixture's whole memory is
/// one 64 KiB page, and it still asks the host to lift roughly six megabytes,
/// because every element of the list it logs aliases the same buffer. A bound
/// on what the guest HOLDS says nothing about what one call COSTS.
///
/// `host_call_bytes` is Wasmtime's hostcall fuel, so exhaustion is a trap on the
/// guest's side rather than a value the host declines to read.
#[tokio::test]
async fn a_guest_may_not_lift_more_bytes_than_its_host_call_budget() {
    let mut rig = Rig::new("wasm.limits.hostcall", 'b');
    let limits = WasmLimits {
        host_call_bytes: 64 * 1024,
        ..test_limits()
    };
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::host_call_flood()],
        limits,
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    let outcome = within_kill_bound(
        "the host-call byte budget",
        activation.activate(&rig.registry),
    )
    .await;
    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a guest past its host-call budget must fail activation, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    assert!(
        !failure.summary().contains("call deadline"),
        "the budget refused this, not the deadline: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

/// The control: the same aliasing guest under a budget that can afford it.
///
/// Without this, the case above cannot tell a budget that refused from a
/// fixture that would have failed anyway. Built on `test_limits` rather than
/// `generous_limits` so that `host_call_bytes` really is the only difference:
/// the generous set also raises `memory_bytes` 32-fold, which would leave the
/// pair varying two things and attributing the result to one. The fixture needs
/// a single page either way.
#[tokio::test]
async fn the_same_flooding_guest_activates_under_a_budget_that_can_afford_it() {
    let mut rig = Rig::new("wasm.limits.hostcall.ok", 'c');
    let limits = WasmLimits {
        host_call_bytes: 64 * 1024 * 1024,
        ..test_limits()
    };
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::host_call_flood()],
        limits,
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    within_kill_bound("the generous budget", activation.activate(&rig.registry))
        .await
        .expect("the same guest activates when the budget can afford the call");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    // The pair is the amplification proof on its own: the refusing half sets the
    // budget to 64 KiB, which is the whole of this guest's memory, and it still
    // ran out. A call whose cost were bounded by what the guest holds could not
    // do that.
    //
    // Corroborated here by what the host had to clip. The guest declares 200
    // values of 30,000 bytes each; the host keeps `max_log_fields` of them and
    // clips every one to `max_log_message_bytes`, so a clip count at the field
    // ceiling means those values really did arrive at full size.
    let host = observer
        .host_observer(&id)
        .expect("a live activation has a host observer");
    assert_eq!(
        host.truncated_values(),
        WasmLimits::default().max_log_fields,
        "every retained field's value should have arrived over the ceiling and been clipped"
    );

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}

/// A guest may not hold more tables than the host allows.
///
/// The table twin of the memory-count pair. `max_tables` had no component
/// evidence at all before this - it was a field the driver set and nothing
/// proved it honoured. The summed element ceiling is a different bound and
/// still has unit evidence only: every table this fixture declares is empty.
#[tokio::test]
async fn a_guest_may_not_hold_more_tables_than_the_host_allows() {
    let mut rig = Rig::new("wasm.limits.tables", 'd');
    let (driver, observer) = driver(&rig.revision, vec![WasmActivationPlan::many_tables()]);

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    let outcome = within_kill_bound("the table count", activation.activate(&rig.registry)).await;
    let failure = match outcome {
        Err(PluginStartError::Driver { failure, .. }) => failure,
        other => panic!("a guest with too many tables must fail activation, got {other:?}"),
    };
    assert_eq!(failure.kind(), DriverActivationErrorKind::Failed);
    // Names the ceiling, not merely "not the deadline". Without this the case
    // passes for any instantiation failure at all - setting `max_instances` to
    // zero, which has nothing to do with tables, satisfies the weaker form.
    assert!(
        failure.summary().contains("table count"),
        "the count ceiling must say it was the table count that refused: {}",
        failure.summary()
    );
    assert!(
        !failure.summary().contains("call deadline"),
        "a count ceiling refuses; it does not interrupt: {}",
        failure.summary()
    );

    activation.finish_stop().await.expect("cleanup completes");
    // `NotAcquired`, not `Released`. A count ceiling is read while the store is
    // being instantiated, so the activation never got as far as acquiring the
    // resource and there is nothing for teardown to release. The host-call
    // budget above is the contrast: that one refuses a call on a store that
    // already exists, and its activation does reach `Released`.
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::NotAcquired));
}

/// The control: the same empty tables under a count that allows them.
///
/// Every table the fixture declares is empty, so the element ceiling never
/// charges for them. Only the count differs between this and the case above,
/// which is what makes that one attributable to `max_tables`.
#[tokio::test]
async fn the_same_empty_tables_are_admitted_when_the_count_allows_them() {
    let mut rig = Rig::new("wasm.limits.tables.ok", 'e');
    let limits = WasmLimits {
        max_tables: 16,
        ..generous_limits()
    };
    let (driver, observer) = driver_with(
        &rig.revision,
        vec![WasmActivationPlan::many_tables()],
        limits,
    );

    let mut activation =
        HostPluginActivation::prepare(&mut rig.slot, rig.epoch, &rig.broker, &rig.grants, driver)
            .expect("preparation succeeds");
    let id = activation.id().clone();

    within_kill_bound(
        "the generous table count",
        activation.activate(&rig.registry),
    )
    .await
    .expect("empty tables within the count are admitted");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Live));

    let (slot, _handle) = activation.release_active().expect("active releases");
    let removed = DesiredComponentState::removed(slot.generation(2));
    slot.reconcile(&rig.registry, removed)
        .expect("component begins stopping");
    slot.finish_stop(rig.epoch)
        .await
        .expect("cleanup completes");
    assert_eq!(observer.resource_state(&id), Ok(ResourceState::Released));
}
