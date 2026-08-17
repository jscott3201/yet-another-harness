//! Wasmtime component driver for the provisional conformance world.
//!
//! One driver object owns an engine and its compiled fixture components; every
//! activation owns its own store and instance, keyed by exact activation
//! identity. Host teardown is the only shutdown authority: the world exports no
//! guest deactivation hook, so `deactivate` drops the store outright rather than
//! asking guest code to cooperate.
//!
//! Every activation runs under the host-owned bounds in [`crate::limits`]:
//! ceilings on its total memory and table size and on how many memories,
//! tables, and instances it may hold, the stack a call runs on and how deep the
//! guest may recurse on it, a call deadline that stops a guest which will not
//! stop itself, and a cap on what one guest-to-host call may transfer. The
//! engine carrying them is built by `WasmLimits::engine`, so a bound cannot be
//! set here and forgotten elsewhere.
//!
//! A guest call runs on a stack of its own, so a call that will not return no
//! longer holds the thread polling its future: at each epoch tick the guest
//! yields to the executor and other work proceeds. The WIT world stays
//! synchronous - nothing declares `async func` - so this is Wasmtime's fiber
//! support rather than Component Model async, which the JavaScript toolchain
//! cannot yet compile.
//!
//! The store lock is still held for the duration of a guest call, so a guest
//! that will not return delays its own deactivation. Deactivation stops the
//! guest before asking for that lock, which is what bounds the delay - but
//! that stop is read at the guest's next epoch deadline, and a call now
//! reaches one only while something is still polling its future. The bound the
//! host relies on comes from destroying the start future first, which unwinds
//! the fiber as it drops; a consumer that parked a start future and then
//! awaited deactivation would wait on a guest nobody is resuming.
//!
//! Granted capabilities cross the ABI as opaque resources: each start permit's
//! capability context rides in its activation's store, `acquire` resolves a
//! grant into a resource whose entry wraps an activation-scoped handle, and
//! every guest call re-enters the broker's revocation and fencing gates
//! ([`crate::capability`]).
//!
//! This driver does not load packages, meter fuel, or contain hostile guest
//! code. Bounding what a guest costs is not isolating what it can reach.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};

use wasmtime::{
    Engine, ResourceLimiter, Store,
    component::{Component, HasSelf, Linker},
};
use yah_plugin_host::{
    DriverActivationError, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginRevisionId, PluginStartContext,
    PreparedDriverActivation,
};

use crate::{
    bindings::Conformance,
    guest::GuestProgram,
    host::{HostObserver, HostState},
    limits::{EpochTicker, GuestInterrupt, WasmLimits},
};

type ObservationMap = HashMap<PluginActivationId, Arc<ActivationObservation>>;

/// Scripted behaviour for one activation of the fixture driver.
///
/// The guest program is real in every case: the start-failure plan instantiates
/// a component whose `activate` returns a `guest-error`, rather than a host
/// stand-in for one. Pending start has no guest analogue, because a synchronous
/// guest call either returns or traps, so the host half of start is what pends.
#[derive(Clone, Copy, Debug)]
pub struct WasmActivationPlan {
    guest: GuestProgram,
    start: StartBehavior,
    deactivate: DeactivateBehavior,
}

impl WasmActivationPlan {
    pub const fn ready() -> Self {
        Self {
            guest: GuestProgram::Conformant,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate, then never resolve, leaving the store live for cleanup.
    pub const fn pending_start() -> Self {
        Self {
            guest: GuestProgram::Conformant,
            start: StartBehavior::PendAfterInstantiate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// The same, behind a guest that runs long enough to yield while it starts.
    ///
    /// Instantiation is a guest call, so a host polling a start can be handed
    /// back `Pending` before there is anything to observe. With the conformant
    /// guest that depends on a tick happening to land inside a few microseconds
    /// of instantiation; here it is what the guest does.
    pub const fn slow_pending_start() -> Self {
        Self {
            guest: GuestProgram::SlowStart,
            start: StartBehavior::PendAfterInstantiate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    pub const fn start_failure() -> Self {
        Self {
            guest: GuestProgram::ActivateFailure,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest that never returns, so the deadline must stop it.
    pub const fn runaway() -> Self {
        Self {
            guest: GuestProgram::Runaway,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest that grows memory until the ceiling refuses.
    pub const fn memory_hog() -> Self {
        Self {
            guest: GuestProgram::MemoryHog,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest whose memory is spread across several memories.
    pub const fn multi_memory() -> Self {
        Self {
            guest: GuestProgram::MultiMemory,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest with more memories than the host allows.
    pub const fn many_memories() -> Self {
        Self {
            guest: GuestProgram::ManyMemories,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest that lifts more bytes than its memory holds.
    pub const fn host_call_flood() -> Self {
        Self {
            guest: GuestProgram::HostCallFlood,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest that consumes a brokered capability via its tool.
    pub const fn capability_consumer() -> Self {
        Self {
            guest: GuestProgram::CapabilityConsumer,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest with more tables than the host allows.
    pub const fn many_tables() -> Self {
        Self {
            guest: GuestProgram::ManyTables,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// Instantiate a guest that recurses until the depth bound stops it.
    pub const fn deep_recursion() -> Self {
        Self {
            guest: GuestProgram::DeepRecursion,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::Release,
        }
    }

    pub const fn deactivation_failure() -> Self {
        Self {
            guest: GuestProgram::Conformant,
            start: StartBehavior::CallActivate,
            deactivate: DeactivateBehavior::ReleaseThenFail,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum StartBehavior {
    CallActivate,
    PendAfterInstantiate,
}

#[derive(Clone, Copy, Debug)]
enum DeactivateBehavior {
    Release,
    ReleaseThenFail,
}

/// Why a driver could not be built from its fixture components.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WasmDriverBuildError {
    summary: String,
}

impl WasmDriverBuildError {
    fn new(summary: impl Into<String>) -> Self {
        Self {
            summary: summary.into(),
        }
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }
}

impl std::fmt::Display for WasmDriverBuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.summary)
    }
}

impl std::error::Error for WasmDriverBuildError {}

/// Read-only evidence view over every activation this driver has prepared.
#[derive(Clone)]
pub struct WasmObserver {
    observations: Arc<Mutex<ObservationMap>>,
}

impl WasmObserver {
    pub fn resource_state(&self, id: &PluginActivationId) -> Result<ResourceState, String> {
        self.observation(id)
            .ok_or_else(|| format!("unknown wasm activation {id}"))?
            .resource_state()
    }

    pub fn deactivation_calls(&self, id: &PluginActivationId) -> usize {
        self.observation(id)
            .map_or(0, |state| state.deactivation_calls.load(Ordering::Acquire))
    }

    /// Whether this activation recorded a failure that leaves it unusable.
    ///
    /// The driver sets this itself rather than reading Wasmtime's state,
    /// because Wasmtime's state does not cover every case: a panicking host
    /// import (see `guarded`) leaves a store Wasmtime still considers
    /// enterable, and a failed instantiation never produced an instance to
    /// poison at all. Once set it never clears.
    pub fn is_faulted(&self, id: &PluginActivationId) -> bool {
        self.observation(id)
            .is_some_and(|state| state.faulted.load(Ordering::Acquire))
    }

    /// Host-side view of this activation's imports.
    ///
    /// Most checked-in fixtures import nothing, so for them this only ever
    /// reports the cancellation signal the driver installed; the flood and
    /// capability fixtures are the exceptions that put evidence here.
    pub fn host_observer(&self, id: &PluginActivationId) -> Option<HostObserver> {
        self.observation(id).map(|state| state.host.clone())
    }

    /// Call the world's `fixture-tool` on a live activation.
    ///
    /// Deliberately not part of `PluginDriver`: the host has no tool-invocation
    /// contract yet, and inventing one here would be inventing it in the wrong
    /// crate. What this exists for is evidence - a guest's answer observed
    /// through the same store, limiter, and call deadline its activation runs
    /// under, rather than through a linker a test built for itself. It lives on
    /// the evidence view so scripted and authored drivers share one entry.
    pub async fn call_fixture_tool(
        &self,
        id: &PluginActivationId,
        input_json: &str,
    ) -> Result<String, DriverActivationError> {
        let core = self
            .observation(id)
            .and_then(|state| {
                state
                    .core
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .upgrade()
            })
            .ok_or_else(|| DriverActivationError::failed("no live activation for that identity"))?;
        core.call_fixture_tool(input_json).await
    }

    fn observation(&self, id: &PluginActivationId) -> Option<Arc<ActivationObservation>> {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }
}

/// Which plan each activation runs, and which plans are still unclaimed.
///
/// Assignment is keyed by activation identity rather than by call order,
/// because the host may retry `prepare` for the same activation after a
/// failure of its own. Popping a fresh plan on that retry would hand the
/// activation a different script, or exhaust the queue outright.
struct ActivationScript {
    pending: VecDeque<WasmActivationPlan>,
    assigned: HashMap<PluginActivationId, WasmActivationPlan>,
}

pub struct WasmComponentDriver {
    revision: PluginRevisionId,
    engine: Engine,
    limits: WasmLimits,
    components: HashMap<GuestProgram, Component>,
    script: Mutex<ActivationScript>,
    /// One authored component every activation runs, when there is one.
    ///
    /// A scripted driver hands each activation the next fixture in a queue,
    /// because each fixture exists to make one bound observable. An authored
    /// plugin is the opposite: one component, any number of activations, and
    /// nothing to script. When this is set the queue is not consulted.
    authored: Option<Component>,
    observations: Arc<Mutex<ObservationMap>>,
    /// Kept alive for the driver's lifetime; dropping it stops the ticker.
    _ticker: EpochTicker,
}

impl WasmComponentDriver {
    /// Compile every fixture component the plans name, then script the driver.
    ///
    /// Compilation happens here so that `prepare` stays inert: by the time the
    /// host prepares an activation there is nothing left to compile, load, or
    /// fail on except in-memory bookkeeping.
    pub fn scripted(
        revision: PluginRevisionId,
        plans: impl IntoIterator<Item = WasmActivationPlan>,
    ) -> Result<(Arc<dyn PluginDriver>, WasmObserver), WasmDriverBuildError> {
        Self::scripted_with_limits(revision, plans, WasmLimits::default())
    }

    /// Script the driver with explicit bounds.
    ///
    /// Tests use this to make a limit reachable in a reasonable wall clock. The
    /// bounds are host-owned in either case: no component influences them.
    pub fn scripted_with_limits(
        revision: PluginRevisionId,
        plans: impl IntoIterator<Item = WasmActivationPlan>,
        limits: WasmLimits,
    ) -> Result<(Arc<dyn PluginDriver>, WasmObserver), WasmDriverBuildError> {
        let plans: VecDeque<WasmActivationPlan> = plans.into_iter().collect();
        // The engine's bounds live with the bounds, not here. Setting them at
        // each construction site is how the recursion bound came to be unset at
        // all of them.
        let engine = limits.engine().map_err(WasmDriverBuildError::new)?;
        let ticker = EpochTicker::start(&engine, limits.epoch_tick);
        let mut components = HashMap::new();
        for plan in &plans {
            if let std::collections::hash_map::Entry::Vacant(slot) = components.entry(plan.guest) {
                let component = Component::new(&engine, plan.guest.text()).map_err(|error| {
                    WasmDriverBuildError::new(format!(
                        "fixture component {:?} did not compile: {}",
                        plan.guest,
                        full_cause(&error)
                    ))
                })?;
                // The corpus is held to the same surface rule as an authored
                // component. A fixture is the one guest the host wrote itself,
                // so exempting it is how the rule would come to be untested.
                crate::surface::check_import_surface(&engine, &component).map_err(|reason| {
                    WasmDriverBuildError::new(format!(
                        "fixture component {:?} was refused: {reason}",
                        plan.guest
                    ))
                })?;
                slot.insert(component);
            }
        }
        let observations: Arc<Mutex<ObservationMap>> = Arc::new(Mutex::new(HashMap::new()));
        let observer = WasmObserver {
            observations: Arc::clone(&observations),
        };
        let driver: Arc<dyn PluginDriver> = Arc::new(Self {
            revision,
            engine,
            limits,
            components,
            script: Mutex::new(ActivationScript {
                pending: plans,
                assigned: HashMap::new(),
            }),
            authored: None,
            observations,
            _ticker: ticker,
        });
        Ok((driver, observer))
    }

    /// Run one authored component, compiled from bytes the caller supplies.
    ///
    /// This is the path an example plugin takes, and the shape a package loader
    /// will need: the driver is handed a component it did not choose, rather
    /// than selecting one from a corpus it owns. Every activation runs that
    /// component and calls its `activate`, so there is no plan queue to exhaust.
    ///
    /// Returns `Arc<Self>` unshrunk; it coerces to `Arc<dyn PluginDriver>` at
    /// the call that hands it to the host, so nothing is lost by not unsizing
    /// here. Tool calls go through [`WasmObserver::call_fixture_tool`], which
    /// is not part of the host contract.
    pub fn for_component(
        revision: PluginRevisionId,
        component: &[u8],
        limits: WasmLimits,
    ) -> Result<(Arc<Self>, WasmObserver), WasmDriverBuildError> {
        let engine = limits.engine().map_err(WasmDriverBuildError::new)?;
        let ticker = EpochTicker::start(&engine, limits.epoch_tick);
        // Compiled here, as fixtures are, so `prepare` stays inert. This is the
        // cost a loader would cache: it dominates instantiation by two orders
        // of magnitude, and by three for a component built from JavaScript.
        let authored = Component::new(&engine, component).map_err(|error| {
            WasmDriverBuildError::new(format!(
                "authored component did not compile: {}",
                full_cause(&error)
            ))
        })?;
        // Before a store, a limiter, or a deadline exists. A component whose
        // declared surface the host will not honour is refused inert, which is
        // where a package loader would want the refusal too.
        crate::surface::check_import_surface(&engine, &authored)
            .map_err(WasmDriverBuildError::new)?;
        let observations: Arc<Mutex<ObservationMap>> = Arc::new(Mutex::new(HashMap::new()));
        let observer = WasmObserver {
            observations: Arc::clone(&observations),
        };
        let driver = Arc::new(Self {
            revision,
            engine,
            limits,
            components: HashMap::new(),
            script: Mutex::new(ActivationScript {
                pending: VecDeque::new(),
                assigned: HashMap::new(),
            }),
            authored: Some(authored),
            observations,
            _ticker: ticker,
        });
        Ok((driver, observer))
    }
}

impl PluginDriver for WasmComponentDriver {
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
        // An authored component has one behaviour - activate it - so there is
        // nothing to script and no queue to run out of.
        let plan = if self.authored.is_some() {
            WasmActivationPlan::ready()
        } else {
            let mut script = self
                .script
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match script.assigned.get(request.id()) {
                Some(plan) => *plan,
                None => {
                    let plan = script
                        .pending
                        .pop_front()
                        .ok_or_else(|| DriverPrepareError::new("wasm activation plan exhausted"))?;
                    script.assigned.insert(request.id().clone(), plan);
                    plan
                }
            }
        };
        let component = match &self.authored {
            Some(authored) => authored.clone(),
            None => self
                .components
                .get(&plan.guest)
                .ok_or_else(|| {
                    DriverPrepareError::new(format!("no compiled component for {:?}", plan.guest))
                })?
                .clone(),
        };
        // The guest's cancellation import answers from this activation's own
        // scope token, so a host cancellation is visible to guest code that
        // bothers to ask. Nothing here depends on the guest asking.
        let cancellation = request.cancellation().clone();
        let observation = Arc::new(ActivationObservation::new(HostObserver::observing(
            Arc::new(move || cancellation.is_cancelled()),
        )));
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(request.id().clone(), Arc::clone(&observation));
        let core = Arc::new(ActivationCore {
            engine: self.engine.clone(),
            component,
            limits: self.limits,
            interrupt: GuestInterrupt::new(),
            observation: Arc::clone(&observation),
            live: tokio::sync::Mutex::new(None),
        });
        *observation
            .core
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::downgrade(&core);
        Ok(Arc::new(PreparedWasmActivation {
            id: request.id().clone(),
            plan,
            core,
        }))
    }
}

/// One activation's store and instance.
///
/// This is deliberately not held by the start future. The host destroys that
/// future to cancel a pending start, and whatever it took with it would never
/// reach `deactivate`.
struct LiveInstance {
    store: Store<HostState>,
    bindings: Conformance,
}

struct PreparedWasmActivation {
    id: PluginActivationId,
    plan: WasmActivationPlan,
    core: Arc<ActivationCore>,
}

/// The activation state a returned future must be able to reach on its own.
///
/// `start` and `deactivate` hand back `'static` futures, so nothing they touch
/// may borrow the prepared control. Sharing this by `Arc` is what lets the
/// store survive a future the host drops mid-start.
struct ActivationCore {
    engine: Engine,
    component: Component,
    limits: WasmLimits,
    interrupt: GuestInterrupt,
    observation: Arc<ActivationObservation>,
    /// The store, behind an async-aware lock.
    ///
    /// A guest call needs `&mut LiveInstance` for its whole duration, so the
    /// critical section contains an `await` and cannot be shortened. That rules
    /// out a blocking mutex: its guard is `!Send`, which would make the futures
    /// this driver hands the host `!Send`, and holding a blocking lock across
    /// an await would stall the executor rather than the caller.
    live: tokio::sync::Mutex<Option<LiveInstance>>,
}

impl ActivationCore {
    /// Refuse to enter the guest if this activation has been stopped or faulted.
    ///
    /// The epoch deadline only stops a call already in flight, and only one
    /// that runs long enough to reach a deadline. A call that returns before
    /// the epoch advances would never consult the flag, so entry is read
    /// separately - on every path that runs guest code, which includes
    /// instantiation and the component initialisation it performs.
    ///
    /// The fault check is the same refusal for a different reason, and it is
    /// what makes [`PreparedWasmActivation::health`]'s claim true. Wasmtime
    /// poisons a store whose guest trapped, so a trap enforces itself - but a
    /// *host* panic caught by [`Self::guarded`] leaves a store that still looks
    /// callable behind guest frames that were abandoned mid-execution, and
    /// nothing in Wasmtime refuses the next call into it. Health has always
    /// reported that activation as unable to run again; until there was a
    /// repeatable entry point it was never possible to disagree with health by
    /// calling anyway.
    fn enter_guest(&self, what: &str) -> Result<(), DriverActivationError> {
        if self.interrupt.is_killed() {
            return Err(DriverActivationError::failed(format!(
                "activation was stopped before {what} could run"
            )));
        }
        if self.observation.faulted.load(Ordering::Acquire) {
            return Err(DriverActivationError::failed(format!(
                "activation cannot run {what}: an earlier failure left it unable to be entered"
            )));
        }
        Ok(())
    }

    async fn instantiate(
        &self,
        context: Option<PluginStartContext>,
    ) -> Result<(), DriverActivationError> {
        self.enter_guest("instantiation")?;
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        Conformance::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(|error| {
                DriverActivationError::failed(format!("host imports did not link: {error}"))
            })?;
        // The permit's capability context rides in the store's own state, so
        // the `capabilities` import can only ever resolve the grants admitted
        // for this exact activation - there is no driver-wide registry a guest
        // could reach past its own store. `None` occurs only on entry paths
        // that never saw a start permit (in-crate interrupt tests); it reads
        // as an activation holding no grants at all.
        let state = match context {
            Some(context) => {
                HostState::with_grants(self.observation.host.clone(), self.limits, context)
            }
            None => HostState::with_limits(self.observation.host.clone(), self.limits),
        };
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state: &mut HostState| state.limiter() as &mut dyn ResourceLimiter);
        // A guest can alias every element of a list at one buffer, so the
        // memory ceiling does not bound what a single call costs the host.
        // This does.
        store.set_hostcall_fuel(self.limits.host_call_bytes);

        // A store's epoch deadline starts at zero, which has already elapsed,
        // so this has to be set before anything runs guest code - including
        // instantiation, which runs the component's own initialisation.
        //
        // Extending one tick at a time keeps the decision in the callback,
        // which is the only place a stuck call can be reached from.
        let interrupt = self.interrupt.clone();
        let budget = self.limits.call_budget_ticks;
        store.set_epoch_deadline(1);
        store.epoch_deadline_callback(move |_| Ok(interrupt.on_deadline(budget)));
        self.interrupt.begin_call();

        let bindings = self
            .guarded(Conformance::instantiate_async(
                &mut store,
                &self.component,
                &linker,
            ))
            .await
            .map_err(|error| {
                self.fault(format!(
                    "component did not instantiate: {}",
                    self.describe_stop("instantiation", &error)
                ))
            })?;
        let mut live = self.live.lock().await;
        // One store per core, asserted rather than assumed - before anything
        // is published and while the lock is held: the live capability counter
        // is shared through the observer, so a second store would read a
        // ceiling the first store's entries still occupy.
        debug_assert!(
            live.is_none(),
            "an activation core must instantiate at most once"
        );
        *live = Some(LiveInstance { store, bindings });
        drop(live);
        self.observation
            .resource
            .store(ResourceState::Live as u8, Ordering::Release);
        Ok(())
    }

    async fn call_activate(&self) -> Result<(), DriverActivationError> {
        self.enter_guest("activate")?;
        let mut guard = self.live.lock().await;
        let live = guard.as_mut().ok_or_else(|| {
            DriverActivationError::failed("activation store was released before activate")
        })?;
        // Re-arm for this call. The deadline is absolute, so a store that has
        // been idle since instantiation is already past the one it was given,
        // and the guest would be charged for time it never ran.
        live.store.set_epoch_deadline(1);
        self.interrupt.begin_call();
        let called = self
            .guarded(
                live.bindings
                    .yah_plugin_lifecycle()
                    .call_activate(&mut live.store),
            )
            .await
            .map_err(|error| self.fault(self.describe_stop("activate", &error)))?;
        // A returned `guest-error` is an ordinary ABI return: the guest chose to
        // refuse, nothing trapped, and the store is still usable. Recording it
        // as a fault would make `health` report a store that cannot be entered
        // when it can.
        called.map_err(|error| {
            DriverActivationError::failed(format!(
                "guest activate returned {:?}: {}",
                error.code, error.message
            ))
        })
    }

    /// Call the world's `fixture-tool` under this activation's own bounds.
    ///
    /// Everything `activate` is subject to applies here unchanged: the entry
    /// check, a re-armed deadline, the per-poll panic guard, and the store's
    /// host-call fuel. The fuel is charged on every guest-to-host lift whichever
    /// export is running - `activate` takes no argument and still spends it on
    /// the strings it logs - so this path is not where the budget applies, only
    /// the easiest place to push a guest past it, since the caller chooses how
    /// large the argument is.
    async fn call_fixture_tool(&self, input_json: &str) -> Result<String, DriverActivationError> {
        self.enter_guest("invoke")?;
        let mut guard = self.live.lock().await;
        let live = guard.as_mut().ok_or_else(|| {
            // Not "released": an activation that was prepared and never started
            // has no store to release, and that is the reachable case here -
            // after teardown the weak handle refuses before this point.
            DriverActivationError::failed("activation has no live store to run invoke on")
        })?;
        live.store.set_epoch_deadline(1);
        self.interrupt.begin_call();
        let called = self
            .guarded(
                live.bindings
                    .yah_plugin_fixture_tool()
                    .call_invoke(&mut live.store, input_json),
            )
            .await
            .map_err(|error| self.fault(self.describe_stop("invoke", &error)))?;
        called.map_err(|error| {
            DriverActivationError::failed(format!(
                "guest invoke returned {:?}: {}",
                error.code, error.message
            ))
        })
    }

    /// Name a guest failure, separating a host-ordered stop from an overrun.
    ///
    /// Wasmtime cannot make this distinction: a kill and a spent tick budget
    /// both leave [`GuestInterrupt::on_deadline`] returning `Interrupt`, and
    /// both arrive here as `Trap::Interrupt`. Only the driver knows which,
    /// because only the driver raised the kill. Reporting them identically
    /// would tell an operator that a guest overran when the host stopped it.
    fn describe_stop(&self, call: &str, error: &wasmtime::Error) -> String {
        if self.interrupt.is_killed()
            && matches!(
                error.downcast_ref::<wasmtime::Trap>(),
                Some(wasmtime::Trap::Interrupt)
            )
        {
            return format!("guest {call} was stopped by the host and did not return");
        }
        describe_guest_failure(call, error)
    }

    /// Run one guest call so a panic cannot escape into the host.
    ///
    /// A trap unwinds no Rust frames, but a panicking host import does - and it
    /// bypasses the flag Wasmtime sets on a trapping store, leaving a store
    /// that still looks callable behind guest frames that were abandoned
    /// mid-execution. Catching here turns that into an ordinary activation
    /// failure whose store `deactivate` still owns and releases.
    ///
    /// The guard wraps each `poll` rather than one synchronous call, because a
    /// guest call now runs on a fiber: the panic surfaces when the fiber is
    /// resumed, which is inside a poll. Catching around the `await` instead
    /// would catch nothing, since `await` itself does not run the guest.
    async fn guarded<T>(
        &self,
        call: impl std::future::Future<Output = wasmtime::Result<T>>,
    ) -> wasmtime::Result<T> {
        let mut call = std::pin::pin!(call);
        std::future::poll_fn(move |cx| {
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| call.as_mut().poll(cx)))
            {
                Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
                Ok(std::task::Poll::Ready(result)) => std::task::Poll::Ready(result),
                Err(panic) => std::task::Poll::Ready(Err(Self::panic_error(&panic))),
            }
        })
        .await
    }

    /// Render a caught panic payload as an ordinary guest-call failure.
    fn panic_error(panic: &Box<dyn std::any::Any + Send>) -> wasmtime::Error {
        let summary = panic
            .downcast_ref::<&str>()
            .map(|text| (*text).to_owned())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown panic".to_owned());
        wasmtime::Error::msg(format!(
            "host code panicked while guest code was running: {summary}"
        ))
    }

    /// Record that this activation can never serve another guest call.
    ///
    /// A faulted activation is known-bad rather than merely unknown, so this is
    /// recorded once and never cleared.
    ///
    /// What happens to the store depends on where the failure was. A failed
    /// `call_activate` keeps its store: it is already parked in `live`, so
    /// `deactivate` finds it and releases it, which is what the host contract
    /// requires of a failed start. A failed `instantiate` cannot, because its
    /// store is a local that drops where it failed — so `resource` never
    /// advances past `NotAcquired`, and `deactivate` reports the activation as
    /// though it had acquired nothing. That reads identically to a start the
    /// host cancelled before any store existed. The two are different events
    /// and the observed state does not yet distinguish them.
    fn fault(&self, summary: String) -> DriverActivationError {
        self.observation.faulted.store(true, Ordering::Release);
        DriverActivationError::failed(summary)
    }
}

impl PreparedDriverActivation for PreparedWasmActivation {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        let id = self.id.clone();
        let plan = self.plan;
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            if permit.id() != &id {
                return Err(DriverActivationError::failed(
                    "host start permit did not match the prepared activation",
                ));
            }
            core.instantiate(Some(permit.context().clone())).await?;
            match plan.start {
                StartBehavior::CallActivate => core.call_activate().await,
                StartBehavior::PendAfterInstantiate => {
                    std::future::pending::<Result<(), DriverActivationError>>().await
                }
            }
        })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        // A faulted store is known-bad, not unknown, so this reports rather
        // than errors. Both reads are atomics: health must never wait on the
        // lock a guest call holds.
        if self.core.observation.faulted.load(Ordering::Acquire) {
            return Ok(PluginHealth::unhealthy(
                "wasm activation cannot be entered again after a guest failure",
            ));
        }
        if self.core.observation.resource_state().ok() == Some(ResourceState::Live) {
            Ok(PluginHealth::Healthy)
        } else {
            Err(DriverHealthError::new("wasm activation store is not live"))
        }
    }

    fn deactivate(
        &self,
        permit: DriverStopPermit,
    ) -> DriverFuture<Result<(), DriverDeactivationError>> {
        let id = self.id.clone();
        let behavior = self.plan.deactivate;
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            if permit.id() != &id {
                return Err(DriverDeactivationError::new(
                    "host stop permit did not match the prepared activation",
                ));
            }
            let observation = Arc::clone(&core.observation);
            observation
                .deactivation_calls
                .fetch_add(1, Ordering::AcqRel);
            // Stop any guest call still running before asking for the lock it
            // holds. Without this the wait is bounded only by whatever the
            // guest chooses to do next, which for a runaway guest is nothing.
            core.interrupt.kill();
            // Dropping the store releases the instance, its memories, and every
            // host binding the linker installed. No guest hook participates.
            //
            // An activation the host cancelled before its first start poll has
            // no store at all. Reporting that as `Released` would make "the
            // driver cleaned up after itself" indistinguishable from "the
            // driver never acquired anything", so the state only advances when
            // there was something to release.
            let released = core.live.lock().await.take();
            if released.is_some() {
                drop(released);
                // The observation outlives the store deliberately, but its
                // retained log records are guest-sized. Counters are evidence
                // and stay; the bytes do not.
                observation.host.release_records();
                observation
                    .resource
                    .store(ResourceState::Released as u8, Ordering::Release);
            }
            match behavior {
                DeactivateBehavior::Release => Ok(()),
                DeactivateBehavior::ReleaseThenFail => Err(DriverDeactivationError::new(
                    "wasm driver plan scripted a deactivation failure",
                )),
            }
        })
    }
}

struct ActivationObservation {
    resource: AtomicU8,
    faulted: AtomicBool,
    deactivation_calls: AtomicUsize,
    host: HostObserver,
    /// Weak on purpose: the prepared activation owns the core, and the host
    /// drops it at teardown. A strong handle here would keep a store - and its
    /// fiber and memory reservations - alive for as long as anything held an
    /// observation, which outlives the activation by design.
    core: Mutex<std::sync::Weak<ActivationCore>>,
}

impl ActivationObservation {
    fn new(host: HostObserver) -> Self {
        Self {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
            faulted: AtomicBool::new(false),
            deactivation_calls: AtomicUsize::new(0),
            host,
            core: Mutex::new(std::sync::Weak::new()),
        }
    }

    fn resource_state(&self) -> Result<ResourceState, String> {
        match self.resource.load(Ordering::Acquire) {
            value if value == ResourceState::NotAcquired as u8 => Ok(ResourceState::NotAcquired),
            value if value == ResourceState::Live as u8 => Ok(ResourceState::Live),
            value if value == ResourceState::Released as u8 => Ok(ResourceState::Released),
            value => Err(format!("invalid wasm resource state {value}")),
        }
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceState {
    NotAcquired = 0,
    Live = 1,
    Released = 2,
}

/// Render an error and every cause behind it as one line.
///
/// Wasmtime keeps the useful half in the source chain. A truncated component, a
/// core module handed to a component parser, and an unknown binary version all
/// *display* as "failed to parse WebAssembly module" and differ only
/// underneath, so formatting with `{error}` tells an operator that something was
/// wrong with the bytes and nothing about which thing - and those want different
/// responses. Joined on one line rather than printed as an `anyhow` report,
/// because this ends up inside a single-line refusal summary.
fn full_cause(error: &wasmtime::Error) -> String {
    let mut rendered = String::new();
    for (depth, cause) in error.chain().enumerate() {
        if depth > 0 {
            rendered.push_str(": ");
        }
        rendered.push_str(&cause.to_string());
    }
    rendered
}

/// Name the limit that stopped a guest, rather than pass the trap through raw.
///
/// The summary is the only place the reason survives into the host's error, so
/// "the deadline killed it" and "the guest executed a bad instruction" must not
/// arrive looking the same.
///
/// There is deliberately no fuel arm. `Trap::OutOfFuel` only exists when
/// `Config::consume_fuel` is set, which this driver never sets, so an arm for
/// it would advertise a budget the driver does not keep. The host-call byte cap
/// is a separate matter: it is enforced, but Wasmtime reports its exhaustion
/// through a private error type rather than a `Trap`, so it arrives here as
/// generic prose. Naming it would mean matching on that prose.
fn describe_guest_failure(call: &str, error: &wasmtime::Error) -> String {
    match error.downcast_ref::<wasmtime::Trap>() {
        Some(wasmtime::Trap::Interrupt) => {
            format!("guest {call} exceeded its call deadline and was interrupted by the host")
        }
        Some(wasmtime::Trap::CannotEnterComponent) => {
            format!("guest {call} was refused because an earlier failure poisoned this activation")
        }
        Some(trap) => format!("guest {call} trapped: {trap}"),
        None => format!("guest {call} failed: {error}"),
    }
}

/// In-crate cases for the parts of the interrupt the host path cannot reach.
///
/// `DriverStartPermit` and `DriverStopPermit` are `pub(crate)` to
/// `yah-plugin-host`, so nothing outside the host can drive an activation, and
/// the host offers no way to deactivate one while a start is still running.
/// That is the right boundary to keep - but it leaves the kill path with no
/// integration case, because every host-reachable failure is reached by the
/// tick budget instead. These drive `ActivationCore` directly.
#[cfg(test)]
#[path = "driver/interrupt_tests.rs"]
mod interrupt_tests;
