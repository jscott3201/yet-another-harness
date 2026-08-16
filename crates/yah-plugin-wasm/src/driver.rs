//! Wasmtime component driver for the provisional conformance world.
//!
//! One driver object owns an engine and its compiled fixture components; every
//! activation owns its own store and instance, keyed by exact activation
//! identity. Host teardown is the only shutdown authority: the world exports no
//! guest deactivation hook, so `deactivate` drops the store outright rather than
//! asking guest code to cooperate.
//!
//! Every activation runs under the host-owned bounds in [`crate::limits`]: a
//! total memory and table ceiling, a call deadline that stops a guest which
//! will not stop itself, and a cap on what one guest-to-host call may transfer.
//!
//! The store lock is still held for the duration of a guest call, so a guest
//! that will not return delays its own deactivation. Deactivation stops the
//! guest before asking for that lock, which is what bounds the delay.
//!
//! This driver does not load packages, meter fuel, transport granted
//! capabilities across the ABI, or contain hostile guest code. Bounding what a
//! guest costs is not isolating what it can reach.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    },
};

use wasmtime::{
    Config, Engine, ResourceLimiter, Store,
    component::{Component, HasSelf, Linker},
};
use yah_plugin_host::{
    DriverActivationError, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginRevisionId,
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
    /// Wasmtime poisons a whole store on any failed guest invocation, so this
    /// is a terminal fact rather than a transient one.
    pub fn is_faulted(&self, id: &PluginActivationId) -> bool {
        self.observation(id)
            .is_some_and(|state| state.faulted.load(Ordering::Acquire))
    }

    /// Host-side view of this activation's imports.
    ///
    /// The checked-in fixtures import nothing, so for them this only ever
    /// reports the cancellation signal the driver installed.
    pub fn host_observer(&self, id: &PluginActivationId) -> Option<HostObserver> {
        self.observation(id).map(|state| state.host.clone())
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
        let mut config = Config::new();
        config.epoch_interruption(true);
        let engine = Engine::new(&config).map_err(|error| {
            WasmDriverBuildError::new(format!("engine did not accept its configuration: {error}"))
        })?;
        let ticker = EpochTicker::start(&engine, limits.epoch_tick);
        let mut components = HashMap::new();
        for plan in &plans {
            if let std::collections::hash_map::Entry::Vacant(slot) = components.entry(plan.guest) {
                let component = Component::new(&engine, plan.guest.text()).map_err(|error| {
                    WasmDriverBuildError::new(format!(
                        "fixture component {:?} did not compile: {error}",
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
        let plan = {
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
        let component = self
            .components
            .get(&plan.guest)
            .ok_or_else(|| {
                DriverPrepareError::new(format!("no compiled component for {:?}", plan.guest))
            })?
            .clone();
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
        Ok(Arc::new(PreparedWasmActivation {
            id: request.id().clone(),
            plan,
            core: Arc::new(ActivationCore {
                engine: self.engine.clone(),
                component,
                limits: self.limits,
                interrupt: GuestInterrupt::new(),
                observation,
                live: Mutex::new(None),
            }),
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
    live: Mutex<Option<LiveInstance>>,
}

impl ActivationCore {
    fn instantiate(&self) -> Result<(), DriverActivationError> {
        let mut linker: Linker<HostState> = Linker::new(&self.engine);
        Conformance::add_to_linker::<HostState, HasSelf<HostState>>(&mut linker, |state| state)
            .map_err(|error| {
                DriverActivationError::failed(format!("host imports did not link: {error}"))
            })?;
        let mut store = Store::new(
            &self.engine,
            HostState::with_limits(self.observation.host.clone(), self.limits),
        );
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
            .guarded(|| Conformance::instantiate(&mut store, &self.component, &linker))
            .map_err(|error| {
                self.fault(format!(
                    "component did not instantiate: {}",
                    describe_guest_failure("instantiation", &error)
                ))
            })?;
        *self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(LiveInstance { store, bindings });
        self.observation
            .resource
            .store(ResourceState::Live as u8, Ordering::Release);
        Ok(())
    }

    fn call_activate(&self) -> Result<(), DriverActivationError> {
        // A killed activation must not enter the guest at all. The epoch
        // deadline only stops a call already in flight, and it only stops one
        // that runs long enough to reach a deadline: a short call returns
        // before the epoch ever advances and would never consult the flag. So
        // the flag is also read here, where entry is the thing being decided.
        if self.interrupt.is_killed() {
            return Err(DriverActivationError::failed(
                "activation was stopped before activate could run",
            ));
        }
        let mut guard = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let live = guard.as_mut().ok_or_else(|| {
            DriverActivationError::failed("activation store was released before activate")
        })?;
        // Re-arm for this call. The deadline is absolute, so a store that has
        // been idle since instantiation is already past the one it was given,
        // and the guest would be charged for time it never ran.
        live.store.set_epoch_deadline(1);
        self.interrupt.begin_call();
        let called = self
            .guarded(|| {
                live.bindings
                    .yah_plugin_lifecycle()
                    .call_activate(&mut live.store)
            })
            .map_err(|error| self.fault(describe_guest_failure("activate", &error)))?;
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

    /// Run one guest call so a panic cannot escape into the host.
    ///
    /// A trap unwinds no Rust frames, but a panicking host import does - and it
    /// bypasses the flag Wasmtime sets on a trapping store, leaving a store
    /// that still looks callable behind guest frames that were abandoned
    /// mid-execution. Catching here turns that into an ordinary activation
    /// failure whose store `deactivate` still owns and releases.
    fn guarded<T>(&self, call: impl FnOnce() -> wasmtime::Result<T>) -> wasmtime::Result<T> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)) {
            Ok(result) => result,
            Err(panic) => {
                let summary = panic
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_owned());
                Err(wasmtime::Error::msg(format!(
                    "host code panicked while guest code was running: {summary}"
                )))
            }
        }
    }

    /// Record that this activation can never serve another guest call.
    ///
    /// Wasmtime poisons the whole store on any failed invocation, so a faulted
    /// activation is known-bad rather than merely unknown. The store stays
    /// owned here: releasing it is `deactivate`'s job, and the host contract
    /// requires a failed start to keep its partial resource for cleanup.
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
            core.instantiate()?;
            match plan.start {
                StartBehavior::CallActivate => core.call_activate(),
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
            let released = core
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
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
}

impl ActivationObservation {
    fn new(host: HostObserver) -> Self {
        Self {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
            faulted: AtomicBool::new(false),
            deactivation_calls: AtomicUsize::new(0),
            host,
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

/// Name the limit that stopped a guest, rather than pass the trap through raw.
///
/// The summary is the only place the reason survives into the host's error, so
/// "the deadline killed it" and "the guest executed a bad instruction" must not
/// arrive looking the same.
fn describe_guest_failure(call: &str, error: &wasmtime::Error) -> String {
    match error.downcast_ref::<wasmtime::Trap>() {
        Some(wasmtime::Trap::Interrupt) => {
            format!("guest {call} exceeded its call deadline and was interrupted by the host")
        }
        Some(wasmtime::Trap::OutOfFuel) => {
            format!("guest {call} exhausted its fuel budget")
        }
        Some(wasmtime::Trap::CannotEnterComponent) => {
            format!("guest {call} was refused because an earlier failure poisoned this activation")
        }
        Some(trap) => format!("guest {call} trapped: {trap}"),
        None => format!("guest {call} failed: {error}"),
    }
}
