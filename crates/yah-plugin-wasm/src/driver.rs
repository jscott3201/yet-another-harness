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
    Engine, ResourceLimiter, Store,
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
                live: tokio::sync::Mutex::new(None),
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
    /// Refuse to enter the guest if this activation has been stopped.
    ///
    /// The epoch deadline only stops a call already in flight, and only one
    /// that runs long enough to reach a deadline. A call that returns before
    /// the epoch advances would never consult the flag, so entry is read
    /// separately - on every path that runs guest code, which includes
    /// instantiation and the component initialisation it performs.
    fn enter_guest(&self, what: &str) -> Result<(), DriverActivationError> {
        if self.interrupt.is_killed() {
            return Err(DriverActivationError::failed(format!(
                "activation was stopped before {what} could run"
            )));
        }
        Ok(())
    }

    async fn instantiate(&self) -> Result<(), DriverActivationError> {
        self.enter_guest("instantiation")?;
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
        *self.live.lock().await = Some(LiveInstance { store, bindings });
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
            core.instantiate().await?;
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
mod interrupt_tests {
    use std::{thread, time::Duration, time::Instant};

    use super::*;

    /// One engine, as the driver has: activations that shared nothing could not
    /// falsify an engine-scoped or ticker-scoped kill, which is the only way
    /// "one activation's kill reaches another" could plausibly go wrong.
    fn engine(limits: WasmLimits) -> Engine {
        limits.engine().expect("engine accepts its configuration")
    }

    /// Drive one future to completion on this thread.
    ///
    /// These cases reach past the host into `ActivationCore`, whose calls are
    /// futures now that a guest call runs on its own stack. A current-thread
    /// runtime is the smallest thing that can resume one.
    fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime is constructible")
            .block_on(future)
    }

    fn core(engine: &Engine, program: GuestProgram, limits: WasmLimits) -> Arc<ActivationCore> {
        let component = Component::new(engine, program.text()).expect("fixture component compiles");
        Arc::new(ActivationCore {
            engine: engine.clone(),
            component,
            limits,
            interrupt: GuestInterrupt::new(),
            observation: Arc::new(ActivationObservation::new(HostObserver::new())),
            live: tokio::sync::Mutex::new(None),
        })
    }

    /// A budget so large that only a kill can end the call.
    ///
    /// If the budget could also end it, this case would pass with `kill` gutted
    /// - which is exactly the hole it exists to close.
    fn kill_only_limits() -> WasmLimits {
        WasmLimits {
            epoch_tick: Duration::from_millis(5),
            call_budget_ticks: u64::MAX,
            ..WasmLimits::default()
        }
    }

    #[test]
    fn a_kill_stops_a_call_that_is_already_running() {
        let limits = kill_only_limits();
        let engine = engine(limits);
        let core = core(&engine, GuestProgram::Runaway, limits);
        let ticker = EpochTicker::start(&engine, limits.epoch_tick);
        block_on(core.instantiate()).expect("the runaway instantiates");

        // The result comes back over a channel rather than from `join`. The
        // budget here is effectively infinite by design, so if the kill fails
        // to land the guest runs forever - and `join` would hang the suite
        // instead of failing it, which is the failure mode this case exists to
        // detect. A leaked thread is the price of reporting that.
        let (done, result) = std::sync::mpsc::channel();
        let calling = Arc::clone(&core);
        thread::spawn(move || done.send(block_on(calling.call_activate())));

        // Wait for evidence the guest is actually inside the call, rather than
        // sleeping and hoping. A nonzero tick count means the guest reached an
        // epoch deadline, which it can only do from inside `activate`. Sleeping
        // instead would let a slow scheduler turn this into a test of the entry
        // check, which passes for the wrong reason.
        while core.interrupt.ticks_used() == 0 {
            thread::sleep(Duration::from_millis(1));
        }
        let killed_at = Instant::now();
        core.interrupt.kill();

        let outcome = result
            .recv_timeout(limits.epoch_tick * 200)
            .expect("a killed call must return; it did not");
        let stopped_after = killed_at.elapsed();
        drop(ticker);

        let failure = outcome.expect_err("a killed call must not report success");
        // A host-ordered stop must not be reported as a guest overrun. The
        // budget here is effectively infinite, so "exceeded its call deadline"
        // would be a false statement about why the call ended.
        assert!(
            failure.summary().contains("stopped by the host"),
            "a kill must be named as a kill, not as a deadline: {}",
            failure.summary()
        );
        // The mechanism's claim is one tick. The receive above already bounds
        // this; recording it keeps the number in the failure message.
        assert!(
            stopped_after < limits.epoch_tick * 200,
            "the kill took {stopped_after:?}, which is not a tick-bounded stop"
        );
    }

    #[test]
    fn a_killed_activation_is_refused_before_it_enters_the_guest() {
        let limits = kill_only_limits();
        let engine = engine(limits);
        let core = core(&engine, GuestProgram::Conformant, limits);
        let _ticker = EpochTicker::start(&engine, limits.epoch_tick);
        block_on(core.instantiate()).expect("the conformant guest instantiates");

        // This guest returns in microseconds, so it would never reach an epoch
        // deadline and never consult the flag. Entry is the only place a stop
        // can be read in time.
        core.interrupt.kill();
        let failure = block_on(core.call_activate())
            .expect_err("a killed activation must not run guest code");
        assert!(
            failure.summary().contains("stopped before activate"),
            "the refusal must name why it was refused: {}",
            failure.summary()
        );
    }

    /// The reason guest calls run on their own stack at all.
    ///
    /// Both activations are driven by ONE current-thread runtime, so there is
    /// exactly one thread between them. A guest call that held that thread
    /// would starve its neighbour completely: the healthy activation could not
    /// even begin until the runaway hit its deadline. The runaway's budget here
    /// is long enough that such a wait would be unmistakable.
    #[test]
    fn a_compute_bound_guest_does_not_starve_a_sibling_on_the_same_thread() {
        let limits = WasmLimits {
            epoch_tick: Duration::from_millis(5),
            // Long enough that "the healthy one waited for the runaway" and
            // "the healthy one interleaved with it" cannot be confused.
            call_budget_ticks: 200,
            ..WasmLimits::default()
        };
        let engine = engine(limits);
        let spinner = core(&engine, GuestProgram::Runaway, limits);
        let healthy = core(&engine, GuestProgram::Conformant, limits);
        let ticker = EpochTicker::start(&engine, limits.epoch_tick);
        // The assertion below counts ticks the ticker actually delivered rather
        // than milliseconds. The two agree on an idle machine and diverge on a
        // loaded one: the ticker's sleeps stretch under load, so a wall-clock
        // bound tightens exactly when the machine is least able to meet it,
        // while the property being claimed - the sibling gets in within a tick
        // or two - is about ticks and holds at any speed.
        let ticks = ticker.tick_counter();

        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime is constructible");
        runtime.block_on(async {
            spinner
                .instantiate()
                .await
                .expect("the runaway instantiates");
            healthy
                .instantiate()
                .await
                .expect("the healthy guest instantiates");

            let started = Instant::now();
            let started_ticks = ticks.load(Ordering::Acquire);
            let mut healthy_waited = None;
            let (spun, _) = tokio::join!(spinner.call_activate(), async {
                let outcome = healthy.call_activate().await;
                healthy_waited = Some((
                    ticks.load(Ordering::Acquire).saturating_sub(started_ticks),
                    started.elapsed(),
                ));
                outcome.expect("a healthy guest activates while a sibling spins");
            });
            let spinner_took = started.elapsed();

            let spun = spun.expect_err("the runaway must still hit its deadline");
            assert!(
                spun.summary().contains("call deadline"),
                "the runaway must end on its deadline, not some other way: {}",
                spun.summary()
            );

            let (healthy_ticks, healthy_took) =
                healthy_waited.expect("the healthy guest completed");
            // Measured against the mechanism's own unit, not against the
            // runaway's total. A ratio would accept a sibling that waited
            // dozens of ticks as long as the runaway ran longer still; the
            // property being claimed is that the sibling gets in within about
            // one tick. The shipped code lands on exactly one when idle and at
            // ordinary load, and on a handful when the machine is oversubscribed
            // several times over, so ten leaves room without being so wide that
            // it stops failing anything that yields rarely instead of every
            // tick.
            assert!(
                healthy_ticks < 10,
                "the healthy guest waited {healthy_ticks} ticks ({healthy_took:?}) \
                 against the runaway's {spinner_took:?}; it is not interleaving \
                 per tick"
            );
        });
    }

    #[test]
    fn a_live_activation_still_runs_after_a_sibling_core_is_killed() {
        let limits = kill_only_limits();
        // Both cores on one engine under one ticker, as the driver runs them.
        let engine = engine(limits);
        let killed = core(&engine, GuestProgram::Conformant, limits);
        let live = core(&engine, GuestProgram::Conformant, limits);
        let _ticker = EpochTicker::start(&engine, limits.epoch_tick);
        block_on(killed.instantiate()).expect("first guest instantiates");
        block_on(live.instantiate()).expect("second guest instantiates");

        killed.interrupt.kill();
        block_on(live.call_activate()).expect("a kill on one activation must not reach another");
    }
}
