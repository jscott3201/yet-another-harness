//! Wasmtime component driver for the provisional conformance world.
//!
//! One driver object owns an engine and its compiled fixture components; every
//! activation owns its own store and instance, keyed by exact activation
//! identity. Host teardown is the only shutdown authority: the world exports no
//! guest deactivation hook, so `deactivate` drops the store outright rather than
//! asking guest code to cooperate.
//!
//! This driver does not load packages, enforce resource, deadline, fuel, or
//! host-call limits, transport granted capabilities across the ABI, or contain
//! hostile guest code. Those remain later roadmap slices.
//!
//! One consequence of having no interruption mechanism: the store lock is held
//! for the duration of a guest call, so a guest that never returns also blocks
//! that activation's deactivation. Only host-owned limits can fix that, and
//! they are not implemented here.

use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
};

use wasmtime::{
    Engine, Store,
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
    components: HashMap<GuestProgram, Component>,
    script: Mutex<ActivationScript>,
    observations: Arc<Mutex<ObservationMap>>,
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
        let plans: VecDeque<WasmActivationPlan> = plans.into_iter().collect();
        let engine = Engine::default();
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
            components,
            script: Mutex::new(ActivationScript {
                pending: plans,
                assigned: HashMap::new(),
            }),
            observations,
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
        let mut store = Store::new(&self.engine, HostState::new(self.observation.host.clone()));
        let bindings =
            Conformance::instantiate(&mut store, &self.component, &linker).map_err(|error| {
                DriverActivationError::failed(format!("component did not instantiate: {error}"))
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
        let mut guard = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let live = guard.as_mut().ok_or_else(|| {
            DriverActivationError::failed("activation store was released before activate")
        })?;
        let called = live
            .bindings
            .yah_plugin_lifecycle()
            .call_activate(&mut live.store)
            .map_err(|error| {
                DriverActivationError::failed(format!("guest activate trapped: {error}"))
            })?;
        called.map_err(|error| {
            DriverActivationError::failed(format!(
                "guest activate returned {:?}: {}",
                error.code, error.message
            ))
        })
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
    deactivation_calls: AtomicUsize,
    host: HostObserver,
}

impl ActivationObservation {
    fn new(host: HostObserver) -> Self {
        Self {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
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
