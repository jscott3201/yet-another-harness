//! Process plugin driver: one activation owns one process and one session.
//!
//! `prepare` is inert bookkeeping; `start` spawns the worker with its fd-3
//! channel, runs the pump, and resolves when negotiation completes. Health
//! reads the pump's snapshot without blocking. Deactivation is the orderly
//! shutdown authority — goodbye, grace, `SIGKILL` to the worker's group,
//! reap — and because the activation core owns the child and pump behind an
//! `Arc`, a start future the host drops mid-poll takes nothing with it that
//! deactivation needs. An activation abandoned without deactivation still
//! reclaims its worker: the pump treats its last command sender dropping as
//! the signal, which is why the observation map below never holds one
//! strongly.
//!
//! A worker that dies is not restarted here. The protocol has no resume, so
//! the death poisons this activation, health reports it, and recovery is a
//! fresh activation with a fresh process — restart *policy* stays where
//! SDK-002 deferred it.
//!
//! The start permit's capability context feeds the activation's
//! worker-to-host application dispatcher, and the negotiated activation
//! publishes an [`ActivationEndpoint`] — the production invocation
//! surface, weakly held, withdrawn before teardown.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::sync::mpsc;
use yah_plugin_host::{
    DriverActivationError, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginRevisionId,
    PreparedDriverActivation,
};
use yah_plugin_ipc::session::SessionConfig;

use yah_plugin_host::PluginStartContext;

use crate::ProcLimits;
use crate::bootstrap::{WorkerCommand, spawn_worker};
use crate::endpoint::{ActivationEndpoint, EndpointError, activation_endpoint, admission_gate};
use crate::pump;
use crate::shared::{DiagnosticStream, PumpCommand, PumpGauges, PumpShared};

type ObservationMap = HashMap<PluginActivationId, Arc<ActivationObservation>>;

/// Scripted behaviour for one activation of the process driver.
///
/// The worker is real in every case: a start failure is a worker that
/// offers a protocol version the host refuses, not a host stand-in for one.
/// Pending start is a worker that connects and says nothing — which is why
/// that plan waits without the handshake deadline: the deadline would turn
/// the pending the case exists to observe into a failure.
#[derive(Clone, Copy, Debug)]
pub struct ProcActivationPlan {
    mode: &'static str,
    handshake: HandshakeBehavior,
    deactivate: DeactivateBehavior,
}

impl ProcActivationPlan {
    /// A conformant worker: hello, accept, then serve until goodbye.
    pub const fn ready() -> Self {
        Self {
            mode: "conformant",
            handshake: HandshakeBehavior::Deadline,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// A worker that connects and never sends its hello, so start pends
    /// until the host cancels it.
    pub const fn pending_start() -> Self {
        Self {
            mode: "silent",
            handshake: HandshakeBehavior::Unbounded,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// A worker whose hello offers only a version the host refuses.
    pub const fn start_failure() -> Self {
        Self {
            mode: "bad-version",
            handshake: HandshakeBehavior::Deadline,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// A conformant worker behind a deactivation the plan scripts to fail
    /// after its real release.
    pub const fn deactivation_failure() -> Self {
        Self {
            mode: "conformant",
            handshake: HandshakeBehavior::Deadline,
            deactivate: DeactivateBehavior::ReleaseThenFail,
        }
    }

    /// A worker running the named fake-worker mode, otherwise ordinary.
    /// The lifecycle tests script disconnects and cancellations this way.
    pub const fn worker(mode: &'static str) -> Self {
        Self {
            mode,
            handshake: HandshakeBehavior::Deadline,
            deactivate: DeactivateBehavior::Release,
        }
    }

    /// A mute worker under the handshake clock: start must fail when the
    /// deadline passes, not pend the way [`Self::pending_start`] arranges.
    pub const fn handshake_timeout() -> Self {
        Self {
            mode: "silent",
            handshake: HandshakeBehavior::Deadline,
            deactivate: DeactivateBehavior::Release,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum HandshakeBehavior {
    Deadline,
    Unbounded,
}

#[derive(Clone, Copy, Debug)]
enum DeactivateBehavior {
    Release,
    ReleaseThenFail,
}

/// Which plan each activation runs, and which plans are still unclaimed.
/// Keyed by activation identity: a host retry of `prepare` for the same
/// activation must get the same script back, not the next one.
struct ActivationScript {
    pending: VecDeque<ProcActivationPlan>,
    assigned: HashMap<PluginActivationId, ProcActivationPlan>,
}

/// The process lane's [`PluginDriver`]: one supervised worker process per
/// activation, speaking protocol v1 over an inherited fd-3 socketpair.
///
/// Scripted construction is the only construction: each activation
/// consumes one [`ProcActivationPlan`], and the paired [`ProcObserver`]
/// is the read-only evidence view plus the test-facing call hooks.
pub struct ProcessPluginDriver {
    revision: PluginRevisionId,
    kind: DriverKind,
    worker_program: PathBuf,
    limits: ProcLimits,
    script: Mutex<ActivationScript>,
    observations: Arc<Mutex<ObservationMap>>,
}

impl ProcessPluginDriver {
    /// Script the driver over a worker program, one plan per activation.
    ///
    /// `kind` is the manifest's lane claim (`NodeProcess`, `PythonProcess`),
    /// not the driver's discovery: this driver serves any command that
    /// speaks the protocol over fd 3, and the tests drive it with a
    /// compiled fake worker.
    pub fn scripted(
        revision: PluginRevisionId,
        kind: DriverKind,
        worker_program: impl Into<PathBuf>,
        plans: impl IntoIterator<Item = ProcActivationPlan>,
    ) -> (Arc<Self>, ProcObserver) {
        Self::scripted_with_limits(revision, kind, worker_program, plans, ProcLimits::default())
    }

    /// [`Self::scripted`] with explicit [`ProcLimits`]. Out-of-range
    /// values are clamped where the field documents a floor rather than
    /// refused; see each field's doc for its bound and unit.
    pub fn scripted_with_limits(
        revision: PluginRevisionId,
        kind: DriverKind,
        worker_program: impl Into<PathBuf>,
        plans: impl IntoIterator<Item = ProcActivationPlan>,
        limits: ProcLimits,
    ) -> (Arc<Self>, ProcObserver) {
        let observations: Arc<Mutex<ObservationMap>> = Arc::new(Mutex::new(HashMap::new()));
        let observer = ProcObserver {
            observations: Arc::clone(&observations),
        };
        let driver = Arc::new(Self {
            revision,
            kind,
            worker_program: worker_program.into(),
            limits,
            script: Mutex::new(ActivationScript {
                pending: plans.into_iter().collect(),
                assigned: HashMap::new(),
            }),
            observations,
        });
        (driver, observer)
    }
}

impl ProcessPluginDriver {
    /// The invocation endpoint for one exact activation.
    ///
    /// Publication is gated on negotiation: before hello/accept there is
    /// no worker to talk to, and the endpoint does not exist. The
    /// returned value holds the pump's command channel only weakly, so
    /// clones of it can never keep an abandoned activation alive; once
    /// deactivation has begun it answers `Closing`, and after release it
    /// answers `Closed` with the retained first-cause summary.
    pub fn endpoint(&self, id: &PluginActivationId) -> Result<ActivationEndpoint, EndpointError> {
        let observations = self
            .observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(observation) = observations.get(id) else {
            return Err(EndpointError::NotStarted);
        };
        let Some(link) = observation.link() else {
            // No live link: either the activation was never started, or
            // it ran and was released. The two are different facts.
            return Err(match observation.retired_summary() {
                None => EndpointError::NotStarted,
                Some(summary) => EndpointError::Closed { summary },
            });
        };
        admission_gate(&link.shared)?;
        Ok(activation_endpoint(
            id.clone(),
            link.commands.clone(),
            link.shared,
        ))
    }
}

impl PluginDriver for ProcessPluginDriver {
    fn kind(&self) -> DriverKind {
        self.kind
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
                    let plan = script.pending.pop_front().ok_or_else(|| {
                        DriverPrepareError::new("process activation plan exhausted")
                    })?;
                    script.assigned.insert(request.id().clone(), plan);
                    plan
                }
            }
        };
        let observation = Arc::new(ActivationObservation::new());
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(request.id().clone(), Arc::clone(&observation));
        Ok(Arc::new(PreparedProcActivation {
            id: request.id().clone(),
            plan,
            core: Arc::new(ActivationCore {
                command: WorkerCommand::new(&self.worker_program).arg(plan.mode),
                limits: self.limits,
                observation,
                live: tokio::sync::Mutex::new(None),
            }),
        }))
    }
}

struct PreparedProcActivation {
    id: PluginActivationId,
    plan: ProcActivationPlan,
    core: Arc<ActivationCore>,
}

/// The activation state a returned future must reach on its own: `start`
/// and `deactivate` hand back `'static` futures, so the child, the pump,
/// and the observation are shared by `Arc`, never borrowed.
struct ActivationCore {
    command: WorkerCommand,
    limits: ProcLimits,
    observation: Arc<ActivationObservation>,
    /// The running pump, if the activation spawned. An async-aware lock
    /// for the same store shape as the wasm lane; both critical sections
    /// here are one statement and never hold the guard across an await.
    live: tokio::sync::Mutex<Option<pump::PumpHandle>>,
}

impl ActivationCore {
    async fn boot(
        &self,
        handshake: HandshakeBehavior,
        context: PluginStartContext,
    ) -> Result<(), DriverActivationError> {
        let worker = spawn_worker(&self.command).map_err(|error| {
            DriverActivationError::failed(format!("worker did not spawn: {error}"))
        })?;
        let config = SessionConfig {
            retired_operation_budget: self.limits.retired_operation_budget,
            ..SessionConfig::default()
        };
        let handle = pump::start(worker, config, Some(context), self.limits);
        let shared = Arc::clone(&handle.shared);
        self.observation.attach(&handle);
        {
            let mut live = self.live.lock().await;
            debug_assert!(live.is_none(), "an activation core must spawn at most once");
            *live = Some(handle);
        }
        // The process exists from here on, whatever the handshake does:
        // deactivation has something to release and must release it.
        self.observation
            .resource
            .store(ResourceState::Live as u8, Ordering::Release);
        let negotiated = match handshake {
            HandshakeBehavior::Unbounded => shared.negotiated().await,
            HandshakeBehavior::Deadline => {
                let budget = Duration::from_millis(self.limits.handshake_deadline_ms);
                match tokio::time::timeout(budget, shared.negotiated()).await {
                    Ok(outcome) => outcome,
                    Err(_) => {
                        return Err(DriverActivationError::failed(format!(
                            "worker handshake did not complete within {}ms",
                            self.limits.handshake_deadline_ms
                        )));
                    }
                }
            }
        };
        if !negotiated {
            return Err(DriverActivationError::failed(
                shared.close_summary().map_or_else(
                    || "worker session closed before negotiation".to_owned(),
                    |summary| format!("worker session closed before negotiation: {summary}"),
                ),
            ));
        }
        Ok(())
    }

    /// Stop the pump and reap the child; idempotent and unconditional.
    async fn shutdown(&self) {
        let taken = self.live.lock().await.take();
        if let Some(handle) = taken {
            // Withdrawal precedes the goodbye: the shared closing flag
            // flips before the watch signal moves, so no endpoint can
            // admit a fresh call into a pump that is already saying
            // goodbye. The two stores are one atomic window from an
            // endpoint's point of view — availability reads the flag
            // first and the watch has no queue to lose.
            handle.shared.set_closing();
            // The watch signal has no queue: deactivation cannot be
            // dropped behind a command flood, and a pump that already
            // ended simply never reads it.
            let _ = handle.shutdown.send(Some("deactivated".to_owned()));
            // The pump's exit path spends the grace twice — the goodbye
            // flush and the reap window — plus its bounded drains, so the
            // join bound must cover both windows or it would abort the
            // pump before the group sweep. It still can trip on a pump
            // stuck beyond its own bounds; the sweep is then repeated here,
            // where the abort cannot cancel it, before kill-on-drop reaps
            // the leader. The aborted task is still awaited: nothing is
            // Released while a kill has yet to be sent.
            let mut task = handle.task;
            let bound = Duration::from_millis(2 * self.limits.kill_grace_ms + 2_000);
            if tokio::time::timeout(bound, &mut task).await.is_err() {
                // Deliberately redundant with the pump's own reap: with the
                // bound covering the exit path's worst case, this fires
                // only for a pump stuck beyond its own bounds — a state no
                // test can honestly construct — and then the sweep must
                // come from somewhere the abort cannot cancel.
                let pgid = handle.shared.worker_pid();
                if pgid > 0 {
                    // SAFETY: kill(2) with a negative pid signals the
                    // worker's process group; no memory is touched. The
                    // same narrow recycled-group race the pump's voluntary
                    // sweep documents is accepted here.
                    unsafe {
                        libc::kill(-pgid, libc::SIGKILL);
                    }
                }
                task.abort();
                let _ = task.await;
            }
            self.observation.retire();
            self.observation
                .resource
                .store(ResourceState::Released as u8, Ordering::Release);
        }
    }
}

impl PreparedDriverActivation for PreparedProcActivation {
    fn id(&self) -> &PluginActivationId {
        &self.id
    }

    fn start(&self, permit: DriverStartPermit) -> DriverFuture<Result<(), DriverActivationError>> {
        let id = self.id.clone();
        let handshake = self.plan.handshake;
        let core = Arc::clone(&self.core);
        Box::pin(async move {
            if permit.id() != &id {
                return Err(DriverActivationError::failed(
                    "host start permit did not match the prepared activation",
                ));
            }
            let context = permit.context().clone();
            core.boot(handshake, context).await
        })
    }

    fn health(&self) -> Result<PluginHealth, DriverHealthError> {
        // Snapshot reads only: health must never wait on the pump.
        let observation = &self.core.observation;
        if observation.resource_state() != Ok(ResourceState::Live) {
            return Err(DriverHealthError::new(
                "process activation has no live worker",
            ));
        }
        let Some(shared) = observation.pump_shared() else {
            return Err(DriverHealthError::new(
                "process activation has no live worker",
            ));
        };
        if shared.is_closed() {
            let summary = shared
                .close_summary()
                .unwrap_or_else(|| "worker session ended".to_owned());
            return Ok(PluginHealth::unhealthy(summary));
        }
        if shared.output_closed() {
            // The session stays open for the worker's goodbye, but nothing
            // can be delivered to it; only deactivation ends this state if
            // the worker also stays silent, and health is the signal.
            return Ok(PluginHealth::unhealthy(
                "worker stopped reading its channel",
            ));
        }
        if shared.is_negotiated() {
            Ok(PluginHealth::Healthy)
        } else {
            Ok(PluginHealth::unhealthy(
                "worker handshake has not completed",
            ))
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
            core.observation
                .deactivation_calls
                .fetch_add(1, Ordering::AcqRel);
            core.shutdown().await;
            match behavior {
                DeactivateBehavior::Release => Ok(()),
                DeactivateBehavior::ReleaseThenFail => Err(DriverDeactivationError::new(
                    "process driver plan scripted a deactivation failure",
                )),
            }
        })
    }
}

struct ActivationObservation {
    resource: AtomicU8,
    deactivation_calls: AtomicUsize,
    link: Mutex<ObservationLink>,
}

/// What the observation holds about its pump, by lifecycle stage.
///
/// The live link's command sender is deliberately weak: the pump treats
/// "every strong sender gone" as a driver dropped without deactivation and
/// self-reaps, and a strong sender parked in the driver's observation map
/// would keep an abandoned activation's worker alive forever. Retirement
/// keeps the terminal facts a test reads after release and drops the pump
/// state — its buffers do not outlive the activation.
enum ObservationLink {
    Idle,
    Live(ActivationLink),
    Retired { close_summary: Option<String> },
}

#[derive(Clone)]
struct ActivationLink {
    shared: Arc<PumpShared>,
    commands: mpsc::WeakSender<PumpCommand>,
}

impl ActivationObservation {
    fn new() -> Self {
        Self {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
            deactivation_calls: AtomicUsize::new(0),
            link: Mutex::new(ObservationLink::Idle),
        }
    }

    fn attach(&self, handle: &pump::PumpHandle) {
        *self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            ObservationLink::Live(ActivationLink {
                shared: Arc::clone(&handle.shared),
                commands: handle.commands.downgrade(),
            });
    }

    /// Swap the live link for its terminal facts once the pump has ended.
    fn retire(&self) {
        let mut held = self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let ObservationLink::Live(link) = &*held {
            *held = ObservationLink::Retired {
                close_summary: link.shared.close_summary(),
            };
        }
    }

    fn link(&self) -> Option<ActivationLink> {
        match &*self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            ObservationLink::Live(link) => Some(link.clone()),
            _ => None,
        }
    }

    /// The retained summary if the activation ran and was released;
    /// `None` while it has never started. Distinguishing the two is the
    /// endpoint's job: never-started is not a closed activation.
    fn retired_summary(&self) -> Option<Option<String>> {
        match &*self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            ObservationLink::Idle => None,
            ObservationLink::Live(_) => None,
            ObservationLink::Retired { close_summary } => Some(close_summary.clone()),
        }
    }

    fn pump_shared(&self) -> Option<Arc<PumpShared>> {
        self.link().map(|link| link.shared)
    }

    fn close_summary(&self) -> Option<String> {
        match &*self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
        {
            ObservationLink::Idle => None,
            ObservationLink::Live(link) => link.shared.close_summary(),
            ObservationLink::Retired { close_summary } => close_summary.clone(),
        }
    }

    fn resource_state(&self) -> Result<ResourceState, String> {
        match self.resource.load(Ordering::Acquire) {
            value if value == ResourceState::NotAcquired as u8 => Ok(ResourceState::NotAcquired),
            value if value == ResourceState::Live as u8 => Ok(ResourceState::Live),
            value if value == ResourceState::Released as u8 => Ok(ResourceState::Released),
            value => Err(format!("invalid process resource state {value}")),
        }
    }
}

/// Where one activation's worker process stands, as the observer reports
/// it: acquisition is the spawn, release is the reaped end of the pump.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceState {
    /// No process was ever spawned — preparation only, or a failed spawn.
    NotAcquired = 0,
    /// The process exists and deactivation owes it a release.
    Live = 1,
    /// Deactivation ran: the pump ended and the child was reaped.
    Released = 2,
}

/// Read-only evidence view over every activation this driver prepared.
///
/// The invocation authority lives in [`ProcessPluginDriver::endpoint`],
/// not here: the observer reports what happened and never drives the
/// wire. A test observes a cancellation or a disconnect through these
/// snapshots while driving the same pump through the endpoint.
#[derive(Clone)]
pub struct ProcObserver {
    observations: Arc<Mutex<ObservationMap>>,
}

impl ProcObserver {
    pub fn resource_state(&self, id: &PluginActivationId) -> Result<ResourceState, String> {
        self.observation(id)
            .ok_or_else(|| format!("unknown process activation {id}"))?
            .resource_state()
    }

    pub fn deactivation_calls(&self, id: &PluginActivationId) -> usize {
        self.observation(id)
            .map_or(0, |state| state.deactivation_calls.load(Ordering::Acquire))
    }

    /// Why the session ended, once it has; survives the activation's
    /// release as a retained terminal fact.
    pub fn close_summary(&self, id: &PluginActivationId) -> Option<String> {
        self.observation(id)?.close_summary()
    }

    /// The worker's pid while the activation is live, so a test can prove
    /// the process itself is gone afterward.
    pub fn worker_pid(&self, id: &PluginActivationId) -> Option<i32> {
        Some(self.observation(id)?.pump_shared()?.worker_pid())
    }

    /// The retained tail of one diagnostic stream.
    pub fn diagnostics_tail(
        &self,
        id: &PluginActivationId,
        stream: DiagnosticStream,
    ) -> Option<String> {
        Some(
            self.observation(id)?
                .pump_shared()?
                .diagnostics_tail(stream),
        )
    }

    /// Read-only memory gauges for a live activation: outbound buffer
    /// bytes versus allocated capacity, pending call waiters, and the
    /// command channel's free and total slots. `None` once the
    /// activation's pump has ended — the gauges are the pump's, and they
    /// do not outlive it.
    pub fn gauges(&self, id: &PluginActivationId) -> Option<PumpGauges> {
        let link = self.observation(id)?.link()?;
        let available = link.commands.upgrade().map(|sender| sender.capacity());
        Some(link.shared.gauges(available))
    }

    fn observation(&self, id: &PluginActivationId) -> Option<Arc<ActivationObservation>> {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }
}
