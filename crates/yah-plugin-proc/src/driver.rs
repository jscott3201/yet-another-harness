//! Process plugin driver: one activation owns one process and one session.
//!
//! `prepare` is inert bookkeeping; `start` spawns the worker with its fd-3
//! channel, runs the pump, and resolves when negotiation completes. Health
//! reads the pump's snapshot without blocking. Deactivation is the only
//! shutdown authority: goodbye, grace, `SIGKILL`, reap — and because the
//! activation core owns the child and pump behind an `Arc`, a start future
//! the host drops mid-poll takes nothing with it that deactivation needs.
//!
//! A worker that dies is not restarted here. The protocol has no resume, so
//! the death poisons this activation, health reports it, and recovery is a
//! fresh activation with a fresh process — restart *policy* stays where
//! SDK-002 deferred it.
//!
//! The start permit's capability context is accepted and deliberately
//! unused: the wire protocol defines the handle encoding, but nothing mints
//! a worker handle from a real grant yet. That binding is a later slice.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use yah_plugin_host::{
    DriverActivationError, DriverDeactivationError, DriverFuture, DriverHealthError, DriverKind,
    DriverPrepareError, DriverStartPermit, DriverStopPermit, PluginActivationId,
    PluginActivationRequest, PluginDriver, PluginHealth, PluginRevisionId,
    PreparedDriverActivation,
};
use yah_plugin_ipc::session::SessionConfig;
use yah_plugin_ipc::types::{CallId, CancelTarget};

use crate::ProcLimits;
use crate::bootstrap::{WorkerCommand, spawn_worker};
use crate::pump::{self, CallEnd, DiagnosticStream, PumpCommand, PumpShared};

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
    ) -> (Arc<dyn PluginDriver>, ProcObserver) {
        Self::scripted_with_limits(revision, kind, worker_program, plans, ProcLimits::default())
    }

    pub fn scripted_with_limits(
        revision: PluginRevisionId,
        kind: DriverKind,
        worker_program: impl Into<PathBuf>,
        plans: impl IntoIterator<Item = ProcActivationPlan>,
        limits: ProcLimits,
    ) -> (Arc<dyn PluginDriver>, ProcObserver) {
        let observations: Arc<Mutex<ObservationMap>> = Arc::new(Mutex::new(HashMap::new()));
        let observer = ProcObserver {
            observations: Arc::clone(&observations),
        };
        let driver: Arc<dyn PluginDriver> = Arc::new(Self {
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
    /// The running pump, if the activation spawned. Behind an async-aware
    /// lock because deactivation takes it while a dropped start future may
    /// still hold nothing — the store rule from the wasm lane, unchanged.
    live: tokio::sync::Mutex<Option<pump::PumpHandle>>,
}

impl ActivationCore {
    async fn boot(&self, handshake: HandshakeBehavior) -> Result<(), DriverActivationError> {
        let worker = spawn_worker(&self.command).map_err(|error| {
            DriverActivationError::failed(format!("worker did not spawn: {error}"))
        })?;
        let handle = pump::start(worker, SessionConfig::default(), self.limits);
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
            let _ = handle.commands.send(PumpCommand::Shutdown {
                reason: "deactivated".to_owned(),
            });
            // The pump's own exit path is goodbye → grace → SIGKILL → wait,
            // so this bound only trips if the pump itself is stuck; then
            // aborting it drops the child, whose kill-on-drop reaps.
            let abort = handle.task.abort_handle();
            let bound = Duration::from_millis(self.limits.kill_grace_ms + 2_000);
            if tokio::time::timeout(bound, handle.task).await.is_err() {
                abort.abort();
            }
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
            core.boot(handshake).await
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
    /// Set once at spawn. `PumpShared` is snapshot state and the sender is
    /// a handle; neither keeps the child or its buffers alive.
    link: Mutex<Option<ActivationLink>>,
}

#[derive(Clone)]
struct ActivationLink {
    shared: Arc<PumpShared>,
    commands: mpsc::UnboundedSender<PumpCommand>,
}

impl ActivationObservation {
    fn new() -> Self {
        Self {
            resource: AtomicU8::new(ResourceState::NotAcquired as u8),
            deactivation_calls: AtomicUsize::new(0),
            link: Mutex::new(None),
        }
    }

    fn attach(&self, handle: &pump::PumpHandle) {
        *self
            .link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(ActivationLink {
            shared: Arc::clone(&handle.shared),
            commands: handle.commands.clone(),
        });
    }

    fn link(&self) -> Option<ActivationLink> {
        self.link
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    fn pump_shared(&self) -> Option<Arc<PumpShared>> {
        self.link().map(|link| link.shared)
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

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceState {
    NotAcquired = 0,
    Live = 1,
    Released = 2,
}

/// Read-only evidence view over every activation this driver prepared,
/// plus the worker-interaction hooks the lifecycle tests drive.
///
/// The call and cancel hooks are deliberately not part of `PluginDriver`:
/// the host has no worker-invocation contract yet, and inventing one here
/// would be inventing it in the wrong crate. They exist so a test can
/// observe a call, a cancellation, or a disconnect through the same pump
/// and session the activation runs on.
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

    /// Why the session ended, once it has.
    pub fn close_summary(&self, id: &PluginActivationId) -> Option<String> {
        self.observation(id)?.pump_shared()?.close_summary()
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

    /// Start a host call toward the worker; settle it via [`PendingCall`].
    pub async fn begin_call(
        &self,
        id: &PluginActivationId,
        method: &str,
        payload: serde_json::Value,
        deadline_ms: Option<u32>,
    ) -> Result<PendingCall, String> {
        let link = self
            .observation(id)
            .and_then(|state| state.link())
            .ok_or_else(|| format!("no live worker for activation {id}"))?;
        let (opened_sender, opened) = oneshot::channel();
        let (settled_sender, settled) = oneshot::channel();
        link.commands
            .send(PumpCommand::Call {
                method: method.to_owned(),
                payload,
                deadline_ms,
                opened: opened_sender,
                settled: settled_sender,
            })
            .map_err(|_| "the worker pump has ended".to_owned())?;
        let call_id = opened
            .await
            .map_err(|_| "the worker pump has ended".to_owned())?
            .map_err(|error| format!("the session refused the call: {error:?}"))?;
        Ok(PendingCall { call_id, settled })
    }

    /// Ask the worker to stop one call, or its stream.
    pub async fn cancel(
        &self,
        id: &PluginActivationId,
        call_id: CallId,
        target: CancelTarget,
    ) -> Result<(), String> {
        let link = self
            .observation(id)
            .and_then(|state| state.link())
            .ok_or_else(|| format!("no live worker for activation {id}"))?;
        let (done_sender, done) = oneshot::channel();
        link.commands
            .send(PumpCommand::Cancel {
                call_id,
                target,
                done: done_sender,
            })
            .map_err(|_| "the worker pump has ended".to_owned())?;
        done.await
            .map_err(|_| "the worker pump has ended".to_owned())?
            .map_err(|error| format!("the session refused the cancel: {error:?}"))
    }

    fn observation(&self, id: &PluginActivationId) -> Option<Arc<ActivationObservation>> {
        self.observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }
}

/// One in-flight host call started through the observer hook.
pub struct PendingCall {
    call_id: CallId,
    settled: oneshot::Receiver<CallEnd>,
}

impl PendingCall {
    pub fn call_id(&self) -> CallId {
        self.call_id
    }

    /// Wait for the call's end — worker terminal or local settlement.
    pub async fn settled(self) -> Result<CallEnd, String> {
        self.settled
            .await
            .map_err(|_| "the worker pump ended without settling the call".to_owned())
    }
}
