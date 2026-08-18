//! The byte pump: one task per activation, and the only place IO meets the
//! session.
//!
//! The session is a pure state machine; the pump feeds it socket bytes,
//! writes out what it queues, advances its clock, and lifts its events into
//! shapes the driver can wait on without touching the wire. Everything the
//! pump owns — the socket, the child, the pending-call table — dies with the
//! pump, and the pump's exit path always ends in a reaped child: goodbye
//! first, `SIGKILL` after the grace window, `wait` regardless.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU8, Ordering},
};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot};
use yah_plugin_ipc::session::{AppError, HostSession, SessionEvent};
use yah_plugin_ipc::types::*;
use yah_plugin_ipc::{frame, session::SessionConfig};

use crate::ProcLimits;
use crate::bootstrap::SpawnedWorker;

/// How a host-initiated call ended, as the driver's hooks observe it.
#[derive(Clone, Debug)]
pub enum CallEnd {
    /// The worker's terminal frame arrived.
    Settled(Outcome),
    /// The session settled it locally: deadline, goodbye, disconnect, or
    /// fatal fault. `reconcile` carries the session's own judgment.
    Lost {
        error: WireErrorKind,
        reconcile: bool,
    },
}

pub(crate) enum PumpCommand {
    Call {
        method: String,
        payload: serde_json::Value,
        deadline_ms: Option<u32>,
        opened: oneshot::Sender<Result<CallId, AppError>>,
        settled: oneshot::Sender<CallEnd>,
    },
    Cancel {
        call_id: CallId,
        target: CancelTarget,
        done: oneshot::Sender<Result<(), AppError>>,
    },
    Shutdown {
        reason: String,
    },
}

const PHASE_HANDSHAKE: u8 = 0;
const PHASE_ACTIVE: u8 = 1;
const PHASE_CLOSED: u8 = 2;

/// The pump's face toward the driver: nonblocking snapshots and a notifier.
pub(crate) struct PumpShared {
    phase: AtomicU8,
    /// Why the session ended; set once, before the phase turns closed.
    close_summary: Mutex<Option<String>>,
    changed: Notify,
    /// Bounded tails of the worker's stdout and stderr, oldest bytes
    /// discarded first. Diagnostics are evidence, not a channel.
    diagnostics: Mutex<Diagnostics>,
    diagnostics_cap: usize,
}

#[derive(Default)]
struct Diagnostics {
    stdout: VecDeque<u8>,
    stderr: VecDeque<u8>,
}

impl PumpShared {
    fn new(limits: &ProcLimits) -> Self {
        Self {
            phase: AtomicU8::new(PHASE_HANDSHAKE),
            close_summary: Mutex::new(None),
            changed: Notify::new(),
            diagnostics: Mutex::new(Diagnostics::default()),
            diagnostics_cap: limits.diagnostics_cap_bytes,
        }
    }

    pub fn is_negotiated(&self) -> bool {
        self.phase.load(Ordering::Acquire) == PHASE_ACTIVE
    }

    pub fn is_closed(&self) -> bool {
        self.phase.load(Ordering::Acquire) == PHASE_CLOSED
    }

    pub fn close_summary(&self) -> Option<String> {
        self.close_summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Wait until the phase leaves handshake; resolves immediately if it
    /// already has. Returns whether the session reached active.
    pub async fn negotiated(&self) -> bool {
        loop {
            let changed = self.changed.notified();
            match self.phase.load(Ordering::Acquire) {
                PHASE_ACTIVE => return true,
                PHASE_CLOSED => return false,
                _ => changed.await,
            }
        }
    }

    /// The retained tail of one diagnostic stream, lossily decoded.
    pub fn diagnostics_tail(&self, stream: DiagnosticStream) -> String {
        let held = self
            .diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bytes = match stream {
            DiagnosticStream::Stdout => &held.stdout,
            DiagnosticStream::Stderr => &held.stderr,
        };
        String::from_utf8_lossy(&bytes.iter().copied().collect::<Vec<u8>>()).into_owned()
    }

    fn set_active(&self) {
        self.phase.store(PHASE_ACTIVE, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn set_closed(&self, summary: String) {
        {
            let mut held = self
                .close_summary
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // First cause wins: a fatal's reason must not be overwritten by
            // the disconnect that follows it.
            held.get_or_insert(summary);
        }
        self.phase.store(PHASE_CLOSED, Ordering::Release);
        self.changed.notify_waiters();
    }

    fn append_diagnostics(&self, stream: DiagnosticStream, bytes: &[u8]) {
        let mut held = self
            .diagnostics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let buffer = match stream {
            DiagnosticStream::Stdout => &mut held.stdout,
            DiagnosticStream::Stderr => &mut held.stderr,
        };
        buffer.extend(bytes.iter().copied());
        while buffer.len() > self.diagnostics_cap {
            buffer.pop_front();
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DiagnosticStream {
    Stdout,
    Stderr,
}

pub(crate) struct PumpHandle {
    pub commands: mpsc::UnboundedSender<PumpCommand>,
    pub shared: Arc<PumpShared>,
    pub task: tokio::task::JoinHandle<()>,
}

/// Start the pump for a freshly spawned worker.
pub(crate) fn start(
    worker: SpawnedWorker,
    config: SessionConfig,
    limits: ProcLimits,
) -> PumpHandle {
    let shared = Arc::new(PumpShared::new(&limits));
    let (commands, receiver) = mpsc::unbounded_channel();
    let pump = Pump {
        session: HostSession::new(config),
        worker,
        commands: receiver,
        shared: Arc::clone(&shared),
        pending: HashMap::new(),
        limits,
    };
    let task = tokio::spawn(pump.run());
    PumpHandle {
        commands,
        shared,
        task,
    }
}

struct Pump {
    session: HostSession,
    worker: SpawnedWorker,
    commands: mpsc::UnboundedReceiver<PumpCommand>,
    shared: Arc<PumpShared>,
    pending: HashMap<CallId, oneshot::Sender<CallEnd>>,
    limits: ProcLimits,
}

impl Pump {
    async fn run(mut self) {
        let started = tokio::time::Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_millis(self.limits.tick_interval_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut stdout = self.worker.child.stdout.take();
        let mut stderr = self.worker.child.stderr.take();
        let mut wire_buf = [0u8; 16 * 1024];
        let mut out_buf = [0u8; 4 * 1024];
        let mut err_buf = [0u8; 4 * 1024];
        let mut input_open = true;
        let mut goodbye_reason: Option<String> = None;

        loop {
            self.apply_events();
            if !self.write_outbox().await {
                input_open = false;
                self.session.end_of_input();
                self.apply_events();
            }
            if self.session.is_closed() {
                // The session's own goodbye (on a fatal) is already in the
                // bytes written above; what remains is process supervision.
                break;
            }
            if let Some(reason) = goodbye_reason.take() {
                self.say_goodbye(&reason).await;
                break;
            }
            tokio::select! {
                read = self.worker.channel.read(&mut wire_buf), if input_open => {
                    match read {
                        Ok(0) | Err(_) => {
                            input_open = false;
                            self.session.end_of_input();
                        }
                        Ok(count) => self.session.feed(&wire_buf[..count]),
                    }
                }
                _ = ticker.tick() => {
                    self.session.tick(started.elapsed().as_millis() as u64);
                }
                read = read_stream(&mut stdout, &mut out_buf) => {
                    self.on_diagnostics(DiagnosticStream::Stdout, &mut stdout, &out_buf, read);
                }
                read = read_stream(&mut stderr, &mut err_buf) => {
                    self.on_diagnostics(DiagnosticStream::Stderr, &mut stderr, &err_buf, read);
                }
                command = self.commands.recv() => match command {
                    Some(PumpCommand::Call { method, payload, deadline_ms, opened, settled }) => {
                        match self.session.call_worker(&method, payload, deadline_ms, false) {
                            Ok(call_id) => {
                                self.pending.insert(call_id, settled);
                                let _ = opened.send(Ok(call_id));
                            }
                            Err(error) => {
                                let _ = opened.send(Err(error));
                            }
                        }
                    }
                    Some(PumpCommand::Cancel { call_id, target, done }) => {
                        let _ = done.send(self.session.cancel(call_id, target));
                    }
                    Some(PumpCommand::Shutdown { reason }) => {
                        goodbye_reason = Some(reason);
                    }
                    // Every command sender is gone: the driver was dropped
                    // without deactivation. The kill path is the safety net.
                    None => {
                        goodbye_reason = Some("driver dropped".to_owned());
                    }
                },
            }
        }

        self.shared.set_closed(
            self.shared
                .close_summary()
                .unwrap_or_else(|| "worker session ended".to_owned()),
        );
        self.reap().await;
        // Whatever is still waiting learns the pump is gone by its sender
        // dropping here; the session already settled real in-flight work.
    }

    /// Lift session facts into driver-visible state.
    fn apply_events(&mut self) {
        for event in self.session.drain_events() {
            match event {
                SessionEvent::Negotiated { .. } => self.shared.set_active(),
                SessionEvent::HostCallSettled { call_id, outcome } => {
                    if let Some(waiter) = self.pending.remove(&call_id) {
                        let _ = waiter.send(CallEnd::Settled(outcome));
                    }
                }
                SessionEvent::HostCallLost {
                    call_id,
                    error,
                    reconcile,
                } => {
                    if let Some(waiter) = self.pending.remove(&call_id) {
                        let _ = waiter.send(CallEnd::Lost { error, reconcile });
                    }
                }
                SessionEvent::CallDelivered { call_id, .. } => {
                    // No application sits above this driver yet, and silence
                    // is not an answer the protocol permits: every worker
                    // call gets its terminal.
                    let _ = self.session.reply_to_worker(
                        call_id,
                        Outcome::Err {
                            error: WireError {
                                kind: WireErrorKind::UnknownMethod,
                                message: "unknown-method".to_owned(),
                                retryable: false,
                                reconcile_required: false,
                            },
                        },
                    );
                }
                SessionEvent::Fatal { kind, detail } => {
                    self.shared
                        .set_closed(format!("protocol fault {}: {detail}", kind_label(kind)));
                }
                SessionEvent::WorkerGoodbye { reason } => {
                    self.shared.set_closed(format!("worker goodbye: {reason}"));
                }
                // No stream or handle consumer exists yet; these become
                // meaningful when a real application sits above the driver.
                _ => {}
            }
        }
    }

    /// Write every queued frame. Returns false when the transport is gone.
    async fn write_outbox(&mut self) -> bool {
        for message in self.session.drain_outbox() {
            let bytes = match serde_json::to_vec(&message) {
                Ok(bytes) => bytes,
                Err(_) => return false,
            };
            let framed = frame::encode(&bytes);
            if self.worker.channel.write_all(&framed).await.is_err() {
                return false;
            }
        }
        true
    }

    /// The orderly half of shutdown: a goodbye frame, then a half-close so
    /// the worker sees end-of-input. The kill in [`Self::reap`] is what
    /// makes it bounded.
    async fn say_goodbye(&mut self, reason: &str) {
        let goodbye = HostMessage::Goodbye(Goodbye {
            reason: reason.to_owned(),
        });
        if let Ok(bytes) = serde_json::to_vec(&goodbye) {
            let _ = self.worker.channel.write_all(&frame::encode(&bytes)).await;
        }
        let _ = self.worker.channel.shutdown().await;
        self.shared.set_closed(format!("host goodbye: {reason}"));
    }

    /// Grace, kill, reap — in that order, unconditionally ending in `wait`.
    async fn reap(&mut self) {
        let grace = Duration::from_millis(self.limits.kill_grace_ms);
        if tokio::time::timeout(grace, self.worker.child.wait())
            .await
            .is_err()
        {
            let _ = self.worker.child.start_kill();
            let _ = self.worker.child.wait().await;
        }
    }

    fn on_diagnostics(
        &mut self,
        stream: DiagnosticStream,
        source: &mut Option<impl Unpin>,
        buffer: &[u8],
        read: std::io::Result<usize>,
    ) {
        match read {
            Ok(0) | Err(_) => *source = None,
            Ok(count) => self.shared.append_diagnostics(stream, &buffer[..count]),
        }
    }
}

/// Read one chunk from an optional stream; pends forever once it is gone,
/// which in a `select!` simply stops the arm from firing.
async fn read_stream<R: tokio::io::AsyncRead + Unpin>(
    source: &mut Option<R>,
    buffer: &mut [u8],
) -> std::io::Result<usize> {
    match source {
        Some(stream) => stream.read(buffer).await,
        None => std::future::pending().await,
    }
}

fn kind_label(kind: WireErrorKind) -> &'static str {
    match kind {
        WireErrorKind::UnsupportedVersion => "unsupported-version",
        WireErrorKind::UnknownRequiredFeature => "unknown-required-feature",
        WireErrorKind::NegotiationRequired => "negotiation-required",
        WireErrorKind::InvalidFrame => "invalid-frame",
        WireErrorKind::FrameTooLarge => "frame-too-large",
        WireErrorKind::PayloadTooLarge => "payload-too-large",
        WireErrorKind::UnknownCall => "unknown-call",
        WireErrorKind::DuplicateCall => "duplicate-call",
        WireErrorKind::UnknownMethod => "unknown-method",
        WireErrorKind::ResourceExhausted => "resource-exhausted",
        WireErrorKind::DeadlineExceeded => "deadline-exceeded",
        WireErrorKind::Cancelled => "cancelled",
        WireErrorKind::UnknownHandle => "unknown-handle",
        WireErrorKind::InvalidRead => "invalid-read",
        WireErrorKind::OutcomeUnknown => "outcome-unknown",
        WireErrorKind::Internal => "internal",
    }
}
