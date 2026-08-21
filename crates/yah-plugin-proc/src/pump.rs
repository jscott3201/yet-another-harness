//! The byte pump: one task per activation, and the only place IO meets the
//! session.
//!
//! The session is a pure state machine; the pump feeds it socket bytes,
//! writes out what it queues, advances its clock, and lifts its events into
//! shapes the driver can wait on without touching the wire. Every kind of
//! progress is its own `select!` arm — reads, writes, ticks, commands, and
//! the child's own exit — so a worker that stops draining its socket stalls
//! only its bytes (and is cut off at the outbound buffer cap, never buffered
//! toward host memory exhaustion), never the clock or the shutdown path, and
//! a worker that dies is seen even when a descendant keeps its socket open.
//! Everything the pump owns — the socket, the child, the pending-call
//! table — dies with the pump, and the pump's exit path always ends in a
//! reaped child: goodbye first, `SIGKILL` to the worker's whole process
//! group after the grace window, `wait` regardless. A worker's last words
//! survive it: terminals already in the socket are drained before input is
//! declared over, and the diagnostic pipes are drained after the reap.

use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Notify, mpsc, oneshot, watch};
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
    /// fatal fault.
    Lost {
        /// Why: `DeadlineExceeded`, `Cancelled` (a worker goodbye),
        /// `OutcomeUnknown` (a bare disconnect), and so on.
        error: WireErrorKind,
        /// The session's own judgment of whether the worker may have acted
        /// on work whose outcome the host never learned. True demands
        /// reconciliation; false (a goodbye, an expired budget the worker
        /// acknowledged) does not.
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
    // Shutdown deliberately is not a command: commands queue, and a
    // deactivation dropped behind a command flood would wait out an
    // arbitrary backlog. It rides a watch signal instead — one value, no
    // queue, always deliverable.
}

const PHASE_HANDSHAKE: u8 = 0;
const PHASE_ACTIVE: u8 = 1;
const PHASE_CLOSED: u8 = 2;

/// A read-only memory snapshot of the pump's buffers and queues. Every
/// field distinguishes logical occupancy from allocated capacity where the
/// container exposes the difference: the outbound buffer's `capacity` is
/// the high-water allocation a live pump keeps for reuse (a `Vec` never
/// shrinks), while `bytes` is what a worker that stopped draining is
/// actually costing. The command channel's numbers come from the channel
/// itself; its capacity is the configured bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PumpGauges {
    /// Encoded frames awaiting a worker that is not draining, in bytes.
    pub outbound_buffer_bytes: usize,
    /// What that buffer is allocated for — the high-water it has reached
    /// this connection. Zero before the first frame and after the
    /// outbound direction dies, where the undeliverable buffer is
    /// discarded outright.
    pub outbound_buffer_capacity: usize,
    /// Host calls with a waiter parked on the pump.
    pub pending_calls: usize,
    /// Free slots in the bounded command channel right now.
    pub command_channel_available: usize,
    /// The channel's total configured slots.
    pub command_channel_capacity: usize,
}

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
    /// The worker's pid, held so tests can prove the process is gone.
    worker_pid: i32,
    /// The outbound direction died (the worker shut its read half) while
    /// the session stayed open for a goodbye. A separate fact from the
    /// close: health must name it, but it must not steal the close
    /// summary a later goodbye wins under the first-cause rule.
    output_closed: AtomicBool,
    /// Memory gauges, updated by the pump task at every buffer change.
    outbound_bytes: AtomicUsize,
    outbound_capacity: AtomicUsize,
    pending_calls: AtomicUsize,
    command_channel_capacity: usize,
}

#[derive(Default)]
struct Diagnostics {
    stdout: VecDeque<u8>,
    stderr: VecDeque<u8>,
}

impl PumpShared {
    fn new(limits: &ProcLimits, worker_pid: i32) -> Self {
        Self {
            phase: AtomicU8::new(PHASE_HANDSHAKE),
            close_summary: Mutex::new(None),
            changed: Notify::new(),
            diagnostics: Mutex::new(Diagnostics::default()),
            diagnostics_cap: limits.diagnostics_cap_bytes,
            worker_pid,
            output_closed: AtomicBool::new(false),
            outbound_bytes: AtomicUsize::new(0),
            outbound_capacity: AtomicUsize::new(0),
            pending_calls: AtomicUsize::new(0),
            command_channel_capacity: limits.command_channel_capacity,
        }
    }

    pub fn worker_pid(&self) -> i32 {
        self.worker_pid
    }

    pub fn output_closed(&self) -> bool {
        self.output_closed.load(Ordering::Acquire)
    }

    fn set_output_closed(&self) {
        self.output_closed.store(true, Ordering::Release);
        self.changed.notify_waiters();
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

    fn record_outbound(&self, bytes: usize, capacity: usize) {
        self.outbound_bytes.store(bytes, Ordering::Release);
        self.outbound_capacity.store(capacity, Ordering::Release);
    }

    fn record_pending(&self, count: usize) {
        self.pending_calls.store(count, Ordering::Release);
    }

    /// One consistent-enough snapshot of the memory gauges. The command
    /// channel numbers arrive from the caller's sender handle, which is
    /// the only place that can read the queue without keeping the channel
    /// alive artificially.
    pub fn gauges(&self, command_available: Option<usize>) -> PumpGauges {
        PumpGauges {
            outbound_buffer_bytes: self.outbound_bytes.load(Ordering::Acquire),
            outbound_buffer_capacity: self.outbound_capacity.load(Ordering::Acquire),
            pending_calls: self.pending_calls.load(Ordering::Acquire),
            command_channel_available: command_available.unwrap_or(0),
            command_channel_capacity: self.command_channel_capacity,
        }
    }
}

/// Which of the worker's two diagnostic pipes a tail is read from.
/// Diagnostics are evidence text, never protocol bytes.
#[derive(Clone, Copy, Debug)]
pub enum DiagnosticStream {
    /// The worker's standard output.
    Stdout,
    /// The worker's standard error.
    Stderr,
}

pub(crate) struct PumpHandle {
    /// Bounded: a caller flood hits [`mpsc::Sender::try_send`] rejection,
    /// never an unbounded backlog. See [`ProcLimits::
    /// command_channel_capacity`].
    pub commands: mpsc::Sender<PumpCommand>,
    /// Deactivation's signal. One value, no queue: a shutdown is never
    /// dropped behind a command flood, and sending to a dead pump is a
    /// harmless no-op.
    pub shutdown: watch::Sender<Option<String>>,
    pub shared: Arc<PumpShared>,
    pub task: tokio::task::JoinHandle<()>,
}

/// Start the pump for a freshly spawned worker.
pub(crate) fn start(
    worker: SpawnedWorker,
    config: SessionConfig,
    mut limits: ProcLimits,
) -> PumpHandle {
    // The cap accuses a worker of not draining, so it must never sit under
    // one frame the session itself admits: a bound below that would kill a
    // conformant worker for a frame it was never given the chance to read.
    limits.outbound_buffer_cap_bytes = limits
        .outbound_buffer_cap_bytes
        .max(yah_plugin_ipc::MAX_FRAME_BYTES + 64);
    // A zero interval would panic tokio's timer; one millisecond is the
    // clock's floor.
    limits.tick_interval_ms = limits.tick_interval_ms.max(1);
    // One slot is the floor for the same reason: a zero-capacity channel
    // would reject everything, including calls the session would admit.
    limits.command_channel_capacity = limits.command_channel_capacity.max(1);
    let shared = Arc::new(PumpShared::new(&limits, worker.pgid));
    let (commands, receiver) = mpsc::channel(limits.command_channel_capacity);
    let (shutdown, shutdown_rx) = watch::channel(None);
    let pump = Pump {
        session: HostSession::new(config),
        worker,
        commands: receiver,
        shutdown: shutdown_rx,
        shutdown_seen: false,
        shared: Arc::clone(&shared),
        pending: HashMap::new(),
        outbuf: Vec::new(),
        output_open: true,
        limits,
    };
    let task = tokio::spawn(pump.run());
    PumpHandle {
        commands,
        shutdown,
        shared,
        task,
    }
}

struct Pump {
    session: HostSession,
    worker: SpawnedWorker,
    commands: mpsc::Receiver<PumpCommand>,
    shutdown: watch::Receiver<Option<String>>,
    shutdown_seen: bool,
    shared: Arc<PumpShared>,
    pending: HashMap<CallId, oneshot::Sender<CallEnd>>,
    /// Encoded frames awaiting the socket. Progress on it is a `select!`
    /// arm, so transport back-pressure never stalls the rest of the pump.
    outbuf: Vec<u8>,
    /// False once a write failed: the peer shut its read half. That kills
    /// only the outbound direction — the peer's own goodbye or terminals
    /// may still be in flight toward the host, so input runs to its
    /// natural end and undeliverable output is discarded.
    output_open: bool,
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
        let mut child_exited = false;

        loop {
            self.apply_events();
            // One gauge update per iteration covers every mutation of the
            // pending table: admissions in the command arm and settlements
            // in `apply_events` alike.
            self.shared.record_pending(self.pending.len());
            if !self.queue_outbox() {
                input_open = false;
                self.end_input(&mut wire_buf).await;
                self.apply_events();
            }
            if self.session.is_closed() {
                // The session's own goodbye (on a fatal) is already queued;
                // what remains is the shared exit path below.
                break;
            }
            // Deterministic shutdown priority: the watch signal is checked
            // before every select round, so a queued command flood can
            // never out-race deactivation the way an unbiased arm order
            // could. A dropped sender reads as shutdown — the same
            // contract as the arm below.
            if !self.shutdown_seen && self.shutdown.has_changed().unwrap_or(true) {
                self.shutdown_seen = true;
                let reason = self
                    .shutdown
                    .borrow()
                    .clone()
                    .unwrap_or_else(|| "deactivated".to_owned());
                self.begin_goodbye(&reason);
                break;
            }
            tokio::select! {
                ready = self.worker.channel.readable(), if input_open => {
                    if ready.is_err() || !self.read_ready(&mut wire_buf) {
                        input_open = false;
                        self.end_input(&mut wire_buf).await;
                    }
                }
                ready = self.worker.channel.writable(), if self.output_open && !self.outbuf.is_empty() => {
                    if std::env::var("PUMP_DEBUG").is_ok() {
                        eprintln!("arm: writable ok={}", ready.is_ok());
                    }
                    if ready.is_err() || !self.write_ready() {
                        // Not an end of input: what the peer already sent —
                        // its goodbye above all — still decides how this
                        // session closes. But a session that can deliver
                        // nothing is not healthy, and only deactivation
                        // ends one whose peer also stays silent, so the
                        // half-death is published for health to name.
                        self.output_open = false;
                        // Release, not clear: `clear()` keeps the
                        // allocation, and the zero this reports next must
                        // be the truth about retained memory, not merely
                        // about occupancy.
                        self.outbuf = Vec::new();
                        self.shared.record_outbound(0, 0);
                        self.shared.set_output_closed();
                    }
                }
                _ = ticker.tick() => {
                    self.session.tick(started.elapsed().as_millis() as u64);
                }
                // The process is the session peer even though a descendant
                // it spawned may hold the socket open past its death: exit
                // ends input once what it already wrote is drained.
                _ = self.worker.child.wait(), if !child_exited => {
                    child_exited = true;
                    if input_open {
                        input_open = false;
                        self.end_input(&mut wire_buf).await;
                    }
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
                                self.shared.record_pending(self.pending.len());
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
                    // Every command sender is gone: the driver was dropped
                    // without deactivation. The kill path is the safety net.
                    None => {
                        self.begin_goodbye("driver dropped");
                        break;
                    }
                },
                // Deactivation. Not a command: the watch signal carries no
                // queue, so a flood of calls cannot delay or drop it. The
                // sender lives in the pump handle; a dropped sender (the
                // driver gone without deactivating) ends the activation the
                // same way the command channel's `None` does.
                _changed = self.shutdown.changed(), if !self.shutdown_seen => {
                    self.shutdown_seen = true;
                    let reason =
                        self.shutdown.borrow().clone().unwrap_or_else(|| "deactivated".to_owned());
                    self.begin_goodbye(&reason);
                    break;
                }
            }
        }

        // The shared exit path. Flush what is queued — bounded, because the
        // kill below is the guarantee and a worker that stopped draining
        // must not stall it — then half-close so the worker sees
        // end-of-input, then settle whatever the worker may still hold: the
        // session marks handed-over work outcome-unknown and
        // reconcile-required, and dropping a waiter silently is not an end
        // this driver permits.
        let _ = self.queue_outbox();
        if self.output_open {
            let flush_bound = Duration::from_millis(self.limits.kill_grace_ms);
            let _ = tokio::time::timeout(flush_bound, self.flush_outbuf()).await;
        }
        let _ = self.worker.channel.shutdown().await;
        self.end_input(&mut wire_buf).await;
        self.apply_events();
        self.reap().await;
        // The group is dead, so these end at end-of-file promptly; text the
        // worker wrote just before dying is retained instead of racing the
        // pump's exit.
        drain_diagnostics(
            &self.shared,
            DiagnosticStream::Stdout,
            &mut stdout,
            &mut out_buf,
        )
        .await;
        drain_diagnostics(
            &self.shared,
            DiagnosticStream::Stderr,
            &mut stderr,
            &mut err_buf,
        )
        .await;
        self.shared.set_closed(
            self.shared
                .close_summary()
                .unwrap_or_else(|| "worker session ended".to_owned()),
        );
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

    /// Encode every session-queued frame into the outbound buffer. False
    /// when the buffer must stop growing: a frame that does not serialize
    /// (unreachable while the session only queues values it admitted), or
    /// pending bytes past the cap — a worker that will not drain its
    /// channel is reclaimed at a bound, never buffered toward host memory
    /// exhaustion. The recorded cause survives; the caller ends the
    /// session.
    fn queue_outbox(&mut self) -> bool {
        if !self.output_open {
            // Undeliverable: the peer shut its read half. Dropping these is
            // not silence — the peer cannot observe anything else — and it
            // keeps a dead write side from tripping the cap. A call whose
            // frame dies here provably never reached the worker, so its
            // waiter settles now, cancelled without reconciliation, rather
            // than waiting for the session's end to claim — wrongly — that
            // the worker may have acted on it.
            for message in self.session.drain_outbox() {
                if let HostMessage::Call(call) = message
                    && let Some(waiter) = self.pending.remove(&call.call_id)
                {
                    let _ = waiter.send(CallEnd::Lost {
                        error: WireErrorKind::Cancelled,
                        reconcile: false,
                    });
                }
            }
            return true;
        }
        for message in self.session.drain_outbox() {
            match serde_json::to_vec(&message) {
                Ok(bytes) => {
                    self.outbuf.extend_from_slice(&frame::encode(&bytes));
                    self.shared
                        .record_outbound(self.outbuf.len(), self.outbuf.capacity());
                }
                Err(_) => {
                    self.shared
                        .set_closed("a host frame did not serialize".to_owned());
                    return false;
                }
            }
            if self.outbuf.len() > self.limits.outbound_buffer_cap_bytes {
                self.shared.set_closed(format!(
                    "worker stopped draining its channel: pending output exceeds the \
                     {}-byte cap",
                    self.limits.outbound_buffer_cap_bytes
                ));
                return false;
            }
        }
        true
    }

    /// Feed one readable burst; false when input reached its end.
    fn read_ready(&mut self, buffer: &mut [u8]) -> bool {
        match self.worker.channel.try_read(buffer) {
            Ok(0) => false,
            Ok(count) => {
                self.session.feed(&buffer[..count]);
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => true,
            Err(_) => false,
        }
    }

    /// Advance the queued outbound bytes; false when the transport is gone.
    fn write_ready(&mut self) -> bool {
        match self.worker.channel.try_write(&self.outbuf) {
            Ok(0) => false,
            Ok(count) => {
                self.outbuf.drain(..count);
                self.shared
                    .record_outbound(self.outbuf.len(), self.outbuf.capacity());
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => true,
            Err(_) => false,
        }
    }

    /// Drain what the peer already delivered, then declare input over.
    /// Every termination routes through here — the write arm's failure and
    /// the child's exit as much as the read arm's end-of-file — so no
    /// `select!` arm ordering can lose a buffered terminal.
    async fn end_input(&mut self, buffer: &mut [u8]) {
        self.drain_buffered_input(buffer).await;
        self.session.end_of_input();
    }

    /// Feed what the peer already wrote before input is declared over — a
    /// buffered goodbye or terminal must not be mistaken for loss. Doubly
    /// bounded: bytes against a chatty descendant, wall-clock against a
    /// trickling one; a peer still writing past either bound is not a
    /// session this driver will keep listening to.
    ///
    /// `WouldBlock` is not trusted as "nothing left": tokio's `try_read`
    /// reports it from cached readiness without a read syscall, so bytes
    /// physically queued in the socket would be lost to it. The reactor is
    /// asked, briefly, before the drain gives up.
    async fn drain_buffered_input(&mut self, buffer: &mut [u8]) {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        let mut budget: usize = 1024 * 1024;
        while budget > 0 && !self.session.is_closed() {
            match self.worker.channel.try_read(buffer) {
                Ok(0) => return,
                Ok(count) => {
                    budget = budget.saturating_sub(count);
                    self.session.feed(&buffer[..count]);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    match tokio::time::timeout_at(deadline, self.worker.channel.readable()).await {
                        Ok(Ok(())) => {}
                        _ => return,
                    }
                }
                Err(_) => return,
            }
            if tokio::time::Instant::now() >= deadline {
                return;
            }
        }
    }

    /// Record the host's goodbye as the close cause — before the
    /// settlements that follow can claim it — and queue its frame.
    fn begin_goodbye(&mut self, reason: &str) {
        self.shared.set_closed(format!("host goodbye: {reason}"));
        // A goodbye the peer cannot read is not queued: appending to a
        // dead output direction would repopulate the buffer the
        // half-close released and report occupancy nothing can drain.
        if !self.output_open {
            return;
        }
        let goodbye = HostMessage::Goodbye(Goodbye {
            reason: reason.to_owned(),
        });
        if let Ok(bytes) = serde_json::to_vec(&goodbye) {
            self.outbuf.extend_from_slice(&frame::encode(&bytes));
            self.shared
                .record_outbound(self.outbuf.len(), self.outbuf.capacity());
        }
    }

    /// Write the queued bytes until empty or the transport is gone. Callers
    /// bound it: this must never be the reason shutdown hangs.
    async fn flush_outbuf(&mut self) {
        while !self.outbuf.is_empty() {
            if self.worker.channel.writable().await.is_err() {
                return;
            }
            match self.worker.channel.try_write(&self.outbuf) {
                Ok(0) => return,
                Ok(count) => {
                    self.outbuf.drain(..count);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(_) => return,
            }
        }
    }

    /// Grace, kill, reap — in that order, unconditionally ending in `wait`.
    /// The kill signals the worker's whole process group, so helpers the
    /// worker spawned die with it instead of orphaning with the host's
    /// ambient authority.
    async fn reap(&mut self) {
        let grace = Duration::from_millis(self.limits.kill_grace_ms);
        let pgid = self.worker.pgid;
        let graced = tokio::time::timeout(grace, self.worker.child.wait()).await;
        if pgid > 0 {
            // SAFETY: kill(2) with a negative pid signals the process
            // group; no memory is touched. On the forced path the leader
            // is still unreaped here, so the group id cannot have been
            // recycled: this hits exactly the worker's group. After a
            // voluntary exit the id stays reserved while any group member
            // lives, so the sweep is precise whenever it has work to do;
            // the remaining race — an empty group's id recycled within
            // microseconds — is accepted.
            unsafe {
                libc::kill(-pgid, libc::SIGKILL);
            }
        }
        if graced.is_err() {
            if pgid > 0 {
                // The group sweep misses a worker that moved itself out of
                // its group, and the wait below is unbounded; the leader is
                // provably unreaped here (its pid still reserved), so it is
                // also signalled directly.
                unsafe {
                    libc::kill(pgid, libc::SIGKILL);
                }
            }
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

/// Retain one diagnostic stream's remaining text at pump exit, to
/// end-of-file or a bound. Runs after the group sweep, so the pipe's
/// writers are gone and end-of-file is prompt; the wall-clock bound covers
/// the paths where something unexpected still holds the write end.
async fn drain_diagnostics<R: tokio::io::AsyncRead + Unpin>(
    shared: &PumpShared,
    stream: DiagnosticStream,
    source: &mut Option<R>,
    buffer: &mut [u8],
) {
    let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
    let mut budget: usize = 256 * 1024;
    let Some(reader) = source.as_mut() else {
        return;
    };
    while budget > 0 {
        match tokio::time::timeout_at(deadline, reader.read(buffer)).await {
            Ok(Ok(0)) => return,
            Ok(Ok(count)) => {
                budget = budget.saturating_sub(count);
                shared.append_diagnostics(stream, &buffer[..count]);
            }
            _ => return,
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
