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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot, watch};
use yah_compose::ScopeCancellation;
use yah_plugin_ipc::session::{AppError, HostSession, SessionEvent};
use yah_plugin_ipc::types::*;
use yah_plugin_ipc::{frame, session::SessionConfig};

use crate::ProcLimits;
use crate::bootstrap::SpawnedWorker;
use crate::dispatch::{self, DispatchRequest, WorkerMethodCancellation, WorkerMethodRegistry};
use crate::endpoint::StreamFrame;
use crate::shared::{
    CallEnd, DiagnosticStream, PumpCommand, PumpShared, ReleaseEnd, drain_diagnostics, kind_label,
};

/// One live stream call's delivery side: where items go and how much
/// credit the host has outstanding toward the worker.
///
/// The conservation law this table maintains, per stream: **frames queued
/// in the delivery channel plus outstanding lossless credit never exceeds
/// the channel's bounded capacity.** Grants replace only frames the
/// consumer has provably drained, lossy frames are admitted only into
/// unreserved capacity, and so a lossless frame within its granted credit
/// always finds a slot on arrival — host-local drops of credited lossless
/// frames are impossible by construction, and every host-local lossy drop
/// is counted where the consumer can see it even after the terminal.
struct HostStream {
    inbound: mpsc::Sender<StreamFrame>,
    /// The delivery channel's total capacity, cached: the right side of
    /// the conservation law.
    max_capacity: u32,
    /// The worker's stream-open acknowledgement has arrived, so the
    /// credit window exists and grants are meaningful. Before it, the
    /// window is simply not open yet — not an error, and never a reason
    /// to mute the call.
    opened: bool,
    /// Lossless items granted but not yet received. With the queued
    /// count, never above [`Self::max_capacity`].
    outstanding_credit: u32,
    /// Frames successfully pushed into `inbound`, cumulative across both
    /// classes. The difference against the current queue depth is what
    /// the consumer has actually drained — the only quantity grants may
    /// replace.
    accepted: u64,
    /// Replacement credits already granted for drained frames, so no
    /// drained frame funds a grant twice.
    replaced: u64,
    /// Host-side drops under the reservation policy, declared to the
    /// consumer in every later frame's `dropped` count *and* mirrored
    /// into the shared counter the terminal reports — a drop followed
    /// immediately by the terminal is still visible.
    local_drops: u64,
    /// The consumer-facing mirror of `local_drops`. Lives as long as the
    /// call's terminal receiver, outlasting this table entry.
    drops: Arc<std::sync::atomic::AtomicU64>,
}

mod streams;

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
///
/// The worker-to-host application dispatcher is built before the pump task
/// starts, so no worker call can race its registration table into existence.
pub(crate) fn start(
    worker: SpawnedWorker,
    config: SessionConfig,
    scope_cancellation: ScopeCancellation,
    methods: WorkerMethodRegistry,
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
    let max_stream_credit = config.ceilings.max_stream_credit;
    let shared = Arc::new(PumpShared::new(
        &limits,
        worker.pgid,
        max_stream_credit,
        Some(scope_cancellation.clone()),
    ));
    let (commands, receiver) = mpsc::channel(limits.command_channel_capacity);
    let (shutdown, shutdown_rx) = watch::channel(None);
    // The dispatcher is created before the pump task spawns, holding the
    // command channel only weakly: it can never keep an abandoned
    // activation's pump alive.
    let dispatcher = dispatch::spawn(
        scope_cancellation.clone(),
        commands.downgrade(),
        limits.dispatch_queue_capacity,
        limits.provider_concurrency,
        methods,
    );
    let pump = Pump {
        session: HostSession::new(config),
        worker,
        commands: receiver,
        shutdown: shutdown_rx,
        shutdown_seen: false,
        shared: Arc::clone(&shared),
        pending: HashMap::new(),
        streams: HashMap::new(),
        dispatcher: Some(dispatcher),
        worker_cancellations: HashMap::new(),
        pending_releases: HashMap::new(),
        outbuf: Vec::new(),
        output_open: true,
        close_orderly: false,
        max_stream_credit,
        scope_cancellation: Some(scope_cancellation),
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
    /// Delivery channels for this activation's stream calls, keyed by
    /// call id. Entries die with their call's terminal.
    streams: HashMap<CallId, HostStream>,
    /// Endpoint-initiated worker-handle releases admitted but not yet
    /// acknowledged, keyed by the worker-held handle. A waiter here has
    /// been told nothing yet: success is the worker ack's to give.
    pending_releases: HashMap<HandleId, oneshot::Sender<Result<ReleaseEnd, AppError>>>,
    /// The bounded lane admitted worker calls are routed into. `None`
    /// keeps the historical unknown-method auto-refusal.
    dispatcher: Option<mpsc::Sender<DispatchRequest>>,
    /// Cooperative cancellation views for worker calls still owned by the
    /// application lane. The callback receives a clone, never this table.
    worker_cancellations: HashMap<CallId, WorkerMethodCancellation>,
    /// Encoded frames awaiting the socket. Progress on it is a `select!`
    /// arm, so transport back-pressure never stalls the rest of the pump.
    outbuf: Vec<u8>,
    /// The session's own lossless-credit ceiling, mirrored for the
    /// auto-credit policy below.
    max_stream_credit: u32,
    /// False once a write failed: the peer shut its read half. That kills
    /// only the outbound direction — the peer's own goodbye or terminals
    /// may still be in flight toward the host, so input runs to its
    /// natural end and undeliverable output is discarded.
    output_open: bool,
    /// How the close that ended this pump should classify itself for
    /// waiters still holding out for an ack: `true` when the host said
    /// goodbye or the worker announced its own stop, `false` for a bare
    /// disconnect or fatal fault. Set by whichever arm learns first.
    close_orderly: bool,
    /// The scope fence fires before the deferred driver cleanup can poll.
    scope_cancellation: Option<ScopeCancellation>,
    limits: ProcLimits,
}

impl Pump {
    async fn run(mut self) {
        let scope_cancellation = self.scope_cancellation.clone();
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
            self.retire_dropped_waiters();
            // One gauge update per iteration covers every mutation of the
            // pending table: admissions in the command arm and settlements
            // in `apply_events` alike.
            self.shared.record_pending(self.pending.len());
            // One gauge update per iteration covers every mutation of the
            // release-waiter table as well.
            self.shared
                .record_pending_releases(self.pending_releases.len());
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
                    self.regrant_stream_credit();
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
                    Some(PumpCommand::Call { method, payload, deadline_ms, stream, inbound, drops, opened, settled }) => {
                        match self.session.call_worker(&method, payload, deadline_ms, stream) {
                            Ok(call_id) => {
                                if let Some(inbound) = inbound {
                                    let max_capacity = inbound.max_capacity() as u32;
                                    self.streams.insert(
                                        call_id,
                                        HostStream {
                                            inbound,
                                            max_capacity,
                                            opened: false,
                                            outstanding_credit: 0,
                                            accepted: 0,
                                            replaced: 0,
                                            local_drops: 0,
                                            drops,
                                        },
                                    );
                                }
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
                    Some(PumpCommand::Reply { call_id, outcome, done }) => {
                        let result = self.session.reply_to_worker(call_id, outcome);
                        if matches!(
                            &result,
                            Ok(()) | Err(AppError::UnknownCall | AppError::AlreadySettled)
                        ) {
                            self.worker_cancellations.remove(&call_id);
                        }
                        let _ = done.send(result);
                    }
                    Some(PumpCommand::SpillReply { call_id, bytes, done }) => {
                        let spilled = self.session.offer_artifact(
                            call_id,
                            bytes,
                            "application/json",
                        );
                        let result = match spilled {
                            Ok(offer) => self
                                .session
                                .reply_to_worker(call_id, Outcome::Spilled { artifact: offer }),
                            // The budget or handle ceiling refused the
                            // spill; the call still owes its terminal.
                            Err(error) => self.session.reply_to_worker(
                                call_id,
                                Outcome::Err {
                                    error: WireError {
                                        kind: WireErrorKind::ResourceExhausted,
                                        message: match error {
                                            AppError::SessionRetired => {
                                                "the activation's correlation budget is spent"
                                                    .to_owned()
                                            }
                                            AppError::HandleCeiling => {
                                                "the activation's live-handle ceiling is exhausted"
                                                    .to_owned()
                                            }
                                            AppError::UnknownCall => {
                                                "the call ended before the result could be answered"
                                                    .to_owned()
                                            }
                                            _ => "the result could not be answered".to_owned(),
                                        },
                                        retryable: false,
                                        reconcile_required: false,
                                    },
                                },
                            ),
                        };
                        if matches!(
                            &result,
                            Ok(()) | Err(AppError::UnknownCall | AppError::AlreadySettled)
                        ) {
                            self.worker_cancellations.remove(&call_id);
                        }
                        let _ = done.send(result);
                    }
                    Some(PumpCommand::ReleaseWorkerHandle { handle, done }) => {
                        // The only handle the host asks a worker to release
                        // is one the worker offered: a spilled artifact.
                        // Admission refusals answer at once; admission is
                        // not acknowledgement, so a successful queueing
                        // retains the waiter for the worker's ack — see
                        // `apply_events`.
                        match self.session.release_worker_handle(handle, HandleKind::Artifact) {
                            Ok(()) => {
                                self.pending_releases.insert(handle, done);
                            }
                            Err(error) => {
                                let _ = done.send(Err(error));
                            }
                        }
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
                // This arrives before the scope's activity drain lets the
                // deferred deactivation cleanup poll. A stuck trusted
                // callback can still delay scope completion, never the
                // worker's bounded shutdown and reap.
                _ = scope_cancelled(&scope_cancellation), if !self.shutdown_seen => {
                    self.shutdown_seen = true;
                    self.shared.set_closing();
                    self.begin_goodbye("activation scope cancelled");
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
        self.worker_cancellations.clear();
        // Nothing after this can acknowledge: every remaining release
        // waiter settles here rather than dying with the task.
        self.settle_pending_releases(self.close_orderly);
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
        let events = self.session.drain_events();
        // A fatal closes calls conservatively as outcome-unknown before its
        // Fatal event names the protocol cause. Preserve both facts at this
        // embedding boundary: reconciliation remains required, while the
        // endpoint can distinguish protocol poison from transport loss.
        let fatal_kind = events.iter().find_map(|event| match event {
            SessionEvent::Fatal { kind, .. } => Some(*kind),
            _ => None,
        });
        for event in events {
            match event {
                SessionEvent::Negotiated { .. } => self.shared.set_active(),
                SessionEvent::HandleReleased { .. } | SessionEvent::HandlesReclaimed { .. } => {}
                SessionEvent::WorkerHandleReleased { handle, .. } => {
                    // The ack is the only thing that turns an admitted
                    // release into reported success.
                    if let Some(waiter) = self.pending_releases.remove(&handle) {
                        let _ = waiter.send(Ok(ReleaseEnd::Acknowledged));
                    }
                }
                SessionEvent::HostCallSettled { call_id, outcome } => {
                    self.streams.remove(&call_id);
                    if let Some(waiter) = self.pending.remove(&call_id) {
                        let _ = waiter.send(CallEnd::Settled(outcome));
                    }
                }
                SessionEvent::HostCallLost {
                    call_id,
                    mut error,
                    reconcile,
                } => {
                    if error == WireErrorKind::OutcomeUnknown
                        && let Some(kind) = fatal_kind
                    {
                        error = kind;
                    }
                    self.streams.remove(&call_id);
                    if let Some(waiter) = self.pending.remove(&call_id) {
                        let _ = waiter.send(CallEnd::Lost { error, reconcile });
                    }
                }
                SessionEvent::StreamOpened { call_id, credit } => {
                    if let Some(stream) = self.streams.get_mut(&call_id) {
                        stream.opened = true;
                        stream.outstanding_credit = credit;
                    }
                }
                SessionEvent::StreamItem {
                    call_id,
                    seq,
                    more,
                    class,
                    dropped,
                    payload,
                } => {
                    self.deliver_stream_item(
                        call_id,
                        StreamFrame {
                            seq,
                            more,
                            class,
                            dropped,
                            payload,
                        },
                    );
                }
                SessionEvent::CallDelivered {
                    call_id,
                    method,
                    payload,
                    ..
                } => {
                    self.route_worker_call(call_id, &method, payload);
                }
                SessionEvent::CancelRequested { call_id, .. } => {
                    if let Some(cancellation) = self.worker_cancellations.remove(&call_id) {
                        cancellation.request();
                        // Cancellation retires the protocol path now. A
                        // synchronous callback may keep running, but its late
                        // reply is an already-settled race, never a second
                        // terminal and never a reason to keep the worker.
                        let _ = self.session.reply_to_worker(
                            call_id,
                            Outcome::Cancelled {
                                reason: CancelReason::Requested,
                            },
                        );
                    }
                }
                SessionEvent::Fatal { kind, detail } => {
                    self.close_orderly = false;
                    self.settle_pending_releases(false);
                    self.shared
                        .set_closed(format!("protocol fault {}: {detail}", kind_label(kind)));
                }
                SessionEvent::WorkerGoodbye { reason } => {
                    self.close_orderly = true;
                    self.settle_pending_releases(true);
                    self.shared.set_closed(format!("worker goodbye: {reason}"));
                }
                _ if self.session.is_closed() && !self.pending_releases.is_empty() => {
                    // A close no event above named — a bare disconnect,
                    // most likely. Nothing will acknowledge now.
                    self.close_orderly = false;
                    self.settle_pending_releases(false);
                }
                _ => {}
            }
        }
    }

    /// Settle every outstanding release waiter as lost — no ack will
    /// ever arrive. `orderly` records whether the end was announced
    /// (goodbye either way, or a release provably never written) or bare.
    fn settle_pending_releases(&mut self, orderly: bool) {
        for (_, waiter) in self.pending_releases.drain() {
            let _ = waiter.send(Ok(ReleaseEnd::Lost { orderly }));
        }
    }

    /// A caller may drop its terminal receiver at any point. Retire that
    /// local waiter immediately and cancel the worker call best-effort; the
    /// session still validates any racing terminal, but the pump retains no
    /// unreachable sender and no stream delivery channel for it.
    fn retire_dropped_waiters(&mut self) {
        let dropped: Vec<CallId> = self
            .pending
            .iter()
            .filter_map(|(call_id, waiter)| waiter.is_closed().then_some(*call_id))
            .collect();
        for call_id in dropped {
            self.pending.remove(&call_id);
            self.streams.remove(&call_id);
            let _ = self.session.cancel(call_id, CancelTarget::Call);
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
                match message {
                    HostMessage::Call(call) => {
                        if let Some(waiter) = self.pending.remove(&call.call_id) {
                            let _ = waiter.send(CallEnd::Lost {
                                error: WireErrorKind::Cancelled,
                                reconcile: false,
                            });
                        }
                    }
                    // A release that can never be written can never be
                    // acknowledged: the waiter learns now, not at pump
                    // exit.
                    HostMessage::Release(release) => {
                        if let Some(waiter) = self.pending_releases.remove(&release.handle) {
                            let _ = waiter.send(Ok(ReleaseEnd::Lost { orderly: true }));
                        }
                    }
                    _ => {}
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
        self.close_orderly = true;
        self.settle_pending_releases(true);
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

async fn scope_cancelled(cancellation: &Option<ScopeCancellation>) {
    match cancellation {
        Some(cancellation) => cancellation.cancelled().await,
        None => std::future::pending().await,
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
