//! The pump's shared snapshot and command vocabulary.
//!
//! Everything here is the stable face other modules hold: the typed
//! command channel the endpoint and dispatcher speak, the loss
//! classification a settled call reports, and the read-only memory and
//! lifecycle snapshot health, gauges, and diagnostics read. The pump task
//! owns the only mutable authority behind it; this module is what outlives
//! that ownership decision — clones hold [`PumpShared`] without keeping
//! the pump alive.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::{
    Mutex, MutexGuard,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};

use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::sync::{Notify, mpsc, oneshot};
use yah_compose::ScopeCancellation;
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::*;

use crate::ProcLimits;
use crate::endpoint::StreamFrame;

/// How a host-initiated endpoint call ended.
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
        /// on work whose outcome the host never learned. `true` requires
        /// reconciliation; `false` covers worker goodbye or a frame proven
        /// never delivered.
        reconcile: bool,
    },
}

pub(crate) enum PumpCommand {
    Call {
        method: String,
        payload: serde_json::Value,
        deadline_ms: Option<u32>,
        /// Stream call: the worker answers with a stream-open ack and
        /// streams framed items toward the host through `inbound`.
        stream: bool,
        /// Where the worker's streamed items are delivered. Bounded to
        /// the negotiated credit ceiling, so a slow consumer throttles
        /// the worker through the credit window instead of growing host
        /// memory. Dropping the receiver mutes delivery and cancels the
        /// stream half; the terminal is still owed.
        inbound: Option<mpsc::Sender<StreamFrame>>,
        /// The consumer-visible mirror of host-local stream drops; the
        /// pump increments it as drops happen so the count outlives the
        /// call's delivery table.
        drops: Arc<std::sync::atomic::AtomicU64>,
        opened: oneshot::Sender<Result<CallId, AppError>>,
        settled: oneshot::Sender<CallEnd>,
    },
    Cancel {
        call_id: CallId,
        target: CancelTarget,
        done: oneshot::Sender<Result<(), AppError>>,
    },
    /// Apply an application-authored outcome as a worker call's terminal.
    Reply {
        call_id: CallId,
        outcome: Outcome,
        done: oneshot::Sender<Result<(), AppError>>,
    },
    /// The application's result was over the inline bound: spill the
    /// bytes through the session (which mints the offer and pins them
    /// host-side) and answer the call with it. A spill the session
    /// refuses is answered a bounded resource refusal instead — the
    /// call still gets its exactly-one terminal either way.
    SpillReply {
        call_id: CallId,
        bytes: Vec<u8>,
        done: oneshot::Sender<Result<(), AppError>>,
    },
    /// Mint a capability handle and mirror its activation-local authority in
    /// the pump-owned table only when the session mint succeeds.
    MintCapability {
        call_id: CallId,
        capability: crate::dispatch::DispatchedTextCapability,
        done: oneshot::Sender<Result<HandleId, AppError>>,
    },
    /// Ask the worker to release a handle it offered (a spilled artifact)
    /// and wait for its acknowledgement. An admission refusal answers at
    /// once; a successful admission does not complete the waiter — the
    /// pump retains it until the worker's ack names the handle, or the
    /// activation ends trying.
    ReleaseWorkerHandle {
        handle: HandleId,
        done: oneshot::Sender<Result<ReleaseEnd, AppError>>,
    },
    // Shutdown deliberately is not a command: commands queue, and a
    // deactivation dropped behind a command flood would wait out an
    // arbitrary backlog. It rides a watch signal instead — one value, no
    // queue, always deliverable.
}

/// How an endpoint-initiated release of a worker-held handle ended. The
/// acknowledgement is what makes "released" a two-party fact, so success
/// is reported only when the worker's ack names the handle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReleaseEnd {
    /// The worker acknowledged the release.
    Acknowledged,
    /// The activation ended before the ack arrived: goodbye, fatal
    /// fault, disconnect, an output direction that can no longer carry
    /// the release, or teardown. `orderly` marks ends where treating the
    /// handle as released is safe — the worker announced its own stop,
    /// the host said goodbye, or the frame provably never reached it —
    /// while `false` means a bare disconnect or fatal fault where the
    /// worker's table state is unknown.
    Lost { orderly: bool },
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
    /// Endpoint-initiated worker-handle releases awaiting the worker's
    /// acknowledgement.
    pub pending_releases: usize,
    /// Free slots in the bounded command channel right now.
    pub command_channel_available: usize,
    /// The channel's total configured slots.
    pub command_channel_capacity: usize,
}

/// Authority-free counts for the two sides of process capability bookkeeping.
/// The terminal snapshot remains observable after the pump and its tables drop.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CapabilityHandleGauges {
    /// Live host-session handles of every kind, including artifacts.
    pub session_live_handles: usize,
    /// Text capability entries in the process activation's private table.
    pub process_capability_entries: usize,
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
    /// Serializes the final endpoint gate check and bounded command enqueue
    /// with explicit deactivation withdrawal.
    admission: Mutex<()>,
    /// Deactivation has begun. Written while `admission` is held, before the
    /// shutdown watch signal moves.
    closing: AtomicBool,
    /// Scope cancellation fires before the activity drain can reach driver
    /// cleanup. Endpoint admission checks it at the final admission point.
    scope_cancellation: Option<ScopeCancellation>,
    /// Memory gauges, updated by the pump task at every buffer change.
    outbound_bytes: AtomicUsize,
    outbound_capacity: AtomicUsize,
    pending_calls: AtomicUsize,
    pending_releases: AtomicUsize,
    session_live_handles: AtomicUsize,
    process_capability_entries: AtomicUsize,
    command_channel_capacity: usize,
    /// The item-channel capacity for stream calls: the negotiated credit
    /// ceiling. Credit never exceeds it, so lossless items can always be
    /// delivered without unbounded host memory.
    stream_channel_capacity: usize,
}

#[derive(Default)]
struct Diagnostics {
    stdout: VecDeque<u8>,
    stderr: VecDeque<u8>,
}

/// Held across an endpoint's final state check and nonblocking enqueue.
pub(crate) struct AdmissionGuard<'a> {
    shared: &'a PumpShared,
    _held: MutexGuard<'a, ()>,
}

impl AdmissionGuard<'_> {
    pub(crate) fn close(&self) {
        self.shared.closing.store(true, Ordering::Release);
    }
}

impl PumpShared {
    pub(crate) fn new(
        limits: &ProcLimits,
        worker_pid: i32,
        max_stream_credit: u32,
        scope_cancellation: Option<ScopeCancellation>,
    ) -> Self {
        Self {
            phase: AtomicU8::new(PHASE_HANDSHAKE),
            close_summary: Mutex::new(None),
            changed: Notify::new(),
            diagnostics: Mutex::new(Diagnostics::default()),
            diagnostics_cap: limits.diagnostics_cap_bytes,
            worker_pid,
            output_closed: AtomicBool::new(false),
            admission: Mutex::new(()),
            closing: AtomicBool::new(false),
            scope_cancellation,
            outbound_bytes: AtomicUsize::new(0),
            outbound_capacity: AtomicUsize::new(0),
            pending_calls: AtomicUsize::new(0),
            pending_releases: AtomicUsize::new(0),
            session_live_handles: AtomicUsize::new(0),
            process_capability_entries: AtomicUsize::new(0),
            command_channel_capacity: limits.command_channel_capacity,
            stream_channel_capacity: max_stream_credit as usize,
        }
    }

    pub fn worker_pid(&self) -> i32 {
        self.worker_pid
    }

    pub fn output_closed(&self) -> bool {
        self.output_closed.load(Ordering::Acquire)
    }

    pub(crate) fn admission_guard(&self) -> AdmissionGuard<'_> {
        AdmissionGuard {
            shared: self,
            _held: self
                .admission
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        }
    }

    pub fn set_closing(&self) {
        self.admission_guard().close();
    }

    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
            || self
                .scope_cancellation
                .as_ref()
                .is_some_and(ScopeCancellation::is_cancelled)
    }

    pub fn stream_channel_capacity(&self) -> usize {
        self.stream_channel_capacity
    }

    pub(crate) fn set_output_closed(&self) {
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

    pub(crate) fn set_active(&self) {
        self.phase.store(PHASE_ACTIVE, Ordering::Release);
        self.changed.notify_waiters();
    }

    pub(crate) fn set_closed(&self, summary: String) {
        let admission = self.admission_guard();
        admission.close();
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

    pub(crate) fn append_diagnostics(&self, stream: DiagnosticStream, bytes: &[u8]) {
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

    pub(crate) fn record_outbound(&self, bytes: usize, capacity: usize) {
        self.outbound_bytes.store(bytes, Ordering::Release);
        self.outbound_capacity.store(capacity, Ordering::Release);
    }

    pub(crate) fn record_pending(&self, count: usize) {
        self.pending_calls.store(count, Ordering::Release);
    }

    pub(crate) fn record_pending_releases(&self, count: usize) {
        self.pending_releases.store(count, Ordering::Release);
    }

    pub(crate) fn record_capability_handles(&self, session: usize, process: usize) {
        self.session_live_handles.store(session, Ordering::Release);
        self.process_capability_entries
            .store(process, Ordering::Release);
    }

    pub fn capability_handle_gauges(&self) -> CapabilityHandleGauges {
        CapabilityHandleGauges {
            session_live_handles: self.session_live_handles.load(Ordering::Acquire),
            process_capability_entries: self.process_capability_entries.load(Ordering::Acquire),
        }
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
            pending_releases: self.pending_releases.load(Ordering::Acquire),
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

pub(crate) async fn drain_diagnostics<R: tokio::io::AsyncRead + Unpin>(
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

pub(crate) fn kind_label(kind: WireErrorKind) -> &'static str {
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
