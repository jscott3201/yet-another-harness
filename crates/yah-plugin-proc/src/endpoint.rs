//! The production invocation surface for one supervised process activation.
//!
//! This is the authority seam the test-facing [`ProcObserver`] hooks were
//! holding open: an activation-scoped, weakly-owned endpoint published only
//! after protocol negotiation and withdrawn before teardown. Clones hold a
//! weak command sender and a shared snapshot — never a strong pump handle —
//! so an abandoned activation still self-reaps with endpoints alive.
//!
//! Errors are typed, never parsed from prose. Worker loss keeps the
//! landed conservative classification: a goodbye (or a frame provably
//! never written) is `LostCancelled` — safe to retry — while a bare
//! disconnect or fatal is `LostOutcomeUnknown`: the worker may have
//! acted, and reconciliation belongs to the caller, not to this crate.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use yah_plugin_host::PluginActivationId;
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::{
    ArtifactOffer, CallId, CancelTarget, HandleId, Outcome, StreamClass, WireErrorKind,
};

use crate::shared::{PumpCommand, PumpShared};

/// How much of one streamed result frame the consumer sees at once.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamFrame {
    pub seq: u64,
    pub more: bool,
    pub class: StreamClass,
    /// Cumulative lossy frames dropped before this one — by the worker
    /// on the wire, or by this host under channel pressure. Declared,
    /// never inferred.
    pub dropped: u64,
    pub payload: serde_json::Value,
}

/// Why an operation could not even be admitted.
///
/// Discriminants are the recovery decision. Everything here is local to
/// the host process except [`EndpointError::Closed`], which carries the
/// retained first-cause summary of why the worker ended.
#[derive(Clone, Debug, PartialEq)]
pub enum EndpointError {
    /// The activation was prepared but never started, or failed to start.
    NotStarted,
    /// The worker has not completed hello/accept yet. The endpoint does
    /// not exist before negotiation; nothing was admitted.
    NotNegotiated,
    /// Deactivation has begun. Fresh admissions fail closed; already
    /// admitted work settles exactly once through the normal paths.
    Closing,
    /// The activation's pump has ended. The summary is the first cause
    /// recorded for the close, retained after the fact for diagnosis.
    Closed { summary: Option<String> },
    /// The bounded command channel is full. Nothing was admitted: no id
    /// minted, no frame queued, no waiter created. Retry after
    /// backoff against the same activation.
    AtCapacity,
    /// The session's retired-correlation budget is exhausted. Not
    /// retryable against this activation; retire it and start fresh.
    SessionRetired,
    /// The session refused the operation on protocol or resource grounds.
    Refused(Refusal),
    /// A call inside a composite operation ended without an answer to
    /// consume — deadline, cancellation, or loss. The inner terminal is
    /// carried whole: its loss kind says whether reconciliation is owed.
    Unsettled(CallTerminal),
}

impl From<AppError> for EndpointError {
    fn from(error: AppError) -> Self {
        match error {
            AppError::SessionRetired => EndpointError::SessionRetired,
            other => EndpointError::Refused(Refusal::from(other)),
        }
    }
}

/// The session-level refusal family, named exactly as the session names
/// them. These are caller bugs or exhausted bounds, not transient
/// pressure.
#[derive(Clone, Debug, PartialEq)]
pub enum Refusal {
    NotActive,
    UnknownCall,
    CallCeiling,
    HandleCeiling,
    PayloadTooLarge {
        bytes: usize,
    },
    InvalidField(&'static str),
    ReleasePending,
    AlreadyReleased,
    UnknownWorkerHandle,
    StreamViolation(&'static str),
    AlreadySettled,
    /// The worker answered a pull with a protocol-level err outcome. The
    /// kind is data; the worker's own text does not cross.
    WorkerRefused {
        kind: WireErrorKind,
    },
}

impl From<AppError> for Refusal {
    fn from(error: AppError) -> Self {
        match error {
            AppError::NotActive => Refusal::NotActive,
            AppError::UnknownCall => Refusal::UnknownCall,
            AppError::CallCeiling => Refusal::CallCeiling,
            AppError::HandleCeiling => Refusal::HandleCeiling,
            AppError::PayloadTooLarge { bytes } => Refusal::PayloadTooLarge { bytes },
            AppError::InvalidField(name) => Refusal::InvalidField(name),
            AppError::ReleasePending => Refusal::ReleasePending,
            AppError::AlreadyReleased => Refusal::AlreadyReleased,
            AppError::UnknownWorkerHandle => Refusal::UnknownWorkerHandle,
            AppError::StreamViolation(name) => Refusal::StreamViolation(name),
            AppError::AlreadySettled => Refusal::AlreadySettled,
            // Unreachable by construction of this mapping: SessionRetired
            // is matched by the caller. Kept whole-set so a new variant
            // breaks the build instead of silently degrading.
            AppError::SessionRetired => Refusal::InvalidField("session retired"),
            AppError::SpillRequired { bytes } => Refusal::PayloadTooLarge { bytes },
        }
    }
}

/// How a call ended. Exactly one terminal per call — the protocol's
/// exactly-once law, surfaced as an enum rather than a boolean the
/// caller can ignore.
#[derive(Clone, Debug, PartialEq)]
pub enum CallTerminal {
    /// The worker answered: ok, spilled, err, or cancelled. A domain
    /// failure rides inside `Outcome::Err` as data, exactly as in the
    /// Wasm lane.
    Completed(Outcome),
    /// The host's deadline clock expired locally. The worker may still
    /// have acted: reconcile external state before retrying.
    DeadlineExceeded,
    /// Orderly loss — the worker said goodbye, or the frame provably
    /// never reached it. Safe to retry on a live activation.
    LostCancelled,
    /// Bare disconnect or fatal fault mid-call. The worker may have
    /// acted: reconcile external state first.
    LostOutcomeUnknown,
}

impl From<crate::CallEnd> for CallTerminal {
    fn from(end: crate::CallEnd) -> Self {
        match end {
            crate::CallEnd::Settled(outcome) => CallTerminal::Completed(outcome),
            crate::CallEnd::Lost { error, reconcile } => match (error, reconcile) {
                (WireErrorKind::DeadlineExceeded, _) => CallTerminal::DeadlineExceeded,
                (WireErrorKind::Cancelled, false) => CallTerminal::LostCancelled,
                (_, true) => CallTerminal::LostOutcomeUnknown,
                // Conservative residue: any other loss kind reconciles.
                (_, false) => CallTerminal::LostOutcomeUnknown,
            },
        }
    }
}

/// Whether the endpoint exists, and if so whether it still admits work.
#[derive(Clone, Debug, PartialEq)]
pub enum Availability {
    Negotiating,
    Active,
    Closing,
    Closed { summary: Option<String> },
}

/// One admitted host-to-worker call awaiting its single terminal.
pub struct PendingCall {
    call_id: CallId,
    settled: oneshot::Receiver<crate::CallEnd>,
    /// The activation's snapshot at admission time, so a waiter whose
    /// pump died before settlement reports the retained first cause
    /// rather than an anonymous closed.
    shared: Arc<PumpShared>,
}

impl std::fmt::Debug for PendingCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingCall")
            .field("call_id", &self.call_id)
            .finish_non_exhaustive()
    }
}

impl PendingCall {
    pub fn call_id(&self) -> CallId {
        self.call_id
    }

    /// Wait for the call's exactly-one terminal.
    pub async fn terminal(self) -> Result<CallTerminal, EndpointError> {
        let end = self.settled.await.map_err(|_| EndpointError::Closed {
            summary: self.shared.close_summary(),
        })?;
        Ok(CallTerminal::from(end))
    }
}

/// A stream call: the worker streams framed items toward the host while
/// the call runs, and exactly one terminal still ends the call.
///
/// Items arrive through a bounded channel sized to the negotiated stream
/// credit ceiling, so a slow consumer throttles the worker through the
/// credit window instead of growing host memory. Dropping the
/// [`StreamCall`] mutes delivery (a best-effort cancel of the stream
/// half); the terminal still lands wherever its receiver lives.
pub struct StreamCall {
    pending: PendingCall,
    items: mpsc::Receiver<StreamFrame>,
}

impl StreamCall {
    pub fn call_id(&self) -> CallId {
        self.pending.call_id()
    }

    /// Receive the next streamed item. `None` after the terminal — the
    /// item channel closes when the call ends, whatever ended it.
    pub async fn next_item(&mut self) -> Option<StreamFrame> {
        self.items.recv().await
    }

    /// Wait for the call's exactly-one terminal.
    pub async fn terminal(self) -> Result<CallTerminal, EndpointError> {
        self.pending.terminal().await
    }

    /// Drop the item channel and keep the terminal. Dropping the receiver
    /// mutes delivery — the pump cancels the stream half and discards
    /// further items — but the call still ends with its one terminal.
    pub fn into_pending(self) -> PendingCall {
        self.pending
    }
}

/// The invocation face of one exact activation.
///
/// Clone freely: clones share the same weak link, so none of them can
/// keep an abandoned activation alive, and all of them fail closed when
/// the activation ends.
#[derive(Clone)]
pub struct ActivationEndpoint {
    // Manual Debug: the shared snapshot is not printable, and its
    // internals are nobody's business but the pump's.
    id: PluginActivationId,
    commands: mpsc::WeakSender<PumpCommand>,
    shared: Arc<PumpShared>,
}

pub(crate) fn activation_endpoint(
    id: PluginActivationId,
    commands: mpsc::WeakSender<PumpCommand>,
    shared: Arc<PumpShared>,
) -> ActivationEndpoint {
    ActivationEndpoint {
        id,
        commands,
        shared,
    }
}

impl std::fmt::Debug for ActivationEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivationEndpoint")
            .field("activation_id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Which admission gate refused, resolved against the shared snapshot.
pub(crate) fn admission_gate(shared: &PumpShared) -> Result<(), EndpointError> {
    if shared.is_closed() {
        return Err(EndpointError::Closed {
            summary: shared.close_summary(),
        });
    }
    if shared.is_closing() {
        return Err(EndpointError::Closing);
    }
    if !shared.is_negotiated() {
        return Err(EndpointError::NotNegotiated);
    }
    Ok(())
}

impl ActivationEndpoint {
    /// The exact activation this endpoint is bound to.
    pub fn activation_id(&self) -> &PluginActivationId {
        &self.id
    }

    /// Current availability, derived from the shared snapshot.
    pub fn availability(&self) -> Availability {
        if self.shared.is_closed() {
            return Availability::Closed {
                summary: self.shared.close_summary(),
            };
        }
        if self.shared.is_closing() {
            return Availability::Closing;
        }
        if !self.shared.is_negotiated() {
            return Availability::Negotiating;
        }
        Availability::Active
    }

    fn link(&self) -> Result<mpsc::Sender<PumpCommand>, EndpointError> {
        admission_gate(&self.shared)?;
        self.commands
            .upgrade()
            .ok_or_else(|| EndpointError::Closed {
                summary: self.shared.close_summary(),
            })
    }

    /// Admit one unary call. Bounds are checked before a command slot is
    /// occupied: an oversized payload is refused without touching the
    /// queue, so a bounded slot count never hides an unbounded body.
    ///
    /// Set `stream` to receive the worker's framed items through
    /// [`ActivationEndpoint::call_stream`] instead.
    pub async fn call(
        &self,
        method: &str,
        payload: serde_json::Value,
        deadline_ms: Option<u32>,
    ) -> Result<PendingCall, EndpointError> {
        self.admit(method, &payload, deadline_ms, false, None).await
    }

    /// Admit one stream call: the worker answers with a stream-open
    /// acknowledgement and streams framed items toward the host, and
    /// exactly one terminal still ends the call.
    pub async fn call_stream(
        &self,
        method: &str,
        payload: serde_json::Value,
        deadline_ms: Option<u32>,
    ) -> Result<StreamCall, EndpointError> {
        let (outbound, items) = mpsc::channel(self.shared.stream_channel_capacity());
        let pending = self
            .admit(method, &payload, deadline_ms, true, Some(outbound))
            .await?;
        Ok(StreamCall { pending, items })
    }

    async fn admit(
        &self,
        method: &str,
        payload: &serde_json::Value,
        deadline_ms: Option<u32>,
        stream: bool,
        inbound: Option<mpsc::Sender<StreamFrame>>,
    ) -> Result<PendingCall, EndpointError> {
        // Byte bounds precede the queue: serialize now, refuse oversize
        // before any slot is occupied.
        let payload_bytes = serde_json::to_vec(payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES {
            return Err(EndpointError::Refused(Refusal::PayloadTooLarge {
                bytes: payload_bytes,
            }));
        }
        if method.is_empty() || method.len() > 128 {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "method outside the 1..=128 byte bound",
            )));
        }
        let commands = self.link()?;
        let (opened_sender, opened) = oneshot::channel();
        let (settled_sender, settled) = oneshot::channel();
        let sent = commands.try_send(PumpCommand::Call {
            method: method.to_owned(),
            payload: payload.clone(),
            deadline_ms,
            stream,
            inbound,
            opened: opened_sender,
            settled: settled_sender,
        });
        if let Err(error) = sent {
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => EndpointError::AtCapacity,
                mpsc::error::TrySendError::Closed(_) => EndpointError::Closed {
                    summary: self.shared.close_summary(),
                },
            });
        }
        let call_id = opened.await.map_err(|_| EndpointError::Closed {
            summary: self.shared.close_summary(),
        })?;
        let call_id = call_id.map_err(EndpointError::from)?;
        Ok(PendingCall {
            call_id,
            settled,
            shared: Arc::clone(&self.shared),
        })
    }

    /// Ask the worker to stop a call, or to stop streaming it. Advisory:
    /// the terminal is still owed, and a completion racing the cancel
    /// wins.
    pub async fn cancel(&self, call_id: CallId, target: CancelTarget) -> Result<(), EndpointError> {
        let commands = self.link()?;
        let (done_sender, done) = oneshot::channel();
        let sent = commands.try_send(PumpCommand::Cancel {
            call_id,
            target,
            done: done_sender,
        });
        if let Err(error) = sent {
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => EndpointError::AtCapacity,
                mpsc::error::TrySendError::Closed(_) => EndpointError::Closed {
                    summary: self.shared.close_summary(),
                },
            });
        }
        done.await
            .map_err(|_| EndpointError::Closed {
                summary: self.shared.close_summary(),
            })?
            .map_err(EndpointError::from)
    }

    /// Release a worker-held handle explicitly and wait for the worker's
    /// acknowledgement. The id is spent either way: a second release is
    /// a named refusal, and a release racing a reclaiming terminal is
    /// tolerated once by the session.
    pub async fn release_worker_handle(&self, handle: HandleId) -> Result<(), EndpointError> {
        let commands = self.link()?;
        let (done_sender, done) = oneshot::channel();
        let sent = commands.try_send(PumpCommand::ReleaseWorkerHandle {
            handle,
            done: done_sender,
        });
        if let Err(error) = sent {
            return Err(match error {
                mpsc::error::TrySendError::Full(_) => EndpointError::AtCapacity,
                mpsc::error::TrySendError::Closed(_) => EndpointError::Closed {
                    summary: self.shared.close_summary(),
                },
            });
        }
        done.await
            .map_err(|_| EndpointError::Closed {
                summary: self.shared.close_summary(),
            })?
            .map_err(EndpointError::from)
    }
}

/// Pull-reads one worker-spilled artifact behind its digest-carrying
/// offer, with preflight refusal, bounded chunks, and BLAKE3
/// verification of the accumulated bytes. The worker's digest is a
/// claim to check, never provenance.
pub struct ArtifactReader<'e> {
    endpoint: &'e ActivationEndpoint,
    offer: ArtifactOffer,
    chunk: u32,
    offset: u64,
    hasher: blake3::Hasher,
    remaining: u64,
}

impl<'e> ArtifactReader<'e> {
    /// Preflight against the caller's limit: an offer over `max_bytes`
    /// is refused before the first pull, which is the entire point of
    /// carrying size up front.
    pub fn new(
        endpoint: &'e ActivationEndpoint,
        offer: ArtifactOffer,
        max_bytes: u64,
    ) -> Result<Self, EndpointError> {
        if offer.bytes == 0 || offer.bytes > max_bytes {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact over the caller's byte limit",
            )));
        }
        let remaining = offer.bytes;
        Ok(Self {
            endpoint,
            offer,
            chunk: yah_plugin_ipc::MAX_ARTIFACT_READ_BYTES as u32,
            offset: 0,
            hasher: blake3::Hasher::new(),
            remaining,
        })
    }

    /// The offer being pulled: size, media type, and the digest claim.
    pub fn offer(&self) -> &ArtifactOffer {
        &self.offer
    }

    /// Pull the next bounded chunk. Offset arithmetic is checked; a
    /// read past the declared size is refused before the wire.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, EndpointError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let len = self
            .chunk
            .min(self.remaining.try_into().unwrap_or(u32::MAX));
        let end = self
            .offset
            .checked_add(u64::from(len))
            .ok_or(EndpointError::Refused(Refusal::InvalidField(
                "artifact read offset overflow",
            )))?;
        if end > self.offer.bytes {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact read past the declared size",
            )));
        }
        let payload = serde_json::json!({
            "handle": self.offer.handle,
            "offset": self.offset,
            "len": len,
        });
        let pending = self.endpoint.call("artifact.read", payload, None).await?;
        let terminal = pending.terminal().await?;
        let outcome = match terminal {
            CallTerminal::Completed(outcome) => outcome,
            other => return Err(EndpointError::Unsettled(other)),
        };
        let result = match outcome {
            Outcome::Ok { result } => result,
            Outcome::Err { error } => {
                return Err(EndpointError::Refused(Refusal::WorkerRefused {
                    kind: error.kind,
                }));
            }
            _ => {
                return Err(EndpointError::Refused(Refusal::InvalidField(
                    "unexpected artifact.read outcome",
                )));
            }
        };
        let hex = result
            .get("bytes_hex")
            .and_then(|value| value.as_str())
            .ok_or(EndpointError::Refused(Refusal::InvalidField(
                "artifact read missing bytes",
            )))?;
        let bytes = base16_decode(hex)?;
        self.hasher.update(&bytes);
        self.offset = end;
        self.remaining -= u64::from(len);
        Ok(Some(bytes))
    }

    /// Verify the accumulated bytes against the offer's claimed digest.
    pub fn verify(self) -> Result<(), EndpointError> {
        let digest = self.hasher.finalize().to_hex().to_string();
        if digest == self.offer.digest_blake3 {
            Ok(())
        } else {
            Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact digest mismatch",
            )))
        }
    }
}

fn base16_decode(hex: &str) -> Result<Vec<u8>, EndpointError> {
    if !hex.len().is_multiple_of(2) {
        return Err(EndpointError::Refused(Refusal::InvalidField(
            "odd-length hex",
        )));
    }
    (0..hex.len() / 2)
        .map(|i| {
            u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|_| EndpointError::Refused(Refusal::InvalidField("invalid hex digit")))
        })
        .collect()
}
