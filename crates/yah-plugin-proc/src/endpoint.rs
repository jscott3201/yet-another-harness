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
    /// A release this endpoint initiated ended without the worker's
    /// acknowledgement because the activation ended first. `orderly`
    /// marks ends where the release provably never reached the worker or
    /// the worker announced its own stop — safe to treat as released on
    /// the next activation — while `false` means the worker's table state
    /// is unknown and reconciliation belongs to the caller. Either way
    /// the id is spent host-side: never reported as acknowledged success.
    ReleaseLost { orderly: bool },
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
    /// Host-side stream drops for this call, mirrored by the pump as
    /// they happen. Zero for non-stream calls; for a stream call it
    /// stays readable after the delivery table dies with the terminal.
    drops: Arc<std::sync::atomic::AtomicU64>,
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

    /// How many streamed frames this host dropped under its delivery
    /// reservation policy. Meaningful only for stream calls; unlike the
    /// per-frame `dropped` counts it survives the terminal, so a final
    /// drop is never lost behind an end-of-stream.
    pub fn local_drops(&self) -> u64 {
        self.drops.load(std::sync::atomic::Ordering::Acquire)
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

    /// Host-side drops from this call's delivery so far. Readable after
    /// the terminal too — see [`PendingCall::local_drops`].
    pub fn local_drops(&self) -> u64 {
        self.pending.local_drops()
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
        // A `serde_json::Value` always serializes; if it ever could not,
        // treating it as over-bound is the fail-closed answer, and the
        // size reported is the honest "unknown, refused anyway".
        let payload_bytes = serde_json::to_vec(payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES {
            return Err(EndpointError::Refused(Refusal::PayloadTooLarge {
                bytes: payload_bytes,
            }));
        }
        // Method length is counted in Unicode scalar values — the same
        // unit the session enforces on worker-authored methods and the
        // protocol documents. Byte length would miscount multibyte
        // names; UTF-16 length would disagree with both.
        let method_chars = method.chars().count();
        if method.is_empty() || method_chars > yah_plugin_ipc::MAX_METHOD_CHARS {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "method outside its length bound",
            )));
        }
        let commands = self.link()?;
        let (opened_sender, opened) = oneshot::channel();
        let (settled_sender, settled) = oneshot::channel();
        let drops = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let sent = commands.try_send(PumpCommand::Call {
            method: method.to_owned(),
            payload: payload.clone(),
            deadline_ms,
            stream,
            inbound,
            drops: Arc::clone(&drops),
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
            drops,
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
    /// acknowledgement. Success is reported only when the ack arrives:
    /// an admission refusal answers at once, but an admitted release
    /// keeps the caller pending until the worker confirms, and an
    /// activation that ends first settles [`EndpointError::
    /// ReleaseLost`]. The id is spent either way: a second release is a
    /// named refusal, and a release racing a reclaiming terminal is
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
        // Admission refusals surface here; acknowledgement is delivered
        // by the pump only when the worker's ack names the handle. Any
        // other resolution is a typed non-success, never success.
        match done.await {
            Ok(Ok(crate::shared::ReleaseEnd::Acknowledged)) => Ok(()),
            Ok(Ok(crate::shared::ReleaseEnd::Lost { orderly })) => {
                Err(EndpointError::ReleaseLost { orderly })
            }
            Ok(Err(error)) => Err(EndpointError::from(error)),
            Err(_) => Err(EndpointError::Closed {
                summary: self.shared.close_summary(),
            }),
        }
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
        // The reply repeats the artifact's media type; a contradiction
        // with the offer is a worker bug or tampering, not noise.
        let media = result
            .get("media_type")
            .and_then(|value| value.as_str())
            .ok_or(EndpointError::Refused(Refusal::InvalidField(
                "artifact read missing its media type",
            )))?;
        if media != self.offer.media_type {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact read contradicts the offered media type",
            )));
        }
        let hex = result
            .get("bytes_hex")
            .and_then(|value| value.as_str())
            .ok_or(EndpointError::Refused(Refusal::InvalidField(
                "artifact read missing bytes",
            )))?;
        let bytes = base16_decode(hex)?;
        if bytes.len() as u64 != u64::from(len) {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact read chunk length disagrees with the request",
            )));
        }
        self.hasher.update(&bytes);
        self.offset = end;
        self.remaining -= u64::from(len);
        Ok(Some(bytes))
    }

    /// Verify the artifact against the offer's claimed digest — but only
    /// once every declared byte has been pulled. A digest over a prefix
    /// proves nothing about the artifact, so verifying early is refused
    /// even when the prefix's own hash would match.
    pub fn verify(self) -> Result<(), EndpointError> {
        if self.offset != self.offer.bytes || self.remaining != 0 {
            return Err(EndpointError::Refused(Refusal::InvalidField(
                "artifact verified before it was fully read",
            )));
        }
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

/// Decode the wire's canonical base16: ASCII lowercase `[0-9a-f]` only,
/// even length, nothing else — no uppercase, sign prefixes, whitespace,
/// separators, or non-ASCII. The bytes are scanned as `u8`, so no slicing
// rule about character boundaries can ever matter here.
fn base16_decode(hex: &str) -> Result<Vec<u8>, EndpointError> {
    let digits = hex.as_bytes();
    if !hex.is_ascii() || !digits.len().is_multiple_of(2) {
        return Err(EndpointError::Refused(Refusal::InvalidField(
            "hex must be canonical lowercase ASCII with even length",
        )));
    }
    let mut nibbles = digits.iter().map(|byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(EndpointError::Refused(Refusal::InvalidField(
            "hex must be canonical lowercase ASCII with even length",
        ))),
    });
    let mut out = Vec::with_capacity(digits.len() / 2);
    while let Some(high) = nibbles.next() {
        let low = nibbles.next().expect("even length checked above");
        out.push(high? * 16 + low?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProcLimits;
    use yah_compose::{
        ComponentDefinition, ComponentRevision, ComponentSlot, ComponentSlotOutcome,
        DesiredComponentState, ProviderAssignments, ReconcileOutcome, Scope, ServiceRegistry,
    };

    /// A real activation identity, the same way the composition mounts
    /// one: the id type is fenced to a selection epoch, so there is no
    /// cheaper constructor to reach for in a test.
    fn unit_activation_id(label: &str) -> PluginActivationId {
        let registry = ServiceRegistry::new();
        let mut slot = ComponentSlot::new(label).expect("slot label is canonical");
        let package =
            yah_plugin_host::PluginPackageId::new(format!("test.unit.{label}")).expect("package");
        let version = yah_plugin_host::PluginVersion::new("1.0.0").expect("version");
        let digest = yah_plugin_host::PackageDigest::new(format!("blake3:{}", "a".repeat(64)))
            .expect("digest");
        let revision_id = yah_plugin_host::PluginRevisionId::new(package, version, digest);
        let desired = DesiredComponentState::enabled(
            slot.generation(1),
            ComponentRevision::new(
                format!("{label}.revision"),
                ComponentDefinition::new(format!("{label}.component")),
                Scope::root(format!("{label}.scope")),
            ),
            ProviderAssignments::new(),
        );
        match slot
            .reconcile(&registry, desired)
            .expect("fresh component begins start")
        {
            ComponentSlotOutcome::Mounted {
                component: ReconcileOutcome::StartBegun { selection },
                ..
            } => PluginActivationId::new(revision_id, selection.epoch()),
            other => panic!("fresh component did not begin start: {other:?}"),
        }
    }

    /// The controlling handoff requires the cancel submission result to
    /// be pinned at the command channel's real capacity and one past:
    /// exactly `capacity` submissions land, the next is named
    /// `AtCapacity`, and a pump that is gone reads as closed — never
    /// silently accepted. A live pump drains too fast to pin this from
    /// an integration test, so the endpoint is exercised against a
    /// channel nobody is draining.
    #[tokio::test]
    async fn cancel_submission_pins_at_command_capacity_and_one_past() {
        let limits = ProcLimits {
            command_channel_capacity: 3,
            ..ProcLimits::default()
        };
        let shared = Arc::new(crate::shared::PumpShared::new(&limits, 1, 16));
        let (sender, receiver) = mpsc::channel::<PumpCommand>(limits.command_channel_capacity);
        let id = unit_activation_id("cancel-pin");
        shared.set_active();
        let endpoint = activation_endpoint(id, sender.downgrade(), Arc::clone(&shared));

        // Occupy every slot with commands no pump will ever receive.
        for call in 1..=limits.command_channel_capacity as u64 {
            let (done_sender, _done) = oneshot::channel();
            sender
                .try_send(PumpCommand::Cancel {
                    call_id: CallId(call),
                    target: CancelTarget::Call,
                    done: done_sender,
                })
                .expect("the slot is free by construction");
        }
        // At capacity: named, retryable refusal.
        assert_eq!(
            endpoint.cancel(CallId(99), CancelTarget::Call).await,
            Err(EndpointError::AtCapacity)
        );
        // One past capacity, submitted directly: the same bound holds on
        // the raw channel the endpoint rides.
        let (done_sender, _done) = oneshot::channel();
        assert!(
            sender
                .try_send(PumpCommand::Cancel {
                    call_id: CallId(100),
                    target: CancelTarget::Call,
                    done: done_sender,
                })
                .is_err()
        );
        // The pump gone: closed with cause, never silent acceptance.
        drop(receiver);
        assert!(matches!(
            endpoint.cancel(CallId(101), CancelTarget::Call).await,
            Err(EndpointError::Closed { .. })
        ));
    }
}
