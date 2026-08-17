//! Calls, both directions, and their one terminal frame each.
//!
//! Correlation is a plain monotonic id per direction — the canonical
//! digest/receipt machinery stays at the durable effect funnel, not in the
//! hot path of every chatty worker call. Replies may arrive in any order;
//! ordering is promised only within one call's stream.

use super::{AppError, HostCall, HostSession, Phase, SessionEvent, WorkerCall};
use crate::types::*;
use crate::{
    MAX_CALL_PAYLOAD_BYTES, MAX_INLINE_RESULT_BYTES, MAX_MEDIA_TYPE_CHARS, MAX_METHOD_CHARS,
    MAX_WIRE_ID,
};

impl HostSession {
    /// A worker-initiated call. Refusable faults answer this call and only
    /// this call; the session continues.
    pub(super) fn on_worker_call(&mut self, call: Call) {
        let call_id = call.call_id;
        // Id range and method length were admitted by `validate_bounds`.
        // A reused id — in flight or already answered — is not a refusable
        // mistake: a reply to that id can no longer be attributed, so the
        // direction's whole correlation space is broken.
        if self.worker_calls.contains_key(&call_id) || self.retired_worker_calls.contains(&call_id)
        {
            self.fatal(WireErrorKind::DuplicateCall, "worker call id reused");
            return;
        }
        let payload_bytes = serde_json::to_vec(&call.payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > MAX_CALL_PAYLOAD_BYTES {
            self.refuse_worker_call(call_id, WireErrorKind::PayloadTooLarge);
            return;
        }
        if self.worker_calls.len() as u32 >= self.config.ceilings.worker_calls_in_flight {
            // Refused, never queued: a queue here is unbounded host memory
            // a hostile worker controls.
            self.refuse_worker_call(call_id, WireErrorKind::ResourceExhausted);
            return;
        }
        // The one method protocol law answers itself: a pull-read against a
        // host-held artifact. It clears the same admission bar as every
        // other call — payload bound, then ceiling — and is then served in
        // the same turn, so it never occupies the slot it was admitted
        // under.
        if call.method == super::handles::ARTIFACT_READ_METHOD {
            self.serve_artifact_read(call);
            return;
        }
        self.worker_calls.insert(
            call_id,
            WorkerCall {
                stream: call.stream,
                stream_credit: None,
                next_seq: 0,
                lossy_dropped: 0,
                last_item_sent: false,
                minted: Vec::new(),
            },
        );
        self.events.push(SessionEvent::CallDelivered {
            call_id,
            method: call.method,
            payload: call.payload,
            stream: call.stream,
            deadline_ms: call.deadline_ms,
        });
    }

    /// Answer one worker call with a protocol refusal. Terminal for that
    /// id: the id is spent even though the call never ran.
    pub(super) fn refuse_worker_call(&mut self, call_id: CallId, kind: WireErrorKind) {
        self.retired_worker_calls.insert(call_id);
        self.outbox.push(HostMessage::Reply(Reply {
            call_id,
            outcome: Outcome::Err {
                error: WireError {
                    kind,
                    message: super::kind_name(kind).to_owned(),
                    retryable: matches!(kind, WireErrorKind::ResourceExhausted),
                    reconcile_required: false,
                },
            },
        }));
        self.events
            .push(SessionEvent::CallRefused { call_id, kind });
    }

    /// The embedding host answers a worker call. An inline result over its
    /// bound is refused with the spill alternative — the protocol never
    /// truncates a result to fit — a hand-built spilled offer passes the
    /// same admission line an inbound one does, and an error message over
    /// the detail bound is clipped: diagnostic text, not data.
    pub fn reply_to_worker(
        &mut self,
        call_id: CallId,
        mut outcome: Outcome,
    ) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(state) = self.worker_calls.get(&call_id) else {
            // Answered and never-existed are different application bugs;
            // name them apart.
            return Err(if self.retired_worker_calls.contains(&call_id) {
                AppError::AlreadySettled
            } else {
                AppError::UnknownCall
            });
        };
        match &mut outcome {
            Outcome::Ok { result } => {
                let bytes = serde_json::to_vec(result).map(|b| b.len()).unwrap_or(0);
                if bytes > MAX_INLINE_RESULT_BYTES {
                    return Err(AppError::SpillRequired { bytes });
                }
                if !super::value_within_ijson(result) {
                    return Err(AppError::InvalidField(
                        "integer outside the I-JSON safe range",
                    ));
                }
            }
            Outcome::Spilled { artifact } => self.validate_outbound_offer(artifact)?,
            Outcome::Err { error } => {
                error.message = super::clip_detail(&error.message);
            }
            Outcome::Cancelled { .. } => {}
        }
        // A refused or cancelled acquire must not leak what it briefly
        // held: reclaim handles this call minted unless it ended ok.
        let reclaim = !matches!(outcome, Outcome::Ok { .. } | Outcome::Spilled { .. });
        let minted = state.minted.clone();
        self.worker_calls.remove(&call_id);
        self.retired_worker_calls.insert(call_id);
        if reclaim {
            self.reclaim_handles(&minted);
        }
        self.outbox
            .push(HostMessage::Reply(Reply { call_id, outcome }));
        Ok(())
    }

    /// The embedding host starts a call toward the worker.
    pub fn call_worker(
        &mut self,
        method: &str,
        payload: serde_json::Value,
        deadline_ms: Option<u32>,
        stream: bool,
    ) -> Result<CallId, AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        // The session refuses to queue a frame the worker is contracted to
        // kill, and bounds precede the ceiling exactly as they do inbound:
        // a call that can never be admitted must not report the
        // retry-shaped ceiling error.
        let method_chars = method.chars().count();
        if method_chars == 0 || method_chars > MAX_METHOD_CHARS {
            return Err(AppError::InvalidField(
                "method name outside its length bound",
            ));
        }
        if !super::value_within_ijson(&payload) {
            return Err(AppError::InvalidField(
                "integer outside the I-JSON safe range",
            ));
        }
        let payload_bytes = serde_json::to_vec(&payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > MAX_CALL_PAYLOAD_BYTES {
            return Err(AppError::PayloadTooLarge {
                bytes: payload_bytes,
            });
        }
        if self.host_calls.len() as u32 >= self.config.ceilings.host_calls_in_flight {
            return Err(AppError::CallCeiling);
        }
        let call_id = CallId(self.next_host_call);
        self.next_host_call += 1;
        self.host_calls.insert(
            call_id,
            HostCall {
                stream,
                stream_open: false,
                credit_left: 0,
                highest_seq: None,
                lossy_dropped: 0,
                last_item_seen: false,
                deadline_at: deadline_ms.map(|budget| self.now_ms + u64::from(budget)),
                stream_muted: false,
            },
        );
        self.outbox.push(HostMessage::Call(Call {
            call_id,
            method: method.to_owned(),
            deadline_ms,
            stream,
            payload,
        }));
        Ok(call_id)
    }

    /// Ask the worker to stop a call, or to stop streaming it. Advisory:
    /// the call still owes its terminal, and the deadline still enforces.
    pub fn cancel(&mut self, call_id: CallId, target: CancelTarget) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(state) = self.host_calls.get_mut(&call_id) else {
            return Err(AppError::UnknownCall);
        };
        if target == CancelTarget::Stream {
            state.stream_muted = true;
        }
        self.outbox
            .push(HostMessage::Cancel(Cancel { call_id, target }));
        Ok(())
    }

    /// The worker's terminal frame for a host call.
    pub(super) fn on_worker_reply(&mut self, reply: Reply) {
        let call_id = reply.call_id;
        let Some(_state) = self.host_calls.get(&call_id) else {
            // A terminal for a retired id is a legal race: the host may
            // have settled the call locally (deadline, cancel) while the
            // worker's answer was in flight. A terminal for an id this
            // session never minted is forgery.
            if self.retired_host_calls.contains(&call_id) {
                return;
            }
            self.fatal(WireErrorKind::UnknownCall, "reply to unknown host call");
            return;
        };
        if let Outcome::Ok { result } = &reply.outcome {
            let bytes = serde_json::to_vec(result).map(|b| b.len()).unwrap_or(0);
            if bytes > MAX_INLINE_RESULT_BYTES {
                // The worker had the spill path and chose to violate the
                // inline bound instead; that is not a per-call mistake.
                self.fatal(WireErrorKind::PayloadTooLarge, "inline result over bound");
                return;
            }
        }
        if let Outcome::Spilled { artifact } = &reply.outcome {
            if artifact.bytes == 0 {
                self.fatal(
                    WireErrorKind::InvalidFrame,
                    "spilled artifact of zero bytes",
                );
                return;
            }
            // A worker handle id is spent once the host's release for it
            // went out or was acked; offering it again is the same
            // id-reuse desync a double release is.
            if self.retired_worker_handles.contains(&artifact.handle)
                || self.pending_worker_releases.contains_key(&artifact.handle)
            {
                self.fatal(
                    WireErrorKind::UnknownHandle,
                    "spilled offer reuses a spent worker handle",
                );
                return;
            }
        }
        self.host_calls.remove(&call_id);
        self.retired_host_calls.insert(call_id);
        self.events.push(SessionEvent::HostCallSettled {
            call_id,
            outcome: reply.outcome,
        });
    }

    /// The worker asks the host to stop serving one of the worker's calls.
    /// Unknown ids are tolerated silently: a cancel that crossed the
    /// terminal reply on the wire is ordinary, not a fault.
    pub(super) fn on_worker_cancel(&mut self, cancel: Cancel) {
        if self.worker_calls.contains_key(&cancel.call_id) {
            self.events.push(SessionEvent::CancelRequested {
                call_id: cancel.call_id,
                target: cancel.target,
            });
        }
    }

    /// Settle a host call locally, without a worker terminal.
    pub(super) fn settle_host_call_locally(
        &mut self,
        call_id: CallId,
        error: WireErrorKind,
        reconcile: bool,
    ) {
        if self.host_calls.remove(&call_id).is_none() {
            return;
        }
        self.retired_host_calls.insert(call_id);
        self.events.push(SessionEvent::HostCallLost {
            call_id,
            error,
            reconcile,
        });
    }

    /// Expired budgets: cancel toward the worker, settle locally. The
    /// worker may still answer — that late terminal is the tolerated race
    /// in [`Self::on_worker_reply`] — but the caller's outcome is decided
    /// here, by the enforcing side.
    pub(super) fn expire_deadlines(&mut self) {
        let expired: Vec<CallId> = self
            .host_calls
            .iter()
            .filter(|(_, state)| state.deadline_at.is_some_and(|at| self.now_ms >= at))
            .map(|(id, _)| *id)
            .collect();
        for call_id in expired {
            self.outbox.push(HostMessage::Cancel(Cancel {
                call_id,
                target: CancelTarget::Call,
            }));
            self.settle_host_call_locally(call_id, WireErrorKind::DeadlineExceeded, true);
        }
    }

    /// [`HostSession::offer_artifact`] only mints conforming offers, but
    /// the reply API accepts any [`Outcome`] — so a hand-built spilled
    /// offer is held to the inbound admission bounds, named with the same
    /// strings, and to the one rule admission cannot check: it must
    /// exactly describe an artifact the session holds, or the reads and
    /// release the offer obliges the worker to send would die against a
    /// handle the session cannot serve.
    fn validate_outbound_offer(&self, artifact: &ArtifactOffer) -> Result<(), AppError> {
        if !(1..=MAX_WIRE_ID).contains(&artifact.handle.0) {
            return Err(AppError::InvalidField("handle id outside wire range"));
        }
        if artifact.bytes == 0 {
            return Err(AppError::InvalidField("spilled artifact of zero bytes"));
        }
        if artifact.bytes > MAX_WIRE_ID {
            return Err(AppError::InvalidField("offer bytes outside wire range"));
        }
        let media_chars = artifact.media_type.chars().count();
        if media_chars == 0 || media_chars > MAX_MEDIA_TYPE_CHARS {
            return Err(AppError::InvalidField(
                "media type outside its length bound",
            ));
        }
        if artifact.digest_blake3.len() != 64
            || !artifact
                .digest_blake3
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return Err(AppError::InvalidField(
                "digest is not 64 lowercase hex characters",
            ));
        }
        let held = self
            .handles
            .get(&artifact.handle)
            .and_then(|entry| entry.artifact.as_ref());
        let Some(held) = held else {
            return Err(AppError::InvalidField(
                "spilled offer for a handle the session does not hold",
            ));
        };
        if held.bytes.len() as u64 != artifact.bytes
            || held.media_type != artifact.media_type
            || held.digest_blake3 != artifact.digest_blake3
        {
            return Err(AppError::InvalidField(
                "spilled offer does not match the held artifact",
            ));
        }
        Ok(())
    }
}
