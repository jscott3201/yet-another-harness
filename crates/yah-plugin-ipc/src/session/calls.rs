//! Calls, both directions, and their one terminal frame each.
//!
//! Correlation is a plain monotonic id per direction — the canonical
//! digest/receipt machinery stays at the durable effect funnel, not in the
//! hot path of every chatty worker call. Replies may arrive in any order;
//! ordering is promised only within one call's stream.

use super::{AppError, HostCall, HostSession, Phase, SessionEvent, WorkerCall};
use crate::types::*;
use crate::{MAX_CALL_PAYLOAD_BYTES, MAX_INLINE_RESULT_BYTES};

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
        // The one method protocol law answers itself: a pull-read against a
        // host-held artifact. Served in the same turn, so it never occupies
        // an in-flight slot the application has to manage.
        if call.method == super::handles::ARTIFACT_READ_METHOD {
            self.serve_artifact_read(call);
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

    /// The embedding host answers a worker call. The one inline bound the
    /// app can violate is checked here and refused with the spill
    /// alternative — the protocol never truncates a result to fit.
    pub fn reply_to_worker(&mut self, call_id: CallId, outcome: Outcome) -> Result<(), AppError> {
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
        if let Outcome::Ok { result } = &outcome {
            let bytes = serde_json::to_vec(result).map(|b| b.len()).unwrap_or(0);
            if bytes > MAX_INLINE_RESULT_BYTES {
                return Err(AppError::SpillRequired { bytes });
            }
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
        if self.host_calls.len() as u32 >= self.config.ceilings.host_calls_in_flight {
            return Err(AppError::CallCeiling);
        }
        // The session refuses to queue a frame the worker is contracted to
        // kill: the payload bound binds this side's application too.
        let payload_bytes = serde_json::to_vec(&payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > MAX_CALL_PAYLOAD_BYTES {
            return Err(AppError::PayloadTooLarge {
                bytes: payload_bytes,
            });
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
        if let Outcome::Spilled { artifact } = &reply.outcome
            && artifact.bytes == 0
        {
            self.fatal(
                WireErrorKind::InvalidFrame,
                "spilled artifact of zero bytes",
            );
            return;
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
}
