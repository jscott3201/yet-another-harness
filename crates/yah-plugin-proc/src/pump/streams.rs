//! Worker-call routing and host-to-worker stream delivery.
//!
//! These are the pump's application-facing arms, split out of the select
//! loop they serve: routing an admitted worker call into the bounded
//! dispatcher lane (or answering it a bounded refusal), delivering
//! streamed items toward a call's consumer with credit accounting, and
//! re-granting credit for drained slots each tick. A child module of the
//! pump precisely so these hands can touch the pump's own state — none of
//! it is a public surface.

use tokio::sync::mpsc;
use yah_plugin_ipc::session::AppError;
use yah_plugin_ipc::types::{CallId, CancelTarget, Outcome, StreamClass, WireError, WireErrorKind};

use crate::dispatch::DispatchRequest;
use crate::endpoint::StreamFrame;

impl super::Pump {
    /// Hand one admitted worker call to the application lane, or answer
    /// it a bounded refusal when the lane cannot take it. Silence is not
    /// an answer the protocol permits: every worker call gets its
    /// terminal, here or through the dispatcher.
    pub(super) fn route_worker_call(
        &mut self,
        call_id: CallId,
        method: &str,
        payload: serde_json::Value,
    ) {
        let Some(dispatcher) = self.dispatcher.clone() else {
            // No application sits above this driver.
            self.refuse_worker_call(
                call_id,
                WireErrorKind::UnknownMethod,
                "unknown-method",
                false,
            );
            return;
        };
        // Byte bounds precede the queue: an oversized body is refused
        // without occupying a dispatch slot, so a bounded slot count
        // never hides an unbounded body.
        let size = serde_json::to_vec(&payload).map(|bytes| bytes.len());
        if size.is_err() || size.is_ok_and(|len| len > yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES) {
            self.refuse_worker_call(
                call_id,
                WireErrorKind::PayloadTooLarge,
                "the request payload exceeds the call bound",
                false,
            );
            return;
        }
        let Some(scope_cancellation) = self.scope_cancellation.clone() else {
            self.refuse_worker_call(
                call_id,
                WireErrorKind::Internal,
                "the host's application lane is not accepting work",
                false,
            );
            return;
        };
        let cancellation = crate::dispatch::WorkerMethodCancellation::new(scope_cancellation);
        match dispatcher.try_send(DispatchRequest {
            call_id,
            method: method.to_owned(),
            payload,
            cancellation: cancellation.clone(),
        }) {
            Ok(()) => {
                self.worker_cancellations.insert(call_id, cancellation);
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Observable back-pressure at the configured bound: the
                // worker may retry once something settles.
                self.refuse_worker_call(
                    call_id,
                    WireErrorKind::ResourceExhausted,
                    "the host's application queue is full",
                    true,
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // The dispatcher task ended without its activation; the
                // call still owes a terminal, so refuse rather than
                // leave the worker waiting out its deadline.
                self.refuse_worker_call(
                    call_id,
                    WireErrorKind::Internal,
                    "the host's application lane is not accepting work",
                    false,
                );
            }
        }
    }

    pub(super) fn refuse_worker_call(
        &mut self,
        call_id: CallId,
        kind: WireErrorKind,
        message: &'static str,
        retryable: bool,
    ) {
        let _ = self.session.reply_to_worker(
            call_id,
            Outcome::Err {
                error: WireError {
                    kind,
                    message: message.to_owned(),
                    retryable,
                    reconcile_required: false,
                },
            },
        );
    }

    /// Deliver one validated stream item toward the call's consumer.
    ///
    /// The conservation law — queued frames plus outstanding lossless
    /// credit at most the channel capacity — decides admission before any
    /// slot is touched. A lossless item spends one credit, so it always
    /// finds a slot when it arrives within credit: spending shrinks the
    /// reservation by exactly what delivery grows occupancy. A lossy item
    /// spends nothing, so it is admitted only into capacity no lossless
    /// credit is reserved against; past that it drops locally, declared.
    pub(super) fn deliver_stream_item(&mut self, call_id: CallId, mut frame: StreamFrame) {
        let delivered = match self.streams.get_mut(&call_id) {
            None => return, // muted or already ended: nothing to deliver to
            Some(stream) => {
                if frame.class == StreamClass::Lossless {
                    stream.outstanding_credit = stream.outstanding_credit.saturating_sub(1);
                }
                // Host-side drops ride the same cumulative counter the
                // wire's own drops use, so the consumer sees one honest
                // monotonic gap count.
                frame.dropped += stream.local_drops;
                match &frame.class {
                    StreamClass::Lossless => stream.inbound.try_send(frame),
                    StreamClass::Lossy => {
                        // Every slot occupied or reserved is unavailable
                        // to lossy traffic. A producer holding the whole
                        // window therefore starves its own lossy frames —
                        // deliberately: the window is the throttle, the
                        // reservation is what guarantees a within-credit
                        // lossless frame a slot, and the drops are
                        // declared either way.
                        let free = stream.inbound.capacity() as u32;
                        let occupied = stream.max_capacity - free.min(stream.max_capacity);
                        if occupied + stream.outstanding_credit < stream.max_capacity {
                            stream.inbound.try_send(frame)
                        } else {
                            Err(mpsc::error::TrySendError::Full(frame))
                        }
                    }
                }
            }
        };
        match delivered {
            Ok(()) => {
                if let Some(stream) = self.streams.get_mut(&call_id) {
                    stream.accepted += 1;
                }
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // A lossless Full cannot happen while the conservation law
                // holds — a within-credit arrival always has a slot — so a
                // Full here means drift or a lossy frame past its
                // reservation. Either way it is declared, never hidden,
                // and mirrored where the terminal can report it.
                if let Some(stream) = self.streams.get_mut(&call_id) {
                    stream.local_drops += 1;
                    stream
                        .drops
                        .fetch_add(1, std::sync::atomic::Ordering::Release);
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // The consumer dropped the item channel: mute delivery by
                // cancelling the stream half. The call's terminal is
                // still owed and still arrives.
                self.streams.remove(&call_id);
                let _ = self.session.cancel(call_id, CancelTarget::Stream);
            }
        }
    }

    /// Re-grant stream credit for freed consumer slots, each tick. The
    /// grant equals what the consumer has drained, capped by the
    /// negotiated ceiling, so a slow consumer throttles the worker
    /// through the credit window instead of growing host memory.
    pub(super) fn regrant_stream_credit(&mut self) {
        for call_id in self.streams.keys().copied().collect::<Vec<_>>() {
            // A muted consumer must be noticed without a delivery to
            // fail: under drainage-only grants a stopped consumer stops
            // the deliveries too, so nothing would ever hit the closed
            // channel. One probe per tick keeps muting prompt.
            if self
                .streams
                .get(&call_id)
                .is_some_and(|stream| stream.inbound.is_closed())
            {
                self.streams.remove(&call_id);
                let _ = self.session.cancel(call_id, CancelTarget::Stream);
                continue;
            }
            let additional = match self.streams.get_mut(&call_id) {
                None => continue,
                // Before the worker's stream-open acknowledgement there
                // is no window to grant into. That is the normal gap
                // between admission and the worker's first ack frame —
                // skipping the tick is all it deserves.
                Some(stream) if !stream.opened => continue,
                Some(stream) => {
                    // Replacement credits are earned by drainage alone:
                    // how many accepted frames the consumer has actually
                    // taken out of the channel right now. Free slots that
                    // were never filled grant nothing, however loudly the
                    // channel advertises them.
                    let occupied =
                        (stream.max_capacity as usize).saturating_sub(stream.inbound.capacity());
                    let drained = stream.accepted.saturating_sub(occupied as u64) as u32;
                    let earned = drained.saturating_sub(stream.replaced as u32);
                    let budget = self
                        .max_stream_credit
                        .saturating_sub(stream.outstanding_credit);
                    earned.min(budget)
                }
            };
            if additional == 0 {
                continue;
            }
            match self.session.grant_credit(call_id, additional) {
                Ok(()) => {
                    if let Some(stream) = self.streams.get_mut(&call_id) {
                        stream.outstanding_credit += additional;
                        stream.replaced += u64::from(additional);
                    }
                }
                Err(AppError::UnknownCall) => {
                    // The call ended under us; the entry dies with its
                    // terminal event.
                    self.streams.remove(&call_id);
                }
                // Any other refusal means the window is not in the state
                // this mirror assumed — stop granting rather than guess,
                // and never mute a live call over bookkeeping.
                Err(_) => {}
            }
        }
    }
}
