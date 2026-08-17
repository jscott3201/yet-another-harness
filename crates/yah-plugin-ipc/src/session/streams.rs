//! Streams: the call id is the stream id, an open-ack precedes data, and
//! the terminal reply ends the stream.
//!
//! Backpressure is a per-stream credit window counted in frames — every
//! stream frame is already byte-bounded, so frame count is the dimension a
//! hostile producer could still flood. Credit is the wire contract; the
//! runtimes' `drain()` advisories are how a cooperative SDK implements it,
//! not what the receiver relies on. Lossless items consume credit and the
//! producer must stop at zero; lossy items may be dropped oldest-first
//! under pressure, with the cumulative drop count carried so the consumer
//! sees the gap rather than infers it.

use super::{AppError, HostSession, Phase, SessionEvent};
use crate::MAX_STREAM_DATA_BYTES;
use crate::types::*;

impl HostSession {
    /// The worker acknowledged a streaming host call and granted the
    /// initial window for its data frames toward us.
    pub(super) fn on_stream_open(&mut self, open: StreamOpen) {
        let Some(state) = self.host_calls.get_mut(&open.call_id) else {
            if self.retired_host_calls.contains(&open.call_id) {
                return; // open racing a local settle: tolerated like a late reply
            }
            self.fatal(WireErrorKind::UnknownCall, "stream open for unknown call");
            return;
        };
        if !state.stream {
            self.fatal(WireErrorKind::InvalidFrame, "stream open on a plain call");
            return;
        }
        if state.stream_open {
            self.fatal(WireErrorKind::InvalidFrame, "second stream open");
            return;
        }
        if open.credit == 0 || open.credit > self.config.ceilings.max_stream_credit {
            self.fatal(WireErrorKind::InvalidFrame, "stream credit outside bounds");
            return;
        }
        state.stream_open = true;
        state.credit_left = open.credit;
        self.events.push(SessionEvent::StreamOpened {
            call_id: open.call_id,
            credit: open.credit,
        });
    }

    /// One item from the worker on a host call's stream.
    pub(super) fn on_stream_data(&mut self, data: StreamData) {
        let payload_bytes = serde_json::to_vec(&data.payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > MAX_STREAM_DATA_BYTES {
            self.fatal(WireErrorKind::PayloadTooLarge, "stream item over bound");
            return;
        }
        let Some(state) = self.host_calls.get_mut(&data.call_id) else {
            if self.retired_host_calls.contains(&data.call_id) {
                return; // items racing a local settle: dropped, not faulted
            }
            self.fatal(WireErrorKind::UnknownCall, "stream data for unknown call");
            return;
        };
        if !state.stream || !state.stream_open {
            self.fatal(WireErrorKind::InvalidFrame, "stream data before open");
            return;
        }
        if state.last_item_seen {
            self.fatal(WireErrorKind::InvalidFrame, "stream data after last item");
            return;
        }
        // Monotonic from zero with no gaps, ever: lossy drops are declared
        // through `dropped`, never by skipping sequence numbers.
        let expected = state.highest_seq.map_or(0, |seq| seq + 1);
        if data.seq != expected {
            self.fatal(WireErrorKind::InvalidFrame, "stream sequence not monotonic");
            return;
        }
        if data.dropped < state.lossy_dropped {
            self.fatal(WireErrorKind::InvalidFrame, "drop count went backwards");
            return;
        }
        match data.class {
            StreamClass::Lossless => {
                // An overdraw is not congestion, it is the producer
                // ignoring the window it was granted.
                if state.credit_left == 0 {
                    self.fatal(WireErrorKind::ResourceExhausted, "stream credit overdrawn");
                    return;
                }
                state.credit_left -= 1;
            }
            StreamClass::Lossy => {}
        }
        state.highest_seq = Some(data.seq);
        state.lossy_dropped = data.dropped;
        if !data.more {
            state.last_item_seen = true;
        }
        if !state.stream_muted {
            self.events.push(SessionEvent::StreamItem {
                call_id: data.call_id,
                seq: data.seq,
                more: data.more,
                class: data.class,
                dropped: data.dropped,
                payload: data.payload,
            });
        }
    }

    /// The worker widened the host's window on a worker call the host is
    /// streaming into.
    pub(super) fn on_credit(&mut self, credit: Credit) {
        let Some(state) = self.worker_calls.get_mut(&credit.call_id) else {
            return; // credit racing the terminal: tolerated
        };
        let Some(window) = state.stream_credit else {
            self.fatal(WireErrorKind::InvalidFrame, "credit before stream open");
            return;
        };
        let Some(next) = window.checked_add(credit.additional) else {
            self.fatal(WireErrorKind::InvalidFrame, "credit overflow");
            return;
        };
        if credit.additional == 0 || next > self.config.ceilings.max_stream_credit {
            self.fatal(WireErrorKind::InvalidFrame, "credit outside bounds");
            return;
        }
        state.stream_credit = Some(next);
        self.events.push(SessionEvent::CreditGranted {
            call_id: credit.call_id,
            additional: credit.additional,
        });
    }

    /// The embedding host widens the worker's window on a host call the
    /// worker is streaming into — the consumer's half of backpressure,
    /// symmetric with the [`Credit`] frames the worker sends the other way.
    pub fn grant_credit(&mut self, call_id: CallId, additional: u32) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(state) = self.host_calls.get_mut(&call_id) else {
            return Err(AppError::UnknownCall);
        };
        if !state.stream_open {
            return Err(AppError::StreamViolation("stream not open"));
        }
        let Some(next) = state.credit_left.checked_add(additional) else {
            return Err(AppError::StreamViolation("credit overflow"));
        };
        if additional == 0 || next > self.config.ceilings.max_stream_credit {
            return Err(AppError::StreamViolation("credit outside bounds"));
        }
        state.credit_left = next;
        self.outbox.push(HostMessage::Credit(Credit {
            call_id,
            additional,
        }));
        Ok(())
    }

    /// The embedding host records lossy items it dropped under pressure on
    /// a worker call's stream; the next item it sends carries the count, so
    /// the worker sees the gap rather than infers it.
    pub fn note_lossy_drops(&mut self, call_id: CallId, dropped: u64) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(state) = self.worker_calls.get_mut(&call_id) else {
            return Err(AppError::UnknownCall);
        };
        if state.stream_credit.is_none() {
            return Err(AppError::StreamViolation("stream not open"));
        }
        state.lossy_dropped = state.lossy_dropped.saturating_add(dropped);
        Ok(())
    }

    /// The embedding host acknowledges a worker's streaming call and
    /// grants the worker's initial window — queued as the stream open.
    pub fn open_stream(&mut self, call_id: CallId, credit: u32) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(state) = self.worker_calls.get_mut(&call_id) else {
            return Err(AppError::UnknownCall);
        };
        if !state.stream {
            return Err(AppError::StreamViolation("call did not ask to stream"));
        }
        if state.stream_credit.is_some() {
            return Err(AppError::StreamViolation("stream already open"));
        }
        if credit == 0 || credit > self.config.ceilings.max_stream_credit {
            return Err(AppError::StreamViolation("credit outside bounds"));
        }
        state.stream_credit = Some(credit);
        self.outbox
            .push(HostMessage::StreamOpen(StreamOpen { call_id, credit }));
        Ok(())
    }

    /// The embedding host sends one item on a worker call's stream,
    /// spending the worker-granted window for lossless items.
    pub fn stream_item(
        &mut self,
        call_id: CallId,
        class: StreamClass,
        more: bool,
        payload: serde_json::Value,
    ) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(state) = self.worker_calls.get_mut(&call_id) else {
            return Err(AppError::UnknownCall);
        };
        let Some(window) = state.stream_credit else {
            return Err(AppError::StreamViolation("stream not open"));
        };
        if state.last_item_sent {
            return Err(AppError::StreamViolation("item after the last one"));
        }
        let payload_bytes = serde_json::to_vec(&payload)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if payload_bytes > MAX_STREAM_DATA_BYTES {
            return Err(AppError::PayloadTooLarge {
                bytes: payload_bytes,
            });
        }
        if class == StreamClass::Lossless {
            if window == 0 {
                return Err(AppError::StreamViolation("no credit left"));
            }
            state.stream_credit = Some(window - 1);
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        if !more {
            state.last_item_sent = true;
        }
        let dropped = state.lossy_dropped;
        self.outbox.push(HostMessage::StreamData(StreamData {
            call_id,
            seq,
            more,
            class,
            dropped,
            payload,
        }));
        Ok(())
    }
}
