//! Streams: open-ack, continuation bit, credit, and the two classes.

#[path = "support/peer.rs"]
mod peer;

use peer::{assert_fatal, call, reply_ok, wire};
use serde_json::json;
use yah_plugin_ipc::session::{AppError, SessionEvent};
use yah_plugin_ipc::types::*;
use yah_plugin_ipc::{MAX_STREAM_CREDIT, MAX_STREAM_DATA_BYTES};

/// A streaming host call with the worker's open already fed.
fn open_stream_call(session: &mut yah_plugin_ipc::session::HostSession, credit: u32) -> CallId {
    let id = session
        .call_worker("guest.stream", json!(null), None, true)
        .expect("call goes out");
    session.drain_outbox();
    session.feed(&wire(&WorkerMessage::StreamOpen(StreamOpen {
        call_id: id,
        credit,
    })));
    assert_eq!(
        session.drain_events(),
        vec![SessionEvent::StreamOpened {
            call_id: id,
            credit
        }]
    );
    id
}

fn item(id: CallId, seq: u64, more: bool, class: StreamClass, dropped: u64) -> WorkerMessage {
    WorkerMessage::StreamData(StreamData {
        call_id: id,
        seq,
        more,
        class,
        dropped,
        payload: json!(seq),
    })
}

#[test]
fn the_skeleton_is_ack_then_data_then_terminal() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossless, 0)));
    session.feed(&wire(&item(id, 1, false, StreamClass::Lossless, 0)));
    session.feed(&wire(&reply_ok(id, json!(null))));
    let events = session.drain_events();
    assert_eq!(events.len(), 3, "two items and the settlement: {events:?}");
    assert!(matches!(
        &events[0],
        SessionEvent::StreamItem {
            seq: 0,
            more: true,
            ..
        }
    ));
    assert!(matches!(
        &events[1],
        SessionEvent::StreamItem {
            seq: 1,
            more: false,
            ..
        }
    ));
    assert!(matches!(&events[2], SessionEvent::HostCallSettled { .. }));
}

#[test]
fn data_before_the_open_ack_is_fatal() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.stream", json!(null), None, true)
        .expect("call goes out");
    session.drain_outbox();
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossless, 0)));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_second_open_is_fatal() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&WorkerMessage::StreamOpen(StreamOpen {
        call_id: id,
        credit: 8,
    })));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn an_open_on_a_plain_call_is_fatal() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.plain", json!(null), None, false)
        .expect("call goes out");
    session.drain_outbox();
    session.feed(&wire(&WorkerMessage::StreamOpen(StreamOpen {
        call_id: id,
        credit: 8,
    })));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_sequence_gap_is_fatal() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossless, 0)));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn data_after_the_last_item_is_fatal() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&item(id, 0, false, StreamClass::Lossless, 0)));
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossless, 0)));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_lossless_overdraw_is_fatal_and_the_window_admits_up_to_it() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 2);
    // The paired half: two frames of credit carry exactly two frames.
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossless, 0)));
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossless, 0)));
    assert_eq!(session.drain_events().len(), 2);
    // The third is not congestion to absorb; it is the producer ignoring
    // the window it was granted.
    session.feed(&wire(&item(id, 2, true, StreamClass::Lossless, 0)));
    assert_fatal(&mut session, WireErrorKind::ResourceExhausted);
}

#[test]
fn lossy_items_spend_no_credit_and_carry_their_drop_count() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 1);
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossy, 0)));
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossy, 3)));
    // The one lossless frame of credit is still unspent.
    session.feed(&wire(&item(id, 2, true, StreamClass::Lossless, 3)));
    let drops: Vec<u64> = session
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::StreamItem { dropped, .. } => Some(dropped),
            _ => None,
        })
        .collect();
    assert_eq!(drops, vec![0, 3, 3]);
}

#[test]
fn a_drop_count_going_backwards_is_fatal() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossy, 5)));
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossy, 4)));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn unsubscribing_mutes_items_but_the_terminal_still_lands() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session
        .cancel(id, CancelTarget::Stream)
        .expect("unsubscribe goes out");
    session.drain_outbox();
    // Stop telling me is not stop working: items are validated and
    // spent against credit, just not delivered.
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossless, 0)));
    session.feed(&wire(&reply_ok(id, json!("finished"))));
    let events = session.drain_events();
    assert_eq!(
        events,
        vec![SessionEvent::HostCallSettled {
            call_id: id,
            outcome: Outcome::Ok {
                result: json!("finished")
            },
        }]
    );
}

#[test]
fn the_host_produces_under_the_worker_granted_window() {
    let mut session = peer::negotiated();
    session.feed(&wire(&WorkerMessage::Call(Call {
        call_id: CallId(5),
        method: "host.watch".to_owned(),
        deadline_ms: None,
        stream: true,
        payload: json!(null),
    })));
    session.drain_events();
    session.open_stream(CallId(5), 1).expect("ack goes out");
    session
        .stream_item(CallId(5), StreamClass::Lossless, true, json!(0))
        .expect("first item fits the window");
    // The window is spent; the wire contract, not politeness, is what
    // stops the producer.
    assert_eq!(
        session.stream_item(CallId(5), StreamClass::Lossless, true, json!(1)),
        Err(AppError::StreamViolation("no credit left"))
    );
    session.feed(&wire(&WorkerMessage::Credit(Credit {
        call_id: CallId(5),
        additional: 1,
    })));
    session.drain_events();
    session
        .stream_item(CallId(5), StreamClass::Lossless, false, json!(1))
        .expect("widened window carries the last item");
    session
        .reply_to_worker(
            CallId(5),
            Outcome::Ok {
                result: json!(null),
            },
        )
        .expect("terminal ends the stream");
    let frames = session.drain_outbox();
    assert_eq!(frames.len(), 4, "open, item, item, terminal: {frames:?}");
}

#[test]
fn credit_before_the_host_opened_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(6, "host.watch", json!(null))));
    session.drain_events();
    session.feed(&wire(&WorkerMessage::Credit(Credit {
        call_id: CallId(6),
        additional: 1,
    })));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_stream_open_with_zero_or_over_ceiling_credit_is_fatal() {
    for credit in [0, MAX_STREAM_CREDIT + 1] {
        let mut session = peer::negotiated();
        let id = session
            .call_worker("guest.stream", json!(null), None, true)
            .expect("call goes out");
        session.drain_outbox();
        session.feed(&wire(&WorkerMessage::StreamOpen(StreamOpen {
            call_id: id,
            credit,
        })));
        assert_fatal(&mut session, WireErrorKind::InvalidFrame);
    }
    // The ceiling itself is admitted — the pair half.
    let mut session = peer::negotiated();
    open_stream_call(&mut session, MAX_STREAM_CREDIT);
}

#[test]
fn a_credit_widening_past_the_ceiling_or_by_zero_is_fatal() {
    for additional in [0u32, 1] {
        let mut session = peer::negotiated();
        session.feed(&wire(&WorkerMessage::Call(Call {
            call_id: CallId(5),
            method: "host.watch".to_owned(),
            deadline_ms: None,
            stream: true,
            payload: json!(null),
        })));
        session.drain_events();
        // Open at the ceiling: any widening overflows it, and a zero grant
        // is noise no correct SDK emits.
        session
            .open_stream(CallId(5), MAX_STREAM_CREDIT)
            .expect("ack goes out");
        session.drain_outbox();
        session.feed(&wire(&WorkerMessage::Credit(Credit {
            call_id: CallId(5),
            additional,
        })));
        assert_fatal(&mut session, WireErrorKind::InvalidFrame);
    }
}

#[test]
fn a_credit_grant_that_overflows_the_window_type_is_fatal() {
    let mut session = peer::negotiated();
    feed_streaming_call(&mut session, 5);
    session.open_stream(CallId(5), 4).expect("ack goes out");
    session.drain_outbox();
    session.feed(&wire(&WorkerMessage::Credit(Credit {
        call_id: CallId(5),
        additional: u32::MAX,
    })));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn an_oversize_stream_item_from_the_worker_is_fatal() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&WorkerMessage::StreamData(StreamData {
        call_id: id,
        seq: 0,
        more: true,
        class: StreamClass::Lossless,
        dropped: 0,
        payload: json!("x".repeat(MAX_STREAM_DATA_BYTES + 1)),
    })));
    assert_fatal(&mut session, WireErrorKind::PayloadTooLarge);
}

#[test]
fn a_stream_open_for_an_unknown_call_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&WorkerMessage::StreamOpen(StreamOpen {
        call_id: CallId(55),
        credit: 8,
    })));
    assert_fatal(&mut session, WireErrorKind::UnknownCall);
}

#[test]
fn stream_data_for_an_unknown_call_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&item(CallId(55), 0, true, StreamClass::Lossless, 0)));
    assert_fatal(&mut session, WireErrorKind::UnknownCall);
}

#[test]
fn the_host_widens_a_worker_streams_window_with_credit() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 1);
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossless, 0)));
    session.drain_events();
    // The worker's window is spent; the host, as consumer, is the side
    // that widens it — the symmetric half of the worker's credit frames.
    session.grant_credit(id, 1).expect("credit goes out");
    let outbox = session.drain_outbox();
    assert!(
        matches!(
            outbox.as_slice(),
            [HostMessage::Credit(credit)] if credit.call_id == id && credit.additional == 1
        ),
        "the widening must reach the wire: {outbox:?}"
    );
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossless, 0)));
    assert_eq!(
        session.drain_events().len(),
        1,
        "the widened window admits the item"
    );
    assert!(!session.is_closed());
}

#[test]
fn a_host_credit_grant_needs_an_open_stream_within_the_ceiling() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.stream", json!(null), None, true)
        .expect("call goes out");
    session.drain_outbox();
    assert_eq!(
        session.grant_credit(id, 1),
        Err(AppError::StreamViolation("stream not open"))
    );
    session.feed(&wire(&WorkerMessage::StreamOpen(StreamOpen {
        call_id: id,
        credit: MAX_STREAM_CREDIT,
    })));
    session.drain_events();
    assert_eq!(
        session.grant_credit(id, 1),
        Err(AppError::StreamViolation("credit outside bounds"))
    );
}

/// A worker call that asked to stream, delivered and undrained.
fn feed_streaming_call(session: &mut yah_plugin_ipc::session::HostSession, id: u64) {
    session.feed(&wire(&WorkerMessage::Call(Call {
        call_id: CallId(id),
        method: "host.watch".to_owned(),
        deadline_ms: None,
        stream: true,
        payload: json!(null),
    })));
    session.drain_events();
}

#[test]
fn the_host_declares_its_lossy_drops() {
    let mut session = peer::negotiated();
    feed_streaming_call(&mut session, 5);
    assert_eq!(
        session.note_lossy_drops(CallId(5), 2),
        Err(AppError::StreamViolation("stream not open")),
        "drops are per-stream accounting; there is no stream yet"
    );
    session.open_stream(CallId(5), 4).expect("ack goes out");
    session
        .note_lossy_drops(CallId(5), 2)
        .expect("drops recorded");
    session
        .stream_item(CallId(5), StreamClass::Lossy, true, json!(null))
        .expect("lossy item goes out");
    let frames = session.drain_outbox();
    assert!(
        matches!(
            frames.as_slice(),
            [HostMessage::StreamOpen(_), HostMessage::StreamData(data)] if data.dropped == 2
        ),
        "the item carries the declared gap: {frames:?}"
    );
}

#[test]
fn drops_declared_after_the_last_item_are_refused() {
    let mut session = peer::negotiated();
    feed_streaming_call(&mut session, 5);
    session.open_stream(CallId(5), 4).expect("ack goes out");
    session
        .stream_item(CallId(5), StreamClass::Lossy, false, json!(null))
        .expect("the last item goes out");
    // No frame is left to carry the count; recording one would be a
    // promise the wire cannot keep.
    assert_eq!(
        session.note_lossy_drops(CallId(5), 1),
        Err(AppError::StreamViolation("drops after the last item"))
    );
}

#[test]
fn an_outbound_stream_item_with_an_unsafe_integer_is_refused() {
    let mut session = peer::negotiated();
    feed_streaming_call(&mut session, 5);
    session.open_stream(CallId(5), 4).expect("ack goes out");
    assert_eq!(
        session.stream_item(CallId(5), StreamClass::Lossless, true, json!(u64::MAX)),
        Err(AppError::InvalidField(
            "integer outside the I-JSON safe range"
        ))
    );
}

#[test]
fn a_drop_count_outside_the_wire_range_is_refused_at_the_bound() {
    let mut session = peer::negotiated();
    feed_streaming_call(&mut session, 5);
    session.open_stream(CallId(5), 4).expect("ack goes out");
    session
        .note_lossy_drops(CallId(5), 9_007_199_254_740_991)
        .expect("the bound itself is recordable");
    // One more would sum past the I-JSON range the peer's admission — and
    // the generated schema's maximum — hold the frame to.
    assert_eq!(
        session.note_lossy_drops(CallId(5), 1),
        Err(AppError::InvalidField("drop count outside wire range"))
    );
}

#[test]
fn an_outbound_stream_item_over_the_byte_bound_is_refused() {
    let mut session = peer::negotiated();
    feed_streaming_call(&mut session, 5);
    session.open_stream(CallId(5), 4).expect("ack goes out");
    match session.stream_item(
        CallId(5),
        StreamClass::Lossless,
        true,
        json!("x".repeat(MAX_STREAM_DATA_BYTES + 1)),
    ) {
        Err(AppError::PayloadTooLarge { bytes }) => assert!(bytes > MAX_STREAM_DATA_BYTES),
        other => panic!("expected the payload refusal, got {other:?}"),
    }
}

#[test]
fn data_after_the_terminal_is_a_tolerated_race_not_a_fault() {
    let mut session = peer::negotiated();
    let id = open_stream_call(&mut session, 8);
    session.feed(&wire(&item(id, 0, true, StreamClass::Lossless, 0)));
    session.feed(&wire(&reply_ok(id, json!(null))));
    session.drain_events();
    // The producer's last items were already on the wire when the terminal
    // crossed them. Unlike data after a declared last item — a producer
    // contradicting itself, which is fatal — this is ordinary crossing
    // traffic: dropped without an event, and the session lives on.
    session.feed(&wire(&item(id, 1, true, StreamClass::Lossless, 0)));
    assert!(session.drain_events().is_empty());
    assert!(!session.is_closed());
}
