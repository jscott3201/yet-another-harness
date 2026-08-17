//! Streams: open-ack, continuation bit, credit, and the two classes.

#[path = "support/peer.rs"]
mod peer;

use peer::{assert_fatal, call, reply_ok, wire};
use serde_json::json;
use yah_plugin_ipc::session::{AppError, SessionEvent};
use yah_plugin_ipc::types::*;

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
