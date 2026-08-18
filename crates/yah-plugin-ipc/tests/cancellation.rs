//! Cancellation and loss: a cancelled call still answers, a completion
//! racing a cancel wins, and a worker that vanishes settles as
//! outcome-unknown — a disconnect is loss of the worker, never proof that
//! its external actions failed.

#[path = "support/peer.rs"]
mod peer;

use peer::{call, reply_ok, wire};
use serde_json::json;
use yah_plugin_ipc::session::SessionEvent;
use yah_plugin_ipc::types::*;

#[test]
fn a_cancelled_call_still_terminates_with_its_reply() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.slow", json!(null), None, false)
        .expect("call goes out");
    session
        .cancel(id, CancelTarget::Call)
        .expect("cancel goes out");
    let frames = session.drain_outbox();
    assert_eq!(frames.len(), 2, "the call and the cancel: {frames:?}");
    // The worker acknowledges by answering — silence would be
    // indistinguishable from a lost worker and settle as outcome-unknown,
    // turning every cheap cancel into owner-facing reconciliation.
    session.feed(&wire(&WorkerMessage::Reply(Reply {
        call_id: id,
        outcome: Outcome::Cancelled {
            reason: CancelReason::Requested,
        },
    })));
    assert_eq!(
        session.drain_events(),
        vec![SessionEvent::HostCallSettled {
            call_id: id,
            outcome: Outcome::Cancelled {
                reason: CancelReason::Requested
            },
        }]
    );
    assert!(!session.is_closed());
}

#[test]
fn a_completion_racing_the_cancel_wins() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.fast", json!(null), None, false)
        .expect("call goes out");
    session
        .cancel(id, CancelTarget::Call)
        .expect("cancel goes out");
    // The answer was already on the wire when the cancel left: the work
    // happened, and the outcome says so.
    session.feed(&wire(&reply_ok(id, json!("done before the cancel"))));
    assert_eq!(
        session.drain_events(),
        vec![SessionEvent::HostCallSettled {
            call_id: id,
            outcome: Outcome::Ok {
                result: json!("done before the cancel")
            },
        }]
    );
}

#[test]
fn a_worker_that_dies_mid_frame_settles_as_outcome_unknown() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.effectful", json!(null), None, false)
        .expect("call goes out");
    session.drain_outbox();
    // Half a reply, then the pipe closes: the classic partial write of a
    // worker that called process.exit mid-flush.
    let full = wire(&reply_ok(id, json!("lost")));
    session.feed(&full[..full.len() / 2]);
    session.end_of_input();
    let events = session.drain_events();
    assert!(
        events.contains(&SessionEvent::HostCallLost {
            call_id: id,
            error: WireErrorKind::OutcomeUnknown,
            reconcile: true,
        }),
        "the in-flight call must settle as outcome-unknown: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            SessionEvent::Fatal {
                kind: WireErrorKind::InvalidFrame,
                ..
            }
        )),
        "a mid-frame end is a framing fault, not a clean close: {events:?}"
    );
    assert!(session.is_closed(), "half a frame has no continuation");
}

#[test]
fn a_clean_disconnect_with_work_in_flight_is_still_outcome_unknown() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.effectful", json!(null), None, false)
        .expect("call goes out");
    session.drain_outbox();
    session.end_of_input();
    let events = session.drain_events();
    assert!(
        events.contains(&SessionEvent::HostCallLost {
            call_id: id,
            error: WireErrorKind::OutcomeUnknown,
            reconcile: true,
        }),
        "a clean byte boundary does not make the outcome known: {events:?}"
    );
    assert!(
        events.iter().any(|event| matches!(
            event,
            SessionEvent::Fatal {
                kind: WireErrorKind::OutcomeUnknown,
                ..
            }
        )),
        "a clean end is loss, not a framing fault: {events:?}"
    );
}

#[test]
fn a_goodbye_settles_in_flight_work_as_cancelled_not_unknown() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.slow", json!(null), None, false)
        .expect("call goes out");
    session.drain_outbox();
    session.feed(&wire(&WorkerMessage::Goodbye(Goodbye {
        reason: "shutting down".to_owned(),
    })));
    let events = session.drain_events();
    assert!(events.contains(&SessionEvent::WorkerGoodbye {
        reason: "shutting down".to_owned(),
    }));
    assert!(
        events.contains(&SessionEvent::HostCallLost {
            call_id: id,
            error: WireErrorKind::Cancelled,
            // An orderly stop is not an unknown outcome; nothing external
            // is left to reconcile that the goodbye did not already state.
            reconcile: false,
        }),
        "goodbye settles cancelled: {events:?}"
    );
    assert!(session.is_closed());
}

#[test]
fn frames_after_the_session_closed_are_ignored() {
    let mut session = peer::negotiated();
    session.feed(&wire(&WorkerMessage::Goodbye(Goodbye {
        reason: "bye".to_owned(),
    })));
    session.drain_events();
    session.feed(&wire(&call(1, "tool.run", json!(null))));
    assert!(session.drain_events().is_empty());
    assert!(session.drain_outbox().is_empty());
}
