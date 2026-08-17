//! Calls: correlation, ceilings, deadlines, and the one terminal each.
//!
//! The ceiling case is paired, like every ceiling in this repository: the
//! same shape refused at the bound and admitted below it, so a failure is
//! attributable to the ceiling rather than to the fixture.

#[path = "support/peer.rs"]
mod peer;

use peer::{assert_fatal, call, reply_ok, wire};
use serde_json::json;
use yah_plugin_ipc::session::{AppError, SessionConfig, SessionEvent};
use yah_plugin_ipc::types::*;
use yah_plugin_ipc::{MAX_CALL_PAYLOAD_BYTES, MAX_INLINE_RESULT_BYTES};

#[test]
fn a_worker_call_is_delivered_and_the_reply_goes_out() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(7, "tool.run", json!({"q": 1}))));
    let events = session.drain_events();
    assert_eq!(
        events,
        vec![SessionEvent::CallDelivered {
            call_id: CallId(7),
            method: "tool.run".to_owned(),
            payload: json!({"q": 1}),
            stream: false,
            deadline_ms: None,
        }]
    );
    session
        .reply_to_worker(
            CallId(7),
            Outcome::Ok {
                result: json!("done"),
            },
        )
        .expect("reply goes out");
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Reply(reply)] => {
            assert_eq!(reply.call_id, CallId(7));
            assert_eq!(
                reply.outcome,
                Outcome::Ok {
                    result: json!("done")
                }
            );
        }
        other => panic!("expected the reply, got {other:?}"),
    }
    // Answered means answered: a second terminal for the same call is an
    // application bug the session refuses to put on the wire, named apart
    // from a call that never existed.
    assert_eq!(
        session.reply_to_worker(CallId(7), Outcome::Ok { result: json!(0) }),
        Err(AppError::AlreadySettled)
    );
    assert_eq!(
        session.reply_to_worker(CallId(99), Outcome::Ok { result: json!(0) }),
        Err(AppError::UnknownCall)
    );
}

#[test]
fn a_reused_in_flight_id_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(7, "tool.run", json!(1))));
    session.feed(&wire(&call(7, "tool.run", json!(2))));
    assert_fatal(&mut session, WireErrorKind::DuplicateCall);
}

#[test]
fn a_refused_id_is_spent_for_the_session() {
    let mut session = peer::negotiated();
    let big = "x".repeat(MAX_CALL_PAYLOAD_BYTES + 1);
    session.feed(&wire(&call(7, "tool.run", json!(big))));
    let events = session.drain_events();
    assert!(
        matches!(
            events.as_slice(),
            [SessionEvent::CallRefused {
                kind: WireErrorKind::PayloadTooLarge,
                ..
            }]
        ),
        "got {events:?}"
    );
    // The refusal was that id's terminal frame; reuse breaks correlation
    // exactly as an in-flight duplicate does.
    session.feed(&wire(&call(7, "tool.run", json!(1))));
    assert_fatal(&mut session, WireErrorKind::DuplicateCall);
}

#[test]
fn call_id_zero_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(0, "tool.run", json!(1))));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn the_in_flight_ceiling_refuses_at_the_bound_and_admits_below_it() {
    let mut config = SessionConfig::default();
    config.ceilings.worker_calls_in_flight = 2;
    let mut session = peer::negotiated_with(config);
    session.feed(&wire(&call(1, "tool.run", json!(1))));
    session.feed(&wire(&call(2, "tool.run", json!(2))));
    session.feed(&wire(&call(3, "tool.run", json!(3))));
    let refused: Vec<_> = session
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::CallRefused { call_id, kind } => Some((call_id, kind)),
            _ => None,
        })
        .collect();
    assert_eq!(refused, vec![(CallId(3), WireErrorKind::ResourceExhausted)]);
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Reply(reply)] => match &reply.outcome {
            Outcome::Err { error } => {
                assert_eq!(error.kind, WireErrorKind::ResourceExhausted);
                // Exhaustion is the one refusal that invites the same call
                // again later.
                assert!(error.retryable);
            }
            other => panic!("expected an error outcome, got {other:?}"),
        },
        other => panic!("expected one refusal reply, got {other:?}"),
    }
    // Below the bound again after one settles: the pair half that makes
    // the refusal attributable to the ceiling.
    session
        .reply_to_worker(
            CallId(1),
            Outcome::Ok {
                result: json!(null),
            },
        )
        .expect("settling makes room");
    session.feed(&wire(&call(4, "tool.run", json!(4))));
    let delivered = session
        .drain_events()
        .iter()
        .any(|event| matches!(event, SessionEvent::CallDelivered { call_id, .. } if *call_id == CallId(4)));
    assert!(delivered, "the call below the ceiling must be admitted");
}

#[test]
fn a_method_name_outside_its_length_bound_is_fatal_and_the_bound_admits() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(1, &"m".repeat(128), json!(null))));
    assert!(
        matches!(
            session.drain_events().as_slice(),
            [SessionEvent::CallDelivered { .. }]
        ),
        "a method at the bound is admitted"
    );
    session.feed(&wire(&call(2, &"m".repeat(129), json!(null))));
    peer::assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_worker_reply_with_an_oversize_inline_result_is_fatal() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.big", json!(null), None, false)
        .expect("call goes out");
    session.drain_outbox();
    // The worker had the spill path and violated the inline bound instead;
    // that is a broken SDK, not a per-call mistake.
    let oversize = "x".repeat(MAX_INLINE_RESULT_BYTES + 1);
    session.feed(&wire(&reply_ok(id, json!(oversize))));
    peer::assert_fatal(&mut session, WireErrorKind::PayloadTooLarge);
}

#[test]
fn an_oversize_outbound_call_payload_is_refused_before_the_wire() {
    let mut session = peer::negotiated();
    // json!(string) serializes to length + 2 quote bytes: this pair sits
    // exactly at and one past the bound.
    session
        .call_worker(
            "guest.big",
            json!("x".repeat(MAX_CALL_PAYLOAD_BYTES - 2)),
            None,
            false,
        )
        .expect("a payload at the bound goes out");
    match session.call_worker(
        "guest.big",
        json!("x".repeat(MAX_CALL_PAYLOAD_BYTES - 1)),
        None,
        false,
    ) {
        Err(AppError::PayloadTooLarge { bytes }) => assert_eq!(bytes, MAX_CALL_PAYLOAD_BYTES + 1),
        other => panic!("expected the payload refusal, got {other:?}"),
    }
    let outbox = session.drain_outbox();
    assert_eq!(outbox.len(), 1, "only the bounded call reached the wire");
}

#[test]
fn out_of_order_replies_settle_each_call_by_id_not_arrival() {
    let mut session = peer::negotiated();
    let first = session
        .call_worker("guest.slow", json!(null), None, false)
        .expect("first call goes out");
    let second = session
        .call_worker("guest.fast", json!(null), None, false)
        .expect("second call goes out");
    session.drain_outbox();
    // The fast call answers first: correlation is the id, never arrival
    // order.
    session.feed(&wire(&reply_ok(second, json!("fast"))));
    session.feed(&wire(&reply_ok(first, json!("slow"))));
    assert_eq!(
        session.drain_events(),
        vec![
            SessionEvent::HostCallSettled {
                call_id: second,
                outcome: Outcome::Ok {
                    result: json!("fast")
                },
            },
            SessionEvent::HostCallSettled {
                call_id: first,
                outcome: Outcome::Ok {
                    result: json!("slow")
                },
            },
        ]
    );
}

#[test]
fn a_host_call_settles_on_the_worker_terminal() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.tool", json!({"x": 1}), None, false)
        .expect("call goes out");
    let outbox = session.drain_outbox();
    assert!(matches!(outbox.as_slice(), [HostMessage::Call(c)] if c.call_id == id));
    session.feed(&wire(&reply_ok(id, json!("answer"))));
    let events = session.drain_events();
    assert_eq!(
        events,
        vec![SessionEvent::HostCallSettled {
            call_id: id,
            outcome: Outcome::Ok {
                result: json!("answer")
            },
        }]
    );
}

#[test]
fn a_reply_to_an_id_the_host_never_minted_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&reply_ok(CallId(41), json!(1))));
    assert_fatal(&mut session, WireErrorKind::UnknownCall);
}

#[test]
fn an_expired_deadline_cancels_settles_and_tolerates_the_late_reply() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.slow", json!(null), Some(50), false)
        .expect("call goes out");
    session.drain_outbox();
    session.tick(49);
    assert!(session.drain_events().is_empty(), "budget not yet spent");
    session.tick(50);
    let outbox = session.drain_outbox();
    assert!(
        matches!(
            outbox.as_slice(),
            [HostMessage::Cancel(cancel)] if cancel.call_id == id && cancel.target == CancelTarget::Call
        ),
        "the enforcer tells the worker to stop: {outbox:?}"
    );
    let events = session.drain_events();
    assert_eq!(
        events,
        vec![SessionEvent::HostCallLost {
            call_id: id,
            error: WireErrorKind::DeadlineExceeded,
            // The worker may have acted before the budget ran out.
            reconcile: true,
        }]
    );
    // The worker's answer was already in flight: a tolerated race, not a
    // fault, and not a second settlement.
    session.feed(&wire(&reply_ok(id, json!("too late"))));
    assert!(session.drain_events().is_empty());
    assert!(!session.is_closed());
}

#[test]
fn the_host_call_ceiling_refuses_the_application() {
    let mut config = SessionConfig::default();
    config.ceilings.host_calls_in_flight = 1;
    let mut session = peer::negotiated_with(config);
    session
        .call_worker("guest.a", json!(null), None, false)
        .expect("first call fits");
    assert_eq!(
        session.call_worker("guest.b", json!(null), None, false),
        Err(AppError::CallCeiling)
    );
}

#[test]
fn a_worker_cancel_is_delivered_and_a_late_one_is_silent() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(9, "tool.run", json!(null))));
    session.drain_events();
    session.feed(&wire(&WorkerMessage::Cancel(Cancel {
        call_id: CallId(9),
        target: CancelTarget::Call,
    })));
    let events = session.drain_events();
    assert_eq!(
        events,
        vec![SessionEvent::CancelRequested {
            call_id: CallId(9),
            target: CancelTarget::Call,
        }]
    );
    session
        .reply_to_worker(
            CallId(9),
            Outcome::Cancelled {
                reason: CancelReason::Requested,
            },
        )
        .expect("the cancelled call still answers");
    // A cancel that crossed the terminal on the wire names a call that no
    // longer exists; that race is ordinary.
    session.feed(&wire(&WorkerMessage::Cancel(Cancel {
        call_id: CallId(9),
        target: CancelTarget::Call,
    })));
    assert!(session.drain_events().is_empty());
    assert!(!session.is_closed());
}
