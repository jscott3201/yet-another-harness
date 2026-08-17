//! Strict decoding and the no-leak rule.
//!
//! Closed within a negotiated version means exactly that: unknown frame
//! tags, unknown enum members, unknown fields, duplicate member names, and
//! unsafe integers are all one refusal, decided before any typed handler
//! runs. And nothing the worker sent comes back to it in an error — an
//! echo is a reflection surface, and internals are a tour.

#[path = "support/peer.rs"]
mod peer;

use peer::{assert_fatal, call, sole_fatal, wire, wire_raw};
use serde_json::json;
use yah_plugin_ipc::MAX_CALL_PAYLOAD_BYTES;
use yah_plugin_ipc::types::*;

#[test]
fn a_duplicate_member_name_is_fatal() {
    let mut session = peer::negotiated();
    // Otherwise a well-formed goodbye: the duplicate is the one violation.
    session.feed(&wire_raw(
        r#"{"frame":"goodbye","reason":"a","reason":"b"}"#,
    ));
    let (kind, detail) = sole_fatal(&mut session);
    assert_eq!(kind, WireErrorKind::InvalidFrame);
    assert!(
        detail.contains("duplicate member"),
        "the strict layer, not the typed decode, must refuse this: {detail}"
    );
}

#[test]
fn an_integer_outside_the_ijson_safe_range_is_fatal() {
    let mut session = peer::negotiated();
    // 2^53: the first integer JavaScript cannot hold exactly. One decoder
    // rounding where another keeps precision is a disagreement about what
    // was said, so the frame is refused for everyone. The frame is an
    // otherwise-valid call so the integer is the one violation.
    session.feed(&wire_raw(
        r#"{"frame":"call","call_id":9007199254740992,"method":"m","deadline_ms":null,"stream":false,"payload":null}"#,
    ));
    let (kind, detail) = sole_fatal(&mut session);
    assert_eq!(kind, WireErrorKind::InvalidFrame);
    assert!(
        detail.contains("I-JSON safe range"),
        "the strict layer, not the typed decode, must refuse this: {detail}"
    );
}

#[test]
fn an_unknown_frame_tag_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire_raw(r#"{"frame":"telepathy"}"#));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn an_unknown_enum_member_is_fatal() {
    let mut session = peer::negotiated();
    // A worker one version ahead does not get leniency; it gets the
    // handshake, where evolution actually happens.
    session.feed(&wire_raw(
        r#"{"frame":"release","handle":1,"kind":"quantum"}"#,
    ));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn an_unknown_field_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire_raw(r#"{"frame":"goodbye","reason":"x","extra":1}"#));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn bytes_that_are_not_json_are_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire_raw("not json"));
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_refusal_echoes_nothing_the_worker_sent() {
    let mut session = peer::negotiated();
    let marker = format!("SECRET-{}", "x".repeat(MAX_CALL_PAYLOAD_BYTES));
    session.feed(&wire(&call(3, "tool.run", json!(marker))));
    let outbox = session.drain_outbox();
    let text = serde_json::to_string(&outbox).expect("outbox serializes");
    assert!(
        !text.contains("SECRET-"),
        "the oversized payload must not come back in the refusal"
    );
    match outbox.as_slice() {
        [HostMessage::Reply(reply)] => match &reply.outcome {
            Outcome::Err { error } => {
                assert_eq!(error.kind, WireErrorKind::PayloadTooLarge);
                // The message is the kind's name and nothing else: no
                // path, no id, no type name, no fragment of the input.
                assert_eq!(error.message, "payload-too-large");
            }
            other => panic!("expected an error outcome, got {other:?}"),
        },
        other => panic!("expected the refusal reply, got {other:?}"),
    }
}

#[test]
fn a_fatal_goodbye_names_the_kind_and_nothing_else() {
    let mut session = peer::negotiated();
    session.feed(&wire_raw(
        r#"{"frame":"goodbye","reason":"/etc/passwd flavored garbage","extra":1}"#,
    ));
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Goodbye(goodbye)] => {
            assert_eq!(goodbye.reason, "protocol fault: invalid-frame");
        }
        other => panic!("expected the goodbye, got {other:?}"),
    }
}
