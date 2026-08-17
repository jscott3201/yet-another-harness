//! Negotiation is the door, and the door is the whole wall.
//!
//! Every fixture here ends a session that never got started properly:
//! wrong first frame, wrong version, unknown required feature, a second
//! hello, or a frame stream that violated its bounds before meaning
//! anything. The one positive case pins exactly what an accept announces.

#[path = "support/peer.rs"]
mod peer;

use peer::{assert_fatal, hello, wire};
use yah_plugin_ipc::session::{HostSession, SessionConfig, SessionEvent};
use yah_plugin_ipc::types::*;
use yah_plugin_ipc::{MAX_CONTROL_FRAME_BYTES, MAX_FRAME_BYTES, PROTOCOL_VERSION};

#[test]
fn the_first_frame_must_be_a_hello() {
    let mut session = HostSession::new(SessionConfig::default());
    session.feed(&wire(&peer::call(1, "tool.run", serde_json::json!({}))));
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Refuse(refuse)] => {
            assert_eq!(refuse.error.kind, WireErrorKind::NegotiationRequired);
            assert_eq!(refuse.supported_versions, vec![PROTOCOL_VERSION]);
        }
        other => panic!("expected a refuse, got {other:?}"),
    }
    assert_fatal(&mut session, WireErrorKind::NegotiationRequired);
}

#[test]
fn an_unsupported_version_is_refused_with_the_supported_list() {
    let mut session = HostSession::new(SessionConfig::default());
    session.feed(&wire(&WorkerMessage::Hello(Hello {
        protocol_versions: vec![99],
        sdk_name: "scripted".to_owned(),
        sdk_version: "0.0.0".to_owned(),
        features: Vec::new(),
        required_features: Vec::new(),
    })));
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Refuse(refuse)] => {
            assert_eq!(refuse.error.kind, WireErrorKind::UnsupportedVersion);
            // The refusal names what the host does speak: the worker fails
            // with a diagnostic, not a bare close.
            assert_eq!(refuse.supported_versions, vec![PROTOCOL_VERSION]);
        }
        other => panic!("expected a refuse, got {other:?}"),
    }
    assert_fatal(&mut session, WireErrorKind::UnsupportedVersion);
}

#[test]
fn an_unknown_required_feature_fails_closed() {
    let mut session = HostSession::new(SessionConfig::default());
    session.feed(&wire(&WorkerMessage::Hello(Hello {
        protocol_versions: vec![PROTOCOL_VERSION],
        sdk_name: "scripted".to_owned(),
        sdk_version: "0.0.0".to_owned(),
        features: Vec::new(),
        required_features: vec!["time-travel".to_owned()],
    })));
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Refuse(refuse)] => {
            assert_eq!(refuse.error.kind, WireErrorKind::UnknownRequiredFeature);
        }
        other => panic!("expected a refuse, got {other:?}"),
    }
    assert_fatal(&mut session, WireErrorKind::UnknownRequiredFeature);
}

#[test]
fn the_accept_announces_limits_ceilings_and_the_feature_intersection() {
    let config = SessionConfig {
        features: vec!["a".to_owned(), "b".to_owned()],
        ..SessionConfig::default()
    };
    let mut session = HostSession::new(config);
    session.feed(&wire(&WorkerMessage::Hello(Hello {
        protocol_versions: vec![PROTOCOL_VERSION],
        sdk_name: "scripted".to_owned(),
        sdk_version: "1.2.3".to_owned(),
        features: vec!["b".to_owned(), "c".to_owned()],
        required_features: vec!["a".to_owned()],
    })));
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Accept(accept)] => {
            assert_eq!(accept.protocol_version, PROTOCOL_VERSION);
            // In force: what both sides know. The worker's "c" is not
            // granted by being asked for, and "a" — named only as a hard
            // requirement — is in force, not silently withheld.
            assert_eq!(accept.features, vec!["a".to_owned(), "b".to_owned()]);
            assert_eq!(accept.limits.max_frame_bytes, MAX_FRAME_BYTES as u64);
            assert_eq!(
                accept.ceilings.live_handles,
                yah_plugin_ipc::DEFAULT_LIVE_HANDLES
            );
            assert_eq!(
                accept.ceilings.max_stream_credit,
                yah_plugin_ipc::MAX_STREAM_CREDIT
            );
        }
        other => panic!("expected the accept, got {other:?}"),
    }
    let events = session.drain_events();
    assert_eq!(
        events,
        vec![SessionEvent::Negotiated {
            sdk_name: "scripted".to_owned(),
            sdk_version: "1.2.3".to_owned(),
        }]
    );
    assert!(!session.is_closed());
}

#[test]
fn an_sdk_identity_outside_its_length_bound_is_fatal() {
    for sdk_name in ["", &"n".repeat(65)] {
        let mut session = HostSession::new(SessionConfig::default());
        session.feed(&wire(&WorkerMessage::Hello(Hello {
            protocol_versions: vec![PROTOCOL_VERSION],
            sdk_name: sdk_name.to_owned(),
            sdk_version: "0.0.0".to_owned(),
            features: Vec::new(),
            required_features: Vec::new(),
        })));
        assert!(
            session.drain_outbox().is_empty(),
            "no accept for a hello the schema forbids"
        );
        assert_fatal(&mut session, WireErrorKind::InvalidFrame);
    }
}

#[test]
fn a_negotiation_fatal_detail_is_bounded() {
    let mut session = HostSession::new(SessionConfig::default());
    // The refusal's event detail quotes the unknown feature; a worker must
    // not be able to write unbounded text into the host's event log.
    session.feed(&wire(&WorkerMessage::Hello(Hello {
        protocol_versions: vec![PROTOCOL_VERSION],
        sdk_name: "scripted".to_owned(),
        sdk_version: "0.0.0".to_owned(),
        features: Vec::new(),
        required_features: vec!["f".repeat(5000)],
    })));
    session.drain_outbox();
    let (kind, detail) = peer::sole_fatal(&mut session);
    assert_eq!(kind, WireErrorKind::UnknownRequiredFeature);
    assert!(
        detail.chars().count() <= 512,
        "detail must be clipped, got {} chars",
        detail.chars().count()
    );
}

#[test]
fn a_second_hello_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&hello()));
    assert_fatal(&mut session, WireErrorKind::NegotiationRequired);
}

#[test]
fn an_oversize_control_frame_is_fatal() {
    let mut session = peer::negotiated();
    let padding = "x".repeat(MAX_CONTROL_FRAME_BYTES);
    session.feed(&wire(&WorkerMessage::Goodbye(Goodbye { reason: padding })));
    assert_fatal(&mut session, WireErrorKind::FrameTooLarge);
}

#[test]
fn an_oversize_length_prefix_is_refused_before_any_payload_byte() {
    let mut session = peer::negotiated();
    // Four prefix bytes declaring more than the bound, and nothing else:
    // the refusal cannot have read a payload that was never sent.
    let declared = (MAX_FRAME_BYTES as u32) + 1;
    session.feed(&declared.to_be_bytes());
    assert_fatal(&mut session, WireErrorKind::FrameTooLarge);
}

#[test]
fn a_zero_length_frame_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&0u32.to_be_bytes());
    assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}

#[test]
fn a_frame_split_across_feeds_is_ordinary() {
    let mut session = HostSession::new(SessionConfig::default());
    let bytes = wire(&hello());
    // One byte at a time: chunk boundaries are transport trivia, not
    // protocol structure.
    for byte in &bytes {
        session.feed(std::slice::from_ref(byte));
    }
    let outbox = session.drain_outbox();
    assert!(matches!(outbox.as_slice(), [HostMessage::Accept(_)]));
}

#[test]
fn goodbye_before_hello_is_still_a_negotiation_fault() {
    let mut session = HostSession::new(SessionConfig::default());
    session.feed(&wire(&WorkerMessage::Goodbye(Goodbye {
        reason: "changed my mind".to_owned(),
    })));
    assert_fatal(&mut session, WireErrorKind::NegotiationRequired);
}
