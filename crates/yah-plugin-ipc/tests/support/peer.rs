//! The scripted worker: helpers every fixture file shares.
//!
//! There is no process and no IO. A fixture builds the exact
//! [`WorkerMessage`] a worker would send, frames it, and feeds the bytes to
//! a [`HostSession`]; what the session queued and established is then
//! asserted directly. Chunk boundaries, races, and malformed inputs are all
//! just byte sequences here, which is what makes every negative fixture
//! deterministic.

#![allow(dead_code)]

use yah_plugin_ipc::frame;
use yah_plugin_ipc::session::{HostSession, SessionConfig, SessionEvent};
use yah_plugin_ipc::types::*;

/// Frame one worker message for the wire.
pub fn wire(message: &WorkerMessage) -> Vec<u8> {
    frame::encode(&serde_json::to_vec(message).expect("worker message serializes"))
}

/// Frame raw JSON text, bypassing the typed layer — for fixtures that need
/// bytes no well-typed worker could produce.
pub fn wire_raw(json: &str) -> Vec<u8> {
    frame::encode(json.as_bytes())
}

/// A hello the default host config accepts.
pub fn hello() -> WorkerMessage {
    WorkerMessage::Hello(Hello {
        protocol_versions: vec![yah_plugin_ipc::PROTOCOL_VERSION],
        sdk_name: "scripted".to_owned(),
        sdk_version: "0.0.0".to_owned(),
        features: Vec::new(),
        required_features: Vec::new(),
    })
}

/// A session past negotiation, with the accept already drained.
pub fn negotiated() -> HostSession {
    negotiated_with(SessionConfig::default())
}

pub fn negotiated_with(config: SessionConfig) -> HostSession {
    let mut session = HostSession::new(config);
    session.feed(&wire(&hello()));
    let outbox = session.drain_outbox();
    assert!(
        matches!(outbox.as_slice(), [HostMessage::Accept(_)]),
        "negotiation must produce exactly the accept: {outbox:?}"
    );
    let events = session.drain_events();
    assert!(
        matches!(events.as_slice(), [SessionEvent::Negotiated { .. }]),
        "negotiation must establish exactly one fact: {events:?}"
    );
    session
}

/// A worker call frame with the boring fields defaulted.
pub fn call(id: u64, method: &str, payload: serde_json::Value) -> WorkerMessage {
    WorkerMessage::Call(Call {
        call_id: CallId(id),
        method: method.to_owned(),
        deadline_ms: None,
        stream: false,
        payload,
    })
}

/// A worker terminal for a host call.
pub fn reply_ok(id: CallId, result: serde_json::Value) -> WorkerMessage {
    WorkerMessage::Reply(Reply {
        call_id: id,
        outcome: Outcome::Ok { result },
    })
}

/// The one fatal event, or a panic naming what was there instead.
pub fn sole_fatal(session: &mut HostSession) -> (WireErrorKind, String) {
    let events = session.drain_events();
    match events.as_slice() {
        [SessionEvent::Fatal { kind, detail }] => (*kind, detail.clone()),
        other => panic!("expected exactly one fatal event, got {other:?}"),
    }
}

/// Assert the session closed fatally with `kind`, tolerating earlier
/// non-fatal events in the drain.
pub fn assert_fatal(session: &mut HostSession, kind: WireErrorKind) {
    assert!(session.is_closed(), "session must be closed");
    let events = session.drain_events();
    let fatal = events.iter().find_map(|event| match event {
        SessionEvent::Fatal { kind, .. } => Some(*kind),
        _ => None,
    });
    assert_eq!(fatal, Some(kind), "events were {events:?}");
}
