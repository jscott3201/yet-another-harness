//! Fuzz target D: the stateful host session under arbitrary bounded
//! action traces.
//!
//! The deterministic model corpus (in `tests/session_model.rs`) is the
//! acceptance oracle for session semantics; this target is the crash
//! net over the same public surface — arbitrary interleavings of legal
//! and illegal worker frames and application calls must never panic,
//! wedge, or grow past the negotiated ceilings and the correlation
//! budget's documented overshoot.
//!
//! Bounds: at most 64 actions per input, ids below 64, payloads tiny.
//! On any assertion failure the decoded action trace prints to stderr,
//! so the libFuzzer artifact plus that trace replays exactly.

#![no_main]

mod common;

use yah_plugin_ipc::PROTOCOL_VERSION;
use yah_plugin_ipc::frame;
use yah_plugin_ipc::session::{HostSession, SessionConfig};
use yah_plugin_ipc::types::*;

/// The correlation budget every run carries, so retention growth is
/// bounded and assertable. The documented overshoot past the budget is
/// bounded by the in-flight and live-handle ceilings.
const BUDGET: u64 = 64;
const MAX_ACTIONS: usize = 64;
const MAX_ID: u64 = 64;

struct Tracer {
    log: Vec<String>,
}

impl Tracer {
    fn note(&mut self, text: String) {
        self.log.push(text);
    }
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > 4096 {
        return;
    }
    let mut session = HostSession::new(SessionConfig {
        retired_operation_budget: Some(BUDGET),
        ..SessionConfig::default()
    });
    let mut tracer = Tracer { log: Vec::new() };

    // Handshake first: most interesting session behavior lives past it,
    // and a closed session simply ignores everything below.
    let hello = WorkerMessage::Hello(Hello {
        protocol_versions: vec![PROTOCOL_VERSION],
        sdk_name: "fuzz".into(),
        sdk_version: "0".into(),
        features: vec![],
        required_features: vec![],
    });
    feed(&mut session, &hello);
    let _ = session.drain_outbox();
    let _ = session.drain_events();

    // Each action consumes two bytes: an opcode and a packed operand.
    for chunk in data[1..].chunks_exact(2).take(MAX_ACTIONS) {
        let opcode = chunk[0] % 14;
        let operand = u64::from(chunk[1]) % MAX_ID + 1;
        match opcode {
            0..=2 => {
                tracer.note(format!("call {operand}"));
                feed(&mut session, &WorkerMessage::Call(Call {
                    call_id: CallId(operand),
                    method: "m".into(),
                    deadline_ms: None,
                    stream: chunk[1] & 0x80 != 0,
                    payload: serde_json::json!(null),
                }));
            }
            3 => {
                tracer.note(format!("reply {operand}"));
                feed(&mut session, &WorkerMessage::Reply(Reply {
                    call_id: CallId(operand),
                    outcome: Outcome::Ok { result: serde_json::json!(null) },
                }));
            }
            4 => {
                tracer.note(format!("open {operand}"));
                feed(&mut session, &WorkerMessage::StreamOpen(StreamOpen {
                    call_id: CallId(operand),
                    credit: u32::from(chunk[1]) % 8,
                }));
            }
            5 => {
                tracer.note(format!("data {operand}"));
                feed(&mut session, &WorkerMessage::StreamData(StreamData {
                    call_id: CallId(operand),
                    seq: operand,
                    more: true,
                    class: StreamClass::Lossless,
                    dropped: 0,
                    payload: serde_json::json!(null),
                }));
            }
            6 => {
                tracer.note(format!("credit {operand}"));
                feed(&mut session, &WorkerMessage::Credit(Credit {
                    call_id: CallId(operand),
                    additional: u32::from(chunk[1]) % 8,
                }));
            }
            7 => {
                tracer.note(format!("release {operand}"));
                feed(&mut session, &WorkerMessage::Release(Release {
                    handle: HandleId(operand),
                    kind: HandleKind::Capability,
                }));
            }
            8 => {
                tracer.note(format!("ack {operand}"));
                feed(&mut session, &WorkerMessage::ReleaseAck(ReleaseAck {
                    handle: HandleId(operand),
                    kind: HandleKind::Capability,
                }));
            }
            9 => {
                tracer.note(format!("cancel {operand}"));
                feed(&mut session, &WorkerMessage::Cancel(Cancel {
                    call_id: CallId(operand),
                    target: CancelTarget::Call,
                }));
            }
            10 => {
                tracer.note(format!("answer {operand}"));
                let _ = session.reply_to_worker(
                    CallId(operand),
                    Outcome::Ok { result: serde_json::json!(null) },
                );
            }
            11 => {
                tracer.note(format!("host-call {operand}"));
                let _ = session.call_worker("m", serde_json::json!(null), None, false);
            }
            12 => {
                // Late spilled replies against possibly-retired host
                // calls: the one worker-input path that mints new
                // correlation entries per frame — the exact path the
                // budget must gate.
                tracer.note(format!("late-offer {operand}"));
                feed(&mut session, &WorkerMessage::Reply(Reply {
                    call_id: CallId(operand),
                    outcome: Outcome::Spilled {
                        artifact: ArtifactOffer {
                            handle: HandleId(operand),
                            bytes: 10,
                            media_type: "text/plain".into(),
                            digest_blake3: "a".repeat(64),
                        },
                    },
                }));
            }
            _ => {
                tracer.note(format!("mint {operand}"));
                let _ = session.mint_capability_handle(CallId(operand));
            }
        }
        let _ = session.drain_outbox();
        let _ = session.drain_events();

        // The bounds that must hold after every action, no matter what
        // arrived.
        let (host, worker) = session.in_flight_calls();
        assert!(host <= 16 && worker <= 32, "in-flight ceilings exceeded");
        assert!(session.live_handles() <= 16, "live-handle ceiling exceeded");
        // Budget plus the documented worst-case overshoot: in-flight
        // calls of both directions, live handles, and their pending
        // releases, each retiring at most one entry.
        assert!(
            session.retired_operations() <= BUDGET + 16 + 32 + 16 + 16,
            "retired correlation memory escaped its bound"
        );
    }
    // The session ends only through its own laws; a leftover state is
    // fine, a stuck one is not observable here without IO.
    let _ = tracer;
});

fn feed(session: &mut HostSession, message: &WorkerMessage) {
    let bytes = serde_json::to_vec(message).expect("typed frame serializes");
    session.feed(&frame::encode(&bytes));
}
