#![no_main]

mod common;

use yah_plugin_ipc::frame;
use yah_plugin_ipc::types::*;

/// Build one well-typed worker message, every field within its declared
/// bound, chosen and filled from `data`. No recursion, no unbounded
/// allocation: the payload is capped well under every frame-class bound.
fn generate(data: &[u8]) -> WorkerMessage {
    let byte = |i: usize| -> u64 {
        data.get(i % data.len().max(1)).map(|b| u64::from(*b)).unwrap_or(0)
    };
    let call_id = CallId(byte(0) % 64 + 1);
    let payload = serde_json::json!({
        "n": byte(1) % 4096,
        "s": "x".repeat((byte(2) % 32) as usize),
    });
    match byte(3) % 6 {
        0 => WorkerMessage::Hello(Hello {
            protocol_versions: vec![yah_plugin_ipc::PROTOCOL_VERSION, byte(4) as u32 % 4],
            sdk_name: "fuzz".to_owned(),
            sdk_version: byte(5).to_string(),
            features: vec!["f".repeat((byte(6) % 8) as usize)],
            required_features: Vec::new(),
        }),
        1 => WorkerMessage::Call(Call {
            call_id,
            method: "m".repeat((byte(7) % 16) as usize + 1),
            deadline_ms: if byte(8) % 2 == 0 { Some(byte(9) as u32) } else { None },
            stream: byte(10) % 2 == 0,
            payload,
        }),
        2 => WorkerMessage::Reply(Reply {
            call_id,
            outcome: match byte(11) % 3 {
                0 => Outcome::Ok { result: payload },
                1 => Outcome::Err {
                    error: WireError {
                        kind: WireErrorKind::Internal,
                        message: "m".repeat((byte(12) % 64) as usize),
                        retryable: byte(13) % 2 == 0,
                        reconcile_required: false,
                    },
                },
                _ => Outcome::Cancelled {
                    reason: if byte(14) % 2 == 0 {
                        CancelReason::Requested
                    } else {
                        CancelReason::Enforced
                    },
                },
            },
        }),
        3 => WorkerMessage::StreamData(StreamData {
            call_id,
            seq: byte(15) % 1024,
            more: byte(16) % 2 == 0,
            class: if byte(17) % 2 == 0 {
                StreamClass::Lossless
            } else {
                StreamClass::Lossy
            },
            dropped: byte(18) % 1024,
            payload,
        }),
        4 => WorkerMessage::Credit(Credit {
            call_id,
            additional: byte(19) as u32 % 1024,
        }),
        _ => WorkerMessage::Goodbye(Goodbye {
            reason: "r".repeat((byte(20) % 256) as usize),
        }),
    }
}

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    if data.is_empty() || data.len() > common::MAX_INPUT_BYTES {
        return;
    }
    let message = generate(data);
    let wire = frame::encode(&serde_json::to_vec(&message).expect("typed frame serializes"));

    // The same bytes, three partitions: the decoded meaning must not move.
    let coalesced = common::decode_all(&[&wire]);
    let mut chunker = common::Chunker::seeded_from(data);
    let prng = chunker.partition(&wire, 16);
    let prng = common::decode_all(&prng);
    let bytewise: Vec<&[u8]> = wire.iter().map(|b| std::slice::from_ref(b)).collect();
    let bytewise = common::decode_all(&bytewise);
    let expected = common::StreamClass::Frames(vec![wire[4..].to_vec()]);
    assert_eq!(coalesced, expected);
    assert_eq!(prng, expected);
    assert_eq!(bytewise, expected);

    // The delivered frame passes strict admission and decodes back to the
    // exact message that was sent.
    let json = yah_plugin_ipc::strict::parse(&wire[4..]).expect("a typed frame is strict JSON");
    let decoded: WorkerMessage =
        serde_json::from_value(json).expect("a typed frame decodes to its type");
    assert_eq!(decoded, message, "the round trip changed the message");
});
