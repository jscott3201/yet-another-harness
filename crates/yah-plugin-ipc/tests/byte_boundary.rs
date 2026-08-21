//! Byte-boundary properties of the incremental frame decoder.
//!
//! The 113 session fixtures pin what a decoded frame means; this file pins
//! what the byte-facing half does under input no well-formed peer would
//! send. The load-bearing claims: the four-byte prefix is authoritative;
//! zero and over-bound declarations are refused from the prefix alone;
//! fragmentation and coalescing cannot change what a stream classifies as;
//! and one framing violation poisons the decoder terminally, so a valid
//! frame riding behind a violation is never delivered.

use yah_plugin_ipc::frame::{EndOfInput, FrameDecoder, FrameStreamError, encode};
use yah_plugin_ipc::{MAX_FRAME_BYTES, PROTOCOL_VERSION};

/// A small legal frame: a hello, the one message every session starts with.
fn legal_frame() -> Vec<u8> {
    let json = format!(
        r#"{{"frame":"hello","protocol_versions":[{PROTOCOL_VERSION}],"sdk_name":"probe","sdk_version":"0.0.0","features":[],"required_features":[]}}"#
    );
    encode(json.as_bytes())
}

/// A second, different legal frame, so multi-frame streams are not one
/// frame repeated.
fn second_legal_frame() -> Vec<u8> {
    encode(br#"{"frame":"goodbye","reason":"done"}"#)
}

/// What one run over a chunked stream produced, collapsed to the
/// classification the fragmentation-invariance property compares.
#[derive(Debug, PartialEq, Eq, Clone)]
enum Outcome {
    /// Every frame arrived whole, in order, and the stream ended clean.
    Frames(Vec<Vec<u8>>),
    /// The decoder refused the stream; the exact error is the identity.
    Poisoned(FrameStreamError),
    /// The stream ended mid-prefix or mid-frame without a violation.
    Truncated(EndOfInput),
}

/// Feed `stream` to a fresh decoder in the given chunks, draining after
/// every feed.
fn run(chunks: &[&[u8]]) -> Outcome {
    let mut decoder = FrameDecoder::new();
    let mut frames = Vec::new();
    for chunk in chunks {
        decoder.feed(chunk);
        loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => {
                    assert!(
                        !frame.is_empty() && frame.len() <= MAX_FRAME_BYTES,
                        "a delivered frame must respect the prefix bound"
                    );
                    frames.push(frame);
                }
                Ok(None) => break,
                Err(error) => return Outcome::Poisoned(error),
            }
        }
        assert!(
            decoder.buffered_len() <= 4 + MAX_FRAME_BYTES,
            "retained state must stay within one maximal frame plus one prefix"
        );
    }
    match decoder.finish() {
        EndOfInput::Clean => {
            assert!(!frames.is_empty() || chunks.iter().all(|chunk| chunk.is_empty()));
            Outcome::Frames(frames)
        }
        truncated => Outcome::Truncated(truncated),
    }
}

/// Split `stream` into a first piece of `split` bytes and the rest.
fn two_way(stream: &[u8], split: usize) -> Vec<&[u8]> {
    vec![&stream[..split], &stream[split..]]
}

/// Split `stream` into one-byte chunks.
fn bytewise(stream: &[u8]) -> Vec<&[u8]> {
    stream.iter().map(std::slice::from_ref).collect()
}

#[test]
fn a_legal_frame_is_delivered_identically_under_its_first_four_splits() {
    let stream = legal_frame();
    for split in 0..=4 {
        let outcome = run(&two_way(&stream, split));
        assert_eq!(
            outcome,
            Outcome::Frames(vec![stream[4..].to_vec()]),
            "prefix split at {split} must not change what the stream means"
        );
    }
}

#[test]
fn every_single_split_point_of_a_representative_stream_agrees() {
    let mut stream = legal_frame();
    stream.extend_from_slice(&second_legal_frame());
    let expected = Outcome::Frames(vec![
        stream[4..legal_frame().len()].to_vec(),
        second_legal_frame()[4..].to_vec(),
    ]);
    for split in 0..=stream.len() {
        let outcome = run(&two_way(&stream, split));
        assert_eq!(outcome, expected, "split at {split}");
    }
}

#[test]
fn one_byte_chunks_across_a_multi_frame_stream_deliver_every_frame() {
    let mut stream = legal_frame();
    stream.extend_from_slice(&second_legal_frame());
    let outcome = run(&bytewise(&stream));
    assert_eq!(
        outcome,
        Outcome::Frames(vec![
            stream[4..legal_frame().len()].to_vec(),
            second_legal_frame()[4..].to_vec(),
        ])
    );
}

#[test]
fn coalesced_frames_in_one_feed_deliver_every_frame() {
    let mut stream = legal_frame();
    stream.extend_from_slice(&second_legal_frame());
    let outcome = run(&[&stream]);
    assert_eq!(
        outcome,
        Outcome::Frames(vec![
            stream[4..legal_frame().len()].to_vec(),
            second_legal_frame()[4..].to_vec(),
        ])
    );
}

#[test]
fn a_zero_declaration_is_refused_from_the_prefix_alone_under_every_prefix_split() {
    let prefix = 0_u32.to_be_bytes();
    for split in 0..=4 {
        let mut decoder = FrameDecoder::new();
        decoder.feed(&prefix[..split]);
        // The refusal must not wait for a payload that never comes.
        if split < 4 {
            assert_eq!(decoder.next_frame(), Ok(None), "split at {split}");
            decoder.feed(&prefix[split..]);
        }
        assert_eq!(
            decoder.next_frame(),
            Err(FrameStreamError::EmptyFrame),
            "split at {split}"
        );
    }
}

#[test]
fn an_over_bound_declaration_is_refused_from_the_prefix_alone() {
    let declared = (MAX_FRAME_BYTES + 1) as u64;
    let prefix = u32::try_from(declared).unwrap().to_be_bytes();
    // Prefix only: no payload byte has arrived, and the refusal is made.
    let mut decoder = FrameDecoder::new();
    decoder.feed(&prefix);
    assert_eq!(
        decoder.next_frame(),
        Err(FrameStreamError::FrameTooLarge { declared })
    );
    // The same refusal with some payload riding behind the prefix.
    let mut decoder = FrameDecoder::new();
    decoder.feed(&prefix);
    decoder.feed(b"partial payload");
    assert_eq!(
        decoder.next_frame(),
        Err(FrameStreamError::FrameTooLarge { declared })
    );
}

#[test]
fn the_maximal_frame_bound_is_admitted_and_one_over_it_is_not() {
    // The bound itself is legal; the paired case is the refusal above.
    let payload = vec![b'x'; MAX_FRAME_BYTES];
    let stream = encode(&payload);
    let outcome = run(&[&stream]);
    assert_eq!(outcome, Outcome::Frames(vec![payload]));
}

#[test]
fn poison_is_terminal_idempotent_and_suffix_insensitive() {
    let mut decoder = FrameDecoder::new();
    decoder.feed(&[0, 0, 0, 0]);
    let first = decoder.next_frame().unwrap_err();
    // Bytes of a perfectly legal frame arrive after the violation.
    decoder.feed(&legal_frame());
    // The same error repeats exactly, and the suffix is never delivered.
    for _ in 0..3 {
        assert_eq!(decoder.next_frame(), Err(first.clone()));
    }
    // A poisoned decoder retains nothing and stays at nothing.
    assert_eq!(decoder.buffered_len(), 0);
    assert_eq!(decoder.finish(), EndOfInput::Clean);
}

#[test]
fn a_valid_frame_after_a_violation_is_never_delivered() {
    let mut decoder = FrameDecoder::new();
    decoder.feed(&[0, 0, 0, 0]);
    assert!(decoder.next_frame().is_err());
    decoder.feed(&legal_frame());
    decoder.feed(&second_legal_frame());
    assert!(matches!(
        decoder.next_frame(),
        Err(FrameStreamError::EmptyFrame)
    ));
    assert_eq!(decoder.buffered_len(), 0);
}

#[test]
fn truncation_is_classified_never_poisoned_and_completable() {
    let stream = legal_frame();
    // Mid-prefix: each partial prefix classifies with what it has.
    for have in 1..4 {
        let mut decoder = FrameDecoder::new();
        decoder.feed(&stream[..have]);
        assert!(decoder.next_frame() == Ok(None));
        assert_eq!(
            decoder.finish(),
            EndOfInput::TruncatedPrefix { have },
            "truncated prefix with {have} bytes"
        );
        // Completing the stream delivers the frame; truncation left no scar.
        decoder.feed(&stream[have..]);
        assert_eq!(decoder.next_frame().unwrap().unwrap(), stream[4..].to_vec());
        assert_eq!(decoder.finish(), EndOfInput::Clean);
    }
    // Mid-frame: the declaration is legal, the body is short.
    let declared = stream.len() - 4;
    for have in 0..declared {
        let mut decoder = FrameDecoder::new();
        decoder.feed(&stream);
        decoder.next_frame().unwrap();
        let kept = decoder.buffered_len();
        let _ = kept;
        let mut partial = FrameDecoder::new();
        partial.feed(&stream[..4 + have]);
        assert!(partial.next_frame() == Ok(None));
        assert_eq!(
            partial.finish(),
            EndOfInput::TruncatedFrame { declared, have },
            "truncated frame with {have} of {declared} body bytes"
        );
    }
}

#[test]
fn fragmentation_cannot_change_classification() {
    // Every malformed shape the corpus inventory pins, plus its legal
    // control, classified identically under every split point.
    let cases: Vec<(Vec<u8>, Outcome)> = vec![
        (legal_frame(), {
            let frame = legal_frame();
            Outcome::Frames(vec![frame[4..].to_vec()])
        }),
        (Vec::new(), Outcome::Frames(Vec::new())),
        (
            0_u32.to_be_bytes().to_vec(),
            Outcome::Poisoned(FrameStreamError::EmptyFrame),
        ),
        (
            u32::MAX.to_be_bytes().to_vec(),
            Outcome::Poisoned(FrameStreamError::FrameTooLarge {
                declared: u32::MAX as u64,
            }),
        ),
        (
            ((MAX_FRAME_BYTES + 1) as u32).to_be_bytes().to_vec(),
            Outcome::Poisoned(FrameStreamError::FrameTooLarge {
                declared: (MAX_FRAME_BYTES + 1) as u64,
            }),
        ),
        (
            b"abc".to_vec(),
            Outcome::Truncated(EndOfInput::TruncatedPrefix { have: 3 }),
        ),
        (
            b"ab".to_vec(),
            Outcome::Truncated(EndOfInput::TruncatedPrefix { have: 2 }),
        ),
        (
            {
                let mut partial = 7_u32.to_be_bytes().to_vec();
                partial.extend_from_slice(b"123");
                partial
            },
            Outcome::Truncated(EndOfInput::TruncatedFrame {
                declared: 7,
                have: 3,
            }),
        ),
    ];
    for (stream, expected) in &cases {
        for split in 0..=stream.len() {
            let outcome = run(&two_way(stream, split));
            assert_eq!(&outcome, expected, "stream {stream:?} split at {split}");
        }
        let outcome = run(&bytewise(stream));
        assert_eq!(&outcome, expected, "stream {stream:?} bytewise");
        let outcome = run(&[stream]);
        assert_eq!(&outcome, expected, "stream {stream:?} coalesced");
    }
}

#[test]
fn retained_state_stays_bounded_across_a_maximal_frame_and_trailing_junk() {
    // One maximal frame followed by junk no decoder should retain past its
    // documented bound, all in a single feed.
    let payload = vec![b'x'; MAX_FRAME_BYTES];
    let mut stream = encode(&payload);
    stream.extend_from_slice(b"trailing junk that is not a frame");
    let mut decoder = FrameDecoder::new();
    decoder.feed(&stream);
    assert_eq!(decoder.next_frame().unwrap().unwrap(), payload);
    assert_eq!(
        decoder.buffered_len(),
        stream.len() - stream.len().min(4 + MAX_FRAME_BYTES),
        "only the unconsumed suffix is retained"
    );
    assert!(matches!(
        decoder.finish(),
        EndOfInput::TruncatedFrame { .. }
    ));
}

#[test]
fn an_empty_feed_changes_nothing() {
    let mut decoder = FrameDecoder::new();
    decoder.feed(b"");
    assert_eq!(decoder.next_frame(), Ok(None));
    assert_eq!(decoder.finish(), EndOfInput::Clean);
    let stream = legal_frame();
    let mut decoder = FrameDecoder::new();
    decoder.feed(&stream[..2]);
    decoder.feed(b"");
    decoder.feed(&stream[2..]);
    assert_eq!(decoder.next_frame().unwrap().unwrap(), stream[4..].to_vec());
    assert_eq!(decoder.finish(), EndOfInput::Clean);
}
