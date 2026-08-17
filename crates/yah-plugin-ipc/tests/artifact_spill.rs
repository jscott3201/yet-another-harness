//! Artifact spill: fail closed, then spill; pull-read in bounded chunks;
//! close is mandatory and disconnect reclaims what close never did.

#[path = "support/peer.rs"]
mod peer;

use peer::{call, wire};
use serde_json::json;
use yah_plugin_ipc::session::{AppError, HostSession, SessionConfig, SessionEvent};
use yah_plugin_ipc::types::*;
use yah_plugin_ipc::{MAX_ARTIFACT_READ_BYTES, MAX_INLINE_RESULT_BYTES};

/// A worker call the host will answer with a spilled artifact of `bytes`.
fn spilled(session: &mut HostSession, content: &[u8]) -> ArtifactOffer {
    session.feed(&wire(&call(1, "tool.big", json!(null))));
    session.drain_events();
    let offer = session
        .offer_artifact(CallId(1), content.to_vec(), "text/plain")
        .expect("offer is minted");
    session
        .reply_to_worker(
            CallId(1),
            Outcome::Spilled {
                artifact: offer.clone(),
            },
        )
        .expect("spilled reply goes out");
    session.drain_outbox();
    offer
}

fn read_frame(id: u64, handle: HandleId, offset: u64, len: u64) -> Vec<u8> {
    wire(&call(
        id,
        "artifact.read",
        json!({"handle": handle, "offset": offset, "len": len}),
    ))
}

/// The hex chunk out of the last reply in the outbox.
fn chunk(session: &mut HostSession) -> Vec<u8> {
    let outbox = session.drain_outbox();
    match outbox.as_slice() {
        [HostMessage::Reply(reply)] => match &reply.outcome {
            Outcome::Ok { result } => {
                let hex = result["bytes_hex"].as_str().expect("chunk carries hex");
                (0..hex.len())
                    .step_by(2)
                    .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex"))
                    .collect()
            }
            other => panic!("expected a chunk, got {other:?}"),
        },
        other => panic!("expected one reply, got {other:?}"),
    }
}

#[test]
fn an_oversize_inline_reply_is_refused_toward_the_spill_path() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(1, "tool.big", json!(null))));
    session.drain_events();
    let oversize = "x".repeat(MAX_INLINE_RESULT_BYTES + 1);
    // Fail closed, then spill: the session will not put a truncated result
    // on the wire, and it will not put this one on either.
    match session.reply_to_worker(
        CallId(1),
        Outcome::Ok {
            result: json!(oversize),
        },
    ) {
        Err(AppError::SpillRequired { bytes }) => assert!(bytes > MAX_INLINE_RESULT_BYTES),
        other => panic!("expected the spill refusal, got {other:?}"),
    }
    let offer = session
        .offer_artifact(
            CallId(1),
            vec![7u8; MAX_INLINE_RESULT_BYTES + 1],
            "text/plain",
        )
        .expect("the same call can offer instead");
    session
        .reply_to_worker(CallId(1), Outcome::Spilled { artifact: offer })
        .expect("the spilled reply goes out");
}

#[test]
fn pull_reads_reassemble_the_content_and_release_closes_it() {
    let mut session = peer::negotiated();
    let content: Vec<u8> = (0..60_000u32).map(|i| (i % 251) as u8).collect();
    let offer = spilled(&mut session, &content);
    assert_eq!(offer.bytes, content.len() as u64);
    assert_eq!(session.live_handles(), 1, "the offer is a live handle");

    let mut got = Vec::new();
    let mut next_call = 10u64;
    while got.len() < content.len() {
        let want = (content.len() - got.len()).min(MAX_ARTIFACT_READ_BYTES) as u64;
        session.feed(&read_frame(next_call, offer.handle, got.len() as u64, want));
        got.extend_from_slice(&chunk(&mut session));
        next_call += 1;
    }
    assert_eq!(got, content, "chunks reassemble the exact bytes");

    session.feed(&wire(&WorkerMessage::Release(Release {
        handle: offer.handle,
        kind: HandleKind::Artifact,
    })));
    let outbox = session.drain_outbox();
    assert!(
        matches!(outbox.as_slice(), [HostMessage::ReleaseAck(ack)] if ack.handle == offer.handle),
        "release is acknowledged: {outbox:?}"
    );
    assert_eq!(
        session.live_handles(),
        0,
        "read while live: the close closed"
    );
}

#[test]
fn reads_outside_the_offer_are_refused() {
    let mut session = peer::negotiated();
    let offer = spilled(&mut session, &[1, 2, 3, 4]);
    for (id, offset, len) in [
        (10u64, 4u64, 1u64), // past the end
        (11, 0, 5),          // over the end
        (12, 0, 0),          // empty read
    ] {
        session.feed(&read_frame(id, offer.handle, offset, len));
    }
    let refused: Vec<WireErrorKind> = session
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            SessionEvent::CallRefused { kind, .. } => Some(kind),
            _ => None,
        })
        .collect();
    assert_eq!(refused, vec![WireErrorKind::InvalidRead; 3]);
    assert!(
        !session.is_closed(),
        "bad reads refuse the call, not the wire"
    );
}

#[test]
fn a_read_over_the_chunk_bound_is_refused_inside_a_larger_artifact() {
    let mut session = peer::negotiated();
    // The artifact is bigger than the chunk bound, so only the bound —
    // not the artifact's end — can refuse this read.
    let content = vec![7u8; MAX_ARTIFACT_READ_BYTES + 1];
    let offer = spilled(&mut session, &content);
    session.feed(&read_frame(
        10,
        offer.handle,
        0,
        MAX_ARTIFACT_READ_BYTES as u64 + 1,
    ));
    let events = session.drain_events();
    assert!(
        matches!(
            events.as_slice(),
            [SessionEvent::CallRefused {
                kind: WireErrorKind::InvalidRead,
                ..
            }]
        ),
        "the chunk bound must refuse it: {events:?}"
    );
    session.drain_outbox();
    // The pair half: the same offset at the bound is served.
    session.feed(&read_frame(
        11,
        offer.handle,
        0,
        MAX_ARTIFACT_READ_BYTES as u64,
    ));
    assert_eq!(chunk(&mut session).len(), MAX_ARTIFACT_READ_BYTES);
}

#[test]
fn a_served_read_retires_its_call_id() {
    let mut session = peer::negotiated();
    let offer = spilled(&mut session, &[9; 8]);
    session.feed(&read_frame(10, offer.handle, 0, 8));
    assert_eq!(chunk(&mut session).len(), 8, "the read was served");
    // Served is answered: the id is spent, and reuse breaks correlation
    // exactly as any other duplicate does.
    session.feed(&wire(&call(10, "tool.run", json!(null))));
    peer::assert_fatal(&mut session, WireErrorKind::DuplicateCall);
}

#[test]
fn a_malformed_read_payload_is_refused_on_that_call_only() {
    let mut session = peer::negotiated();
    let _offer = spilled(&mut session, &[9; 8]);
    session.feed(&wire(&call(10, "artifact.read", json!({"nope": 1}))));
    let events = session.drain_events();
    assert!(
        matches!(
            events.as_slice(),
            [SessionEvent::CallRefused {
                kind: WireErrorKind::InvalidFrame,
                ..
            }]
        ),
        "a read the shape rejects refuses the call: {events:?}"
    );
    assert!(!session.is_closed());
}

#[test]
fn a_read_against_a_capability_handle_is_refused() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(1, "cap.acquire", json!(null))));
    session.drain_events();
    let handle = session
        .mint_capability_handle(CallId(1))
        .expect("grant is minted");
    // A capability handle names no bytes; reading it is a category error
    // answered as an unknown handle, not a tour of what the id maps to.
    session.feed(&read_frame(10, handle, 0, 1));
    let events = session.drain_events();
    assert!(
        matches!(
            events.as_slice(),
            [SessionEvent::CallRefused {
                kind: WireErrorKind::UnknownHandle,
                ..
            }]
        ),
        "got {events:?}"
    );
}

#[test]
fn a_read_against_a_released_handle_is_refused() {
    let mut session = peer::negotiated();
    let offer = spilled(&mut session, &[9; 16]);
    session.feed(&wire(&WorkerMessage::Release(Release {
        handle: offer.handle,
        kind: HandleKind::Artifact,
    })));
    session.drain_outbox();
    session.drain_events();
    session.feed(&read_frame(20, offer.handle, 0, 8));
    let events = session.drain_events();
    assert!(
        matches!(
            events.as_slice(),
            [SessionEvent::CallRefused {
                kind: WireErrorKind::UnknownHandle,
                ..
            }]
        ),
        "released means gone: {events:?}"
    );
}

#[test]
fn an_offer_media_type_outside_its_length_bound_is_refused() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(1, "tool.big", json!(null))));
    session.drain_events();
    session
        .offer_artifact(CallId(1), vec![7; 4], &"m".repeat(128))
        .expect("a media type at the bound is minted");
    // Unbounded, this string rides the spilled reply past the frame bound
    // and into the encoder's assert — the send side must refuse it first.
    for media_type in [String::new(), "m".repeat(129)] {
        assert_eq!(
            session
                .offer_artifact(CallId(1), vec![7; 4], &media_type)
                .err(),
            Some(AppError::InvalidField(
                "media type outside its length bound"
            ))
        );
    }
}

#[test]
fn an_empty_artifact_offer_is_refused() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(1, "tool.big", json!(null))));
    session.drain_events();
    // Both directions agree a spill of zero bytes is not a spill: inbound
    // it is fatal, outbound it is the application's refusal to hear.
    assert_eq!(
        session
            .offer_artifact(CallId(1), Vec::new(), "text/plain")
            .err(),
        Some(AppError::InvalidField("spilled artifact of zero bytes"))
    );
}

#[test]
fn a_hand_built_spilled_reply_passes_the_same_admission() {
    let mut session = peer::negotiated();
    session.feed(&wire(&call(1, "tool.big", json!(null))));
    session.drain_events();
    // `offer_artifact` mints conforming offers, but the reply API accepts
    // any outcome — so a hand-built offer is held to the inbound bounds.
    let zero_handle = ArtifactOffer {
        handle: HandleId(0),
        bytes: 4,
        media_type: "text/plain".to_owned(),
        digest_blake3: "ab".repeat(32),
    };
    assert_eq!(
        session.reply_to_worker(
            CallId(1),
            Outcome::Spilled {
                artifact: zero_handle
            }
        ),
        Err(AppError::InvalidField("handle id outside wire range"))
    );
    let uppercase_digest = ArtifactOffer {
        handle: HandleId(3),
        bytes: 4,
        media_type: "text/plain".to_owned(),
        digest_blake3: "AB".repeat(32),
    };
    assert_eq!(
        session.reply_to_worker(
            CallId(1),
            Outcome::Spilled {
                artifact: uppercase_digest
            }
        ),
        Err(AppError::InvalidField(
            "digest is not 64 lowercase hex characters"
        ))
    );
}

#[test]
fn a_read_is_admitted_through_the_same_ceiling_as_any_call() {
    let mut config = SessionConfig::default();
    config.ceilings.worker_calls_in_flight = 1;
    let mut session = peer::negotiated_with(config);
    let offer = spilled(&mut session, &[9; 8]);
    // One slot, and a plain call holds it: the served read clears the same
    // admission bar as every other call, then never occupies the slot.
    session.feed(&wire(&call(20, "tool.run", json!(null))));
    session.drain_events();
    session.feed(&read_frame(21, offer.handle, 0, 8));
    let events = session.drain_events();
    assert!(
        matches!(
            events.as_slice(),
            [SessionEvent::CallRefused {
                kind: WireErrorKind::ResourceExhausted,
                ..
            }]
        ),
        "a full table refuses reads too, retryably: {events:?}"
    );
    session.drain_outbox();
    session
        .reply_to_worker(
            CallId(20),
            Outcome::Ok {
                result: json!(null),
            },
        )
        .expect("settling makes room");
    session.drain_outbox();
    session.feed(&read_frame(22, offer.handle, 0, 8));
    assert_eq!(
        chunk(&mut session).len(),
        8,
        "below the ceiling the read serves"
    );
}

#[test]
fn a_disconnect_reclaims_an_offer_that_was_never_closed() {
    let mut session = peer::negotiated();
    let _offer = spilled(&mut session, &[5; 32]);
    assert_eq!(session.live_handles(), 1);
    session.end_of_input();
    let events = session.drain_events();
    assert!(
        events.contains(&SessionEvent::HandlesReclaimed { count: 1 }),
        "the close that never came is the host's job now: {events:?}"
    );
    assert_eq!(session.live_handles(), 0);
}

#[test]
fn a_worker_spilled_reply_is_delivered_and_a_zero_byte_offer_is_fatal() {
    let mut session = peer::negotiated();
    let id = session
        .call_worker("guest.big", json!(null), None, false)
        .expect("call goes out");
    session.drain_outbox();
    session.feed(&wire(&WorkerMessage::Reply(Reply {
        call_id: id,
        outcome: Outcome::Spilled {
            artifact: ArtifactOffer {
                handle: HandleId(4),
                bytes: 100,
                media_type: "text/plain".to_owned(),
                digest_blake3: "ab".repeat(32),
            },
        },
    })));
    let events = session.drain_events();
    assert!(
        matches!(events.as_slice(), [SessionEvent::HostCallSettled { .. }]),
        "the worker-held offer reaches the application: {events:?}"
    );

    let id = session
        .call_worker("guest.big", json!(null), None, false)
        .expect("second call goes out");
    session.drain_outbox();
    session.feed(&wire(&WorkerMessage::Reply(Reply {
        call_id: id,
        outcome: Outcome::Spilled {
            artifact: ArtifactOffer {
                handle: HandleId(5),
                bytes: 0,
                media_type: "text/plain".to_owned(),
                digest_blake3: "ab".repeat(32),
            },
        },
    })));
    peer::assert_fatal(&mut session, WireErrorKind::InvalidFrame);
}
