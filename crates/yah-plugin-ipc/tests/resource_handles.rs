//! Handle lifetime: explicit release with an ack, a live gauge read while
//! the session is active, and leak-safe reclamation on every path a
//! release frame will never travel.

#[path = "support/peer.rs"]
mod peer;

use peer::{assert_fatal, call, wire};
use serde_json::json;
use yah_plugin_ipc::session::{AppError, HostSession, SessionConfig, SessionEvent};
use yah_plugin_ipc::types::*;

/// A worker call delivered and left in flight, so the host can mint
/// against it.
fn in_flight_call(session: &mut HostSession, id: u64) -> CallId {
    session.feed(&wire(&call(id, "cap.acquire", json!(null))));
    session.drain_events();
    CallId(id)
}

fn release(handle: HandleId, kind: HandleKind) -> WorkerMessage {
    WorkerMessage::Release(Release { handle, kind })
}

#[test]
fn mint_use_release_ack_with_the_gauge_read_while_live() {
    let mut session = peer::negotiated();
    let serving = in_flight_call(&mut session, 1);
    let handle = session
        .mint_capability_handle(serving)
        .expect("grant is minted");
    // The gauge is read while the session is active — after teardown a
    // zero is unconditional and proves nothing (the Wasm lane's lesson).
    assert_eq!(session.live_handles(), 1);
    session
        .reply_to_worker(
            serving,
            Outcome::Ok {
                result: json!({"handle": handle}),
            },
        )
        .expect("grant reply goes out");
    session.drain_outbox();
    assert_eq!(
        session.live_handles(),
        1,
        "an ok reply keeps the grant alive"
    );

    session.feed(&wire(&release(handle, HandleKind::Capability)));
    let outbox = session.drain_outbox();
    assert!(
        matches!(
            outbox.as_slice(),
            [HostMessage::ReleaseAck(ack)] if ack.handle == handle && ack.kind == HandleKind::Capability
        ),
        "release is a two-party fact: {outbox:?}"
    );
    assert_eq!(
        session.drain_events(),
        vec![SessionEvent::HandleReleased {
            handle,
            kind: HandleKind::Capability
        }]
    );
    assert_eq!(session.live_handles(), 0);
    assert!(!session.is_closed());
}

#[test]
fn the_handle_ceiling_refuses_at_the_bound_and_admits_after_a_release() {
    let mut config = SessionConfig::default();
    config.ceilings.live_handles = 2;
    let mut session = peer::negotiated_with(config);
    let serving = in_flight_call(&mut session, 1);
    let first = session.mint_capability_handle(serving).expect("first fits");
    let _second = session
        .mint_capability_handle(serving)
        .expect("second fits");
    // The refusal is the application's to translate — the Wasm lane turns
    // it into its own `handle-limit` code.
    assert_eq!(
        session.mint_capability_handle(serving),
        Err(AppError::HandleCeiling)
    );
    session.feed(&wire(&release(first, HandleKind::Capability)));
    session.drain_outbox();
    session.drain_events();
    // Below the bound again: the pair half that pins the refusal on the
    // ceiling rather than the fixture.
    session
        .mint_capability_handle(serving)
        .expect("the freed slot admits a new grant");
}

#[test]
fn releasing_a_handle_never_minted_is_fatal() {
    let mut session = peer::negotiated();
    session.feed(&wire(&release(HandleId(9), HandleKind::Capability)));
    assert_fatal(&mut session, WireErrorKind::UnknownHandle);
}

#[test]
fn releasing_twice_is_fatal() {
    let mut session = peer::negotiated();
    let serving = in_flight_call(&mut session, 1);
    let handle = session.mint_capability_handle(serving).expect("minted");
    session.feed(&wire(&release(handle, HandleKind::Capability)));
    session.drain_outbox();
    session.drain_events();
    // The id retired at release; a second release names a handle the
    // worker no longer holds — the same desync the Wasm lane traps.
    session.feed(&wire(&release(handle, HandleKind::Capability)));
    assert_fatal(&mut session, WireErrorKind::UnknownHandle);
}

#[test]
fn releasing_with_the_wrong_kind_is_fatal() {
    let mut session = peer::negotiated();
    let serving = in_flight_call(&mut session, 1);
    let handle = session.mint_capability_handle(serving).expect("minted");
    session.feed(&wire(&release(handle, HandleKind::Artifact)));
    assert_fatal(&mut session, WireErrorKind::UnknownHandle);
}

#[test]
fn a_failed_minting_call_reclaims_what_it_briefly_held() {
    let mut session = peer::negotiated();
    let serving = in_flight_call(&mut session, 1);
    let _handle = session.mint_capability_handle(serving).expect("minted");
    assert_eq!(session.live_handles(), 1);
    // The acquire failed after the grant was staged: the err terminal must
    // not leak the handle to a worker that never learned it existed.
    session
        .reply_to_worker(
            serving,
            Outcome::Err {
                error: WireError {
                    kind: WireErrorKind::Internal,
                    message: "internal".to_owned(),
                    retryable: false,
                    reconcile_required: false,
                },
            },
        )
        .expect("err reply goes out");
    assert_eq!(
        session.drain_events(),
        vec![SessionEvent::HandlesReclaimed { count: 1 }]
    );
    assert_eq!(session.live_handles(), 0);
    assert!(
        !session.is_closed(),
        "reclamation is bookkeeping, not a fault"
    );
}

#[test]
fn a_disconnect_reclaims_every_live_handle() {
    let mut session = peer::negotiated();
    let serving = in_flight_call(&mut session, 1);
    let _handle = session.mint_capability_handle(serving).expect("minted");
    session
        .reply_to_worker(
            serving,
            Outcome::Ok {
                result: json!(null),
            },
        )
        .expect("grant reply goes out");
    assert_eq!(session.live_handles(), 1);
    session.end_of_input();
    let events = session.drain_events();
    assert!(
        events.contains(&SessionEvent::HandlesReclaimed { count: 1 }),
        "the release frames that will never come are the host's job: {events:?}"
    );
    assert_eq!(session.live_handles(), 0);
}

#[test]
fn a_released_id_is_never_minted_again() {
    let mut session = peer::negotiated();
    let serving = in_flight_call(&mut session, 1);
    let first = session.mint_capability_handle(serving).expect("minted");
    session.feed(&wire(&release(first, HandleKind::Capability)));
    session.drain_outbox();
    session.drain_events();
    // A fresh id after release, never the old one back: reuse would let a
    // stale release frame race a new grant into a false ack.
    let second = session
        .mint_capability_handle(serving)
        .expect("minted again");
    assert_ne!(second, first);
    assert!(second.0 > first.0, "ids move forward only");
}
