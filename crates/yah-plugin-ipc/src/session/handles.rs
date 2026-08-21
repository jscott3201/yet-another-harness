//! Handle lifetime: explicit release, acknowledged, with a live gauge.
//!
//! No lane this protocol serves runs finalizers reliably — the Wasm lane
//! measured GC never releasing an undisposed handle, and Node and CPython
//! are no different — so release is wire law: explicit, acknowledged, and
//! backed by a host-side gauge with a ceiling. A handle id retires forever
//! at release; the ack is what makes "released" a two-party fact instead of
//! a hope, and the never-reused id is what closes the release/reuse race.
//!
//! Artifact handles carry the spill mechanism: the owner keeps the bytes,
//! the other side pull-reads bounded chunks (hex, inside an ordinary call
//! result) and then releases the handle. One mechanism, both directions.

use super::{AppError, ArtifactBytes, Handle, HostSession, Phase, SessionEvent};
use crate::types::*;
use crate::{MAX_ARTIFACT_READ_BYTES, MAX_MEDIA_TYPE_CHARS, MAX_WIRE_ID};

/// The one method the session serves itself: a pull-read against an
/// artifact handle the host owns. Everything else is the application's.
pub const ARTIFACT_READ_METHOD: &str = "artifact.read";

impl HostSession {
    /// Mint a handle for a resource granted while serving `minted_for`.
    /// Refused at the ceiling — the application turns that refusal into
    /// the capability family's own answer (the Wasm lane's `handle-limit`).
    pub fn mint_capability_handle(&mut self, minted_for: CallId) -> Result<HandleId, AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        if !self.worker_calls.contains_key(&minted_for) {
            return Err(AppError::UnknownCall);
        }
        if self.budget_full() {
            return Err(AppError::SessionRetired);
        }
        let handle = self.insert_handle(HandleKind::Capability, None)?;
        self.worker_calls
            .get_mut(&minted_for)
            .expect("checked above")
            .minted
            .push(handle);
        Ok(handle)
    }

    /// Retire a live capability handle the worker released through the
    /// application lane (`capability.release`), not through a Release
    /// frame. The call's reply is that release's acknowledgement, so no
    /// `ReleaseAck` frame rides the wire — there was no Release frame to
    /// answer. The id is spent either way: a second retirement is refused
    /// as already-released, and a later Release frame for it is the same
    /// double-release fault a frame duplicate would be.
    pub fn retire_worker_capability(&mut self, handle: HandleId) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        let Some(entry) = self.handles.get(&handle) else {
            return Err(if self.retired_handles.contains(&handle) {
                AppError::AlreadyReleased
            } else {
                AppError::UnknownWorkerHandle
            });
        };
        if entry.kind != HandleKind::Capability {
            return Err(AppError::InvalidField("release with the wrong kind"));
        }
        self.drop_handle(handle, true);
        Ok(())
    }

    /// Offer spilled bytes behind an artifact handle. The offer carries
    /// size and digest so the reader can refuse before the first pull and
    /// verify after the last. Counts against the same live-handle ceiling
    /// as a capability grant: each live artifact pins its bytes host-side,
    /// so an uncounted offer would be unbounded memory a worker controls by
    /// never releasing.
    pub fn offer_artifact(
        &mut self,
        minted_for: CallId,
        bytes: Vec<u8>,
        media_type: &str,
    ) -> Result<ArtifactOffer, AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        if !self.worker_calls.contains_key(&minted_for) {
            return Err(AppError::UnknownCall);
        }
        // The same admission bounds the host holds an inbound offer to: an
        // offer this side cannot pass is an offer this side must not mint.
        if bytes.is_empty() {
            return Err(AppError::InvalidField("spilled artifact of zero bytes"));
        }
        let media_chars = media_type.chars().count();
        if media_chars == 0 || media_chars > MAX_MEDIA_TYPE_CHARS {
            return Err(AppError::InvalidField(
                "media type outside its length bound",
            ));
        }
        if self.budget_full() {
            return Err(AppError::SessionRetired);
        }
        let offer_bytes = bytes.len() as u64;
        let digest = hex(blake3::hash(&bytes).as_bytes());
        let handle = self.insert_handle(
            HandleKind::Artifact,
            Some(ArtifactBytes {
                bytes,
                media_type: media_type.to_owned(),
                digest_blake3: digest.clone(),
            }),
        )?;
        self.worker_calls
            .get_mut(&minted_for)
            .expect("checked above")
            .minted
            .push(handle);
        Ok(ArtifactOffer {
            handle,
            bytes: offer_bytes,
            media_type: media_type.to_owned(),
            digest_blake3: digest,
        })
    }

    fn insert_handle(
        &mut self,
        kind: HandleKind,
        artifact: Option<ArtifactBytes>,
    ) -> Result<HandleId, AppError> {
        if self.live_handle_count >= self.config.ceilings.live_handles {
            return Err(AppError::HandleCeiling);
        }
        let handle = self.next_handle_id();
        self.handles.insert(handle, Handle { kind, artifact });
        self.live_handle_count += 1;
        Ok(handle)
    }

    /// A pull-read the session answers itself. The call was id-checked by
    /// the caller; it never enters the in-flight table because it is
    /// answered in the same turn.
    pub(super) fn serve_artifact_read(&mut self, call: Call) {
        let call_id = call.call_id;
        let Ok(read) = serde_json::from_value::<ArtifactRead>(call.payload) else {
            self.refuse_worker_call(call_id, WireErrorKind::InvalidFrame);
            return;
        };
        let Some(entry) = self.handles.get(&read.handle) else {
            self.refuse_worker_call(call_id, WireErrorKind::UnknownHandle);
            return;
        };
        let Some(artifact) = &entry.artifact else {
            self.refuse_worker_call(call_id, WireErrorKind::UnknownHandle);
            return;
        };
        let len = read.len as usize;
        let offset = read.offset as usize;
        if len == 0
            || len > MAX_ARTIFACT_READ_BYTES
            || offset >= artifact.bytes.len()
            || offset
                .checked_add(len)
                .is_none_or(|end| end > artifact.bytes.len())
        {
            self.refuse_worker_call(call_id, WireErrorKind::InvalidRead);
            return;
        }
        let chunk = &artifact.bytes[offset..offset + len];
        let result = serde_json::json!({
            "bytes_hex": hex(chunk),
            "media_type": artifact.media_type,
        });
        // Served is answered: the reply below is this id's terminal frame,
        // so the id retires exactly as it would through any other path.
        self.retired_worker_calls.insert(call_id);
        self.outbox.push(HostMessage::Reply(Reply {
            call_id,
            outcome: Outcome::Ok { result },
        }));
    }

    /// The worker gives a handle back. Releasing what you do not hold is a
    /// desync, and so is releasing twice — the Wasm lane traps a double
    /// dispose for the same reason.
    pub(super) fn on_release(&mut self, release: Release) {
        let Some(entry) = self.handles.get(&release.handle) else {
            // A release for a handle the host reclaimed is a tolerated
            // race, not a desync: the worker may have learned the id
            // mid-call, and its release can cross the reclaiming terminal
            // on the wire. Acked so the worker's table closes cleanly; no
            // event, because the application already heard the
            // reclamation. The kind still has to agree, and the ack spends
            // the id, so releasing it twice stays fatal.
            if let Some(kind) = self.reclaimed_handles.get(&release.handle).copied() {
                if kind != release.kind {
                    self.fatal(WireErrorKind::UnknownHandle, "release with the wrong kind");
                    return;
                }
                self.reclaimed_handles.remove(&release.handle);
                self.retired_handles.insert(release.handle);
                self.outbox.push(HostMessage::ReleaseAck(ReleaseAck {
                    handle: release.handle,
                    kind,
                }));
                return;
            }
            let detail = if self.retired_handles.contains(&release.handle) {
                "release of a handle already released"
            } else {
                "release of a handle never held"
            };
            self.fatal(WireErrorKind::UnknownHandle, detail);
            return;
        };
        if entry.kind != release.kind {
            self.fatal(WireErrorKind::UnknownHandle, "release with the wrong kind");
            return;
        }
        let kind = entry.kind;
        self.drop_handle(release.handle, true);
        self.outbox.push(HostMessage::ReleaseAck(ReleaseAck {
            handle: release.handle,
            kind,
        }));
        self.events.push(SessionEvent::HandleReleased {
            handle: release.handle,
            kind,
        });
    }

    /// Reclaim specific handles without a release frame — the leak-safe
    /// path for a call that ended err or cancelled after minting.
    pub(super) fn reclaim_handles(&mut self, handles: &[HandleId]) {
        let reclaimed: Vec<HandleId> = handles
            .iter()
            .filter(|handle| self.handles.contains_key(handle))
            .copied()
            .collect();
        for handle in &reclaimed {
            self.drop_handle(*handle, false);
        }
        if !reclaimed.is_empty() {
            self.events
                .push(SessionEvent::HandlesReclaimed { handles: reclaimed });
        }
    }

    /// Reclaim everything: goodbye, disconnect, or fatal fault. The gauge
    /// returns to zero without waiting for release frames that will never
    /// come — host-side reclamation is the disconnect story.
    pub(super) fn reclaim_all_handles(&mut self) {
        let all: Vec<HandleId> = self.handles.keys().copied().collect();
        self.reclaim_handles(&all);
    }

    /// Ask the worker to release a handle it holds (an artifact it spilled
    /// toward us). Only ids the worker actually offered are releasable;
    /// the worker's own table stays the authority on the bytes behind
    /// them, and a release it cannot honor is its protocol fault to raise.
    pub fn release_worker_handle(
        &mut self,
        handle: HandleId,
        kind: HandleKind,
    ) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        if !(1..=MAX_WIRE_ID).contains(&handle.0) {
            return Err(AppError::InvalidField("handle id outside wire range"));
        }
        // Only what the worker offered is the worker's to be asked for,
        // and only under the kind the offer carried. A release for any
        // other id — a host-minted handle, a typo — or the wrong kind
        // would arm a desync against a worker that did nothing wrong.
        match self.offered_worker_handles.get(&handle) {
            None => return Err(AppError::UnknownWorkerHandle),
            Some(offered) if *offered != kind => {
                return Err(AppError::InvalidField("release with the wrong kind"));
            }
            Some(_) => {}
        }
        if self.retired_worker_handles.contains(&handle) {
            return Err(AppError::AlreadyReleased);
        }
        if self.pending_worker_releases.contains_key(&handle) {
            return Err(AppError::ReleasePending);
        }
        // A release asks the worker to retire an id on its side; at the
        // correlation budget the session stops asking for new
        // retirements, exactly as it stops making them.
        if self.budget_full() {
            return Err(AppError::SessionRetired);
        }
        // Pending acks are handle-shaped state; the ceiling that bounds
        // live handles bounds them too, so a worker that withholds acks
        // cannot grow this side of the table without limit.
        if self.pending_worker_releases.len() as u32 >= self.config.ceilings.live_handles {
            return Err(AppError::HandleCeiling);
        }
        self.pending_worker_releases.insert(handle, kind);
        self.outbox
            .push(HostMessage::Release(Release { handle, kind }));
        Ok(())
    }

    /// The worker confirmed a release the host initiated. An ack with no
    /// release pending is a desync — the host asked for nothing — and so
    /// is an ack naming a kind the release did not: the same wrong-kind
    /// trap the host springs on a worker release, mirrored.
    pub(super) fn on_release_ack(&mut self, ack: ReleaseAck) {
        let Some(kind) = self.pending_worker_releases.get(&ack.handle).copied() else {
            self.fatal(WireErrorKind::UnknownHandle, "unsolicited release-ack");
            return;
        };
        if kind != ack.kind {
            self.fatal(
                WireErrorKind::UnknownHandle,
                "release-ack with the wrong kind",
            );
            return;
        }
        self.pending_worker_releases.remove(&ack.handle);
        self.retired_worker_handles.insert(ack.handle);
        self.events.push(SessionEvent::WorkerHandleReleased {
            handle: ack.handle,
            kind,
        });
    }

    /// Remove a live handle. `released` says a worker release frame ended
    /// it; a reclaimed id is remembered apart with its kind, because a
    /// release racing the reclamation is tolerated where a double release
    /// is not.
    fn drop_handle(&mut self, handle: HandleId, released: bool) {
        if let Some(entry) = self.handles.remove(&handle) {
            self.live_handle_count -= 1;
            if released {
                self.retired_handles.insert(handle);
            } else {
                self.reclaimed_handles.insert(handle, entry.kind);
            }
        }
    }

    fn next_handle_id(&mut self) -> HandleId {
        let handle = HandleId(self.next_handle);
        debug_assert!(self.next_handle <= MAX_WIRE_ID);
        self.next_handle += 1;
        handle
    }
}

/// Wire shape of a pull-read request.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRead {
    handle: HandleId,
    offset: u64,
    len: u64,
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}
