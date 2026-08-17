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
use crate::{MAX_ARTIFACT_READ_BYTES, MAX_WIRE_ID};

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
        let handle = self.insert_handle(HandleKind::Capability, None)?;
        self.worker_calls
            .get_mut(&minted_for)
            .expect("checked above")
            .minted
            .push(handle);
        Ok(handle)
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
        let offer_bytes = bytes.len() as u64;
        let digest = hex(blake3::hash(&bytes).as_bytes());
        let handle = self.insert_handle(
            HandleKind::Artifact,
            Some(ArtifactBytes {
                bytes,
                media_type: media_type.to_owned(),
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
        self.drop_handle(release.handle);
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
        let mut count = 0;
        for handle in handles {
            if self.handles.contains_key(handle) {
                self.drop_handle(*handle);
                count += 1;
            }
        }
        if count > 0 {
            self.events.push(SessionEvent::HandlesReclaimed { count });
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
    /// toward us). The session tracks only the pending ack — the worker's
    /// own table is the authority on what it holds, and a release it cannot
    /// honor is the worker's protocol fault to raise.
    pub fn release_worker_handle(
        &mut self,
        handle: HandleId,
        kind: HandleKind,
    ) -> Result<(), AppError> {
        if self.phase != Phase::Active {
            return Err(AppError::NotActive);
        }
        if !self.pending_worker_releases.insert(handle) {
            return Err(AppError::ReleasePending);
        }
        self.outbox
            .push(HostMessage::Release(Release { handle, kind }));
        Ok(())
    }

    /// The worker confirmed a release the host initiated. An ack with no
    /// release pending is a desync — the host asked for nothing.
    pub(super) fn on_release_ack(&mut self, ack: ReleaseAck) {
        if !self.pending_worker_releases.remove(&ack.handle) {
            self.fatal(WireErrorKind::UnknownHandle, "unsolicited release-ack");
            return;
        }
        self.events.push(SessionEvent::WorkerHandleReleased {
            handle: ack.handle,
            kind: ack.kind,
        });
    }

    fn drop_handle(&mut self, handle: HandleId) {
        if self.handles.remove(&handle).is_some() {
            self.live_handle_count -= 1;
            self.retired_handles.insert(handle);
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
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
