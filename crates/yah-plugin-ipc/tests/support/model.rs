//! The independent reference model's state machine.
//!
//! Written from `docs/plugin-worker-protocol.md`, not from the session
//! modules: the representations are deliberately simpler (flat `Vec`s, a
//! linear phase scan, no B-tree correlation), and the only things compared
//! against production are externally observable facts — the vocabulary in
//! `model_facts.rs`. Nothing here imports session internals, matches its
//! private enums, or restates its helper functions; where the doc
//! underdetermines a rule the model records the decision it encodes, and
//! a mismatch with production is a finding to resolve, not noise to
//! silence. A trace is a `Vec<Action>`; it serializes to JSON, so a
//! failing generated trace pins as a replayable regression.

#![allow(dead_code)]

pub(super) use super::model_facts::MAX_STREAM_CREDIT;
use super::model_facts::{Action, EventFact, Kind, OutcomeFact, WOutcome, WireFact};

/// One live handle: id, kind, and the artifact payload it pins.
#[derive(Clone, Debug)]
pub(super) struct MHandle {
    pub(super) id: u64,
    pub(super) kind: Kind,
    pub(super) artifact: Option<(u32, String)>,
}

/// One worker call the host is serving.
#[derive(Clone, Debug)]
pub(super) struct MWorkerCall {
    pub(super) stream: bool,
    pub(super) stream_credit: Option<u32>,
    pub(super) next_seq: u64,
    pub(super) lossy_dropped: u64,
    pub(super) last_sent: bool,
    pub(super) minted: Vec<u64>,
    /// Artifact handles this call spilled, with their byte counts.
    pub(super) artifacts: Vec<(u64, u32)>,
}

/// One host call in flight toward the worker.
#[derive(Clone, Debug)]
pub(super) struct MHostCall {
    pub(super) stream: bool,
    pub(super) open: bool,
    pub(super) credit_left: u32,
    pub(super) highest_seq: Option<u64>,
    pub(super) lossy_dropped: u64,
    pub(super) last_seen: bool,
    pub(super) deadline_at: Option<u64>,
    pub(super) muted: bool,
}

/// The model's own state representation: flat vectors and linear scans.
#[derive(Debug)]
pub struct ModelSession {
    active: bool,
    closed: bool,
    pub(super) worker_calls: Vec<(u64, MWorkerCall)>,
    pub(super) host_calls: Vec<(u64, MHostCall)>,
    pub(super) retired_worker_calls: Vec<u64>,
    pub(super) retired_host_calls: Vec<u64>,
    /// handle id, kind, and the artifact payload (bytes, media type).
    pub(super) handles: Vec<MHandle>,
    pub(super) live_handles: u32,
    pub(super) retired_handles: Vec<u64>,
    pub(super) reclaimed_handles: Vec<(u64, Kind)>,
    pub(super) pending_releases: Vec<(u64, Kind)>,
    pub(super) retired_worker_handles: Vec<u64>,
    pub(super) offered_worker_handles: Vec<(u64, Kind)>,
    pub(super) next_host_call: u64,
    pub(super) next_handle: u64,
    pub(super) now: u64,
    budget: Option<u64>,
    pub(super) wire: Vec<WireFact>,
    pub(super) events: Vec<EventFact>,
}

pub const WORKER_CALLS_MAX: u32 = 4;
pub const HOST_CALLS_MAX: u32 = 4;
pub const LIVE_HANDLES_MAX: u32 = 4;

impl ModelSession {
    pub fn new(budget: Option<u64>) -> Self {
        Self {
            active: false,
            closed: false,
            worker_calls: Vec::new(),
            host_calls: Vec::new(),
            retired_worker_calls: Vec::new(),
            retired_host_calls: Vec::new(),
            handles: Vec::new(),
            live_handles: 0,
            retired_handles: Vec::new(),
            reclaimed_handles: Vec::new(),
            pending_releases: Vec::new(),
            retired_worker_handles: Vec::new(),
            offered_worker_handles: Vec::new(),
            next_host_call: 1,
            next_handle: 1,
            now: 0,
            budget,
            wire: Vec::new(),
            events: Vec::new(),
        }
    }

    /// The observable facts queued since the last collection, in order.
    pub fn collect(&mut self) -> (Vec<WireFact>, Vec<EventFact>) {
        (
            std::mem::take(&mut self.wire),
            std::mem::take(&mut self.events),
        )
    }

    pub fn closed(&self) -> bool {
        self.closed
    }

    pub fn live_handles(&self) -> u32 {
        self.live_handles
    }

    pub fn retired_operations(&self) -> u64 {
        (self.retired_worker_calls.len()
            + self.retired_host_calls.len()
            + self.retired_handles.len()
            + self.reclaimed_handles.len()
            + self.retired_worker_handles.len()
            + self.offered_worker_handles.len()) as u64
    }

    pub fn in_flight(&self) -> (u32, u32) {
        (
            (self.host_calls.len()) as u32,
            (self.worker_calls.len()) as u32,
        )
    }

    pub fn pending_releases(&self) -> u32 {
        self.pending_releases.len() as u32
    }

    pub(crate) fn budget_full(&self) -> bool {
        self.budget.is_some_and(|b| self.retired_operations() >= b)
    }

    pub(super) fn fatal(&mut self, kind: &'static str) {
        if self.closed {
            return;
        }
        if self.active {
            self.wire.push(WireFact::Goodbye);
        }
        // In-flight host calls settle outcome-unknown; handles reclaim.
        for (id, _) in std::mem::take(&mut self.host_calls) {
            self.retired_host_calls.push(id);
            self.events.push(EventFact::HostCallLost {
                call_id: id,
                kind: "outcome-unknown",
                reconcile: true,
            });
        }
        self.reclaim_all_handles();
        self.pending_releases.clear();
        self.reclaimed_handles.clear();
        self.worker_calls.clear();
        self.closed = true;
        self.active = false;
        self.events.push(EventFact::Fatal { kind });
    }

    pub(super) fn reclaim_all_handles(&mut self) {
        let mut count = 0;
        for entry in std::mem::take(&mut self.handles) {
            self.reclaimed_handles.push((entry.id, entry.kind));
            self.live_handles -= 1;
            count += 1;
        }
        if count > 0 {
            self.events.push(EventFact::HandlesReclaimed { count });
        }
    }

    pub(crate) fn worker_call_mut(&mut self, id: u64) -> Option<&mut MWorkerCall> {
        self.worker_calls
            .iter_mut()
            .find(|(k, _)| *k == id)
            .map(|(_, v)| v)
    }

    pub(crate) fn worker_call(&self, id: u64) -> Option<&MWorkerCall> {
        self.worker_calls
            .iter()
            .find(|(k, _)| *k == id)
            .map(|(_, v)| v)
    }

    pub(super) fn host_call_mut(&mut self, id: u64) -> Option<&mut MHostCall> {
        self.host_calls
            .iter_mut()
            .find(|(k, _)| *k == id)
            .map(|(_, v)| v)
    }

    pub(super) fn host_call(&self, id: u64) -> Option<&MHostCall> {
        self.host_calls
            .iter()
            .find(|(k, _)| *k == id)
            .map(|(_, v)| v)
    }

    pub(super) fn refuse_worker_call(
        &mut self,
        id: u64,
        kind: &'static str,
        retryable: bool,
        spend: bool,
    ) {
        if spend {
            self.retired_worker_calls.push(id);
        }
        self.wire.push(WireFact::Reply {
            call_id: id,
            outcome: OutcomeFact::Err { kind, retryable },
        });
        self.events
            .push(EventFact::CallRefused { call_id: id, kind });
    }

    /// Apply one action to the model. The transition rules are the
    /// protocol doc's, in the order the doc states them.
    pub fn apply(&mut self, action: &Action) {
        if self.closed {
            // A closed session ignores everything; the adapter still
            // drains, and the model still answers application calls with
            // NotActive.
            if let Action::HostCall { .. }
            | Action::AnswerWorkerCall { .. }
            | Action::Mint { .. }
            | Action::OfferArtifact { .. }
            | Action::HostRelease { .. }
            | Action::HostOpenStream { .. }
            | Action::HostGrantCredit { .. }
            | Action::HostStreamItem { .. }
            | Action::HostNoteDrops { .. }
            | Action::HostCancel { .. } = action
            {
                self.events.push(EventFact::AppErr("NotActive"));
            }
            return;
        }
        // Field bounds are part of admission, before any correlation
        // rule: an id outside the wire range is invalid-frame, exactly
        // as the doc's "a frame a conformant schema validator would
        // refuse, this host refuses at the same line" promises. The
        // generator can only produce zero here; the upper I-JSON bound
        // is byte-admission territory, pinned by the strict-JSON suite.
        if let Some(id) = action.wire_id()
            && !(1..=(1u64 << 53) - 1).contains(&id)
        {
            self.fatal("invalid-frame");
            return;
        }
        match *action {
            Action::Hello
            | Action::HelloBadVersion
            | Action::HelloUnknownRequired
            | Action::NonHelloFirst
            | Action::HelloAgain => self.hello(action),
            Action::WorkerCall { id, stream } => self.take_worker_call(id, stream),
            Action::ArtifactRead {
                id,
                handle,
                ok_range,
            } => self.artifact_read(id, handle, ok_range),
            Action::WorkerReply { id, ref outcome } => self.worker_reply(id, outcome),
            Action::StreamOpen { id, credit } => self.stream_open(id, credit),
            Action::StreamData {
                id,
                seq,
                more,
                lossless,
                dropped,
            } => self.stream_data(id, seq, more, lossless, dropped),
            Action::Credit { id, additional } => self.credit(id, additional),
            Action::WorkerCancel { id, target_call } => {
                if self.worker_call(id).is_some() {
                    self.events.push(EventFact::CancelRequested {
                        call_id: id,
                        target_call,
                    });
                }
            }
            Action::Release { handle, kind } => self.release(handle, kind),
            Action::ReleaseAck { handle, kind } => self.release_ack(handle, kind),
            Action::Goodbye => self.goodbye(),
            Action::Eof => self.fatal("outcome-unknown"),
            Action::HostCall { deadline_ms } => self.begin_host_call(deadline_ms),
            Action::Tick { now_ms } => self.tick(now_ms),
            Action::HostCancel { id, target_call } => self.host_cancel(id, target_call),
            Action::AnswerWorkerCall { id, ref outcome } => self.answer(id, outcome),
            Action::Mint { id } => self.mint(id),
            Action::OfferArtifact { id, bytes } => self.offer_artifact(id, bytes),
            Action::HostRelease { handle, kind } => self.host_release(handle, kind),
            Action::HostOpenStream { id, credit } => self.host_open_stream(id, credit),
            Action::HostGrantCredit { id, additional } => self.host_grant_credit(id, additional),
            Action::HostStreamItem { id, lossless, more } => {
                self.host_stream_item(id, lossless, more)
            }
            Action::HostNoteDrops { id, dropped } => self.host_note_drops(id, dropped),
        }
    }

    pub(super) fn hello(&mut self, action: &Action) {
        if self.active {
            // Second hello: fatal, negotiation cannot restart.
            self.fatal("negotiation-required");
            return;
        }
        match action {
            Action::Hello => {
                self.wire.push(WireFact::Accept);
                self.active = true;
                self.events.push(EventFact::Negotiated);
            }
            Action::HelloBadVersion => {
                self.wire.push(WireFact::Refuse("unsupported-version"));
                self.closed = true;
                self.events.push(EventFact::Fatal {
                    kind: "unsupported-version",
                });
            }
            Action::HelloUnknownRequired => {
                self.wire.push(WireFact::Refuse("unknown-required-feature"));
                self.closed = true;
                self.events.push(EventFact::Fatal {
                    kind: "unknown-required-feature",
                });
            }
            _ => {
                // Any other decodable frame first: refuse, name the rule, close.
                self.wire.push(WireFact::Refuse("negotiation-required"));
                self.closed = true;
                self.events.push(EventFact::Fatal {
                    kind: "negotiation-required",
                });
            }
        }
    }

    pub(super) fn take_worker_call(&mut self, id: u64, stream: bool) {
        if self.worker_call(id).is_some() || self.retired_worker_calls.contains(&id) {
            self.fatal("duplicate-call");
            return;
        }
        // Budget precedes bounds and ceilings: at the budget no new id is
        // ever spent, so the refusal is unspent and non-retryable.
        if self.budget_full() {
            self.refuse_worker_call(id, "resource-exhausted", false, false);
            return;
        }
        if self.worker_calls.len() as u32 >= WORKER_CALLS_MAX {
            // The ceiling refusal spends the id and is retry-shaped.
            self.refuse_worker_call(id, "resource-exhausted", true, true);
            return;
        }
        self.worker_calls.push((
            id,
            MWorkerCall {
                stream,
                stream_credit: None,
                next_seq: 0,
                lossy_dropped: 0,
                last_sent: false,
                minted: Vec::new(),
                artifacts: Vec::new(),
            },
        ));
        self.events.push(EventFact::CallDelivered { call_id: id });
    }

    pub(super) fn artifact_read(&mut self, id: u64, handle: u64, ok_range: bool) {
        if self.worker_call(id).is_some() || self.retired_worker_calls.contains(&id) {
            self.fatal("duplicate-call");
            return;
        }
        if self.budget_full() {
            self.refuse_worker_call(id, "resource-exhausted", false, false);
            return;
        }
        // A served read clears the same admission bar as any other call:
        // the worker in-flight ceiling precedes the dispatch.
        if self.worker_calls.len() as u32 >= WORKER_CALLS_MAX {
            self.refuse_worker_call(id, "resource-exhausted", true, true);
            return;
        }
        let Some(entry) = self.handles.iter().find(|entry| entry.id == handle) else {
            self.refuse_worker_call(id, "unknown-handle", false, true);
            return;
        };
        if entry.kind != Kind::Artifact || entry.artifact.is_none() {
            self.refuse_worker_call(id, "unknown-handle", false, true);
            return;
        }
        if !ok_range {
            self.refuse_worker_call(id, "invalid-read", false, true);
            return;
        }
        self.retired_worker_calls.push(id);
        self.wire.push(WireFact::Reply {
            call_id: id,
            outcome: OutcomeFact::Ok,
        });
    }

    pub(super) fn worker_reply(&mut self, id: u64, outcome: &WOutcome) {
        if self.host_call(id).is_none() {
            if self.retired_host_calls.contains(&id) {
                // Tolerated race; a spilled offer inside it still spends
                // its handle id — unless the correlation budget is full,
                // where the spend is refused and the race simply stays
                // tolerated. A repeat offer is the reuse fault either way.
                if let WOutcome::Spilled { handle, .. } = outcome {
                    if self
                        .offered_worker_handles
                        .iter()
                        .any(|(h, _)| *h == *handle)
                    {
                        self.fatal("unknown-handle");
                        return;
                    }
                    if !self.budget_full() {
                        self.offered_worker_handles.push((*handle, Kind::Artifact));
                    }
                }
                return;
            }
            self.fatal("unknown-call");
            return;
        }
        match outcome {
            WOutcome::Ok => {}
            WOutcome::Spilled { handle, bytes } => {
                if *bytes == 0 {
                    self.fatal("invalid-frame");
                    return;
                }
                if self
                    .offered_worker_handles
                    .iter()
                    .any(|(h, _)| *h == *handle)
                {
                    self.fatal("unknown-handle");
                    return;
                }
                self.offered_worker_handles.push((*handle, Kind::Artifact));
            }
            WOutcome::ErrUnknownCall => {}
            WOutcome::ErrInternal => {}
            WOutcome::Cancelled => {}
        }
        let outcome_fact = match outcome {
            WOutcome::Ok => OutcomeFact::Ok,
            WOutcome::Spilled { handle, .. } => OutcomeFact::Spilled { handle: *handle },
            WOutcome::ErrUnknownCall => OutcomeFact::Err {
                kind: "unknown-call",
                retryable: false,
            },
            WOutcome::ErrInternal => OutcomeFact::Err {
                kind: "internal",
                retryable: false,
            },
            WOutcome::Cancelled => OutcomeFact::Cancelled,
        };
        self.host_calls.retain(|(k, _)| *k != id);
        self.retired_host_calls.push(id);
        self.events.push(EventFact::HostCallSettled {
            call_id: id,
            outcome: outcome_fact,
        });
    }

    pub(super) fn stream_open(&mut self, id: u64, credit: u32) {
        let Some(state) = self.host_call_mut(id) else {
            if self.retired_host_calls.contains(&id) {
                return; // racing a local settle: tolerated like a late reply
            }
            self.fatal("unknown-call");
            return;
        };
        if !state.stream {
            self.fatal("invalid-frame");
            return;
        }
        if state.open {
            self.fatal("invalid-frame");
            return;
        }
        if credit == 0 || credit > MAX_STREAM_CREDIT {
            self.fatal("invalid-frame");
            return;
        }
        state.open = true;
        state.credit_left = credit;
        self.events.push(EventFact::StreamOpened {
            call_id: id,
            credit,
        });
    }

    pub(super) fn stream_data(
        &mut self,
        id: u64,
        seq: u64,
        more: bool,
        lossless: bool,
        dropped: u64,
    ) {
        let Some(state) = self.host_call_mut(id) else {
            if self.retired_host_calls.contains(&id) {
                return; // items racing a local settle: dropped, not faulted
            }
            self.fatal("unknown-call");
            return;
        };
        if !state.stream || !state.open {
            self.fatal("invalid-frame");
            return;
        }
        if state.last_seen {
            self.fatal("invalid-frame");
            return;
        }
        let expected = state.highest_seq.map_or(0, |s| s + 1);
        if seq != expected {
            self.fatal("invalid-frame");
            return;
        }
        if dropped < state.lossy_dropped {
            self.fatal("invalid-frame");
            return;
        }
        if lossless {
            if state.credit_left == 0 {
                self.fatal("resource-exhausted");
                return;
            }
            state.credit_left -= 1;
        }
        state.highest_seq = Some(seq);
        state.lossy_dropped = dropped;
        if !more {
            state.last_seen = true;
        }
        if !state.muted {
            self.events.push(EventFact::StreamItem {
                call_id: id,
                seq,
                more,
                lossless,
                dropped,
            });
        }
    }

    pub(super) fn credit(&mut self, id: u64, additional: u32) {
        let Some(state) = self.worker_call_mut(id) else {
            return; // credit racing the terminal: tolerated
        };
        let Some(window) = state.stream_credit else {
            self.fatal("invalid-frame");
            return;
        };
        let Some(next) = window.checked_add(additional) else {
            self.fatal("invalid-frame");
            return;
        };
        if additional == 0 || next > MAX_STREAM_CREDIT {
            self.fatal("invalid-frame");
            return;
        }
        state.stream_credit = Some(next);
        self.events.push(EventFact::CreditGranted {
            call_id: id,
            additional,
        });
    }

    pub(super) fn release(&mut self, handle: u64, kind: Kind) {
        if let Some(entry) = self.handles.iter().find(|entry| entry.id == handle) {
            if entry.kind != kind {
                self.fatal("unknown-handle");
                return;
            }
            self.handles.retain(|entry| entry.id != handle);
            self.live_handles -= 1;
            self.retired_handles.push(handle);
            self.wire.push(WireFact::ReleaseAck { handle, kind });
            self.events.push(EventFact::HandleReleased { handle, kind });
            return;
        }
        if let Some(pos) = self
            .reclaimed_handles
            .iter()
            .position(|(h, _)| *h == handle)
        {
            let (_, held_kind) = self.reclaimed_handles[pos];
            if held_kind != kind {
                self.fatal("unknown-handle");
                return;
            }
            self.reclaimed_handles.remove(pos);
            self.retired_handles.push(handle);
            self.wire.push(WireFact::ReleaseAck { handle, kind });
            return;
        }
        // Retired and never-held are different faults with the same kind.
        self.fatal("unknown-handle");
    }

    pub(super) fn release_ack(&mut self, handle: u64, kind: Kind) {
        let Some(pos) = self.pending_releases.iter().position(|(h, _)| *h == handle) else {
            self.fatal("unknown-handle");
            return;
        };
        if self.pending_releases[pos].1 != kind {
            self.fatal("unknown-handle");
            return;
        }
        self.pending_releases.remove(pos);
        self.retired_worker_handles.push(handle);
        self.events
            .push(EventFact::WorkerHandleReleased { handle, kind });
    }

    pub(super) fn goodbye(&mut self) {
        self.events.push(EventFact::WorkerGoodbye);
        for (id, _) in std::mem::take(&mut self.host_calls) {
            self.retired_host_calls.push(id);
            self.events.push(EventFact::HostCallLost {
                call_id: id,
                kind: "cancelled",
                reconcile: false,
            });
        }
        self.reclaim_all_handles();
        self.pending_releases.clear();
        self.reclaimed_handles.clear();
        self.worker_calls.clear();
        self.closed = true;
        self.active = false;
    }
}
