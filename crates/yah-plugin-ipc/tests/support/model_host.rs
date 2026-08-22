//! The model's host-application half: the transitions the embedding
//! host drives through the public API — its own calls, ticks, cancels,
//! answers, handle mints and offers, releases, and stream production.
//! Same state, same rules as the worker-facing half in `model.rs`;
//! split by ownership seam, not by size.

#![allow(dead_code)]

use super::model::{
    HOST_CALLS_MAX, LIVE_HANDLES_MAX, MAX_STREAM_CREDIT, MHandle, MHostCall, ModelSession,
};
use super::model_facts::{EventFact, Kind, OutcomeFact, WOutcome, WireFact};

impl ModelSession {
    pub(super) fn begin_host_call(&mut self, deadline_ms: Option<u32>) {
        if self.budget_full() {
            self.events.push(EventFact::AppErr("SessionRetired"));
            return;
        }
        if self.host_calls.len() as u32 >= HOST_CALLS_MAX {
            self.events.push(EventFact::AppErr("CallCeiling"));
            return;
        }
        let id = self.next_host_call;
        self.next_host_call += 1;
        self.host_calls.push((
            id,
            MHostCall {
                stream: false,
                open: false,
                credit_left: 0,
                highest_seq: None,
                lossy_dropped: 0,
                last_seen: false,
                deadline_at: deadline_ms.map(|b| self.now + u64::from(b)),
                muted: false,
            },
        ));
        self.wire.push(WireFact::Call { call_id: id });
    }

    pub(super) fn tick(&mut self, now_ms: u64) {
        self.now = self.now.max(now_ms);
        let expired: Vec<u64> = self
            .host_calls
            .iter()
            .filter(|(_, s)| s.deadline_at.is_some_and(|at| self.now >= at))
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.wire.push(WireFact::Cancel {
                call_id: id,
                target_call: true,
            });
            self.host_calls.retain(|(k, _)| *k != id);
            self.retired_host_calls.push(id);
            self.events.push(EventFact::HostCallLost {
                call_id: id,
                kind: "deadline-exceeded",
                reconcile: true,
            });
        }
    }

    pub(super) fn host_cancel(&mut self, id: u64, target_call: bool) {
        let Some(state) = self.host_call_mut(id) else {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        };
        if !target_call {
            state.muted = true;
        }
        self.wire.push(WireFact::Cancel {
            call_id: id,
            target_call,
        });
    }

    pub(super) fn answer(&mut self, id: u64, outcome: &WOutcome) {
        let Some(pos) = self.worker_calls.iter().position(|(k, _)| *k == id) else {
            if self.retired_worker_calls.contains(&id) {
                self.events.push(EventFact::AppErr("AlreadySettled"));
            } else {
                self.events.push(EventFact::AppErr("UnknownCall"));
            }
            return;
        };
        let minted = self.worker_calls[pos].1.minted.clone();
        let reclaim = !matches!(outcome, WOutcome::Ok | WOutcome::Spilled { .. });
        match outcome {
            WOutcome::Spilled { handle, bytes } => {
                // The offer must ride the call that minted the handle and
                // describe it exactly.
                if !minted.contains(handle) {
                    self.events.push(EventFact::AppErr("InvalidField"));
                    return;
                }
                let Some(entry) = self.handles.iter().find(|entry| entry.id == *handle) else {
                    self.events.push(EventFact::AppErr("InvalidField"));
                    return;
                };
                match &entry.artifact {
                    Some((held_bytes, _)) if *held_bytes == *bytes => {}
                    _ => {
                        self.events.push(EventFact::AppErr("InvalidField"));
                        return;
                    }
                }
                self.wire.push(WireFact::Reply {
                    call_id: id,
                    outcome: OutcomeFact::Spilled { handle: *handle },
                });
            }
            WOutcome::Ok => self.wire.push(WireFact::Reply {
                call_id: id,
                outcome: OutcomeFact::Ok,
            }),
            WOutcome::ErrUnknownCall => self.wire.push(WireFact::Reply {
                call_id: id,
                outcome: OutcomeFact::Err {
                    kind: "unknown-call",
                    retryable: false,
                },
            }),
            WOutcome::ErrInternal => self.wire.push(WireFact::Reply {
                call_id: id,
                outcome: OutcomeFact::Err {
                    kind: "internal",
                    retryable: false,
                },
            }),
            WOutcome::Cancelled => self.wire.push(WireFact::Reply {
                call_id: id,
                outcome: OutcomeFact::Cancelled,
            }),
        }
        self.worker_calls.remove(pos);
        self.retired_worker_calls.push(id);
        if reclaim {
            let mut reclaimed = Vec::new();
            for handle in minted {
                if let Some(pos) = self.handles.iter().position(|entry| entry.id == handle) {
                    let entry = self.handles.remove(pos);
                    let kind = entry.kind;
                    self.live_handles -= 1;
                    self.reclaimed_handles.push((handle, kind));
                    reclaimed.push(handle);
                }
            }
            if !reclaimed.is_empty() {
                self.events
                    .push(EventFact::HandlesReclaimed { handles: reclaimed });
            }
        }
    }

    pub(super) fn mint(&mut self, id: u64) {
        if self.worker_call(id).is_none() {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        }
        if self.budget_full() {
            self.events.push(EventFact::AppErr("SessionRetired"));
            return;
        }
        if self.live_handles >= LIVE_HANDLES_MAX {
            self.events.push(EventFact::AppErr("HandleCeiling"));
            return;
        }
        let handle = self.next_handle;
        self.next_handle += 1;
        self.handles.push(MHandle {
            id: handle,
            kind: Kind::Capability,
            artifact: None,
        });
        self.live_handles += 1;
        self.worker_call_mut(id)
            .expect("checked")
            .minted
            .push(handle);
    }

    pub(super) fn offer_artifact(&mut self, id: u64, bytes: u32) {
        if self.worker_call(id).is_none() {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        }
        if bytes == 0 {
            self.events.push(EventFact::AppErr("InvalidField"));
            return;
        }
        if self.budget_full() {
            self.events.push(EventFact::AppErr("SessionRetired"));
            return;
        }
        if self.live_handles >= LIVE_HANDLES_MAX {
            self.events.push(EventFact::AppErr("HandleCeiling"));
            return;
        }
        let handle = self.next_handle;
        self.next_handle += 1;
        self.handles.push(MHandle {
            id: handle,
            kind: Kind::Artifact,
            artifact: Some((bytes, "text/plain".to_owned())),
        });
        self.live_handles += 1;
        self.worker_call_mut(id)
            .expect("checked")
            .minted
            .push(handle);
        self.worker_call_mut(id)
            .expect("checked")
            .artifacts
            .push((handle, bytes));
    }

    pub(super) fn host_release(&mut self, handle: u64, kind: Kind) {
        match self
            .offered_worker_handles
            .iter()
            .find(|(h, _)| *h == handle)
        {
            None => {
                self.events.push(EventFact::AppErr("UnknownWorkerHandle"));
                return;
            }
            Some((_, offered)) if *offered != kind => {
                self.events.push(EventFact::AppErr("InvalidField"));
                return;
            }
            Some(_) => {}
        }
        if self.retired_worker_handles.contains(&handle) {
            self.events.push(EventFact::AppErr("AlreadyReleased"));
            return;
        }
        if self.pending_releases.iter().any(|(h, _)| *h == handle) {
            self.events.push(EventFact::AppErr("ReleasePending"));
            return;
        }
        if self.budget_full() {
            self.events.push(EventFact::AppErr("SessionRetired"));
            return;
        }
        if self.pending_releases.len() as u32 >= LIVE_HANDLES_MAX {
            self.events.push(EventFact::AppErr("HandleCeiling"));
            return;
        }
        self.pending_releases.push((handle, kind));
        self.wire.push(WireFact::Release { handle, kind });
    }

    pub(super) fn host_open_stream(&mut self, id: u64, credit: u32) {
        let Some(state) = self.worker_call_mut(id) else {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        };
        if !state.stream {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        if state.stream_credit.is_some() {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        if credit == 0 || credit > MAX_STREAM_CREDIT {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        state.stream_credit = Some(credit);
        self.wire.push(WireFact::StreamOpen {
            call_id: id,
            credit,
        });
    }

    pub(super) fn host_grant_credit(&mut self, id: u64, additional: u32) {
        let Some(state) = self.host_call_mut(id) else {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        };
        if !state.open {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        let Some(next) = state.credit_left.checked_add(additional) else {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        };
        if additional == 0 || next > MAX_STREAM_CREDIT {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        state.credit_left = next;
        self.wire.push(WireFact::Credit {
            call_id: id,
            additional,
        });
    }

    pub(super) fn host_stream_item(&mut self, id: u64, lossless: bool, more: bool) {
        let Some(state) = self.worker_call_mut(id) else {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        };
        let Some(window) = state.stream_credit else {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        };
        if state.last_sent {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        if lossless {
            if window == 0 {
                self.events.push(EventFact::AppErr("StreamViolation"));
                return;
            }
            state.stream_credit = Some(window - 1);
        }
        let seq = state.next_seq;
        state.next_seq += 1;
        if !more {
            state.last_sent = true;
        }
        let dropped = state.lossy_dropped;
        self.wire.push(WireFact::StreamData {
            call_id: id,
            seq,
            more,
            lossless,
            dropped,
        });
    }

    pub(super) fn host_note_drops(&mut self, id: u64, dropped: u64) {
        let Some(state) = self.worker_call_mut(id) else {
            self.events.push(EventFact::AppErr("UnknownCall"));
            return;
        };
        if state.stream_credit.is_none() {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        if state.last_sent {
            self.events.push(EventFact::AppErr("StreamViolation"));
            return;
        }
        state.lossy_dropped = state.lossy_dropped.saturating_add(dropped);
    }
}
