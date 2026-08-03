//! Deterministic command-mix generation (EXP-001 §5).
//!
//! Store-independent: a `CommitSpec` says what one transaction must write —
//! the current-state CAS, the receipt claim, the journal appends, and any
//! effect or artifact rows — and the store layer executes it. Everything
//! derives from the trial seed; no wall clock leaks into the spec stream
//! (§10: a failing trial replays exactly).
//!
//! The pool is claim-based: a writer claims a unit slot, applies the spec,
//! releases the slot. That models R2's disjoint write ownership — fan-in
//! pressure lands on the commit pipeline, where §5 wants it, not on
//! generator-side CAS thrash. Two deliberate exceptions exercise rejection
//! paths: seeded stale-lease injections (bar 6 needs superseded
//! `(unit, epoch, holder)` triples in the stream) and the funnel's own CAS,
//! which stays armed as defense against harness bugs and kill races.

use crate::schema::{digest, CommandKind, EffectState, SemanticEvent};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitPhase {
    Ready,
    Working,
    Reviewing,
    Terminal,
}

/// A rejection the generator *planned*: the spec is expected to fail typed,
/// and an accepted apply is a bar-6 violation, not a success.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedRejection {
    /// Superseded attempt epoch on a renewal or write.
    StaleLease,
    /// Wrong holder for the current epoch — bar 6's "or a write" half needs
    /// both dimensions injected (review finding, 2026-08-03).
    StaleHolder,
}

/// One transaction's obligations. `expected_version` makes the CAS explicit:
/// the store layer must fail the commit if the unit's stored version moved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitSpec {
    pub writer: u32,
    pub step: u32,
    pub kind: CommandKind,
    pub slot: usize,
    pub unit_id: u64,
    pub expected_version: u64,
    /// The attempt epoch this command claims to act under. Stale-lease
    /// injections deliberately carry a superseded value.
    pub attempt_epoch: u32,
    pub holder_id: u64,
    pub new_phase: UnitPhase,
    pub expect_reject: Option<ExpectedRejection>,
    pub command_id: u64,
    pub request_digest: String,
    pub events: Vec<SemanticEvent>,
    /// Set on Dispatch (prepared) and ToolCompletion (terminal observation) —
    /// write-once observation rows, not in-place state flips.
    pub effect: Option<EffectRow>,
    /// Set on ReviewEvidence: a content-addressed artifact row committed in
    /// the same write set — the §6 artifact-publication kill cell depends on
    /// these existing in the stream.
    pub artifact_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectRow {
    pub intent_id: u64,
    pub operation_key: String,
    pub state: EffectState,
}

#[derive(Debug, Clone)]
pub struct Slot {
    pub unit_id: u64,
    pub phase: UnitPhase,
    pub version: u64,
    pub attempt_epoch: u32,
    pub claimed: bool,
    /// Holder of record for the current epoch: the writer that dispatched it.
    /// Non-dispatch commands act on the holder's behalf and carry this id;
    /// the funnel rejects a mismatch typed (bar 6's holder dimension).
    pub holder: u64,
    /// Operation key of the outstanding effect intent, if any. ToolCompletion
    /// reuses it for the terminal observation row (A13-style key stability);
    /// a key still open at audit time is a nonterminal intent.
    pub open_op: Option<String>,
}

/// Shared unit population. Sized larger than the writer count so claims
/// rarely starve, small enough that commit fan-in stays dense.
pub struct UnitPool {
    next_unit: u64,
    pub slots: Vec<Slot>,
}

impl UnitPool {
    pub fn new(size: usize) -> UnitPool {
        let slots = (0..size as u64)
            .map(|id| Slot {
                unit_id: id,
                phase: UnitPhase::Ready,
                version: 0,
                attempt_epoch: 0,
                claimed: false,
                holder: 0,
                open_op: None,
            })
            .collect();
        UnitPool { next_unit: size as u64, slots }
    }

    /// Release a claimed slot, folding in the apply outcome. A rejected apply
    /// releases with `advanced = false`: the store did not move, so the pool
    /// must not either.
    pub fn release(&mut self, slot: usize, spec: &CommitSpec, advanced: bool) {
        debug_assert!(self.slots[slot].claimed);
        if advanced {
            if spec.new_phase == UnitPhase::Terminal {
                self.next_unit += 1;
                self.slots[slot] = Slot {
                    unit_id: self.next_unit,
                    phase: UnitPhase::Ready,
                    version: 0,
                    attempt_epoch: 0,
                    claimed: false,
                    holder: 0,
                    open_op: None,
                };
                return;
            }
            let s = &mut self.slots[slot];
            s.phase = spec.new_phase;
            s.version += 1;
            match spec.kind {
                CommandKind::Dispatch => {
                    s.attempt_epoch = spec.attempt_epoch;
                    s.holder = spec.holder_id;
                    s.open_op = spec.effect.as_ref().map(|e| e.operation_key.clone());
                }
                CommandKind::ToolCompletion => s.open_op = None,
                _ => {}
            }
        }
        self.slots[slot].claimed = false;
    }
}

pub struct WriterStream {
    pub writer: u32,
    rng: ChaCha20Rng,
    step: u32,
    command_seq: u64,
}

impl WriterStream {
    pub fn new(trial_seed: u64, writer: u32) -> WriterStream {
        let mut seed = [0u8; 32];
        seed[..8].copy_from_slice(&trial_seed.to_le_bytes());
        seed[8..12].copy_from_slice(&writer.to_le_bytes());
        WriterStream { writer, rng: ChaCha20Rng::from_seed(seed), step: 0, command_seq: 0 }
    }

    /// Claim an unclaimed slot and produce its next legal command. Returns
    /// `None` when every slot is claimed (caller backs off and retries) —
    /// with pool size ≥ 2× writers this is rare, and the miss itself consumes
    /// no randomness, so determinism per stream is unaffected by timing.
    pub fn next_spec(&mut self, pool: &mut UnitPool) -> Option<CommitSpec> {
        let free: Vec<usize> = pool
            .slots
            .iter()
            .enumerate()
            .filter(|(_, s)| !s.claimed && s.phase != UnitPhase::Terminal)
            .map(|(i, _)| i)
            .collect();
        if free.is_empty() {
            return None;
        }
        let slot = free[self.rng.random_range(0..free.len())];
        let s = pool.slots[slot].clone();
        pool.slots[slot].claimed = true;

        let kind = match s.phase {
            UnitPhase::Ready => CommandKind::Dispatch,
            UnitPhase::Working => match self.rng.random_range(0..10u32) {
                0..=3 => CommandKind::LeaseRenewal,
                4..=6 => CommandKind::ProgressRollup,
                7..=8 => CommandKind::ToolCompletion,
                _ => CommandKind::Cancellation,
            },
            UnitPhase::Reviewing => match self.rng.random_range(0..3u32) {
                0..=1 => CommandKind::ReviewEvidence,
                _ => CommandKind::OwnerDecision,
            },
            UnitPhase::Terminal => unreachable!("terminal slots are filtered out"),
        };

        // Cancellation is rework, not death: the unit returns to Ready and the
        // next Dispatch opens a new attempt epoch (mirrors R2.6). Only
        // OwnerDecision retires a unit. Epochs therefore grow past 1, which
        // the stale-lease injection below needs.
        let new_phase = match kind {
            CommandKind::Dispatch
            | CommandKind::LeaseRenewal
            | CommandKind::ProgressRollup => UnitPhase::Working,
            CommandKind::ToolCompletion | CommandKind::ReviewEvidence => UnitPhase::Reviewing,
            CommandKind::Cancellation => UnitPhase::Ready,
            CommandKind::OwnerDecision => UnitPhase::Terminal,
        };

        let mut attempt_epoch = if kind == CommandKind::Dispatch { s.attempt_epoch + 1 } else { s.attempt_epoch };
        // Bar 6 injections cover both dimensions of the superseded triple, on
        // renewals AND writes: a stale epoch (needs a genuinely superseded
        // epoch, so >= 2) or a wrong holder for the current epoch.
        let mut holder_id = if kind == CommandKind::Dispatch { u64::from(self.writer) } else { s.holder };
        let mut expect_reject = None;
        let injectable = matches!(
            kind,
            CommandKind::LeaseRenewal | CommandKind::ProgressRollup | CommandKind::ToolCompletion
        );
        if injectable {
            match self.rng.random_range(0..32u32) {
                0 if s.attempt_epoch >= 2 => {
                    attempt_epoch = s.attempt_epoch - 1;
                    expect_reject = Some(ExpectedRejection::StaleLease);
                }
                1 => {
                    holder_id = s.holder ^ 0x5741_1E00;
                    expect_reject = Some(ExpectedRejection::StaleHolder);
                }
                _ => {}
            }
        }

        self.step += 1;
        self.command_seq += 1;
        let command_id = (u64::from(self.writer) << 40) | self.command_seq;
        let payload = self.payload(kind, s.unit_id);
        let event = SemanticEvent {
            event_id: (command_id << 8) | 1,
            aggregate_id: s.unit_id,
            aggregate_version: s.version + 1,
            ordinal: 0,
            kind: format!("{kind:?}"),
            payload: payload.clone(),
            causation_ref: Some(command_id),
        };

        let effect = match kind {
            CommandKind::Dispatch => Some(EffectRow {
                intent_id: (command_id << 8) | 2,
                operation_key: format!("op-{}-{command_id}", s.unit_id),
                state: EffectState::Prepared,
            }),
            // Terminal observation of the outstanding intent: SAME operation
            // key, fresh row. Falls back to a self-keyed row if no intent is
            // open (a cancelled-then-redispatched lineage can complete a tool
            // under a fresh epoch's key).
            CommandKind::ToolCompletion => Some(EffectRow {
                intent_id: (command_id << 8) | 2,
                operation_key: s
                    .open_op
                    .clone()
                    .unwrap_or_else(|| format!("op-{}-{command_id}", s.unit_id)),
                state: EffectState::Succeeded,
            }),
            _ => None,
        };

        let artifact_ref = (kind == CommandKind::ReviewEvidence).then(|| digest(payload.as_bytes()));

        Some(CommitSpec {
            writer: self.writer,
            step: self.step,
            kind,
            slot,
            unit_id: s.unit_id,
            expected_version: s.version,
            attempt_epoch,
            holder_id,
            new_phase,
            expect_reject,
            command_id,
            request_digest: digest(payload.as_bytes()),
            events: vec![event],
            effect,
            artifact_ref,
        })
    }

    /// 200–500 bytes of deterministic JSON per §4's load-sizing placeholder.
    /// Skeleton + values add ~55–75 bytes; this filler range keeps the whole
    /// payload inside the placeholder at both extremes.
    fn payload(&mut self, kind: CommandKind, unit_id: u64) -> String {
        let filler_len = self.rng.random_range(160..420usize);
        let filler: String = (0..filler_len)
            .map(|_| char::from(self.rng.random_range(b'a'..=b'z')))
            .collect();
        format!(
            r#"{{"kind":"{kind:?}","unit":{unit_id},"writer":{},"step":{},"filler":"{filler}"}}"#,
            self.writer, self.step
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Single-threaded claim/apply/release loop: every spec applies, so this
    /// exercises generation determinism, not contention.
    fn generate(seed: u64, writers: u32, steps: u32) -> Vec<CommitSpec> {
        let mut pool = UnitPool::new((writers * 4) as usize);
        let mut streams: Vec<_> = (0..writers).map(|w| WriterStream::new(seed, w)).collect();
        let mut out = Vec::new();
        for _ in 0..steps {
            for s in &mut streams {
                let spec = s.next_spec(&mut pool).expect("pool has free slots");
                let advanced = spec.expect_reject.is_none();
                pool.release(spec.slot, &spec, advanced);
                out.push(spec);
            }
        }
        out
    }

    #[test]
    fn streams_replay_byte_identically() {
        let a = serde_json::to_string(&generate(42, 8, 50)).unwrap();
        let b = serde_json::to_string(&generate(42, 8, 50)).unwrap();
        assert_eq!(a, b);
        let c = serde_json::to_string(&generate(43, 8, 50)).unwrap();
        assert_ne!(a, c);
    }

    #[test]
    fn payloads_hold_the_size_placeholder() {
        for spec in generate(7, 2, 200) {
            let len = spec.events[0].payload.len();
            assert!((200..=500).contains(&len), "payload {len} bytes");
        }
    }

    #[test]
    fn mix_covers_all_seven_commands() {
        let specs = generate(11, 8, 400);
        for kind in CommandKind::MIX {
            assert!(specs.iter().any(|s| s.kind == kind), "{kind:?} never generated");
        }
    }

    #[test]
    fn artifact_refs_ride_review_evidence() {
        let specs = generate(11, 8, 400);
        assert!(specs
            .iter()
            .all(|s| s.artifact_ref.is_some() == (s.kind == CommandKind::ReviewEvidence)));
        assert!(specs.iter().any(|s| s.artifact_ref.is_some()));
    }

    #[test]
    fn rework_grows_epochs_and_injects_stale_leases() {
        let specs = generate(13, 8, 600);
        assert!(
            specs.iter().any(|s| s.kind == CommandKind::Dispatch && s.attempt_epoch >= 2),
            "no unit ever re-dispatched; stale-lease coverage is impossible"
        );
        let stale: Vec<_> = specs.iter().filter(|s| s.expect_reject.is_some()).collect();
        assert!(!stale.is_empty(), "no bar-6 injection in 4800 specs");
        assert!(
            stale.iter().any(|s| s.expect_reject == Some(ExpectedRejection::StaleLease)),
            "no stale-epoch injection"
        );
        assert!(
            stale.iter().any(|s| s.expect_reject == Some(ExpectedRejection::StaleHolder)),
            "no stale-holder injection"
        );
        assert!(
            stale.iter().any(|s| s.kind != CommandKind::LeaseRenewal),
            "bar 6's write half never injected"
        );
    }

    #[test]
    fn claims_are_exclusive() {
        let mut pool = UnitPool::new(4);
        let mut a = WriterStream::new(5, 0);
        let mut b = WriterStream::new(5, 1);
        let sa = a.next_spec(&mut pool).unwrap();
        let sb = b.next_spec(&mut pool).unwrap();
        assert_ne!(sa.slot, sb.slot, "two writers claimed one slot");
        // Exhaust the pool: claims without release must eventually return None.
        assert!(a.next_spec(&mut pool).is_some());
        assert!(b.next_spec(&mut pool).is_some());
        assert!(a.next_spec(&mut pool).is_none());
    }
}
