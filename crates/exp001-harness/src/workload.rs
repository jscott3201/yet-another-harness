//! Deterministic command-mix generation (EXP-001 §5).
//!
//! Store-independent: a `CommitSpec` says what one transaction must write —
//! the current-state CAS, the receipt claim, the journal appends, and any
//! effect-intent or artifact rows — and the Selene mapping layer executes it.
//! Everything derives from the trial seed; no wall clock, no thread timing
//! leaks into the spec stream (§10: a failing trial replays exactly).

use crate::schema::{digest, CommandKind, EffectState, SemanticEvent};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

/// Unit lifecycle the mix walks. Mirrors the dispatch → work → review → decide
/// flow in validation/01's transition protocol; terminal units leave the
/// population and a fresh unit takes their slot, so long arms don't starve.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnitPhase {
    Ready,
    Working,
    Reviewing,
    Terminal,
}

/// One transaction's obligations. `expected_version` makes the CAS explicit:
/// the mapping layer must fail the commit if the unit's stored version moved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitSpec {
    pub writer: u32,
    pub step: u32,
    pub kind: CommandKind,
    pub unit_id: u64,
    pub expected_version: u64,
    pub new_phase: UnitPhase,
    pub command_id: u64,
    pub request_digest: String,
    pub events: Vec<SemanticEvent>,
    /// Set on Dispatch (prepared) and ToolCompletion (terminal observation).
    pub effect: Option<EffectRow>,
    /// Set on ReviewEvidence: a content-addressed artifact reference committed
    /// in the same write set — the §6 artifact-publication kill cell depends
    /// on these existing in the stream.
    pub artifact_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectRow {
    pub intent_id: u64,
    pub operation_key: String,
    pub state: EffectState,
}

/// Shared-unit population sized to force CAS contention between writer
/// threads: fewer units than writers × in-flight steps means overlapping
/// claims are common, not rare.
pub struct UnitPool {
    next_unit: u64,
    pub units: Vec<(u64, UnitPhase, u64)>, // (unit_id, phase, version)
}

impl UnitPool {
    pub fn new(size: usize) -> UnitPool {
        let units = (0..size as u64).map(|id| (id, UnitPhase::Ready, 0)).collect();
        UnitPool { next_unit: size as u64, units }
    }

    fn replace_terminal(&mut self, slot: usize) {
        self.next_unit += 1;
        self.units[slot] = (self.next_unit, UnitPhase::Ready, 0);
    }
}

/// Per-writer deterministic stream. Writer streams interleave through the
/// store's own concurrency control at run time; determinism holds per-stream,
/// not across the interleaving — the §7 HARD bars are exactly the properties
/// that must survive any interleaving.
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
        WriterStream {
            writer,
            rng: ChaCha20Rng::from_seed(seed),
            step: 0,
            command_seq: 0,
        }
    }

    /// Next legal command against the pool. Takes the pool by exclusive
    /// reference: phase bookkeeping here is the generator's view; the store's
    /// CAS remains the arbiter under real interleaving.
    pub fn next_spec(&mut self, pool: &mut UnitPool) -> CommitSpec {
        let slot = self.rng.random_range(0..pool.units.len());
        let (unit_id, phase, version) = pool.units[slot];

        let kind = match phase {
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
            UnitPhase::Terminal => unreachable!("terminal units are replaced on transition"),
        };

        let new_phase = match kind {
            CommandKind::Dispatch => UnitPhase::Working,
            CommandKind::LeaseRenewal | CommandKind::ProgressRollup => UnitPhase::Working,
            CommandKind::ToolCompletion => UnitPhase::Reviewing,
            CommandKind::ReviewEvidence => UnitPhase::Reviewing,
            CommandKind::Cancellation | CommandKind::OwnerDecision => UnitPhase::Terminal,
        };

        self.step += 1;
        self.command_seq += 1;
        let command_id = (u64::from(self.writer) << 40) | self.command_seq;
        let payload = self.payload(kind, unit_id);
        let event = SemanticEvent {
            event_id: (command_id << 8) | 1,
            aggregate_id: unit_id,
            aggregate_version: version + 1,
            ordinal: 0,
            kind: format!("{kind:?}"),
            payload: payload.clone(),
            causation_ref: Some(command_id),
        };

        let effect = match kind {
            CommandKind::Dispatch => Some(EffectRow {
                intent_id: (command_id << 8) | 2,
                operation_key: format!("op-{unit_id}-{command_id}"),
                state: EffectState::Prepared,
            }),
            CommandKind::ToolCompletion => Some(EffectRow {
                intent_id: (command_id << 8) | 2,
                operation_key: format!("op-{unit_id}-{command_id}"),
                state: EffectState::Succeeded,
            }),
            _ => None,
        };

        let artifact_ref = (kind == CommandKind::ReviewEvidence)
            .then(|| digest(payload.as_bytes()));

        let spec = CommitSpec {
            writer: self.writer,
            step: self.step,
            kind,
            unit_id,
            expected_version: version,
            new_phase,
            command_id,
            request_digest: digest(payload.as_bytes()),
            events: vec![event],
            effect,
            artifact_ref,
        };

        if new_phase == UnitPhase::Terminal {
            pool.replace_terminal(slot);
        } else {
            pool.units[slot] = (unit_id, new_phase, version + 1);
        }
        spec
    }

    /// 200–500 bytes of deterministic JSON per §4's load-sizing placeholder.
    fn payload(&mut self, kind: CommandKind, unit_id: u64) -> String {
        // Skeleton + values add ~55–75 bytes; this range keeps the whole
        // payload inside §4's 200–500 byte placeholder at both extremes.
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

    fn generate(seed: u64, writers: u32, steps: u32) -> Vec<CommitSpec> {
        let mut pool = UnitPool::new((writers * 4) as usize);
        let mut streams: Vec<_> = (0..writers).map(|w| WriterStream::new(seed, w)).collect();
        let mut out = Vec::new();
        for _ in 0..steps {
            for s in &mut streams {
                out.push(s.next_spec(&mut pool));
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
            assert!(
                specs.iter().any(|s| s.kind == kind),
                "{kind:?} never generated"
            );
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
}
