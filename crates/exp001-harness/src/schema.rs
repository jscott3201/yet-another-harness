//! Harness-shaped records and cell vocabulary (EXP-001 §4–§6).
//!
//! Illustrative by ruling: ADR-001 owns the byte-bounded field lists and closed
//! enums. These are the minimum shapes needed to generate a realistic write mix
//! against Selene; the gate re-runs when ADR-001's real schema supersedes them
//! (EXP-001 §4). Field names mirror the card's table so trial logs read against
//! the card without a translation step.

use serde::{Deserialize, Serialize};

/// Wire form fixed by the 2026-08-02 integration ruling (R-DIG): BLAKE3-256,
/// `blake3:<64 hex>`.
pub fn digest(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionUnit {
    pub unit_id: u64,
    pub work_id: u64,
    pub current_attempt_epoch: u32,
    pub state: String,
    pub policy_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Active,
    Terminal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attempt {
    pub unit_id: u64,
    pub attempt_epoch: u32,
    pub holder_id: u64,
    pub state: AttemptState,
    pub terminal_result_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lease {
    pub unit_id: u64,
    pub attempt_epoch: u32,
    pub holder_id: u64,
    /// Milliseconds on the harness's logical clock — the gate never reads
    /// wall-clock time inside a trial, so replays are exact (EXP-001 §10 seed
    /// obligation).
    pub expiry: u64,
    pub last_renewal: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Prepared,
    Dispatched,
    Succeeded,
    Failed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialObservation {
    pub target: String,
    pub digest: String,
    pub state: EffectState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectIntent {
    pub intent_id: u64,
    pub unit_id: u64,
    pub attempt_epoch: u32,
    pub operation_key: String,
    pub adapter_id: String,
    pub state: EffectState,
    pub partial_observations: Vec<PartialObservation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub scope: String,
    pub command_id: u64,
    pub request_digest: String,
    pub result_ref: String,
    pub transition_ref: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SemanticEvent {
    pub event_id: u64,
    pub aggregate_id: u64,
    pub aggregate_version: u64,
    pub ordinal: u32,
    pub kind: String,
    /// 200–500 bytes of JSON per EXP-001 §4's load-sizing placeholder.
    pub payload: String,
    pub causation_ref: Option<u64>,
}

/// The §5 command mix. One transaction per semantic transition; every commit
/// carries both a current-state change and a journal append.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandKind {
    Dispatch,
    LeaseRenewal,
    /// Durable rollup, not per-tick journaling — ADR-002 P8.5 makes live
    /// progress ephemeral, so the mix substitutes a periodic rollup commit
    /// (EXP-001 §5).
    ProgressRollup,
    ToolCompletion,
    ReviewEvidence,
    Cancellation,
    OwnerDecision,
}

impl CommandKind {
    pub const MIX: [CommandKind; 7] = [
        CommandKind::Dispatch,
        CommandKind::LeaseRenewal,
        CommandKind::ProgressRollup,
        CommandKind::ToolCompletion,
        CommandKind::ReviewEvidence,
        CommandKind::Cancellation,
        CommandKind::OwnerDecision,
    ];
}

/// §6's seven crash-injection points. The artifact row injects both a pre- and
/// a post-publication kill but counts as one point in §9's trial arithmetic
/// (6 arms × 7 points × reps).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KillPoint {
    PreSeal,
    PostSealPrePublish,
    PostPublishPreResponse,
    ArtifactPublication,
    MidEffectIntent,
    WriterTakeover,
    CursorAllocAbort,
}

impl KillPoint {
    pub const ALL: [KillPoint; 7] = [
        KillPoint::PreSeal,
        KillPoint::PostSealPrePublish,
        KillPoint::PostPublishPreResponse,
        KillPoint::ArtifactPublication,
        KillPoint::MidEffectIntent,
        KillPoint::WriterTakeover,
        KillPoint::CursorAllocAbort,
    ];
}

/// A §6 cell: a kill point, or one of the correctness cells that inject no
/// kill. The corruption drill sits outside §9's 840+240 trial minimum but is
/// still a required cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cell {
    Kill(KillPoint),
    JournalImmutability,
    SnapshotWatchHandoff,
    CorruptionDrill,
}

impl Cell {
    pub const NON_KILL: [Cell; 3] = [
        Cell::JournalImmutability,
        Cell::SnapshotWatchHandoff,
        Cell::CorruptionDrill,
    ];

    pub fn all() -> impl Iterator<Item = Cell> {
        KillPoint::ALL
            .into_iter()
            .map(Cell::Kill)
            .chain(Cell::NON_KILL)
    }
}

/// §5 durability axis. `On` uses Selene's documented default bound
/// (≤64 commits / 8 MiB per fsync); R25c rules out a bound sweep for this gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Batching {
    Off,
    DefaultBound,
}

/// One of the six §5 workload arms (3 writer counts × 2 durability modes).
/// Readers are held at 8 across all arms. The 8- and 32-writer arms are
/// capacity evidence only — R2's two-writer admission cap is not reopened here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Arm {
    pub writers: u32,
    pub batching: Batching,
}

pub const READERS: u32 = 8;

impl Arm {
    pub const ALL: [Arm; 6] = [
        Arm {
            writers: 2,
            batching: Batching::Off,
        },
        Arm {
            writers: 2,
            batching: Batching::DefaultBound,
        },
        Arm {
            writers: 8,
            batching: Batching::Off,
        },
        Arm {
            writers: 8,
            batching: Batching::DefaultBound,
        },
        Arm {
            writers: 32,
            batching: Batching::Off,
        },
        Arm {
            writers: 32,
            batching: Batching::DefaultBound,
        },
    ];

    pub fn label(&self) -> String {
        let b = match self.batching {
            Batching::Off => "batchOFF",
            Batching::DefaultBound => "batchON",
        };
        format!("w{}-{}", self.writers, b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_wire_form() {
        let d = digest(b"x");
        assert!(d.starts_with("blake3:"));
        assert_eq!(d.len(), "blake3:".len() + 64);
    }

    #[test]
    fn cell_counts_match_card_arithmetic() {
        // §9: 6 arms × 7 kill points × 20 reps = 840, plus 2 non-kill
        // correctness cells × 20 reps × 6 arms = 240; the corruption drill is
        // additional to that arithmetic.
        assert_eq!(KillPoint::ALL.len(), 7);
        assert_eq!(Cell::all().count(), 10);
        assert_eq!(Arm::ALL.len(), 6);
    }
}
