//! Post-run audit read-back and WAL polling (split from the store to
//! honor the per-file LOC cap).

use super::*;

/// Row maps the auditor scores against (one field per property the bars
/// reference; payload carried whole for byte-identity checks).
#[derive(Debug, Default)]
pub struct AuditSnapshot {
    pub units: HashMap<u64, UnitRow>,
    pub events: HashMap<u64, EventRow>,
    pub receipts: HashMap<String, ReceiptRow>,
    pub effects: HashMap<u64, EffectStoreRow>,
    pub artifacts: std::collections::BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct UnitRow {
    pub version: u64,
    pub attempt_epoch: u32,
    pub phase: String,
    pub artifact_ref: Option<String>,
}

#[derive(Debug, Clone)]
pub struct EventRow {
    pub aggregate_id: u64,
    pub aggregate_version: u64,
    pub payload: String,
}

#[derive(Debug, Clone)]
pub struct ReceiptRow {
    pub request_digest: String,
    pub transition_ref: u64,
}

#[derive(Debug, Clone)]
pub struct EffectStoreRow {
    pub operation_key: String,
    pub state: String,
    pub unit_id: u64,
}

/// The R26c watch substitute: poll the WAL for entries after `after`, trusting
/// only entries at or below `watermark` (the committer's `durable_at`) — an
/// unfiltered poll can observe appended-but-unflushed entries, which is the
/// I13 hazard the amended cell documents.
pub fn poll_wal(
    wal_path: &Path,
    after: u64,
    watermark: u64,
) -> PersistResult<Vec<(u64, Vec<Change>)>> {
    let reader = WalReader::open(wal_path)?;
    let stream = reader.iterate(move |h| h.sequence > after && h.sequence <= watermark)?;
    let mut out = Vec::new();
    for entry in stream {
        match entry {
            Ok(entry) => {
                let seq = entry.header.sequence;
                out.push((seq, entry.body()?));
            }
            // A torn tail is a live in-flight append, necessarily past the
            // durability frontier (the committer fsyncs before acking), so
            // stopping here returns the complete durable prefix. Any other
            // error (e.g. a checksum mismatch mid-file) propagates.
            Err(PersistError::TruncatedEntry { .. }) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}
