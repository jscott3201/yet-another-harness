//! HARD-bar auditor (EXP-001 §7): zero-tolerance scoring of a recovered
//! store against the sidecar's confirmed commits.
//!
//! Direction of the two evidence sources matters. Sidecar ⇒ store is the
//! loss bar: a confirmed commit must be fully present. Store ⇏ sidecar: a
//! durable commit with no sidecar line is legal (killed in the response
//! window) but must still be internally consistent — that is the
//! agreement/atomicity family. The auditor fails closed and names the
//! affected aggregate (substrate gap 5's obligation); it never repairs.

use serde::{Deserialize, Serialize};

use crate::sidecar::Confirmed;
use crate::store::AuditSnapshot;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bar {
    /// Current state advanced without its paired journal event, or the
    /// reverse — any partial write-set (invariant I1).
    Atomicity,
    /// A sidecar-confirmed commit is missing or byte-different after reopen.
    CommittedTransitionLoss,
    /// Receipt/state/event triple disagreement after recovery.
    Agreement,
    /// A planned-stale spec was accepted at apply time.
    StaleLeaseAcceptance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Violation {
    pub bar: Bar,
    /// Names the affected aggregate/row — fail closed, never invent.
    pub detail: String,
}

/// Nonterminal effect intents surface as classification data, not violations:
/// §6's mid-effect cell requires them reported and left unresolved, and any
/// auto-repair here would itself violate the cell.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    pub violations: Vec<Violation>,
    pub nonterminal_intents: Vec<u64>,
    pub units_checked: usize,
    pub events_checked: usize,
    pub confirmed_checked: usize,
}

pub fn score(snap: &AuditSnapshot, confirmed: &[Confirmed]) -> AuditReport {
    let mut v = Vec::new();

    // Loss bar: sidecar ⇒ store, byte-identical.
    for rec in confirmed {
        if rec.stale_accepted {
            v.push(Violation {
                bar: Bar::StaleLeaseAcceptance,
                detail: format!(
                    "unit {} accepted epoch {} while current epoch was newer",
                    rec.spec.unit_id, rec.spec.attempt_epoch
                ),
            });
        }
        let ev = &rec.spec.events[0];
        match snap.events.get(&ev.event_id) {
            None => v.push(Violation {
                bar: Bar::CommittedTransitionLoss,
                detail: format!(
                    "event {} (unit {}, version {}) confirmed durable but absent after reopen",
                    ev.event_id, ev.aggregate_id, ev.aggregate_version
                ),
            }),
            Some(row) => {
                if row.payload != ev.payload {
                    v.push(Violation {
                        bar: Bar::CommittedTransitionLoss,
                        detail: format!("event {} payload differs after reopen", ev.event_id),
                    });
                }
            }
        }
        let receipt_key = format!("{}/{}", rec.spec.unit_id, rec.spec.command_id);
        match snap.receipts.get(&receipt_key) {
            None => v.push(Violation {
                bar: Bar::CommittedTransitionLoss,
                detail: format!("receipt {receipt_key} confirmed durable but absent after reopen"),
            }),
            Some(r) => {
                if r.request_digest != rec.spec.request_digest || r.transition_ref != ev.event_id {
                    v.push(Violation {
                        bar: Bar::Agreement,
                        detail: format!("receipt {receipt_key} disagrees with its confirmed spec"),
                    });
                }
            }
        }
        match snap.units.get(&rec.spec.unit_id) {
            None => v.push(Violation {
                bar: Bar::CommittedTransitionLoss,
                detail: format!(
                    "unit {} confirmed durable but absent after reopen",
                    rec.spec.unit_id
                ),
            }),
            Some(u) => {
                if u.version < ev.aggregate_version {
                    v.push(Violation {
                        bar: Bar::CommittedTransitionLoss,
                        detail: format!(
                            "unit {} at version {} below confirmed transition {}",
                            rec.spec.unit_id, u.version, ev.aggregate_version
                        ),
                    });
                }
            }
        }
    }

    // Agreement/atomicity family over the whole store, independent of the
    // sidecar: every unit's version V is matched by events at exactly
    // versions 1..=V, each with exactly one receipt; no orphans in either
    // direction; artifact refs resolve.
    let mut events_by_aggregate: std::collections::HashMap<u64, Vec<u64>> = Default::default();
    for row in snap.events.values() {
        events_by_aggregate
            .entry(row.aggregate_id)
            .or_default()
            .push(row.aggregate_version);
    }
    for (unit_id, unit) in &snap.units {
        let mut versions = events_by_aggregate.remove(unit_id).unwrap_or_default();
        versions.sort_unstable();
        let expected: Vec<u64> = (1..=unit.version).collect();
        if versions != expected {
            v.push(Violation {
                bar: Bar::Atomicity,
                detail: format!(
                    "unit {unit_id} at version {} has journal versions {versions:?} (want 1..={})",
                    unit.version, unit.version
                ),
            });
        }
        if let Some(a) = &unit.artifact_ref
            && !snap.artifacts.contains(a)
        {
            v.push(Violation {
                bar: Bar::Atomicity,
                detail: format!("unit {unit_id} references unpublished artifact {a}"),
            });
        }
    }
    for (aggregate_id, versions) in events_by_aggregate {
        v.push(Violation {
            bar: Bar::Agreement,
            detail: format!("events {versions:?} exist for unknown unit {aggregate_id}"),
        });
    }

    let mut receipts_by_event: std::collections::HashMap<u64, u32> = Default::default();
    for r in snap.receipts.values() {
        *receipts_by_event.entry(r.transition_ref).or_default() += 1;
    }
    for (event_id, row) in &snap.events {
        match receipts_by_event.remove(event_id) {
            Some(1) => {}
            Some(n) => v.push(Violation {
                bar: Bar::Agreement,
                detail: format!(
                    "event {event_id} (unit {}) has {n} receipts",
                    row.aggregate_id
                ),
            }),
            None => v.push(Violation {
                bar: Bar::Atomicity,
                detail: format!(
                    "event {event_id} (unit {}) has no receipt",
                    row.aggregate_id
                ),
            }),
        }
    }
    for (event_id, n) in receipts_by_event {
        v.push(Violation {
            bar: Bar::Agreement,
            detail: format!("{n} receipt(s) reference nonexistent event {event_id}"),
        });
    }

    // Nonterminal = an operation key with a Prepared row and no terminal
    // observation row sharing the key (op-key continuity: ToolCompletion
    // reuses the dispatch's key).
    let mut terminal_ops: std::collections::HashSet<&str> = Default::default();
    for e in snap.effects.values() {
        if e.state != "Prepared" && e.state != "Dispatched" {
            terminal_ops.insert(e.operation_key.as_str());
        }
    }
    let nonterminal_intents: Vec<u64> = snap
        .effects
        .iter()
        .filter(|(_, e)| {
            (e.state == "Prepared" || e.state == "Dispatched")
                && !terminal_ops.contains(e.operation_key.as_str())
        })
        .map(|(id, _)| *id)
        .collect();

    AuditReport {
        violations: v,
        nonterminal_intents,
        units_checked: snap.units.len(),
        events_checked: snap.events.len(),
        confirmed_checked: confirmed.len(),
    }
}
