//! Selene mapping layer: the write funnel (EXP-001 §4 as amended by R26).
//!
//! Every store write flows through [`Store::apply`] — the funnel is the only
//! code path that touches the graph, which is what makes the R26a typed
//! delete rejection meaningful rather than a convention. Enforcement split
//! (the gate report names this per row):
//!
//! - store-enforced: uniqueness (`event_id`, derived `agg_ver_ord`, receipt
//!   key, effect intent, artifact digest) and update-immutability on journal
//!   payload properties — both on a closed graph;
//! - funnel-enforced: journal deletes (Selene has no append-only flag), the
//!   current-state CAS, and the stale-lease epoch check.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use selene_core::Change;
use selene_core::{GraphId, LabelSet, NodeId, PropertyDiff, PropertyMap, Value};
use selene_graph::{CommitBatching, GraphError, RowIndex, SharedGraph};
use selene_persist::{DEFAULT_WAL_FILE_NAME, PersistResult, WalReader};

use crate::schema::{Batching, CommandKind};
use crate::workload::CommitSpec;

// Integration tests and the auditor match on these without depending on the
// selene crates directly.
pub use selene_graph::TypeViolation;
pub use selene_persist::PersistError;

pub const GRAPH_ID: u64 = 1;

mod audit;
mod schema;

pub use audit::*;
pub use schema::graph_type;
use schema::{db, no_labels, props_set, value_u64};

/// Typed funnel rejections. The store never sees the write (EXP-001 §4 as
/// amended: funnel-enforced rows).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rejection {
    StaleVersion {
        unit_id: u64,
        expected: u64,
        actual: u64,
    },
    StaleLease {
        unit_id: u64,
        claimed_epoch: u32,
        current_epoch: u32,
    },
    UnknownUnit {
        unit_id: u64,
    },
    /// Wrong holder for the unit's current epoch (bar 6's second dimension).
    StaleHolder {
        unit_id: u64,
        claimed_holder: u64,
        current_holder: u64,
    },
    /// R26a: deletes of committed journal rows have no store-level rejection
    /// path, so the funnel types the rejection itself.
    JournalDelete {
        event_id: u64,
    },
}

/// A delete request routed through the funnel. Journal-class rows reject
/// typed (R26a); nothing else is deletable in v0 either — the dispatch here
/// is the enforcement point the amended card names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeleteTarget {
    Event { event_id: u64 },
    Receipt { receipt_key: String },
    Artifact { digest: String },
}

#[derive(Debug)]
pub enum ApplyError {
    Rejected(Rejection),
    Graph(GraphError),
    /// Harness-internal incoherence (e.g. an address-book miss for a row the
    /// spec requires). Never a domain rejection: callers treat it as a
    /// harness defect and fail the trial loudly.
    Harness(String),
}

impl From<GraphError> for ApplyError {
    fn from(e: GraphError) -> Self {
        ApplyError::Graph(e)
    }
}

/// What one accepted commit reports back. `durable_at` is the store-global
/// monotonic cursor (WAL sequence; gapped across checkpoints) — the watch
/// cell's watermark and the §5 cursor-allocation evidence.
#[derive(Debug, Clone, Copy)]
pub struct Applied {
    pub generation: u64,
    pub durable_at: Option<u64>,
    pub event_node: NodeId,
}

#[derive(Default)]
struct Books {
    units: HashMap<u64, NodeId>,
    events: HashMap<u64, NodeId>,
    attempts: HashMap<(u64, u32), NodeId>,
    leases: HashMap<(u64, u32), NodeId>,
}

pub struct Store {
    // Private on purpose: the funnel premise (R26a) is that no code path
    // reaches the graph except through this module's methods.
    shared: SharedGraph,
    books: Mutex<Books>,
}

impl Store {
    pub fn wal_path(dir: &Path) -> PathBuf {
        dir.join(DEFAULT_WAL_FILE_NAME)
    }

    /// Create a fresh store in `dir` with the closed harness schema and a WAL.
    pub fn create(dir: &Path, batching: Batching) -> Result<Store, GraphError> {
        let commit_batching = match batching {
            Batching::Off => CommitBatching::Off,
            Batching::DefaultBound => CommitBatching::DEFAULT_ON,
        };
        let shared = SharedGraph::builder(GraphId::new(GRAPH_ID))
            .bound_to(graph_type())?
            .with_wal(Self::wal_path(dir), selene_persist::WalConfig::default())?
            .with_commit_batching(commit_batching)
            .build()?;
        Ok(Store {
            shared,
            books: Mutex::new(Books::default()),
        })
    }

    /// Reopen after a kill. Recovery is a writer takeover: a live holder makes
    /// this fail fast with `WriterLockHeld` rather than block or corrupt.
    pub fn recover(dir: &Path) -> Result<Store, GraphError> {
        let shared = SharedGraph::recover_closed(dir, GraphId::new(GRAPH_ID), graph_type())?;
        let store = Store {
            shared,
            books: Mutex::new(Books::default()),
        };
        store.rebuild_books();
        Ok(store)
    }

    /// Rebuild the id → node address book by scanning live nodes. The book is
    /// an address cache, never truth: every apply re-reads versions in-txn.
    fn rebuild_books(&self) {
        let g = self.shared.read();
        let mut books = self.books.lock().expect("books lock");
        for raw in g.live_nodes().iter() {
            // The bitmap is over internal row indices; node_id_for_row is the
            // sanctioned reverse mapping (rows remap under compaction).
            let Some(id) = g.node_id_for_row(RowIndex::new(raw)) else {
                continue;
            };
            let Some(labels) = g.node_labels(id) else {
                continue;
            };
            let Some(props) = g.node_properties(id) else {
                continue;
            };
            let get_u64 = |name: &str| props.get(&db(name)).and_then(value_u64);
            if labels.contains(&db("Unit")) {
                if let Some(uid) = get_u64("unit_id") {
                    books.units.insert(uid, id);
                }
            } else if labels.contains(&db("Event")) {
                if let Some(eid) = get_u64("event_id") {
                    books.events.insert(eid, id);
                }
            } else if labels.contains(&db("Attempt")) {
                if let (Some(uid), Some(ep)) = (get_u64("unit_id"), get_u64("attempt_epoch")) {
                    books.attempts.insert((uid, ep as u32), id);
                }
            } else if labels.contains(&db("Lease")) {
                // lease_key is "unit/epoch"; recover it from the key string.
                if let Some(Value::String(k)) = props.get(&db("lease_key")) {
                    let s = k.as_str();
                    if let Some((u, e)) = s.split_once('/')
                        && let (Ok(u), Ok(e)) = (u.parse(), e.parse())
                    {
                        books.leases.insert((u, e), id);
                    }
                }
            }
        }
    }

    /// Execute one CommitSpec as one durable transaction: current-state CAS +
    /// receipt claim + journal append (+ effect/artifact rows) — the exact
    /// shape the R1 hypothesis requires (EXP-001 §1).
    pub fn apply(&self, spec: &CommitSpec) -> Result<Applied, ApplyError> {
        let unit_node = self
            .books
            .lock()
            .expect("books lock")
            .units
            .get(&spec.unit_id)
            .copied();

        let mut txn = self.shared.begin_write();

        // CAS + epoch check against the write-side working graph (the
        // published read snapshot can lag sealed commits; WriteTxn::read
        // cannot).
        let existing = match unit_node {
            Some(n) => {
                let props = txn.read().node_properties(n);
                let (version, epoch, holder) = match props {
                    Some(p) => (
                        p.get(&db("version")).and_then(value_u64).unwrap_or(0),
                        p.get(&db("attempt_epoch")).and_then(value_u64).unwrap_or(0) as u32,
                        p.get(&db("holder_id")).and_then(value_u64).unwrap_or(0),
                    ),
                    None => {
                        txn.rollback();
                        return Err(ApplyError::Rejected(Rejection::UnknownUnit {
                            unit_id: spec.unit_id,
                        }));
                    }
                };
                if version != spec.expected_version {
                    txn.rollback();
                    return Err(ApplyError::Rejected(Rejection::StaleVersion {
                        unit_id: spec.unit_id,
                        expected: spec.expected_version,
                        actual: version,
                    }));
                }
                let current_ok = match spec.kind {
                    CommandKind::Dispatch => spec.attempt_epoch == epoch + 1,
                    _ => spec.attempt_epoch == epoch,
                };
                if !current_ok {
                    txn.rollback();
                    return Err(ApplyError::Rejected(Rejection::StaleLease {
                        unit_id: spec.unit_id,
                        claimed_epoch: spec.attempt_epoch,
                        current_epoch: epoch,
                    }));
                }
                // Bar 6's holder dimension: any non-dispatch command must act
                // as the holder of record for the current epoch.
                if spec.kind != CommandKind::Dispatch && spec.holder_id != holder {
                    txn.rollback();
                    return Err(ApplyError::Rejected(Rejection::StaleHolder {
                        unit_id: spec.unit_id,
                        claimed_holder: spec.holder_id,
                        current_holder: holder,
                    }));
                }
                Some(n)
            }
            None => {
                if spec.kind != CommandKind::Dispatch || spec.expected_version != 0 {
                    txn.rollback();
                    return Err(ApplyError::Rejected(Rejection::UnknownUnit {
                        unit_id: spec.unit_id,
                    }));
                }
                None
            }
        };

        let mut created_unit = None;
        let mut created_attempt = None;
        let mut created_lease = None;
        let event_node;
        {
            let mut m = txn.mutator();

            match existing {
                Some(n) => {
                    let mut set = vec![
                        (db("version"), Value::Uint(spec.expected_version + 1)),
                        (
                            db("phase"),
                            Value::String(db(&format!("{:?}", spec.new_phase))),
                        ),
                    ];
                    if spec.kind == CommandKind::Dispatch {
                        set.push((
                            db("attempt_epoch"),
                            Value::Uint(u64::from(spec.attempt_epoch)),
                        ));
                        set.push((db("holder_id"), Value::Uint(spec.holder_id)));
                    }
                    if let Some(a) = &spec.artifact_ref {
                        set.push((db("artifact_ref"), Value::String(db(a))));
                    }
                    m.update_node(n, no_labels(), props_set(set))?;
                }
                None => {
                    let n = m.create_node(
                        LabelSet::single(db("Unit")),
                        PropertyMap::from_pairs([
                            (db("unit_id"), Value::Uint(spec.unit_id)),
                            (
                                db("phase"),
                                Value::String(db(&format!("{:?}", spec.new_phase))),
                            ),
                            (db("version"), Value::Uint(1)),
                            (
                                db("attempt_epoch"),
                                Value::Uint(u64::from(spec.attempt_epoch)),
                            ),
                            (db("holder_id"), Value::Uint(spec.holder_id)),
                        ])
                        .expect("unit property map"),
                    )?;
                    created_unit = Some(n);
                }
            }

            if spec.kind == CommandKind::Dispatch {
                let key = format!("{}/{}", spec.unit_id, spec.attempt_epoch);
                let a = m.create_node(
                    LabelSet::single(db("Attempt")),
                    PropertyMap::from_pairs([
                        (db("attempt_key"), Value::String(db(&key))),
                        (db("unit_id"), Value::Uint(spec.unit_id)),
                        (
                            db("attempt_epoch"),
                            Value::Uint(u64::from(spec.attempt_epoch)),
                        ),
                        (db("holder_id"), Value::Uint(spec.holder_id)),
                        (db("state"), Value::String(db("active"))),
                    ])
                    .expect("attempt property map"),
                )?;
                created_attempt = Some(a);
                let l = m.create_node(
                    LabelSet::single(db("Lease")),
                    PropertyMap::from_pairs([
                        (db("lease_key"), Value::String(db(&key))),
                        (db("holder_id"), Value::Uint(spec.holder_id)),
                        (db("expiry"), Value::Uint(u64::from(spec.step) + 100)),
                        (db("last_renewal"), Value::Uint(u64::from(spec.step))),
                    ])
                    .expect("lease property map"),
                )?;
                created_lease = Some(l);
            }

            if spec.kind == CommandKind::LeaseRenewal {
                let lease = self
                    .books
                    .lock()
                    .expect("books lock")
                    .leases
                    .get(&(spec.unit_id, spec.attempt_epoch))
                    .copied();
                if let Some(l) = lease {
                    m.update_node(
                        l,
                        no_labels(),
                        PropertyDiff {
                            set: [
                                (db("expiry"), Value::Uint(u64::from(spec.step) + 100)),
                                (db("last_renewal"), Value::Uint(u64::from(spec.step))),
                            ]
                            .into_iter()
                            .collect(),
                            removed: Default::default(),
                        },
                    )?;
                }
            }

            if matches!(
                spec.kind,
                CommandKind::Cancellation | CommandKind::OwnerDecision
            ) {
                let attempt = self
                    .books
                    .lock()
                    .expect("books lock")
                    .attempts
                    .get(&(spec.unit_id, spec.attempt_epoch))
                    .copied();
                match attempt {
                    Some(a) => {
                        let terminal = match spec.kind {
                            CommandKind::Cancellation => "cancelled",
                            _ => "decided",
                        };
                        m.update_node(
                            a,
                            no_labels(),
                            props_set([(db("state"), Value::String(db(terminal)))]),
                        )?;
                    }
                    None => {
                        // End the mutator borrow before rolling back; the
                        // guard has no Drop, so a plain rebind releases it.
                        let _released = m;
                        txn.rollback();
                        return Err(ApplyError::Harness(format!(
                            "attempt {}/{} missing from address book",
                            spec.unit_id, spec.attempt_epoch
                        )));
                    }
                }
            }

            let ev = &spec.events[0];
            event_node = m.create_node(
                LabelSet::single(db("Event")),
                PropertyMap::from_pairs([
                    (db("event_id"), Value::Uint(ev.event_id)),
                    (
                        db("agg_ver_ord"),
                        Value::String(db(&format!(
                            "{}/{}/{}",
                            ev.aggregate_id, ev.aggregate_version, ev.ordinal
                        ))),
                    ),
                    (db("aggregate_id"), Value::Uint(ev.aggregate_id)),
                    (db("aggregate_version"), Value::Uint(ev.aggregate_version)),
                    (db("ordinal"), Value::Uint(u64::from(ev.ordinal))),
                    (db("kind"), Value::String(db(&ev.kind))),
                    (db("payload"), Value::String(db(&ev.payload))),
                    (
                        db("causation_ref"),
                        Value::Uint(ev.causation_ref.unwrap_or(0)),
                    ),
                ])
                .expect("event property map"),
            )?;

            m.create_node(
                LabelSet::single(db("Receipt")),
                PropertyMap::from_pairs([
                    (
                        db("receipt_key"),
                        Value::String(db(&format!("{}/{}", spec.unit_id, spec.command_id))),
                    ),
                    (
                        db("request_digest"),
                        Value::String(db(&spec.request_digest)),
                    ),
                    (db("transition_ref"), Value::Uint(spec.events[0].event_id)),
                ])
                .expect("receipt property map"),
            )?;

            if let Some(e) = &spec.effect {
                m.create_node(
                    LabelSet::single(db("Effect")),
                    PropertyMap::from_pairs([
                        (db("intent_id"), Value::Uint(e.intent_id)),
                        (db("operation_key"), Value::String(db(&e.operation_key))),
                        (db("state"), Value::String(db(&format!("{:?}", e.state)))),
                        (db("unit_id"), Value::Uint(spec.unit_id)),
                        (
                            db("attempt_epoch"),
                            Value::Uint(u64::from(spec.attempt_epoch)),
                        ),
                    ])
                    .expect("effect property map"),
                )?;
            }

            if let Some(a) = &spec.artifact_ref {
                m.create_node(
                    LabelSet::single(db("Artifact")),
                    PropertyMap::from_pairs([(db("artifact_digest"), Value::String(db(a)))])
                        .expect("artifact property map"),
                )?;
            }
        }

        let outcome = txn.commit()?;

        let mut books = self.books.lock().expect("books lock");
        if let Some(n) = created_unit {
            books.units.insert(spec.unit_id, n);
        }
        if let Some(a) = created_attempt {
            books.attempts.insert((spec.unit_id, spec.attempt_epoch), a);
        }
        if let Some(l) = created_lease {
            books.leases.insert((spec.unit_id, spec.attempt_epoch), l);
        }
        books.events.insert(spec.events[0].event_id, event_node);

        Ok(Applied {
            generation: outcome.generation,
            durable_at: outcome.durable_at,
            event_node,
        })
    }

    /// §6 journal-mutation cell, update arm: attempt an in-place payload
    /// update of a committed event. Must return
    /// `TypeViolation::ImmutablePropertyUpdate` (store-enforced; rejects at
    /// the mutator call, before commit).
    pub fn try_update_event(&self, event_id: u64) -> Result<(), GraphError> {
        let node = self
            .books
            .lock()
            .expect("books lock")
            .events
            .get(&event_id)
            .copied()
            .expect("event exists in book");
        let mut txn = self.shared.begin_write();
        let result = txn.mutator().update_node(
            node,
            no_labels(),
            props_set([(db("payload"), Value::String(db("tampered")))]),
        );
        txn.rollback();
        result
    }

    /// §6 journal-mutation cell, delete arm: a delete REQUEST routed through
    /// the funnel's dispatch (R26a). Journal-class targets reject typed;
    /// Selene offers no store-level rejection to delegate to, so this
    /// dispatch — plus `shared` being private — is the enforcement.
    pub fn apply_delete(&self, target: DeleteTarget) -> Result<(), ApplyError> {
        match target {
            DeleteTarget::Event { event_id } => {
                Err(ApplyError::Rejected(Rejection::JournalDelete { event_id }))
            }
            DeleteTarget::Receipt { receipt_key } => Err(ApplyError::Harness(format!(
                "receipt {receipt_key} is journal-class evidence; deletes are not implemented in the v0 funnel"
            ))),
            DeleteTarget::Artifact { digest } => Err(ApplyError::Harness(format!(
                "artifact {digest} is content-addressed evidence; deletes are not implemented in the v0 funnel"
            ))),
        }
    }

    /// §6 journal-mutation cell, duplicate arm: a fresh write carrying a
    /// duplicate `event_id` (or duplicate derived composite key). Must fail
    /// at commit with `TypeViolation::UniquePropertyDuplicate`.
    pub fn try_duplicate_event(&self, ev: &crate::schema::SemanticEvent) -> Result<(), GraphError> {
        let mut txn = self.shared.begin_write();
        {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Event")),
                PropertyMap::from_pairs([
                    (db("event_id"), Value::Uint(ev.event_id)),
                    (
                        db("agg_ver_ord"),
                        Value::String(db(&format!(
                            "{}/{}/{}",
                            ev.aggregate_id, ev.aggregate_version, ev.ordinal
                        ))),
                    ),
                    (db("aggregate_id"), Value::Uint(ev.aggregate_id)),
                    (db("aggregate_version"), Value::Uint(ev.aggregate_version)),
                    (db("ordinal"), Value::Uint(u64::from(ev.ordinal))),
                    (db("kind"), Value::String(db(&ev.kind))),
                    (db("payload"), Value::String(db(&ev.payload))),
                    (db("causation_ref"), Value::Uint(0)),
                ])
                .expect("duplicate event property map"),
            )?;
        }
        txn.commit().map(|_| ())
    }

    /// Current-state read for audits: (version, attempt_epoch, phase).
    pub fn unit_state(&self, unit_id: u64) -> Option<(u64, u32, String)> {
        let node = self
            .books
            .lock()
            .expect("books lock")
            .units
            .get(&unit_id)
            .copied()?;
        let g = self.shared.read();
        let props = g.node_properties(node)?;
        let version = props.get(&db("version")).and_then(value_u64)?;
        let epoch = props.get(&db("attempt_epoch")).and_then(value_u64)? as u32;
        let phase = match props.get(&db("phase")) {
            Some(Value::String(s)) => s.as_str().to_string(),
            _ => return None,
        };
        Some((version, epoch, phase))
    }

    /// Every `event_id` visible in the current published read snapshot, for
    /// the watch cell's snapshot half and the auditor's agreement checks.
    pub fn snapshot_event_ids(&self) -> Vec<u64> {
        let g = self.shared.read();
        let mut out = Vec::new();
        for raw in g.live_nodes().iter() {
            let Some(id) = g.node_id_for_row(RowIndex::new(raw)) else {
                continue;
            };
            let Some(labels) = g.node_labels(id) else {
                continue;
            };
            if !labels.contains(&db("Event")) {
                continue;
            }
            if let Some(props) = g.node_properties(id)
                && let Some(eid) = props.get(&db("event_id")).and_then(value_u64)
            {
                out.push(eid);
            }
        }
        out.sort_unstable();
        out
    }

    /// One coherent scan of the published snapshot into the auditor's row
    /// maps. Reads only — the auditor never writes (§6 corruption drill: it
    /// names and quarantines, it does not repair).
    pub fn audit_snapshot(&self) -> AuditSnapshot {
        let g = self.shared.read();
        let mut snap = AuditSnapshot::default();
        for raw in g.live_nodes().iter() {
            let Some(id) = g.node_id_for_row(RowIndex::new(raw)) else {
                continue;
            };
            let Some(labels) = g.node_labels(id) else {
                continue;
            };
            let Some(props) = g.node_properties(id) else {
                continue;
            };
            let get_u64 = |name: &str| props.get(&db(name)).and_then(value_u64);
            let get_str = |name: &str| match props.get(&db(name)) {
                Some(Value::String(s)) => Some(s.as_str().to_string()),
                _ => None,
            };
            if labels.contains(&db("Unit")) {
                if let (Some(unit_id), Some(version), Some(epoch), Some(phase)) = (
                    get_u64("unit_id"),
                    get_u64("version"),
                    get_u64("attempt_epoch"),
                    get_str("phase"),
                ) {
                    snap.units.insert(
                        unit_id,
                        UnitRow {
                            version,
                            attempt_epoch: epoch as u32,
                            phase,
                            artifact_ref: get_str("artifact_ref"),
                        },
                    );
                }
            } else if labels.contains(&db("Event")) {
                if let (
                    Some(event_id),
                    Some(aggregate_id),
                    Some(aggregate_version),
                    Some(payload),
                ) = (
                    get_u64("event_id"),
                    get_u64("aggregate_id"),
                    get_u64("aggregate_version"),
                    get_str("payload"),
                ) {
                    snap.events.insert(
                        event_id,
                        EventRow {
                            aggregate_id,
                            aggregate_version,
                            payload,
                        },
                    );
                }
            } else if labels.contains(&db("Receipt")) {
                if let (Some(key), Some(digest), Some(transition_ref)) = (
                    get_str("receipt_key"),
                    get_str("request_digest"),
                    get_u64("transition_ref"),
                ) {
                    snap.receipts.insert(
                        key,
                        ReceiptRow {
                            request_digest: digest,
                            transition_ref,
                        },
                    );
                }
            } else if labels.contains(&db("Effect")) {
                if let (Some(intent_id), Some(op), Some(state), Some(unit_id)) = (
                    get_u64("intent_id"),
                    get_str("operation_key"),
                    get_str("state"),
                    get_u64("unit_id"),
                ) {
                    snap.effects.insert(
                        intent_id,
                        EffectStoreRow {
                            operation_key: op,
                            state,
                            unit_id,
                        },
                    );
                }
            } else if labels.contains(&db("Artifact"))
                && let Some(d) = get_str("artifact_digest")
            {
                snap.artifacts.insert(d);
            }
        }
        snap
    }

    /// Idempotent-replay probe for the post-publish-pre-response cell: a
    /// fresh write claiming an existing receipt key must reject as a unique
    /// duplicate — the substrate-level proof that retrying a command whose
    /// response was lost cannot re-execute it.
    pub fn probe_duplicate_receipt(&self, receipt_key: &str) -> Result<(), GraphError> {
        let mut txn = self.shared.begin_write();
        {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Receipt")),
                PropertyMap::from_pairs([
                    (db("receipt_key"), Value::String(db(receipt_key))),
                    (db("request_digest"), Value::String(db("replay-probe"))),
                    (db("transition_ref"), Value::Uint(0)),
                ])
                .expect("probe property map"),
            )?;
        }
        txn.commit().map(|_| ())
    }

    /// Read one event's payload bytes back, for byte-identity assertions
    /// around mutation attempts.
    pub fn event_payload(&self, event_id: u64) -> Option<String> {
        let node = self
            .books
            .lock()
            .expect("books lock")
            .events
            .get(&event_id)
            .copied()?;
        let g = self.shared.read();
        let props = g.node_properties(node)?;
        match props.get(&db("payload")) {
            Some(Value::String(s)) => Some(s.as_str().to_string()),
            _ => None,
        }
    }
}
