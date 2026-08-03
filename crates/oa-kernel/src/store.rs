//! Kernel control-graph store: the single mutation funnel's substrate
//! (ADR-001 §2, §3, §7.3, §9 steps 1–2).
//!
//! Funnel premise, carried over from the G02-proven EXP-001 layer: `shared`
//! is private and every write flows through this module. The typed
//! journal-delete rejection becomes a full enforcement only when the funnel
//! layer (task 5b) routes every delete request through
//! `request_journal_delete` — until then it is the sanctioned dispatch point
//! with in-crate discipline, because Selene has no append-only flag to
//! delegate to. Enforcement split:
//!
//! - store-enforced (closed graph, Strict validation): uniqueness of
//!   `event_id`, `cursor`, the derived `agg_ver_ord` composite, receipt and
//!   evidence keys, `operation_key`; update-immutability of journal
//!   properties;
//! - funnel-enforced: journal deletes, the version CAS, the §3.3 fence
//!   checks, and write-once `terminal`.
//!
//! What this module deliberately does NOT do: interpret commands. The §2.2
//! eight-step funnel transaction is built ON these seams by the command
//! layer (`funnel`, next); this layer owns open/claim, the cursor allocator,
//! and raw record transactions.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use selene_core::{
    DbString, GraphId, LabelDiff, LabelSet, NodeId, PropertyDiff, PropertyMap, PropertyValueType,
    Value, db_string,
};
use selene_graph::{
    CommitBatching, GraphError, GraphTypeDef, NodeTypeDef, PropertyTypeDef, RowIndex, SharedGraph,
    ValidationMode,
};
use selene_persist::DEFAULT_WAL_FILE_NAME;

use crate::ids::{AttemptEpoch, AuthorityEpoch, Stamp};

pub use selene_graph::TypeViolation;
pub use selene_persist::PersistError;

pub const GRAPH_ID: u64 = 7;

fn db(s: &str) -> DbString {
    db_string(s).expect("kernel identifiers are valid db strings")
}

fn value_u64(v: &Value) -> Option<u64> {
    match v {
        Value::Uint(u) => Some(*u),
        Value::Int(i) => u64::try_from(*i).ok(),
        _ => None,
    }
}

fn value_str(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.as_str().to_string()),
        _ => None,
    }
}

fn no_labels() -> LabelDiff {
    LabelDiff::new([], []).expect("empty label diff is valid")
}

fn props_set(set: impl IntoIterator<Item = (DbString, Value)>) -> PropertyDiff {
    PropertyDiff::new(set, []).expect("property diff keys are distinct")
}

fn prop_def(
    name: &str,
    value_type: PropertyValueType,
    required: bool,
    immutable: bool,
    unique: bool,
) -> PropertyTypeDef {
    PropertyTypeDef {
        name: db(name),
        value_type,
        list_element_type: None,
        required,
        default: None,
        immutable,
        unique,
        decimal_type: None,
        character_string_type: None,
        byte_string_type: None,
        record_field_types: None,
    }
}

fn node_type(name: &str, label: &str, properties: Vec<PropertyTypeDef>) -> NodeTypeDef {
    NodeTypeDef {
        name: db(name),
        key_labels: LabelSet::single(db(label)),
        properties,
        validation_mode: ValidationMode::Strict,
    }
}

/// The closed control-graph schema (ADR-001 §1 records, store-checkable
/// subset). Composite uniques ride derived string keys because Selene's
/// `unique` is single-property (G02 finding, R26a). Full records travel as
/// canonical-JSON `record` payloads beside the typed fence/CAS fields; the
/// typed fields are the ones transactions compare, so divergence between
/// them and the payload is an auditor-checkable defect, not a truth split.
pub fn graph_type() -> GraphTypeDef {
    use PropertyValueType::{String as PStr, Uint};
    GraphTypeDef {
        name: db("oa.control"),
        node_types: vec![
            // §7.3: singleton per control graph; the fixed unique key makes
            // a second row unrepresentable.
            node_type(
                "oa.authority",
                "Authority",
                vec![
                    prop_def("authority_key", PStr, true, true, true),
                    prop_def("authority_epoch", Uint, true, false, false),
                    prop_def("holder_instance_id", PStr, true, false, false),
                    prop_def("prior_instance_id", PStr, false, false, false),
                    prop_def("status", PStr, true, false, false),
                ],
            ),
            // Holder identity deliberately absent: §1.2's ExecutionUnit has
            // none — the fence reads the holder from the lease (§3.3), and a
            // second copy here would invite checking the wrong record.
            node_type(
                "oa.unit",
                "Unit",
                vec![
                    prop_def("unit_id", PStr, true, true, true),
                    prop_def("version", Uint, true, false, false),
                    prop_def("current_attempt_epoch", Uint, true, false, false),
                    prop_def("stamp", Uint, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("work_item_id", PStr, true, true, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            node_type(
                "oa.attempt",
                "Attempt",
                vec![
                    // UNIQUE(unit_id, attempt_epoch) as a derived key.
                    prop_def("attempt_key", PStr, true, true, true),
                    prop_def("unit_id", PStr, true, true, false),
                    prop_def("attempt_epoch", Uint, true, true, false),
                    prop_def("holder_id", PStr, true, true, false),
                    prop_def("status", PStr, true, false, false),
                ],
            ),
            // One lease row per unit (unique key = unit_id): "at most one
            // active lease per unit" holds by construction; lease history
            // lives in the journal, not in superseded rows.
            node_type(
                "oa.lease",
                "Lease",
                vec![
                    prop_def("unit_id", PStr, true, true, true),
                    prop_def("attempt_epoch", Uint, true, false, false),
                    prop_def("holder_id", PStr, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("version", Uint, true, false, false),
                ],
            ),
            node_type(
                "oa.work_item",
                "WorkItem",
                vec![
                    prop_def("work_item_id", PStr, true, true, true),
                    prop_def("version", Uint, true, false, false),
                    prop_def("status", PStr, true, false, false),
                    // Pinned (§1.1): a contract change mints a NEW work item
                    // (§6.1); re-pinning a committed row is unlawful even
                    // for drafts, since drafts can re-mint too.
                    prop_def("acceptance_contract_digest", PStr, true, true, false),
                    prop_def("declared_write_scope", PStr, true, false, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            // §2.3: deterministic rejections persist too; status/result stay
            // mutable only for the §2.2 outcome_unknown resolution path.
            node_type(
                "oa.receipt",
                "Receipt",
                vec![
                    prop_def("receipt_key", PStr, true, true, true),
                    prop_def("request_digest", PStr, true, true, false),
                    prop_def("status", PStr, true, false, false),
                    prop_def("result", PStr, true, false, false),
                    prop_def("first_cursor", Uint, false, false, false),
                    prop_def("last_cursor", Uint, false, false, false),
                ],
            ),
            // §2.4: every payload-bearing property immutable; deletes are
            // funnel-rejected `journal_immutable`.
            node_type(
                "oa.event",
                "Event",
                vec![
                    prop_def("event_id", PStr, true, true, true),
                    prop_def("cursor", Uint, true, true, true),
                    prop_def("agg_ver_ord", PStr, true, true, true),
                    prop_def("aggregate_kind", PStr, true, true, false),
                    prop_def("aggregate_id", PStr, true, true, false),
                    prop_def("aggregate_version", Uint, true, true, false),
                    prop_def("ordinal", Uint, true, true, false),
                    prop_def("event_kind", PStr, true, true, false),
                    prop_def("payload", PStr, true, true, false),
                    prop_def("command_id", PStr, true, true, false),
                ],
            ),
            node_type(
                "oa.effect",
                "Effect",
                vec![
                    prop_def("operation_key", PStr, true, true, true),
                    prop_def("effect_intent_id", PStr, true, true, true),
                    prop_def("unit_id", PStr, true, true, false),
                    // The effect's own §1 aggregate-version axis: settle
                    // events stamp this, never the unit's version (§3.3
                    // settle does not advance unit state).
                    prop_def("version", Uint, true, false, false),
                    prop_def("state", PStr, true, false, false),
                    // Absent = unset; write-once is funnel-enforced (settle).
                    prop_def("terminal", PStr, false, false, false),
                    prop_def("record", PStr, true, false, false),
                ],
            ),
            // §8.1: immutable once sealed — every property immutable.
            node_type(
                "oa.evidence",
                "Evidence",
                vec![
                    prop_def("evidence_key", PStr, true, true, true),
                    prop_def("work_item_id", PStr, true, true, false),
                    prop_def("artifact_set_digest", PStr, true, true, false),
                    prop_def("record", PStr, true, true, false),
                ],
            ),
        ],
        edge_types: Vec::new(),
    }
}

/// §3.3 typed fence rejection. `FenceRejected` is the wire name (ADR-002
/// §10); the variants carry which axis failed for diagnostics only — clients
/// branch on the kind, never the axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRejection {
    FenceRejected {
        detail: String,
    },
    VersionConflict {
        aggregate: String,
        expected: u64,
        actual: u64,
    },
    JournalImmutable {
        detail: String,
    },
    NotFound {
        aggregate: String,
    },
    TerminalAlreadySet {
        operation_key: String,
    },
}

#[derive(Debug)]
pub enum StoreError {
    Rejected(StoreRejection),
    Graph(GraphError),
    /// `create` on a directory that already holds a control graph. Selene
    /// opens an existing WAL in append mode, so an unguarded second genesis
    /// would regress the authority epoch to 1, reuse committed cursors, and
    /// leave the WAL permanently unreplayable — recover is the only lawful
    /// reopen (§9 steps 1–2).
    AlreadyInitialized(PathBuf),
    /// Kernel-internal incoherence — fail loudly, never a domain rejection.
    Internal(String),
}

impl From<GraphError> for StoreError {
    fn from(e: GraphError) -> Self {
        StoreError::Graph(e)
    }
}

/// The §2.1 attempt token's sealed claims. In-process MILE-001 carries the
/// struct directly; MAC sealing over these fields is the daemon boundary's
/// job (INSTALL-001), not this milestone's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptTokenClaims {
    pub unit_id: String,
    pub attempt_epoch: AttemptEpoch,
    pub stamp: Stamp,
    pub authority_epoch: AuthorityEpoch,
    pub holder_id: String,
}

/// Address book: id → node cache. Never truth — every transaction re-reads
/// versions from the write-side working graph.
#[derive(Default)]
struct Books {
    authority: Option<NodeId>,
    units: HashMap<String, NodeId>,
    /// Keyed by `attempt_key` (`unit_id/epoch`) — §9 step 4 and §6.1 rework
    /// both need the current attempt addressable after recovery.
    attempts: HashMap<String, NodeId>,
    leases: HashMap<String, NodeId>,
    work_items: HashMap<String, NodeId>,
    receipts: HashMap<String, NodeId>,
    effects: HashMap<String, NodeId>,
    events: HashMap<String, NodeId>,
    evidence: HashMap<String, NodeId>,
}

pub struct Store {
    shared: SharedGraph,
    #[allow(dead_code)] // recovery-scan work (task 5e) reads the WAL from here
    dir: PathBuf,
    books: Mutex<Books>,
    /// §2.4 cursor allocator: monotonic, MAY be gapped, never reused. On
    /// open it is restored strictly above the maximum committed cursor.
    next_cursor: Mutex<u64>,
    authority_epoch: Mutex<AuthorityEpoch>,
}

impl Store {
    pub fn wal_path(dir: &Path) -> PathBuf {
        dir.join(DEFAULT_WAL_FILE_NAME)
    }

    /// Fresh control graph: claims `ControlAuthority` at epoch 1. Refuses a
    /// directory that already holds one — see
    /// [`StoreError::AlreadyInitialized`].
    pub fn create(dir: &Path, instance_id: &str) -> Result<Store, StoreError> {
        if Self::wal_path(dir).exists() {
            return Err(StoreError::AlreadyInitialized(dir.to_path_buf()));
        }
        let shared = SharedGraph::builder(GraphId::new(GRAPH_ID))
            .bound_to(graph_type())?
            .with_wal(Self::wal_path(dir), selene_persist::WalConfig::default())?
            .with_commit_batching(CommitBatching::Off)
            .build()?;
        let store = Store {
            shared,
            dir: dir.to_path_buf(),
            books: Mutex::new(Books::default()),
            next_cursor: Mutex::new(1),
            authority_epoch: Mutex::new(AuthorityEpoch(1)),
        };
        let mut txn = store.shared.begin_write();
        let node = {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Authority")),
                PropertyMap::from_pairs([
                    (db("authority_key"), Value::String(db("control"))),
                    (db("authority_epoch"), Value::Uint(1)),
                    (db("holder_instance_id"), Value::String(db(instance_id))),
                    (db("status"), Value::String(db("active"))),
                ])
                .expect("authority property map"),
            )?
        };
        txn.commit()?;
        store.books.lock().expect("books").authority = Some(node);
        Ok(store)
    }

    /// Reopen after a kill: §9 steps 1–2. A live holder surfaces as
    /// `WriterLockHeld` from Selene — losing the single-instance race is
    /// normal operation. The claim transaction increments `authority_epoch`
    /// by one and records the prior instance; the cursor allocator is
    /// restored strictly above the graph's maximum committed cursor, so an
    /// aborted allocation is never reused (I13).
    pub fn recover(dir: &Path, instance_id: &str) -> Result<Store, StoreError> {
        let shared = SharedGraph::recover_closed(dir, GraphId::new(GRAPH_ID), graph_type())?;
        let store = Store {
            shared,
            dir: dir.to_path_buf(),
            books: Mutex::new(Books::default()),
            next_cursor: Mutex::new(1),
            authority_epoch: Mutex::new(AuthorityEpoch(0)),
        };
        store.rebuild_books_and_cursor();
        store.claim_authority(instance_id)?;
        Ok(store)
    }

    fn rebuild_books_and_cursor(&self) {
        let g = self.shared.read();
        let mut books = self.books.lock().expect("books");
        let mut max_cursor = 0u64;
        for raw in g.live_nodes().iter() {
            // Row indices remap under compaction; node_id_for_row is the
            // sanctioned reverse mapping (G02 auditor trap).
            let Some(id) = g.node_id_for_row(RowIndex::new(raw)) else {
                continue;
            };
            let Some(labels) = g.node_labels(id) else {
                continue;
            };
            let Some(props) = g.node_properties(id) else {
                continue;
            };
            let get_str = |name: &str| props.get(&db(name)).and_then(value_str);
            if labels.contains(&db("Authority")) {
                books.authority = Some(id);
            } else if labels.contains(&db("Unit")) {
                if let Some(k) = get_str("unit_id") {
                    books.units.insert(k, id);
                }
            } else if labels.contains(&db("Attempt")) {
                if let Some(k) = get_str("attempt_key") {
                    books.attempts.insert(k, id);
                }
            } else if labels.contains(&db("Evidence")) {
                if let Some(k) = get_str("evidence_key") {
                    books.evidence.insert(k, id);
                }
            } else if labels.contains(&db("Lease")) {
                if let Some(k) = get_str("unit_id") {
                    books.leases.insert(k, id);
                }
            } else if labels.contains(&db("WorkItem")) {
                if let Some(k) = get_str("work_item_id") {
                    books.work_items.insert(k, id);
                }
            } else if labels.contains(&db("Receipt")) {
                if let Some(k) = get_str("receipt_key") {
                    books.receipts.insert(k, id);
                }
            } else if labels.contains(&db("Effect")) {
                if let Some(k) = get_str("operation_key") {
                    books.effects.insert(k, id);
                }
            } else if labels.contains(&db("Event")) {
                if let Some(c) = props.get(&db("cursor")).and_then(value_u64) {
                    max_cursor = max_cursor.max(c);
                }
                if let Some(k) = get_str("event_id") {
                    books.events.insert(k, id);
                }
            }
        }
        *self.next_cursor.lock().expect("cursor") = max_cursor + 1;
    }

    fn claim_authority(&self, instance_id: &str) -> Result<(), StoreError> {
        let node = self
            .books
            .lock()
            .expect("books")
            .authority
            .ok_or_else(|| StoreError::Internal("control graph has no Authority row".into()))?;
        let mut txn = self.shared.begin_write();
        let (epoch, prior) = {
            let props = txn
                .read()
                .node_properties(node)
                .ok_or_else(|| StoreError::Internal("authority node unreadable".into()))?;
            (
                props
                    .get(&db("authority_epoch"))
                    .and_then(value_u64)
                    .unwrap_or(0),
                props
                    .get(&db("holder_instance_id"))
                    .and_then(value_str)
                    .unwrap_or_default(),
            )
        };
        {
            let mut m = txn.mutator();
            m.update_node(
                node,
                no_labels(),
                props_set([
                    (db("authority_epoch"), Value::Uint(epoch + 1)),
                    (db("holder_instance_id"), Value::String(db(instance_id))),
                    (db("prior_instance_id"), Value::String(db(&prior))),
                    (db("status"), Value::String(db("active"))),
                ]),
            )?;
        }
        txn.commit()?;
        *self.authority_epoch.lock().expect("epoch") = AuthorityEpoch(epoch + 1);
        Ok(())
    }

    pub fn authority_epoch(&self) -> AuthorityEpoch {
        *self.authority_epoch.lock().expect("epoch")
    }

    /// Allocate the next store-global cursor. Monotonic and never reused
    /// within a lifetime — the allocation is handed to a transaction that
    /// may abort, and the value is burned either way (§2.4).
    ///
    /// Ordering contract: call ONLY while holding the write transaction the
    /// value commits under. Selene's single writer serializes those callers,
    /// so allocation order equals commit order; allocating before
    /// `begin_write` would let a later allocation commit first, and a
    /// consumer past the higher cursor must then treat the committed lower
    /// one as an abort gap — a delivered-stream loss (I13).
    pub(crate) fn allocate_cursor(&self) -> u64 {
        let mut c = self.next_cursor.lock().expect("cursor");
        let v = *c;
        *c += 1;
        v
    }

    // The accessors below are the funnel layer's seams (task 5b); the allow
    // markers come off with the first funnel commit that consumes them.
    #[allow(dead_code)]
    pub(crate) fn shared(&self) -> &SharedGraph {
        &self.shared
    }

    #[allow(dead_code)]
    pub(crate) fn unit_node(&self, unit_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .units
            .get(unit_id)
            .copied()
    }

    #[allow(dead_code)]
    pub(crate) fn lease_node(&self, unit_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .leases
            .get(unit_id)
            .copied()
    }

    #[allow(dead_code)]
    pub(crate) fn work_item_node(&self, id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .work_items
            .get(id)
            .copied()
    }

    #[allow(dead_code)]
    pub(crate) fn receipt_node(&self, key: &str) -> Option<NodeId> {
        self.books.lock().expect("books").receipts.get(key).copied()
    }

    #[allow(dead_code)]
    pub(crate) fn attempt_node(&self, attempt_key: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .attempts
            .get(attempt_key)
            .copied()
    }

    #[allow(dead_code)]
    pub(crate) fn evidence_node(&self, evidence_key: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .evidence
            .get(evidence_key)
            .copied()
    }

    #[allow(dead_code)]
    pub(crate) fn effect_node(&self, operation_key: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .effects
            .get(operation_key)
            .copied()
    }

    #[allow(dead_code)] // funnel seam (5b); exercised by the store tests today
    pub(crate) fn book_insert(&self, kind: BookKind, key: String, node: NodeId) {
        let mut books = self.books.lock().expect("books");
        match kind {
            BookKind::Unit => books.units.insert(key, node),
            BookKind::Attempt => books.attempts.insert(key, node),
            BookKind::Lease => books.leases.insert(key, node),
            BookKind::WorkItem => books.work_items.insert(key, node),
            BookKind::Receipt => books.receipts.insert(key, node),
            BookKind::Effect => books.effects.insert(key, node),
            BookKind::Event => books.events.insert(key, node),
            BookKind::Evidence => books.evidence.insert(key, node),
        };
    }

    /// §3.3 holder fence over the write-side working graph: authority epoch,
    /// attempt epoch, stamp, holder identity, and lease liveness — all five
    /// checks, every holder-authorized boundary.
    #[allow(dead_code)] // funnel seam (5b); exercised by the store tests today
    pub(crate) fn check_holder_fence(
        &self,
        read: &impl UnitFenceRead,
        claims: &AttemptTokenClaims,
    ) -> Result<(), StoreRejection> {
        if claims.authority_epoch != self.authority_epoch() {
            return Err(StoreRejection::FenceRejected {
                detail: format!(
                    "authority epoch {} behind current {}",
                    claims.authority_epoch.0,
                    self.authority_epoch().0
                ),
            });
        }
        let unit = read
            .unit_fence(&claims.unit_id)
            .ok_or(StoreRejection::NotFound {
                aggregate: format!("unit {}", claims.unit_id),
            })?;
        if claims.attempt_epoch != unit.attempt_epoch {
            return Err(StoreRejection::FenceRejected {
                detail: format!(
                    "attempt epoch {} vs current {}",
                    claims.attempt_epoch.0, unit.attempt_epoch.0
                ),
            });
        }
        if claims.stamp != unit.stamp {
            return Err(StoreRejection::FenceRejected {
                detail: format!("stamp {} vs current {}", claims.stamp.0, unit.stamp.0),
            });
        }
        let lease = read
            .lease_fence(&claims.unit_id)
            .ok_or(StoreRejection::NotFound {
                aggregate: format!("lease for unit {}", claims.unit_id),
            })?;
        if lease.status != "active" {
            return Err(StoreRejection::FenceRejected {
                detail: format!("lease status {}", lease.status),
            });
        }
        if lease.holder_id != claims.holder_id {
            return Err(StoreRejection::FenceRejected {
                detail: format!(
                    "holder {} vs lease holder {}",
                    claims.holder_id, lease.holder_id
                ),
            });
        }
        Ok(())
    }

    /// Journal mutation requests dispatch here so the rejection is typed
    /// (§2.4; the store cannot reject a delete of a committed row itself).
    pub fn request_journal_delete(&self, event_id: &str) -> StoreRejection {
        StoreRejection::JournalImmutable {
            detail: format!("delete of committed event {event_id}"),
        }
    }
}

#[allow(dead_code)] // funnel seam (5b)
#[derive(Clone, Copy, Debug)]
pub(crate) enum BookKind {
    Unit,
    Attempt,
    Lease,
    WorkItem,
    Receipt,
    Effect,
    Event,
    Evidence,
}

/// Fence-relevant fields of a unit row.
#[allow(dead_code)] // funnel seam (5b)
pub(crate) struct UnitFence {
    pub attempt_epoch: AttemptEpoch,
    pub stamp: Stamp,
}

#[allow(dead_code)] // funnel seam (5b)
pub(crate) struct LeaseFence {
    pub holder_id: String,
    pub status: String,
}

/// Read seam the fence check runs against — always the write transaction's
/// working graph, never the published snapshot (it can lag sealed commits).
#[allow(dead_code)] // funnel seam (5b)
pub(crate) trait UnitFenceRead {
    fn unit_fence(&self, unit_id: &str) -> Option<UnitFence>;
    fn lease_fence(&self, unit_id: &str) -> Option<LeaseFence>;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn event_pairs(event_id: &str, cursor: u64, agg: &str, ver: u64) -> PropertyMap {
        PropertyMap::from_pairs([
            (db("event_id"), Value::String(db(event_id))),
            (db("cursor"), Value::Uint(cursor)),
            (
                db("agg_ver_ord"),
                Value::String(db(&format!("{agg}/{ver}/0"))),
            ),
            (db("aggregate_kind"), Value::String(db("unit"))),
            (db("aggregate_id"), Value::String(db(agg))),
            (db("aggregate_version"), Value::Uint(ver)),
            (db("ordinal"), Value::Uint(0)),
            (db("event_kind"), Value::String(db("test.evt"))),
            (db("payload"), Value::String(db("{}"))),
            (db("command_id"), Value::String(db("cmd-1"))),
        ])
        .expect("event property map")
    }

    pub(crate) fn append_event(store: &Store, event_id: &str, agg: &str, ver: u64) -> u64 {
        // Allocation under the open write txn — the allocate_cursor
        // ordering contract.
        let mut txn = store.shared.begin_write();
        let cursor = store.allocate_cursor();
        let node = {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Event")),
                event_pairs(event_id, cursor, agg, ver),
            )
            .expect("event create")
        };
        txn.commit().expect("event commit");
        store.book_insert(BookKind::Event, event_id.to_owned(), node);
        cursor
    }

    #[test]
    fn authority_epoch_increments_on_every_takeover() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        assert_eq!(store.authority_epoch(), AuthorityEpoch(1));
        drop(store);
        let store = Store::recover(dir.path(), "inst-2").unwrap();
        assert_eq!(store.authority_epoch(), AuthorityEpoch(2));
        drop(store);
        let store = Store::recover(dir.path(), "inst-3").unwrap();
        assert_eq!(store.authority_epoch(), AuthorityEpoch(3));
    }

    #[test]
    fn cursor_allocator_restores_strictly_above_committed_max() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        append_event(&store, "e1", "u1", 1);
        append_event(&store, "e2", "u1", 2);
        // Burn an allocation with no commit behind it: the value must stay
        // burned across recovery only if committed — an aborted allocation
        // MAY be reused after restart because nothing durable observed it;
        // what is forbidden is allocating at or below the committed max.
        let burned = store.allocate_cursor();
        assert_eq!(burned, 3);
        drop(store);
        let store = Store::recover(dir.path(), "inst-2").unwrap();
        // The uncommitted allocation was not durable, so recovery restores
        // to committed-max + 1 = 3 — and this probe burns 3 in doing so.
        assert_eq!(store.allocate_cursor(), 3);
        // The next committed event therefore lands at cursor 4, and the
        // floor after another recovery sits strictly above it.
        let committed_at = append_event(&store, "e3", "u1", 3);
        assert_eq!(committed_at, 4);
        drop(store);
        let store = Store::recover(dir.path(), "inst-3").unwrap();
        assert_eq!(store.allocate_cursor(), 5);
    }

    #[test]
    fn duplicate_event_identity_rejects_at_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        append_event(&store, "e1", "u1", 1);
        // Same event_id, fresh cursor and composite: unique on event_id.
        let cursor = store.allocate_cursor();
        let mut txn = store.shared.begin_write();
        {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Event")),
                event_pairs("e1", cursor, "u1", 9),
            )
            .expect("mutator accepts; uniqueness is checked at commit");
        }
        assert!(txn.commit().is_err(), "duplicate event_id must fail commit");
        // Same (aggregate, version, ordinal) composite under a new id.
        let cursor = store.allocate_cursor();
        let mut txn = store.shared.begin_write();
        {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Event")),
                event_pairs("e9", cursor, "u1", 1),
            )
            .expect("mutator accepts; uniqueness is checked at commit");
        }
        assert!(
            txn.commit().is_err(),
            "duplicate agg_ver_ord must fail commit"
        );
    }

    #[test]
    fn committed_event_payload_is_immutable() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        append_event(&store, "e1", "u1", 1);
        let node = store
            .books
            .lock()
            .unwrap()
            .events
            .get("e1")
            .copied()
            .unwrap();
        let mut txn = store.shared.begin_write();
        let result = txn.mutator().update_node(
            node,
            no_labels(),
            props_set([(db("payload"), Value::String(db("tampered")))]),
        );
        txn.rollback();
        assert!(result.is_err(), "immutable payload update must be rejected");
    }

    #[test]
    fn journal_delete_requests_reject_typed() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        assert_eq!(
            store.request_journal_delete("e1"),
            StoreRejection::JournalImmutable {
                detail: "delete of committed event e1".to_owned()
            }
        );
    }

    pub(crate) struct FakeFenceRead {
        pub(crate) unit: Option<UnitFence>,
        pub(crate) lease: Option<LeaseFence>,
    }

    impl UnitFenceRead for FakeFenceRead {
        fn unit_fence(&self, _unit_id: &str) -> Option<UnitFence> {
            self.unit.as_ref().map(|u| UnitFence {
                attempt_epoch: u.attempt_epoch,
                stamp: u.stamp,
            })
        }
        fn lease_fence(&self, _unit_id: &str) -> Option<LeaseFence> {
            self.lease.as_ref().map(|l| LeaseFence {
                holder_id: l.holder_id.clone(),
                status: l.status.clone(),
            })
        }
    }

    fn claims(store: &Store) -> AttemptTokenClaims {
        AttemptTokenClaims {
            unit_id: "u1".into(),
            attempt_epoch: AttemptEpoch(3),
            stamp: Stamp(1),
            authority_epoch: store.authority_epoch(),
            holder_id: "h1".into(),
        }
    }

    fn healthy_read() -> FakeFenceRead {
        FakeFenceRead {
            unit: Some(UnitFence {
                attempt_epoch: AttemptEpoch(3),
                stamp: Stamp(1),
            }),
            lease: Some(LeaseFence {
                holder_id: "h1".into(),
                status: "active".into(),
            }),
        }
    }

    #[test]
    fn holder_fence_checks_all_five_axes() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        let ok = claims(&store);
        assert_eq!(store.check_holder_fence(&healthy_read(), &ok), Ok(()));

        // Axis 1: authority epoch behind current.
        let mut stale = ok.clone();
        stale.authority_epoch = AuthorityEpoch(0);
        assert!(matches!(
            store.check_holder_fence(&healthy_read(), &stale),
            Err(StoreRejection::FenceRejected { .. })
        ));

        // Axis 2: attempt epoch superseded.
        let mut stale = ok.clone();
        stale.attempt_epoch = AttemptEpoch(2);
        assert!(matches!(
            store.check_holder_fence(&healthy_read(), &stale),
            Err(StoreRejection::FenceRejected { .. })
        ));

        // Axis 3: stamp bumped (A4's axis — no new attempt required).
        let mut stale = ok.clone();
        stale.stamp = Stamp(0);
        assert!(matches!(
            store.check_holder_fence(&healthy_read(), &stale),
            Err(StoreRejection::FenceRejected { .. })
        ));

        // Axis 4: lease not active.
        let mut read = healthy_read();
        read.lease = Some(LeaseFence {
            holder_id: "h1".into(),
            status: "revoked".into(),
        });
        assert!(matches!(
            store.check_holder_fence(&read, &ok),
            Err(StoreRejection::FenceRejected { .. })
        ));

        // Axis 5: wrong holder for the active lease.
        let mut read = healthy_read();
        read.lease = Some(LeaseFence {
            holder_id: "h2".into(),
            status: "active".into(),
        });
        assert!(matches!(
            store.check_holder_fence(&read, &ok),
            Err(StoreRejection::FenceRejected { .. })
        ));

        // Missing unit is NotFound, not a fence rejection.
        let read = FakeFenceRead {
            unit: None,
            lease: None,
        };
        assert!(matches!(
            store.check_holder_fence(&read, &ok),
            Err(StoreRejection::NotFound { .. })
        ));
    }
}

#[cfg(test)]
mod review_tests {
    //! Tests added from the 3-lens adversarial review: lifecycle guard,
    //! full book rebuild, every unique and immutable flag provoked, and the
    //! WriterLockHeld surface pinned.

    use super::*;

    fn pairs_for(label: &str, key: &str) -> PropertyMap {
        let p = |pairs: Vec<(DbString, Value)>| PropertyMap::from_pairs(pairs).expect("pairs");
        let s = |v: &str| Value::String(db(v));
        match label {
            "Authority" => p(vec![
                (db("authority_key"), s(key)),
                (db("authority_epoch"), Value::Uint(1)),
                (db("holder_instance_id"), s("inst-x")),
                (db("status"), s("active")),
            ]),
            "Unit" => p(vec![
                (db("unit_id"), s(key)),
                (db("version"), Value::Uint(1)),
                (db("current_attempt_epoch"), Value::Uint(1)),
                (db("stamp"), Value::Uint(0)),
                (db("status"), s("admitted")),
                (db("work_item_id"), s("w1")),
                (db("record"), s("{}")),
            ]),
            "Attempt" => p(vec![
                (db("attempt_key"), s(key)),
                (db("unit_id"), s("u1")),
                (db("attempt_epoch"), Value::Uint(1)),
                (db("holder_id"), s("h1")),
                (db("status"), s("active")),
            ]),
            "Lease" => p(vec![
                (db("unit_id"), s(key)),
                (db("attempt_epoch"), Value::Uint(1)),
                (db("holder_id"), s("h1")),
                (db("status"), s("active")),
                (db("version"), Value::Uint(1)),
            ]),
            "WorkItem" => p(vec![
                (db("work_item_id"), s(key)),
                (db("version"), Value::Uint(1)),
                (db("status"), s("ready")),
                (db("acceptance_contract_digest"), s("blake3:00")),
                (db("declared_write_scope"), s("[]")),
                (db("record"), s("{}")),
            ]),
            "Receipt" => p(vec![
                (db("receipt_key"), s(key)),
                (db("request_digest"), s("blake3:00")),
                (db("status"), s("completed")),
                (db("result"), s("{}")),
            ]),
            "Evidence" => p(vec![
                (db("evidence_key"), s(key)),
                (db("work_item_id"), s("w1")),
                (db("artifact_set_digest"), s("blake3:00")),
                (db("record"), s("{}")),
            ]),
            other => panic!("no pair builder for {other}"),
        }
    }

    fn effect_pairs(operation_key: &str, intent_id: &str) -> PropertyMap {
        PropertyMap::from_pairs([
            (db("operation_key"), Value::String(db(operation_key))),
            (db("effect_intent_id"), Value::String(db(intent_id))),
            (db("unit_id"), Value::String(db("u1"))),
            (db("version"), Value::Uint(1)),
            (db("state"), Value::String(db("prepared"))),
            (db("record"), Value::String(db("{}"))),
        ])
        .expect("effect pairs")
    }

    fn create_raw(store: &Store, label: &str, pairs: PropertyMap) -> Result<NodeId, String> {
        let mut txn = store.shared.begin_write();
        let node = {
            let mut m = txn.mutator();
            m.create_node(LabelSet::single(db(label)), pairs)
        };
        match node {
            Ok(n) => match txn.commit() {
                Ok(_) => Ok(n),
                Err(e) => Err(format!("{e:?}")),
            },
            Err(e) => {
                txn.rollback();
                Err(format!("{e:?}"))
            }
        }
    }

    #[test]
    fn create_refuses_an_existing_control_graph() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        drop(store);
        match Store::create(dir.path(), "inst-2").err() {
            Some(StoreError::AlreadyInitialized(_)) => {}
            other => panic!("expected AlreadyInitialized, got {other:?}"),
        }
        // The lawful reopen still works and takes the epoch forward.
        let store = Store::recover(dir.path(), "inst-2").unwrap();
        assert_eq!(store.authority_epoch(), AuthorityEpoch(2));
    }

    #[test]
    fn recover_rebuilds_every_book() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        for (label, key) in [
            ("Unit", "u1"),
            ("Attempt", "u1/1"),
            ("Lease", "u1"),
            ("WorkItem", "w1"),
            ("Receipt", "r1"),
            ("Evidence", "ev1"),
        ] {
            create_raw(&store, label, pairs_for(label, key)).unwrap();
        }
        create_raw(&store, "Effect", effect_pairs("op1", "ei1")).unwrap();
        super::tests::append_event(&store, "e1", "u1", 1);
        drop(store);
        let store = Store::recover(dir.path(), "inst-2").unwrap();
        assert!(store.unit_node("u1").is_some(), "unit book");
        assert!(store.attempt_node("u1/1").is_some(), "attempt book");
        assert!(store.lease_node("u1").is_some(), "lease book");
        assert!(store.work_item_node("w1").is_some(), "work item book");
        assert!(store.receipt_node("r1").is_some(), "receipt book");
        assert!(store.effect_node("op1").is_some(), "effect book");
        assert!(store.evidence_node("ev1").is_some(), "evidence book");
        assert!(
            store.books.lock().unwrap().events.contains_key("e1"),
            "event book"
        );
    }

    #[test]
    fn every_unique_key_rejects_a_duplicate() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        // The Authority singleton: create() already committed the row.
        assert!(
            create_raw(&store, "Authority", pairs_for("Authority", "control")).is_err(),
            "second Authority row must be unrepresentable"
        );
        for (label, key) in [
            ("Unit", "u1"),
            ("Attempt", "u1/1"),
            ("Lease", "u1"),
            ("WorkItem", "w1"),
            ("Receipt", "r1"),
            ("Evidence", "ev1"),
        ] {
            create_raw(&store, label, pairs_for(label, key)).unwrap();
            assert!(
                create_raw(&store, label, pairs_for(label, key)).is_err(),
                "duplicate {label} key must fail commit"
            );
        }
        create_raw(&store, "Effect", effect_pairs("op1", "ei1")).unwrap();
        assert!(
            create_raw(&store, "Effect", effect_pairs("op1", "ei2")).is_err(),
            "duplicate operation_key must fail"
        );
        assert!(
            create_raw(&store, "Effect", effect_pairs("op2", "ei1")).is_err(),
            "duplicate effect_intent_id must fail"
        );
    }

    #[test]
    fn duplicate_cursor_rejects_at_commit() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        let cursor = super::tests::append_event(&store, "e1", "u1", 1);
        let mut txn = store.shared.begin_write();
        {
            let mut m = txn.mutator();
            m.create_node(
                LabelSet::single(db("Event")),
                super::tests::event_pairs("e2", cursor, "u2", 1),
            )
            .expect("mutator accepts; uniqueness checks at commit");
        }
        assert!(txn.commit().is_err(), "cursor reuse must fail commit");
    }

    #[test]
    fn immutable_fields_reject_updates() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        let cases = [
            ("Unit", pairs_for("Unit", "u1"), "work_item_id"),
            (
                "WorkItem",
                pairs_for("WorkItem", "w1"),
                "acceptance_contract_digest",
            ),
            ("Receipt", pairs_for("Receipt", "r1"), "request_digest"),
            ("Evidence", pairs_for("Evidence", "ev1"), "record"),
        ];
        for (label, pairs, frozen) in cases {
            let node = create_raw(&store, label, pairs).unwrap();
            let mut txn = store.shared.begin_write();
            let result = txn.mutator().update_node(
                node,
                no_labels(),
                props_set([(db(frozen), Value::String(db("tampered")))]),
            );
            txn.rollback();
            assert!(result.is_err(), "{label}.{frozen} must be immutable");
        }
    }

    #[test]
    fn recover_under_a_live_holder_reports_writer_lock_held() {
        let dir = tempfile::tempdir().unwrap();
        let _held = Store::create(dir.path(), "inst-1").unwrap();
        match Store::recover(dir.path(), "inst-2").err() {
            Some(StoreError::Graph(e)) => {
                let detail = format!("{e:?}");
                assert!(
                    detail.contains("WriterLockHeld"),
                    "expected WriterLockHeld, got {detail}"
                );
            }
            other => panic!("expected Graph(WriterLockHeld), got {other:?}"),
        }
    }

    #[test]
    fn missing_lease_with_present_unit_is_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::create(dir.path(), "inst-1").unwrap();
        let read = super::tests::FakeFenceRead {
            unit: Some(UnitFence {
                attempt_epoch: AttemptEpoch(3),
                stamp: Stamp(1),
            }),
            lease: None,
        };
        let claims = AttemptTokenClaims {
            unit_id: "u1".into(),
            attempt_epoch: AttemptEpoch(3),
            stamp: Stamp(1),
            authority_epoch: store.authority_epoch(),
            holder_id: "h1".into(),
        };
        assert!(matches!(
            store.check_holder_fence(&read, &claims),
            Err(StoreRejection::NotFound { .. })
        ));
    }
}
