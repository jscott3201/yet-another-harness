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

use selene_core::{GraphId, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::{CommitBatching, GraphError, RowIndex, SharedGraph};
use selene_persist::DEFAULT_WAL_FILE_NAME;

use crate::ids::{AttemptEpoch, AuthorityEpoch, Stamp};

pub use selene_graph::TypeViolation;
pub use selene_persist::PersistError;

pub const GRAPH_ID: u64 = 7;

#[cfg(test)]
mod review_tests;
mod schema;
#[cfg(test)]
mod tests;

pub use schema::graph_type;
pub(crate) use schema::{db, no_labels, props_set, value_str, value_u64};

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

    pub(crate) fn shared(&self) -> &SharedGraph {
        &self.shared
    }

    pub(crate) fn unit_node(&self, unit_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .units
            .get(unit_id)
            .copied()
    }

    pub(crate) fn lease_node(&self, unit_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .leases
            .get(unit_id)
            .copied()
    }

    pub(crate) fn work_item_node(&self, id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .work_items
            .get(id)
            .copied()
    }

    pub(crate) fn receipt_node(&self, key: &str) -> Option<NodeId> {
        self.books.lock().expect("books").receipts.get(key).copied()
    }

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

    /// Attempt-row status read-back — audit/test seam for the §1.2 "at most
    /// one active attempt per unit" invariant; the §9 recovery scan reads
    /// the same answer through its own snapshot pass.
    pub fn attempt_status(&self, unit_id: &str, epoch: u64) -> Option<String> {
        let node = self.attempt_node(&format!("{unit_id}/{epoch}"))?;
        let txn = self.shared.begin_write();
        let status = txn
            .read()
            .node_properties(node)
            .and_then(|p| p.get(&db("status")).and_then(value_str));
        txn.rollback();
        status
    }

    /// The committed journal in cursor order. Reads the write-side working
    /// graph (briefly taking the writer lock), so it reflects every commit
    /// that has returned — an audit/test seam for the obligation-2 "exactly
    /// one set of appended events" half and the V1 replay comparison, not a
    /// hot path.
    pub fn journal(&self) -> Vec<EventRecord> {
        let nodes: Vec<NodeId> = self
            .books
            .lock()
            .expect("books")
            .events
            .values()
            .copied()
            .collect();
        let txn = self.shared.begin_write();
        let mut out = Vec::with_capacity(nodes.len());
        {
            let read = txn.read();
            for node in nodes {
                let Some(p) = read.node_properties(node) else {
                    continue;
                };
                let get_u = |k: &str| p.get(&db(k)).and_then(value_u64).unwrap_or(0);
                let get_s = |k: &str| p.get(&db(k)).and_then(value_str).unwrap_or_default();
                out.push(EventRecord {
                    cursor: get_u("cursor"),
                    event_id: get_s("event_id"),
                    aggregate_kind: get_s("aggregate_kind"),
                    aggregate_id: get_s("aggregate_id"),
                    aggregate_version: get_u("aggregate_version"),
                    ordinal: get_u("ordinal"),
                    event_kind: get_s("event_kind"),
                    payload: get_s("payload"),
                    command_id: get_s("command_id"),
                });
            }
        }
        txn.rollback();
        out.sort_by_key(|e| e.cursor);
        out
    }
}

/// One committed journal row as read back through [`Store::journal`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRecord {
    pub cursor: u64,
    pub event_id: String,
    pub aggregate_kind: String,
    pub aggregate_id: String,
    pub aggregate_version: u64,
    pub ordinal: u64,
    pub event_kind: String,
    pub payload: String,
    pub command_id: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum BookKind {
    Unit,
    Attempt,
    Lease,
    WorkItem,
    Receipt,
    #[allow(dead_code)] // effect-over-store layer (task 5c) inserts these
    Effect,
    Event,
    #[allow(dead_code)] // evidence rows land with the cancellation layer (5d)
    Evidence,
}

/// Fence-relevant fields of a unit row.
pub(crate) struct UnitFence {
    pub attempt_epoch: AttemptEpoch,
    pub stamp: Stamp,
}

pub(crate) struct LeaseFence {
    pub holder_id: String,
    pub status: String,
}

/// Read seam the fence check runs against — always the write transaction's
/// working graph, never the published snapshot (it can lag sealed commits).
pub(crate) trait UnitFenceRead {
    fn unit_fence(&self, unit_id: &str) -> Option<UnitFence>;
    fn lease_fence(&self, unit_id: &str) -> Option<LeaseFence>;
}
