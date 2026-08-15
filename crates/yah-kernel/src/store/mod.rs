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

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rand::RngCore;
use selene_core::{GraphId, LabelSet, NodeId, PropertyMap, Value};
use selene_graph::{CommitBatching, GraphError, RowIndex, SharedGraph};
use selene_persist::DEFAULT_WAL_FILE_NAME;

use crate::ids::{AttemptEpoch, AuthorityEpoch, Stamp};

pub use selene_graph::TypeViolation;
pub use selene_persist::PersistError;

pub const GRAPH_ID: u64 = 7;

mod cancel_recovery;
#[cfg(test)]
mod cancel_recovery_tests;
mod read;
mod receipt;
mod receipt_event;
#[cfg(test)]
mod receipt_tests;
mod recovery;
#[cfg(test)]
mod review_tests;
mod schema;
#[cfg(test)]
mod tests;

pub use read::{CancelDeliveryRow, CancelRequestRow, EffectRecordRow, EventRecord, ReceiptRecord};
pub(crate) use recovery::commit_error;
pub use schema::graph_type;
pub(crate) use schema::{db, no_labels, props_set, value_str, value_u64};

/// §3.3 typed fence rejection. `FenceRejected` is the wire name (ADR-002
/// §10); the variants carry which axis failed for diagnostics only — clients
/// branch on the kind, never the axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRejection {
    InvalidRequest {
        detail: String,
    },
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
    /// The engine returned an error after commit began, so recovery must
    /// determine whether the write became durable.
    CommitUnknown(String),
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
    pub nonce: String,
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
    attempt_ids: HashMap<String, NodeId>,
    leases: HashMap<String, NodeId>,
    work_items: HashMap<String, NodeId>,
    receipts: HashMap<String, NodeId>,
    effects: HashMap<String, NodeId>,
    effect_intent_ids: HashMap<String, NodeId>,
    events: BTreeMap<u64, NodeId>,
    evidence: HashMap<String, NodeId>,
    runs: HashMap<String, NodeId>,
    /// Keyed by `cancel_request_id`. §5.2 rule 4 has to answer "is there an
    /// applicable cancellation" on every admission, so the whole set stays
    /// addressable rather than being found by scan.
    cancel_requests: HashMap<String, NodeId>,
    cancel_roots: HashMap<String, NodeId>,
    /// Keyed by the derived `delivery_key` — §5.1's
    /// UNIQUE(cancel_request_id, member_id).
    cancel_deliveries: HashMap<String, NodeId>,
    cancel_deliveries_by_request: HashMap<String, Vec<NodeId>>,
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
    project_id: String,
    min_retained_cursor: Mutex<u64>,
    token_key: [u8; 32],
}

impl Store {
    pub fn wal_path(dir: &Path) -> PathBuf {
        dir.join(DEFAULT_WAL_FILE_NAME)
    }

    /// Fresh control graph: claims `ControlAuthority` at epoch 1. Refuses a
    /// directory that already holds one — see
    /// [`StoreError::AlreadyInitialized`].
    pub fn create(dir: &Path, instance_id: &str) -> Result<Store, StoreError> {
        Self::create_project(dir, instance_id, "default")
    }

    pub fn create_project(
        dir: &Path,
        instance_id: &str,
        project_id: &str,
    ) -> Result<Store, StoreError> {
        if Self::wal_path(dir).exists() {
            return Err(StoreError::AlreadyInitialized(dir.to_path_buf()));
        }
        if !crate::ids::valid_wire_identifier(project_id) {
            return Err(StoreError::Internal("project_id is invalid".into()));
        }
        let shared = SharedGraph::builder(GraphId::new(GRAPH_ID))
            .bound_to(graph_type())?
            .with_wal(Self::wal_path(dir), selene_persist::WalConfig::default())?
            .with_commit_batching(CommitBatching::Off)
            .build()?;
        let mut token_key = [0_u8; 32];
        rand::rng().fill_bytes(&mut token_key);
        let store = Store {
            shared,
            dir: dir.to_path_buf(),
            books: Mutex::new(Books::default()),
            next_cursor: Mutex::new(1),
            authority_epoch: Mutex::new(AuthorityEpoch(1)),
            project_id: project_id.to_owned(),
            min_retained_cursor: Mutex::new(1),
            token_key,
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
                    (db("project_id"), Value::String(db(project_id))),
                    (
                        db("token_key"),
                        Value::String(db(&encode_token_key(&token_key))),
                    ),
                    (db("min_retained_cursor"), Value::Uint(1)),
                    (db("status"), Value::String(db("active"))),
                ])
                .expect("authority property map"),
            )?
        };
        txn.commit().map_err(recovery::commit_error)?;
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
            project_id: String::new(),
            min_retained_cursor: Mutex::new(1),
            token_key: [0; 32],
        };
        let (project_id, min_retained_cursor, token_key) = store.rebuild_books_and_cursor()?;
        let store = Store {
            project_id,
            min_retained_cursor: Mutex::new(min_retained_cursor),
            token_key,
            ..store
        };
        store.validate_all_cancellation_lifecycles()?;
        let mut cursor = 0;
        loop {
            let events = store.events_after_limit(cursor, crate::protocol::MAX_RESUME_EVENTS)?;
            if events.is_empty() {
                break;
            }
            for event in events {
                cursor = event.cursor;
                crate::protocol::event::project(event).map_err(|detail| {
                    StoreError::Internal(format!("durable event is not wire-safe: {detail}"))
                })?;
            }
        }
        store.validate_all_receipt_semantics()?;
        store.claim_authority(instance_id)?;
        Ok(store)
    }

    fn rebuild_books_and_cursor(&self) -> Result<(String, u64, [u8; 32]), StoreError> {
        let g = self.shared.read();
        let mut books = self.books.lock().expect("books");
        let mut max_cursor = 0u64;
        let mut project_id = None;
        let mut min_retained_cursor = None;
        let mut token_key = None;
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
                if books.authority.is_some() {
                    return Err(StoreError::Internal(
                        "control graph has multiple Authority rows".into(),
                    ));
                }
                books.authority = Some(id);
                project_id = get_str("project_id");
                token_key =
                    get_str("token_key").and_then(|value| recovery::decode_token_key(&value));
                min_retained_cursor = props.get(&db("min_retained_cursor")).and_then(value_u64);
            } else if labels.contains(&db("Unit")) {
                if let Some(k) = get_str("unit_id") {
                    books.units.insert(k, id);
                }
            } else if labels.contains(&db("Attempt")) {
                recovery::validate_attempt(props)?;
                if let Some(k) = get_str("attempt_key") {
                    books.attempts.insert(k, id);
                }
                if let Some(k) = get_str("attempt_id") {
                    books.attempt_ids.insert(k, id);
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
                recovery::validate_receipt(props)?;
                if let Some(k) = get_str("receipt_key") {
                    books.receipts.insert(k, id);
                }
            } else if labels.contains(&db("Effect")) {
                recovery::validate_effect(props)?;
                if let Some(k) = get_str("operation_key") {
                    books.effects.insert(k, id);
                }
                if let Some(k) = get_str("effect_intent_id") {
                    books.effect_intent_ids.insert(k, id);
                }
            } else if labels.contains(&db("Run")) {
                if let Some(k) = get_str("run_id") {
                    books.runs.insert(k, id);
                }
            } else if labels.contains(&db("CancelRequest")) {
                recovery::validate_cancel_request(props)?;
                if let Some(k) = get_str("cancel_request_id") {
                    books.cancel_requests.insert(k, id);
                }
                if let (Some(kind), Some(root_id)) = (get_str("root_kind"), get_str("root_id")) {
                    books.cancel_roots.insert(format!("{kind}/{root_id}"), id);
                }
            } else if labels.contains(&db("CancelDelivery")) {
                recovery::validate_cancel_delivery(props)?;
                if let Some(k) = get_str("delivery_key") {
                    books.cancel_deliveries.insert(k, id);
                }
                if let Some(k) = get_str("cancel_request_id") {
                    books
                        .cancel_deliveries_by_request
                        .entry(k)
                        .or_default()
                        .push(id);
                }
            } else if labels.contains(&db("Event")) {
                let c = props
                    .get(&db("cursor"))
                    .and_then(value_u64)
                    .ok_or_else(|| StoreError::Internal("Event row has invalid cursor".into()))?;
                if c == 0 {
                    return Err(StoreError::Internal(
                        "Event row cursor must be greater than zero".into(),
                    ));
                }
                max_cursor = max_cursor.max(c);
                books.events.insert(c, id);
            }
        }
        *self.next_cursor.lock().expect("cursor") = max_cursor
            .checked_add(1)
            .ok_or_else(|| StoreError::Internal("event cursor space exhausted".into()))?;
        let project_id = project_id
            .filter(|project_id| crate::ids::valid_wire_identifier(project_id))
            .ok_or_else(|| StoreError::Internal("Authority project_id is invalid".into()))?;
        let min_retained_cursor = min_retained_cursor.ok_or_else(|| {
            StoreError::Internal("Authority min_retained_cursor is invalid".into())
        })?;
        if min_retained_cursor > max_cursor && !(min_retained_cursor == 1 && max_cursor == 0) {
            return Err(StoreError::Internal(format!(
                "retention floor {min_retained_cursor} exceeds latest cursor {max_cursor}"
            )));
        }
        let token_key = token_key
            .ok_or_else(|| StoreError::Internal("Authority token_key is invalid".into()))?;
        let receipt_keys = books.receipts.keys().cloned().collect::<Vec<_>>();
        drop(books);
        drop(g);
        for key in receipt_keys {
            self.receipt(&key)?;
        }
        self.validate_event_receipts()?;
        Ok((project_id, min_retained_cursor, token_key))
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
                    .ok_or_else(|| {
                        StoreError::Internal("Authority authority_epoch is invalid".into())
                    })?,
                props
                    .get(&db("holder_instance_id"))
                    .and_then(value_str)
                    .ok_or_else(|| {
                        StoreError::Internal("Authority holder_instance_id is invalid".into())
                    })?,
            )
        };
        let next_epoch = epoch
            .checked_add(1)
            .ok_or_else(|| StoreError::Internal("authority epoch space exhausted".into()))?;
        {
            let mut m = txn.mutator();
            m.update_node(
                node,
                no_labels(),
                props_set([
                    (db("authority_epoch"), Value::Uint(next_epoch)),
                    (db("holder_instance_id"), Value::String(db(instance_id))),
                    (db("prior_instance_id"), Value::String(db(&prior))),
                    (db("status"), Value::String(db("active"))),
                ]),
            )?;
        }
        txn.commit().map_err(recovery::commit_error)?;
        *self.authority_epoch.lock().expect("epoch") = AuthorityEpoch(next_epoch);
        Ok(())
    }

    pub fn authority_epoch(&self) -> AuthorityEpoch {
        *self.authority_epoch.lock().expect("epoch")
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn token_key(&self) -> &[u8; 32] {
        &self.token_key
    }

    pub fn min_retained_cursor(&self) -> u64 {
        *self.min_retained_cursor.lock().expect("retention floor")
    }

    pub fn set_min_retained_cursor(&self, cursor: u64) -> Result<(), StoreError> {
        let mut floor = self.min_retained_cursor.lock().expect("retention floor");
        if cursor == *floor {
            return Ok(());
        }
        if cursor < *floor {
            return Err(StoreError::Rejected(StoreRejection::InvalidRequest {
                detail: format!(
                    "retention floor cannot decrease from {} to {cursor}",
                    *floor
                ),
            }));
        }
        let latest_cursor = self
            .books
            .lock()
            .expect("books")
            .events
            .last_key_value()
            .map(|(cursor, _)| *cursor)
            .unwrap_or(0);
        if cursor > latest_cursor {
            return Err(StoreError::Rejected(StoreRejection::InvalidRequest {
                detail: format!(
                    "retention floor {cursor} exceeds latest committed cursor {latest_cursor}"
                ),
            }));
        }
        let node = self
            .books
            .lock()
            .expect("books")
            .authority
            .ok_or_else(|| StoreError::Internal("control graph has no Authority row".into()))?;
        let mut txn = self.shared.begin_write();
        txn.mutator().update_node(
            node,
            no_labels(),
            props_set([(db("min_retained_cursor"), Value::Uint(cursor))]),
        )?;
        txn.commit().map_err(recovery::commit_error)?;
        *floor = cursor;
        Ok(())
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
    pub(crate) fn allocate_cursor(&self) -> Result<u64, StoreError> {
        let mut c = self.next_cursor.lock().expect("cursor");
        let v = *c;
        *c = c
            .checked_add(1)
            .ok_or_else(|| StoreError::Internal("event cursor space exhausted".into()))?;
        Ok(v)
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

    // The four seams below address the §5 rows. They land here rather than
    // with the funnel methods that consume them because `rebuild_books_and_cursor`
    // already repopulates all three books on recover — the addressing and its
    // recovery path are one decision, and splitting them across increments is
    // how a book gets a writer but no rebuild arm.
    #[allow(dead_code)]
    pub(crate) fn run_node(&self, run_id: &str) -> Option<NodeId> {
        self.books.lock().expect("books").runs.get(run_id).copied()
    }

    #[allow(dead_code)]
    pub(crate) fn cancel_request_node(&self, cancel_request_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .cancel_requests
            .get(cancel_request_id)
            .copied()
    }

    #[allow(dead_code)]
    pub(crate) fn cancel_delivery_node(&self, delivery_key: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .cancel_deliveries
            .get(delivery_key)
            .copied()
    }

    /// Every committed cancel-request node. §5.2 rule 4 asks "does an
    /// applicable cancellation cover this member" on every admission, and
    /// the answer has to come from committed state — so this returns the
    /// node set and the caller reads it through the open write transaction,
    /// never the published snapshot.
    /// Every (unit_id, node) pair. I11's close predicate has to find the
    /// units belonging to a run, and `run_id` lives on the unit row rather
    /// than in an edge — the control graph declares no edge types.
    pub(crate) fn unit_entries(&self) -> Vec<(String, NodeId)> {
        self.books
            .lock()
            .expect("books")
            .units
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Every (operation_key, node) pair, for the same walk.
    pub(crate) fn effect_entries(&self) -> Vec<(String, NodeId)> {
        self.books
            .lock()
            .expect("books")
            .effects
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    /// Every (delivery_key, node) pair. §5 settlement reads the delivered
    /// set for a request, and I11's close predicate needs the same walk per
    /// run — provided live, like every §5 read, through the working graph.
    pub(crate) fn cancel_delivery_entries(&self) -> Vec<(String, NodeId)> {
        self.books
            .lock()
            .expect("books")
            .cancel_deliveries
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect()
    }

    pub(crate) fn attempt_id_node(&self, attempt_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .attempt_ids
            .get(attempt_id)
            .copied()
    }

    /// Resolve an `effect_intent_id` to its node, by the same live scan.
    pub(crate) fn effect_intent_id_node(&self, effect_intent_id: &str) -> Option<NodeId> {
        self.books
            .lock()
            .expect("books")
            .effect_intent_ids
            .get(effect_intent_id)
            .copied()
    }

    pub(crate) fn book_insert(&self, kind: BookKind, key: String, node: NodeId) {
        let mut books = self.books.lock().expect("books");
        match kind {
            BookKind::Unit => books.units.insert(key, node),
            BookKind::Attempt { attempt_id } => {
                books.attempt_ids.insert(attempt_id, node);
                books.attempts.insert(key, node)
            }
            BookKind::Lease => books.leases.insert(key, node),
            BookKind::WorkItem => books.work_items.insert(key, node),
            BookKind::Receipt => books.receipts.insert(key, node),
            BookKind::Effect { effect_intent_id } => {
                books.effect_intent_ids.insert(effect_intent_id, node);
                books.effects.insert(key, node)
            }
            BookKind::Event => books.events.insert(
                key.parse().expect("event book key is the decimal cursor"),
                node,
            ),
            BookKind::Evidence => books.evidence.insert(key, node),
            BookKind::Run => books.runs.insert(key, node),
            BookKind::CancelRequest { root_key } => {
                books.cancel_roots.insert(root_key, node);
                books.cancel_requests.insert(key, node)
            }
            BookKind::CancelDelivery { request_id } => {
                books
                    .cancel_deliveries_by_request
                    .entry(request_id)
                    .or_default()
                    .push(node);
                books.cancel_deliveries.insert(key, node)
            }
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
}

#[derive(Clone, Debug)]
pub(crate) enum BookKind {
    Unit,
    Attempt {
        attempt_id: String,
    },
    Lease,
    WorkItem,
    Receipt,
    #[allow(dead_code)] // effect-over-store layer (task 5c) inserts these
    Effect {
        effect_intent_id: String,
    },
    Event,
    #[allow(dead_code)] // evidence rows land with the §8 gateway layer
    Evidence,
    #[allow(dead_code)] // the §5 funnel methods construct these
    Run,
    #[allow(dead_code)]
    CancelRequest {
        root_key: String,
    },
    #[allow(dead_code)]
    CancelDelivery {
        request_id: String,
    },
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

fn encode_token_key(key: &[u8; 32]) -> String {
    key.iter().map(|byte| format!("{byte:02x}")).collect()
}
