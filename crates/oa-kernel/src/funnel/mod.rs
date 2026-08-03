//! The single mutation funnel (ADR-001 §2.2): every accepted command is one
//! Selene transaction that claims-or-resolves the receipt, validates
//! authority and fence, CASes the named aggregate, appends semantic events,
//! and finalizes the receipt — commit or nothing.
//!
//! Method registry discipline (§2.1): every method carries exactly one
//! authorization class. Authority methods require the current
//! `authority_epoch` and no token; holder methods require the full §3.3
//! five-axis fence AND that the token's sealed `unit_id` names the method's
//! target; `token.reissue` is the single `holder_reauth` method — it
//! forgives a stale stamp (that is its purpose, §3.4) but nothing else.
//!
//! `submit` is serialized end to end by the funnel gate: Selene's writer
//! lock releases at seal, before the commit's durability wait, so the
//! address-book update and the next command's claim step would otherwise
//! race the commit tail (observed: a concurrent identical retry drew
//! `internal` instead of `Replayed`). One command at a time IS the §2.2
//! funnel; the gate makes it literal.
//!
//! A commit error is not proof of rejection (§2.2): the write may be durable
//! but unpublished. The funnel answers `outcome_unknown`, poisons itself,
//! and rejects everything after as `unavailable` — recovery (a new lifetime
//! over `Store::recover`) is the reconcile path, because resubmitting the
//! command there resolves it by durable receipt.
//!
//! Time and identity are injected: the funnel owns a logical clock advanced
//! by the caller and mints Uuid7s from (clock, authority_epoch, counter).
//! The epoch rides the entropy high bits, so two lifetimes cannot mint the
//! same event id even at the same logical millisecond (epochs are monotonic
//! across recover; the unique `event_id` constraint remains the backstop
//! beyond 4096 takeovers, where the 12-bit rand_a field wraps).

use crate::error::ErrorKind;
use crate::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp, Uuid7};
use crate::store::{
    AttemptTokenClaims, BookKind, LeaseFence, Store, StoreRejection, UnitFence, UnitFenceRead, db,
    no_labels, props_set, value_str, value_u64,
};
use selene_core::{LabelSet, NodeId, PropertyMap, Value};
use serde_json::json;
use std::sync::Mutex;

mod validate;

/// ADR-002 §5 scope kinds; the receipt key is the P5.2 idempotency triple.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScopeKind {
    Global,
    Project,
    Run,
    Unit,
}

impl ScopeKind {
    fn wire(self) -> &'static str {
        match self {
            ScopeKind::Global => "global",
            ScopeKind::Project => "project",
            ScopeKind::Run => "run",
            ScopeKind::Unit => "unit",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Command {
    pub command_id: Uuid7,
    pub scope_kind: ScopeKind,
    pub scope_id: String,
    /// P3.4's digest over the canonical request; equality decides replay vs
    /// `idempotency_conflict` (§2.2, I3).
    pub request_digest: Digest,
    /// Simplified §2.1 `expected_versions`: the one unit aggregate this
    /// increment's methods mutate. REQUIRED on every method that mutates an
    /// existing unit (I2's optimistic-concurrency rule); `None` is lawful
    /// only for pure creations and for `token.reissue` (no transition).
    pub expected_unit_version: Option<u64>,
    pub authority_epoch: Option<AuthorityEpoch>,
    pub attempt_token: Option<AttemptTokenClaims>,
    pub method: Method,
}

#[derive(Clone, Debug)]
pub enum Method {
    /// Authority: create a work item (pure creation).
    WorkItemCreate {
        work_item_id: String,
        acceptance_contract_digest: Digest,
        declared_write_scope: Vec<String>,
    },
    /// Authority: admit a unit against a work item. Epoch starts at 0 —
    /// §3.1 mints 1 on the first acquisition (dispatch).
    UnitAdmit {
        unit_id: String,
        work_item_id: String,
    },
    /// Authority: acquire or reassign — increments `current_attempt_epoch`,
    /// creates the Attempt row (superseding the prior one, §1.2's at most
    /// one active attempt per unit), upserts the single lease row to the
    /// new holder, and mints the attempt token (§3.1, §3.2).
    UnitDispatch { unit_id: String, holder_id: String },
    /// Holder: the fence-exercising state callback (obligation 3's shape).
    ProgressReport { unit_id: String, note: String },
    /// Authority: §3.4's second invalidation axis — kills outstanding
    /// tokens without minting an attempt, lease untouched (row A4).
    StampBump { unit_id: String },
    /// Holder-reauth: re-arm after a stamp bump. No journal event and no
    /// version bump — minting a token is not a domain transition.
    TokenReissue { unit_id: String },
}

impl Method {
    fn unit_id(&self) -> Option<&str> {
        match self {
            Method::WorkItemCreate { .. } => None,
            Method::UnitAdmit { unit_id, .. }
            | Method::UnitDispatch { unit_id, .. }
            | Method::ProgressReport { unit_id, .. }
            | Method::StampBump { unit_id }
            | Method::TokenReissue { unit_id } => Some(unit_id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Submission {
    Completed {
        result: serde_json::Value,
    },
    /// A completed receipt returned to a retry — byte-identical result.
    Replayed {
        result: serde_json::Value,
    },
    Rejected {
        kind: ErrorKind,
        detail: String,
        /// True when this rejection was served from a persisted receipt.
        replayed: bool,
    },
}

/// Fence-relevant unit row, pre-read under the open write transaction.
struct UnitRow {
    node: NodeId,
    version: u64,
    epoch: u64,
    stamp: u64,
}

struct LeaseRow {
    node: NodeId,
    holder_id: String,
    status: String,
    version: u64,
}

/// Pre-read fence state, honest about which unit it was read for: the fence
/// resolves the TOKEN's sealed unit_id through this seam, and answering for
/// any other unit would silently re-point the token at the method's target
/// (the wildcard-credential hole). A lookup for a different unit returns
/// `None` and the fence fails closed.
struct PreRead {
    for_unit: Option<String>,
    unit: Option<UnitRow>,
    lease: Option<LeaseRow>,
}

impl UnitFenceRead for PreRead {
    fn unit_fence(&self, unit_id: &str) -> Option<UnitFence> {
        if self.for_unit.as_deref() != Some(unit_id) {
            return None;
        }
        self.unit.as_ref().map(|u| UnitFence {
            attempt_epoch: AttemptEpoch(u.epoch),
            stamp: Stamp(u.stamp),
        })
    }
    fn lease_fence(&self, unit_id: &str) -> Option<LeaseFence> {
        if self.for_unit.as_deref() != Some(unit_id) {
            return None;
        }
        self.lease.as_ref().map(|l| LeaseFence {
            holder_id: l.holder_id.clone(),
            status: l.status.clone(),
        })
    }
}

/// What an accepted command will do, staged as concrete values so the
/// mutation phase performs no reads.
struct Accepted {
    plan: Plan,
    events: Vec<EventDraft>,
    result: serde_json::Value,
}

enum Plan {
    CreateWorkItem {
        work_item_id: String,
        acceptance_contract_digest: String,
        declared_write_scope: String,
    },
    CreateUnit {
        unit_id: String,
        work_item_id: String,
    },
    Dispatch {
        unit_node: NodeId,
        unit_id: String,
        new_version: u64,
        new_epoch: u64,
        holder_id: String,
        existing_lease: Option<(NodeId, u64)>,
        /// §1.2: at most one active attempt per unit — the prior epoch's
        /// row flips to `superseded` in this same transaction (§3.1).
        prior_attempt: Option<NodeId>,
    },
    BumpUnit {
        unit_node: NodeId,
        new_version: u64,
        new_stamp: Option<u64>,
    },
    Nothing,
}

struct EventDraft {
    aggregate_kind: &'static str,
    aggregate_id: String,
    aggregate_version: u64,
    event_kind: &'static str,
    payload: serde_json::Value,
}

type Rejection = (ErrorKind, String, /* persist receipt */ bool);

pub struct Funnel {
    store: Store,
    /// Serializes `submit` end to end and carries the §2.2 poison latch:
    /// `Some(detail)` after an uncertain commit, at which point every
    /// subsequent submit answers `unavailable` until recovery.
    gate: Mutex<Option<String>>,
    clock_ms: Mutex<u64>,
    mint_seq: Mutex<u64>,
}

impl Funnel {
    pub fn new(store: Store, clock_ms: u64) -> Funnel {
        Funnel {
            store,
            gate: Mutex::new(None),
            clock_ms: Mutex::new(clock_ms),
            mint_seq: Mutex::new(0),
        }
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn into_store(self) -> Store {
        self.store
    }

    /// Advance the logical clock (monotonic; lower values are ignored).
    pub fn tick(&self, to_ms: u64) {
        let mut c = self.clock_ms.lock().expect("clock");
        *c = (*c).max(to_ms);
    }

    fn mint_id(&self) -> Uuid7 {
        let ms = *self.clock_ms.lock().expect("clock");
        let mut seq = self.mint_seq.lock().expect("seq");
        *seq += 1;
        // Authority epoch in the entropy high bits: cross-lifetime
        // uniqueness by construction, not by caller clock discipline.
        let entropy = (u128::from(self.store.authority_epoch().0) << 64) | u128::from(*seq);
        Uuid7::mint(ms, entropy)
    }

    pub fn submit(&self, cmd: &Command) -> Submission {
        let mut gate = self.gate.lock().expect("funnel gate");
        if let Some(detail) = gate.as_ref() {
            return Submission::Rejected {
                kind: ErrorKind::Unavailable,
                detail: format!("funnel poisoned by uncertain commit: {detail}"),
                replayed: false,
            };
        }

        let receipt_key = format!(
            "{}/{}/{}",
            cmd.scope_kind.wire(),
            cmd.scope_id,
            cmd.command_id
        );

        let mut txn = self.store.shared().begin_write();

        // §2.2 step 2: claim or resolve. A stored receipt with an equal
        // digest replays its stable answer; an unequal digest is
        // idempotency_conflict with no transition (I3).
        if let Some(node) = self.store.receipt_node(&receipt_key) {
            let stored = txn.read().node_properties(node).map(|p| {
                (
                    p.get(&db("request_digest")).and_then(value_str),
                    p.get(&db("status")).and_then(value_str),
                    p.get(&db("result")).and_then(value_str),
                )
            });
            txn.rollback();
            return Self::replay_stored(cmd, stored);
        }

        // §2.2 steps 1 + 3: pre-read fence state from the WRITE-side working
        // graph (the published snapshot can lag — G02 trap), then validate.
        let pre = {
            let read = txn.read();
            let unit = cmd.method.unit_id().and_then(|uid| {
                let node = self.store.unit_node(uid)?;
                let props = read.node_properties(node)?;
                Some(UnitRow {
                    node,
                    version: props.get(&db("version")).and_then(value_u64).unwrap_or(0),
                    epoch: props
                        .get(&db("current_attempt_epoch"))
                        .and_then(value_u64)
                        .unwrap_or(0),
                    stamp: props.get(&db("stamp")).and_then(value_u64).unwrap_or(0),
                })
            });
            let lease = cmd.method.unit_id().and_then(|uid| {
                let node = self.store.lease_node(uid)?;
                let props = read.node_properties(node)?;
                Some(LeaseRow {
                    node,
                    holder_id: props.get(&db("holder_id")).and_then(value_str)?,
                    status: props.get(&db("status")).and_then(value_str)?,
                    version: props.get(&db("version")).and_then(value_u64).unwrap_or(0),
                })
            });
            PreRead {
                for_unit: cmd.method.unit_id().map(str::to_owned),
                unit,
                lease,
            }
        };

        let accepted = match self.validate(cmd, &pre) {
            Ok(a) => a,
            Err((kind, detail, persist)) => {
                if !persist {
                    txn.rollback();
                    return Submission::Rejected {
                        kind,
                        detail,
                        replayed: false,
                    };
                }
                // §2.3: rejections that can never heal persist so a
                // timed-out retry gets the same answer — with no
                // transition. version_conflict and not_found do NOT
                // persist: both depend on mutable state a later command
                // can change, so a retry must re-validate.
                let result = json!({ "error_kind": kind, "detail": detail });
                let node = {
                    let mut m = txn.mutator();
                    m.create_node(
                        LabelSet::single(db("Receipt")),
                        PropertyMap::from_pairs([
                            (db("receipt_key"), Value::String(db(&receipt_key))),
                            (
                                db("request_digest"),
                                Value::String(db(cmd.request_digest.as_str())),
                            ),
                            (db("status"), Value::String(db("rejected"))),
                            (db("result"), Value::String(db(&result.to_string()))),
                        ])
                        .expect("rejection receipt property map"),
                    )
                };
                match node {
                    Ok(n) => match txn.commit() {
                        Ok(_) => self.store.book_insert(BookKind::Receipt, receipt_key, n),
                        // The domain answer (rejection, no transition) is
                        // certain either way; the engine's health is not —
                        // poison so the next submit halts (§2.2).
                        Err(e) => *gate = Some(format!("{e:?}")),
                    },
                    Err(_) => txn.rollback(),
                }
                return Submission::Rejected {
                    kind,
                    detail,
                    replayed: false,
                };
            }
        };

        // §2.2 steps 4–7: apply the staged plan, append events, finalize
        // the completed receipt. No reads below this line.
        let mut new_books: Vec<(BookKind, String, NodeId)> = Vec::new();
        let mut cursors: Vec<u64> = Vec::new();
        let staged: Result<(), String> = (|| {
            let mut m = txn.mutator();
            let s = |v: &str| Value::String(db(v));
            match &accepted.plan {
                Plan::CreateWorkItem {
                    work_item_id,
                    acceptance_contract_digest,
                    declared_write_scope,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("WorkItem")),
                            PropertyMap::from_pairs([
                                (db("work_item_id"), s(work_item_id)),
                                (db("version"), Value::Uint(1)),
                                (db("status"), s("ready")),
                                (
                                    db("acceptance_contract_digest"),
                                    s(acceptance_contract_digest),
                                ),
                                (db("declared_write_scope"), s(declared_write_scope)),
                                (db("record"), s("{}")),
                            ])
                            .expect("work item property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::WorkItem, work_item_id.clone(), n));
                }
                Plan::CreateUnit {
                    unit_id,
                    work_item_id,
                } => {
                    let n = m
                        .create_node(
                            LabelSet::single(db("Unit")),
                            PropertyMap::from_pairs([
                                (db("unit_id"), s(unit_id)),
                                (db("version"), Value::Uint(1)),
                                (db("current_attempt_epoch"), Value::Uint(0)),
                                (db("stamp"), Value::Uint(0)),
                                (db("status"), s("admitted")),
                                (db("work_item_id"), s(work_item_id)),
                                (db("record"), s("{}")),
                            ])
                            .expect("unit property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::Unit, unit_id.clone(), n));
                }
                Plan::Dispatch {
                    unit_node,
                    unit_id,
                    new_version,
                    new_epoch,
                    holder_id,
                    existing_lease,
                    prior_attempt,
                } => {
                    m.update_node(
                        *unit_node,
                        no_labels(),
                        props_set([
                            (db("version"), Value::Uint(*new_version)),
                            (db("current_attempt_epoch"), Value::Uint(*new_epoch)),
                            (db("status"), s("dispatched")),
                        ]),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                    if let Some(prior) = prior_attempt {
                        m.update_node(
                            *prior,
                            no_labels(),
                            props_set([(db("status"), s("superseded"))]),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    }
                    let attempt_key = format!("{unit_id}/{new_epoch}");
                    let a = m
                        .create_node(
                            LabelSet::single(db("Attempt")),
                            PropertyMap::from_pairs([
                                (db("attempt_key"), s(&attempt_key)),
                                (db("unit_id"), s(unit_id)),
                                (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                (db("holder_id"), s(holder_id)),
                                (db("status"), s("active")),
                            ])
                            .expect("attempt property map"),
                        )
                        .map_err(|e| format!("{e:?}"))?;
                    new_books.push((BookKind::Attempt, attempt_key, a));
                    match existing_lease {
                        Some((lease_node, lease_version)) => {
                            m.update_node(
                                *lease_node,
                                no_labels(),
                                props_set([
                                    (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                    (db("holder_id"), s(holder_id)),
                                    (db("status"), s("active")),
                                    (db("version"), Value::Uint(lease_version + 1)),
                                ]),
                            )
                            .map_err(|e| format!("{e:?}"))?;
                        }
                        None => {
                            let l = m
                                .create_node(
                                    LabelSet::single(db("Lease")),
                                    PropertyMap::from_pairs([
                                        (db("unit_id"), s(unit_id)),
                                        (db("attempt_epoch"), Value::Uint(*new_epoch)),
                                        (db("holder_id"), s(holder_id)),
                                        (db("status"), s("active")),
                                        (db("version"), Value::Uint(1)),
                                    ])
                                    .expect("lease property map"),
                                )
                                .map_err(|e| format!("{e:?}"))?;
                            new_books.push((BookKind::Lease, unit_id.clone(), l));
                        }
                    }
                }
                Plan::BumpUnit {
                    unit_node,
                    new_version,
                    new_stamp,
                } => {
                    let mut set = vec![(db("version"), Value::Uint(*new_version))];
                    if let Some(stamp) = new_stamp {
                        set.push((db("stamp"), Value::Uint(*stamp)));
                    }
                    m.update_node(*unit_node, no_labels(), props_set(set))
                        .map_err(|e| format!("{e:?}"))?;
                }
                Plan::Nothing => {}
            }

            for draft in &accepted.events {
                // Cursor allocation under the open write txn — the
                // allocate_cursor ordering contract.
                let cursor = self.store.allocate_cursor();
                let event_id = self.mint_id().to_string();
                let n = m
                    .create_node(
                        LabelSet::single(db("Event")),
                        PropertyMap::from_pairs([
                            (db("event_id"), s(&event_id)),
                            (db("cursor"), Value::Uint(cursor)),
                            (
                                // Kind-prefixed: aggregate ids are unique per
                                // kind only, so cross-kind id reuse must not
                                // collide the derived composite.
                                db("agg_ver_ord"),
                                s(&format!(
                                    "{}/{}/{}/0",
                                    draft.aggregate_kind,
                                    draft.aggregate_id,
                                    draft.aggregate_version
                                )),
                            ),
                            (db("aggregate_kind"), s(draft.aggregate_kind)),
                            (db("aggregate_id"), s(&draft.aggregate_id)),
                            (
                                db("aggregate_version"),
                                Value::Uint(draft.aggregate_version),
                            ),
                            (db("ordinal"), Value::Uint(0)),
                            (db("event_kind"), s(draft.event_kind)),
                            (db("payload"), s(&draft.payload.to_string())),
                            (db("command_id"), s(&cmd.command_id.to_string())),
                        ])
                        .expect("event property map"),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                new_books.push((BookKind::Event, event_id, n));
                cursors.push(cursor);
            }

            let mut receipt_pairs = vec![
                (db("receipt_key"), s(&receipt_key)),
                (db("request_digest"), s(cmd.request_digest.as_str())),
                (db("status"), s("completed")),
                (db("result"), s(&accepted.result.to_string())),
            ];
            if let (Some(first), Some(last)) = (cursors.first(), cursors.last()) {
                receipt_pairs.push((db("first_cursor"), Value::Uint(*first)));
                receipt_pairs.push((db("last_cursor"), Value::Uint(*last)));
            }
            let r = m
                .create_node(
                    LabelSet::single(db("Receipt")),
                    PropertyMap::from_pairs(receipt_pairs).expect("receipt property map"),
                )
                .map_err(|e| format!("{e:?}"))?;
            new_books.push((BookKind::Receipt, receipt_key.clone(), r));
            Ok(())
        })();

        if let Err(detail) = staged {
            txn.rollback();
            return Submission::Rejected {
                kind: ErrorKind::Internal,
                detail,
                replayed: false,
            };
        }
        // §2.2 step 8. An Err here is NOT proof of rejection — the write
        // may be durable but unpublished — so the answer is outcome_unknown
        // and the funnel halts; a recovered lifetime resolves the command
        // by resubmitting against its durable receipt.
        if let Err(e) = txn.commit() {
            let detail = format!("{e:?}");
            *gate = Some(detail.clone());
            return Submission::Rejected {
                kind: ErrorKind::OutcomeUnknown,
                detail: format!("commit outcome uncertain: {detail}"),
                replayed: false,
            };
        }
        for (kind, key, node) in new_books {
            self.store.book_insert(kind, key, node);
        }
        Submission::Completed {
            result: accepted.result,
        }
    }

    fn replay_stored(
        cmd: &Command,
        stored: Option<(Option<String>, Option<String>, Option<String>)>,
    ) -> Submission {
        let Some((Some(digest), Some(status), Some(result))) = stored else {
            return Submission::Rejected {
                kind: ErrorKind::Internal,
                detail: "receipt row unreadable".into(),
                replayed: false,
            };
        };
        if digest != cmd.request_digest.as_str() {
            return Submission::Rejected {
                kind: ErrorKind::IdempotencyConflict,
                detail: "same command_id, different request digest".into(),
                replayed: false,
            };
        }
        let result: serde_json::Value =
            serde_json::from_str(&result).unwrap_or(serde_json::Value::Null);
        if status == "completed" {
            Submission::Replayed { result }
        } else {
            let kind = result
                .get("error_kind")
                .cloned()
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or(ErrorKind::Internal);
            let detail = result
                .get("detail")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();
            Submission::Rejected {
                kind,
                detail,
                replayed: true,
            }
        }
    }
}

/// Rebuild token claims from a dispatch or reissue result — the in-process
/// stand-in for the daemon's sealed token (MILE-001 carries claims unsealed;
/// the MAC boundary is INSTALL-001's).
pub fn token_from_result(result: &serde_json::Value) -> Option<AttemptTokenClaims> {
    Some(AttemptTokenClaims {
        unit_id: result.get("unit_id")?.as_str()?.to_owned(),
        attempt_epoch: AttemptEpoch(result.get("attempt_epoch")?.as_u64()?),
        stamp: Stamp(result.get("stamp")?.as_u64()?),
        authority_epoch: AuthorityEpoch(result.get("authority_epoch")?.as_u64()?),
        holder_id: result.get("holder_id")?.as_str()?.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_funnel() -> (tempfile::TempDir, Funnel) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::create(dir.path(), "kernel-t").expect("create");
        (dir, Funnel::new(store, 1_000))
    }

    /// Commit faults cannot be injected through Selene, so the latch is
    /// exercised directly: a poisoned funnel answers `unavailable` to
    /// everything, including a replay of an already-completed command.
    #[test]
    fn poisoned_funnel_rejects_every_submit_unavailable() {
        let (_dir, funnel) = scratch_funnel();
        let cmd = Command {
            command_id: Uuid7::mint(1, 1),
            scope_kind: ScopeKind::Global,
            scope_id: "g".into(),
            request_digest: Digest::of_bytes(b"create wi-1"),
            expected_unit_version: None,
            authority_epoch: Some(funnel.store().authority_epoch()),
            attempt_token: None,
            method: Method::WorkItemCreate {
                work_item_id: "wi-1".into(),
                acceptance_contract_digest: Digest::of_bytes(b"c"),
                declared_write_scope: vec![],
            },
        };
        assert!(matches!(funnel.submit(&cmd), Submission::Completed { .. }));

        *funnel.gate.lock().expect("gate") = Some("injected".into());
        match funnel.submit(&cmd) {
            Submission::Rejected {
                kind: ErrorKind::Unavailable,
                replayed: false,
                ..
            } => {}
            other => panic!("expected Unavailable, got {other:?}"),
        }
    }

    /// Two lifetimes minting at the SAME logical millisecond and sequence
    /// produce distinct ids because the authority epoch rides the entropy.
    #[test]
    fn minted_ids_differ_across_authority_epochs() {
        let a = Uuid7::mint(1_000, (1u128 << 64) | 1);
        let b = Uuid7::mint(1_000, (2u128 << 64) | 1);
        assert_ne!(a, b);
    }
}
