//! Shared harness for the funnel integration suites.
#![allow(dead_code)] // each test binary consumes a subset of these helpers

use oa_kernel::effect::{
    EffectIntent, EffectState, EffectTerminal, RetryClass, ReversibilityClass, TargetEnumeration,
    TargetObservation, TargetState,
};
use oa_kernel::error::ErrorKind;
use oa_kernel::funnel::{
    Command, EffectSpec, Funnel, Method, PrincipalKind, ScopeKind, SettleEvidence, Submission,
    token_from_result,
};
use oa_kernel::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp, Uuid7};
use oa_kernel::store::{AttemptTokenClaims, Store};

pub struct Ctx {
    pub dir: tempfile::TempDir,
    pub funnel: Funnel,
    seq: u128,
}

impl Ctx {
    pub fn new() -> Ctx {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::create(dir.path(), "kernel-a").expect("create store");
        Ctx {
            dir,
            funnel: Funnel::new(store, 1_000),
            seq: 0,
        }
    }

    /// Continue over a recovered funnel. `seq_from` must clear the prior
    /// lifetime's command ids — receipts key on them.
    pub fn resume(dir: tempfile::TempDir, funnel: Funnel, seq_from: u128) -> Ctx {
        Ctx {
            dir,
            funnel,
            seq: seq_from,
        }
    }

    pub fn next_id(&mut self) -> Uuid7 {
        self.seq += 1;
        Uuid7::mint(1, self.seq)
    }

    pub fn authority(&self) -> AuthorityEpoch {
        self.funnel.store().authority_epoch()
    }

    /// Authority-class command: daemon principal, current epoch, no token.
    pub fn authority_cmd(
        &mut self,
        digest_src: &str,
        expected: Option<u64>,
        method: Method,
    ) -> Command {
        Command {
            command_id: self.next_id(),
            scope_kind: ScopeKind::Global,
            scope_id: "g".into(),
            request_digest: Digest::of_bytes(digest_src.as_bytes()),
            expected_version: expected,
            principal_kind: PrincipalKind::Daemon,
            authority_epoch: Some(self.authority()),
            attempt_token: None,
            method,
        }
    }

    /// Holder-class command: agent principal, sealed token, no envelope
    /// epoch (the token carries the authority axis).
    pub fn holder_cmd(
        &mut self,
        digest_src: &str,
        token: AttemptTokenClaims,
        expected: Option<u64>,
        method: Method,
    ) -> Command {
        Command {
            command_id: self.next_id(),
            scope_kind: ScopeKind::Unit,
            scope_id: "g".into(),
            request_digest: Digest::of_bytes(digest_src.as_bytes()),
            expected_version: expected,
            principal_kind: PrincipalKind::Agent,
            authority_epoch: None,
            attempt_token: Some(token),
            method,
        }
    }

    pub fn create_work_item(&mut self, wi: &str) -> Command {
        self.authority_cmd(
            &format!("create {wi}"),
            None,
            Method::WorkItemCreate {
                work_item_id: wi.into(),
                acceptance_contract_digest: Digest::of_bytes(b"contract"),
                declared_write_scope: vec!["scope/a".into()],
            },
        )
    }

    pub fn admit(&mut self, unit: &str, wi: &str) -> Command {
        self.authority_cmd(
            &format!("admit {unit}"),
            None,
            Method::UnitAdmit {
                unit_id: unit.into(),
                work_item_id: wi.into(),
            },
        )
    }

    /// Submit a dispatch at the given expected version; return the token.
    pub fn dispatch(&mut self, unit: &str, holder: &str, expected: u64) -> AttemptTokenClaims {
        let cmd = self.dispatch_cmd(unit, holder, expected);
        let result = completed(self.funnel.submit(&cmd));
        token_from_result(&result).expect("dispatch result carries token claims")
    }

    pub fn dispatch_cmd(&mut self, unit: &str, holder: &str, expected: u64) -> Command {
        self.authority_cmd(
            &format!("dispatch {unit} {holder} {}", self.seq),
            Some(expected),
            Method::UnitDispatch {
                unit_id: unit.into(),
                holder_id: holder.into(),
            },
        )
    }

    pub fn progress_cmd(
        &mut self,
        digest_src: &str,
        unit: &str,
        token: AttemptTokenClaims,
        expected: Option<u64>,
        note: &str,
    ) -> Command {
        self.holder_cmd(
            digest_src,
            token,
            expected,
            Method::ProgressReport {
                unit_id: unit.into(),
                note: note.into(),
            },
        )
    }

    /// Standard opening: work item wi-1 created, unit u-1 admitted (v1).
    pub fn seed_unit(&mut self) {
        let wi = self.create_work_item("wi-1");
        completed(self.funnel.submit(&wi));
        let admit = self.admit("u-1", "wi-1");
        completed(self.funnel.submit(&admit));
    }

    pub fn prepare_cmd(
        &mut self,
        digest_src: &str,
        unit: &str,
        token: AttemptTokenClaims,
        spec: EffectSpec,
    ) -> Command {
        self.holder_cmd(
            digest_src,
            token,
            None,
            Method::EffectPrepare {
                unit_id: unit.into(),
                spec,
            },
        )
    }

    /// §3.3 row 5's authorizing transition (`prepared -> dispatching`).
    pub fn dispatch_effect_cmd(
        &mut self,
        digest_src: &str,
        unit: &str,
        token: AttemptTokenClaims,
        key: &str,
        expected: u64,
    ) -> Command {
        self.holder_cmd(
            digest_src,
            token,
            Some(expected),
            Method::EffectDispatch {
                unit_id: unit.into(),
                operation_key: key.into(),
            },
        )
    }

    pub fn record_dispatched_cmd(
        &mut self,
        digest_src: &str,
        unit: &str,
        token: AttemptTokenClaims,
        key: &str,
        expected: u64,
        at: u64,
    ) -> Command {
        self.holder_cmd(
            digest_src,
            token,
            Some(expected),
            Method::EffectRecordDispatched {
                unit_id: unit.into(),
                operation_key: key.into(),
                dispatched_at: at,
            },
        )
    }

    /// Drive both halves of §4.2 step 2 and return the effect version after
    /// the dispatch is recorded.
    pub fn dispatch_and_record(
        &mut self,
        tag: &str,
        unit: &str,
        token: &AttemptTokenClaims,
        key: &str,
        expected: u64,
        at: u64,
    ) -> u64 {
        let d = self.dispatch_effect_cmd(
            &format!("{tag} dispatch"),
            unit,
            token.clone(),
            key,
            expected,
        );
        completed(self.funnel.submit(&d));
        let r = self.record_dispatched_cmd(
            &format!("{tag} record"),
            unit,
            token.clone(),
            key,
            expected + 1,
            at,
        );
        completed(self.funnel.submit(&r));
        expected + 2
    }

    /// Settle is AUTHORITY class (§3.3 row 6): daemon, no token.
    #[allow(clippy::too_many_arguments)] // a settle names every §4.3 axis
    pub fn settle_cmd(
        &mut self,
        digest_src: &str,
        unit: &str,
        key: &str,
        expected: u64,
        terminal: EffectTerminal,
        targets: Vec<TargetObservation>,
        evidence: SettleEvidence,
        at: u64,
    ) -> Command {
        self.authority_cmd(
            digest_src,
            Some(expected),
            Method::EffectSettle {
                unit_id: unit.into(),
                operation_key: key.into(),
                terminal,
                targets,
                evidence,
                settled_at: at,
            },
        )
    }

    /// The common settle: clean wait status, no cancellation report.
    #[allow(clippy::too_many_arguments)]
    pub fn settle_clean(
        &mut self,
        digest_src: &str,
        unit: &str,
        key: &str,
        expected: u64,
        terminal: EffectTerminal,
        targets: Vec<TargetObservation>,
        at: u64,
    ) -> Command {
        self.settle_cmd(
            digest_src,
            unit,
            key,
            expected,
            terminal,
            targets,
            SettleEvidence {
                clean_wait_status: true,
                target_reported_cancellation: false,
            },
            at,
        )
    }

    /// Park is AUTHORITY class, like settle.
    pub fn park_cmd(
        &mut self,
        digest_src: &str,
        unit: &str,
        key: &str,
        expected: u64,
        at: u64,
    ) -> Command {
        self.authority_cmd(
            digest_src,
            Some(expected),
            Method::EffectParkReconciling {
                unit_id: unit.into(),
                operation_key: key.into(),
                next_reconcile_at: at,
            },
        )
    }
}

/// A declared target with prepare-time digests (the §4.3 declared class).
pub fn target_declared(id: &str, expected: &str, pre: &str) -> TargetObservation {
    TargetObservation {
        target_id: id.into(),
        expected_digest: Some(Digest::of_bytes(expected.as_bytes())),
        pre_digest: Some(Digest::of_bytes(pre.as_bytes())),
        observed_digest: None,
        state: TargetState::Unknown,
        observed_at: None,
    }
}

/// A settle-time observation row: only `target_id`, `observed_digest`, and
/// `observed_at` are consulted for declared targets (states are recomputed).
pub fn target_observed(id: &str, observed: &str) -> TargetObservation {
    TargetObservation {
        target_id: id.into(),
        expected_digest: None,
        pre_digest: None,
        observed_digest: Some(Digest::of_bytes(observed.as_bytes())),
        state: TargetState::Unknown,
        observed_at: Some(100),
    }
}

/// A bufferable, key-idempotent, POST-HOC spec — the arbitrary-shell shape.
/// Post-hoc is explicit here: an empty target list is never readable as an
/// enumeration mode, so a test that wants the declared rules must say so.
pub fn effect_spec(logical_op: Uuid7, req: &str) -> EffectSpec {
    EffectSpec {
        logical_operation_id: logical_op,
        request_digest: Digest::of_bytes(req.as_bytes()),
        adapter_id: "fake".into(),
        adapter_version: "0".into(),
        retry_class: RetryClass::SafeWithOperationKey,
        reversibility_class: ReversibilityClass::Bufferable,
        target_enumeration: TargetEnumeration::PostHoc,
        approval_ref: None,
        policy_snapshot_digest: Digest::of_bytes(b"policy"),
        declared_targets: vec![],
        decomposable: false,
        compensation_intent_id: None,
    }
}

/// A declared-enumeration spec over the given target ids (§4.3).
pub fn declared_spec(logical_op: Uuid7, req: &str, target_ids: &[&str]) -> EffectSpec {
    EffectSpec {
        target_enumeration: TargetEnumeration::Declared,
        declared_targets: target_ids
            .iter()
            .map(|id| target_declared(id, "want", "pre"))
            .collect(),
        ..effect_spec(logical_op, req)
    }
}

/// A target row in a settle-time observation state.
pub fn target_in_state(id: &str, state: TargetState) -> TargetObservation {
    TargetObservation {
        target_id: id.into(),
        expected_digest: Some(Digest::of_bytes(b"expected")),
        pre_digest: Some(Digest::of_bytes(b"pre")),
        observed_digest: None,
        state,
        observed_at: Some(100),
    }
}

/// Minimal intent for [`FakeEffectBackend::dispatch`], which records only
/// the operation key and intent id — the rest is filler.
pub fn backend_intent(key: &str, intent_id: Uuid7, unit: &str) -> EffectIntent {
    EffectIntent {
        effect_intent_id: intent_id,
        version: 1,
        unit_id: unit.into(),
        attempt_epoch: AttemptEpoch(1),
        stamp: Stamp(0),
        authority_epoch: AuthorityEpoch(1),
        adapter_id: "fake".into(),
        adapter_version: "0".into(),
        operation_key: key.into(),
        logical_operation_id: intent_id,
        request_digest: Digest::of_bytes(b"req"),
        retry_class: RetryClass::SafeWithOperationKey,
        reversibility_class: ReversibilityClass::Bufferable,
        approval_ref: None,
        policy_snapshot_digest: Digest::of_bytes(b"policy"),
        state: EffectState::Dispatched,
        terminal: None,
        target_enumeration: TargetEnumeration::PostHoc,
        targets: vec![],
        decomposable: false,
        parent_effect_intent_id: None,
        compensation_intent_id: None,
        dispatched_at: None,
        settled_at: None,
        next_reconcile_at: None,
    }
}

pub fn completed(s: Submission) -> serde_json::Value {
    match s {
        Submission::Completed { result } => result,
        other => panic!("expected Completed, got {other:?}"),
    }
}

pub fn replayed(s: Submission) -> serde_json::Value {
    match s {
        Submission::Replayed { result } => result,
        other => panic!("expected Replayed, got {other:?}"),
    }
}

pub fn rejected(s: Submission) -> (ErrorKind, bool) {
    match s {
        Submission::Rejected { kind, replayed, .. } => (kind, replayed),
        other => panic!("expected Rejected, got {other:?}"),
    }
}
