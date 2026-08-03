//! Shared harness for the funnel integration suites.
#![allow(dead_code)] // each test binary consumes a subset of these helpers

use oa_kernel::error::ErrorKind;
use oa_kernel::funnel::{Command, Funnel, Method, ScopeKind, Submission, token_from_result};
use oa_kernel::ids::{AuthorityEpoch, Digest, Uuid7};
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

    pub fn next_id(&mut self) -> Uuid7 {
        self.seq += 1;
        Uuid7::mint(1, self.seq)
    }

    pub fn authority(&self) -> AuthorityEpoch {
        self.funnel.store().authority_epoch()
    }

    /// Authority-class command: current epoch, no token.
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
            expected_unit_version: expected,
            authority_epoch: Some(self.authority()),
            attempt_token: None,
            method,
        }
    }

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
            expected_unit_version: expected,
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
