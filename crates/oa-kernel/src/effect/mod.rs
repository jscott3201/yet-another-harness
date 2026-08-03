//! External-effect ledger types (ADR-001 §4) and the fake-effect backend
//! that scripts their outcomes (MILE-001 Component 2).
//!
//! Everything outside the funnel transaction — tool runs, Git, artifact
//! publication, model calls — is a durable effect intent whose terminal
//! outcome MAY be `unknown` (R1.3). The types here carry that discipline;
//! the state machines that enforce it over Selene are the funnel's job
//! (task 5), and the §4.3 settlement classification lives on
//! [`EffectIntent::classify_targets`] so both the kernel and the recovery
//! scan compute it one way.

pub mod fake;

use crate::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp, Uuid7};
use serde::{Deserialize, Serialize};

/// R1.3's closed terminal enum for every external effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectTerminal {
    Succeeded,
    Failed,
    Cancelled,
    Compensated,
    Unknown,
}

/// §4.1 lifecycle states. `Settled` is reached only together with a terminal
/// value; `Reconciling` is where a cancelled-but-dispatched effect waits
/// (§5.3) and where restart parks dispatch-evidence-without-outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectState {
    Prepared,
    Dispatching,
    Dispatched,
    Observing,
    Reconciling,
    Compensating,
    Settled,
}

/// Per-adapter retry capability declaration (§4.2): an adapter offering
/// neither a stable idempotency key nor an authoritative result query MUST
/// declare `NoRetry`, and automatic retry for that class is rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryClass {
    SafeIdempotent,
    SafeWithOperationKey,
    QueryThenRetry,
    NoRetry,
}

/// R20's orthogonal axis: reversibility is declared per adapter operation
/// exactly as `RetryClass` is — an effect can be idempotent AND
/// irreversible, so neither class derives from the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReversibilityClass {
    /// Lands only in the attempt's isolated workspace; the §8 integration
    /// gateway is what makes it externally visible.
    Bufferable,
    /// Escapes the workspace but a registered compensation intent can undo
    /// it; §4.2 step 5 requires the registration before dispatch where the
    /// pinned ConstraintSet demands one.
    ReversibleExternal,
    /// No compensation restores prior state. An unresolved `unknown` here
    /// escalates immediately (§9 step 8, row A16), and dispatch requires a
    /// committed approval (`approval_ref`).
    Irreversible,
}

/// §4.3's adapter declaration of how its targets become knowable. This is
/// DECLARED, never inferred: an empty target list must not be readable as
/// "post-hoc", or an adapter that simply skipped its enumeration duty would
/// silently buy the untrusted classification path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetEnumeration {
    /// Targets are knowable at prepare (patch paths, ref names, artifact
    /// paths) and MUST be enumerated there; settle observations are matched
    /// against that declaration and classified by digest comparison.
    Declared,
    /// Targets are not knowable in advance (arbitrary shell). The adapter
    /// records the workspace subtree digest before dispatch and the observed
    /// set after; classification is by observation plus wait status, and the
    /// declared-target digest rules do not apply.
    PostHoc,
}

/// §4.3 per-target observation states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetState {
    NotApplied,
    Applied,
    PartiallyApplied,
    Unknown,
}

/// One target of a (possibly compound) effect: path, ref name, or resource
/// URI, with the digests that make its classification checkable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetObservation {
    pub target_id: String,
    /// None for post-hoc targets (arbitrary shell): there is no expected
    /// digest to compare, so declared-target classification rules do not
    /// apply (§4.3).
    pub expected_digest: Option<Digest>,
    pub pre_digest: Option<Digest>,
    pub observed_digest: Option<Digest>,
    pub state: TargetState,
    pub observed_at: Option<u64>,
}

impl TargetObservation {
    /// Declared-target classification (§4.3): `applied` only on an exact
    /// expected match, `not_applied` only on an exact pre-image match,
    /// anything else partial or unknown.
    pub fn classify_declared(&mut self) {
        self.state = match (
            &self.observed_digest,
            &self.expected_digest,
            &self.pre_digest,
        ) {
            (Some(obs), Some(exp), _) if obs == exp => TargetState::Applied,
            (Some(obs), _, Some(pre)) if obs == pre => TargetState::NotApplied,
            (Some(_), _, _) => TargetState::PartiallyApplied,
            (None, _, _) => TargetState::Unknown,
        };
    }
}

/// §4.1 durable effect record. Carries every §4.1 field; times are logical
/// milliseconds from the kernel's injected clock (deterministic replay —
/// the RFC 3339 wire projection is the adapter layer's job), and the §4.1
/// list constraints (targets sorted by target_id, <=1024,
/// UNIQUE(effect_intent_id, target_id)) plus state-transition legality are
/// funnel-enforced at write time, not struct-enforced. `version` is the §1
/// mutable-aggregate axis the funnel CASes and stamps into this aggregate's
/// semantic events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectIntent {
    pub effect_intent_id: Uuid7,
    pub version: u64,
    pub unit_id: String,
    pub attempt_epoch: AttemptEpoch,
    pub stamp: Stamp,
    pub authority_epoch: AuthorityEpoch,
    pub adapter_id: String,
    pub adapter_version: String,
    pub operation_key: String,
    pub logical_operation_id: Uuid7,
    pub request_digest: Digest,
    pub retry_class: RetryClass,
    pub reversibility_class: ReversibilityClass,
    pub approval_ref: Option<Uuid7>,
    pub policy_snapshot_digest: Digest,
    pub state: EffectState,
    /// Write-once: set through [`EffectIntent::settle`], never overwritten.
    pub terminal: Option<EffectTerminal>,
    /// §4.3 adapter declaration, pinned at prepare — decides which
    /// classification rules settle applies.
    pub target_enumeration: TargetEnumeration,
    pub targets: Vec<TargetObservation>,
    pub decomposable: bool,
    pub parent_effect_intent_id: Option<Uuid7>,
    pub compensation_intent_id: Option<Uuid7>,
    pub dispatched_at: Option<u64>,
    pub settled_at: Option<u64>,
    /// Where the §11.1 reconcile-backoff schedule lives — row A16 asserts
    /// the bufferable arm follows it while the irreversible arm bypasses it,
    /// and recovery must restore the backoff position from this field.
    pub next_reconcile_at: Option<u64>,
}

/// Settlement or transition rejection, typed so callers cannot confuse a
/// rule violation with an absent record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EffectError {
    /// §4.1: `terminal` is write-once.
    TerminalAlreadySet { existing: EffectTerminal },
    /// §4.3: `succeeded` requires every target applied.
    SucceededWithUnappliedTarget { target_id: String },
    /// §4.3: any unknown target forces `unknown` — `failed` may not mask it.
    FailedWithUnknownTarget { target_id: String },
}

impl EffectIntent {
    /// §4.1 operation-key derivation:
    /// `blake3:<hex(BLAKE3-256(work_item_id · logical_operation_id · request_digest))>`.
    /// Deliberately excludes `attempt_epoch`, `attempt_id`, `holder_id`, and
    /// `authority_epoch`, so the key survives retries, recovery takeovers,
    /// and rework of the same logical operation (row A13). Concatenation is
    /// unambiguous because the SUFFIX is fixed-width (36-char Uuid7 display,
    /// 71-char digest wire form), which pins the split from the end even
    /// though `work_item_id` is variable-length; a fourth variable-width
    /// input must NOT be appended without adding a separator scheme.
    pub fn derive_operation_key(
        work_item_id: &str,
        logical_operation_id: Uuid7,
        request_digest: &Digest,
    ) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(work_item_id.as_bytes());
        hasher.update(logical_operation_id.to_string().as_bytes());
        hasher.update(request_digest.as_str().as_bytes());
        format!("blake3:{}", hasher.finalize().to_hex())
    }

    /// Write-once terminal settlement, enforcing the §4.3 rules that are
    /// decidable from the record alone:
    ///
    /// - `Succeeded` is refused unless every recorded target is `Applied` —
    ///   a partial application settles `Failed` carrying the per-target
    ///   record, never a silent success (row A5). With ZERO recorded
    ///   targets, `Succeeded` is accepted only because §9.7 lets some
    ///   adapter classes settle on non-target evidence (wait status,
    ///   finalized response); the caller carries that evidence burden, and
    ///   `classify_targets` deliberately refuses to make the claim for it.
    /// - `Failed` is refused while any target is `Unknown` — §4.3 forces
    ///   `Unknown` there, and write-once terminal would otherwise mask an
    ///   unproven outcome as a known failure, bypassing the §9 step 8
    ///   escalation (row A16).
    ///
    /// Not enforced here, deliberately: state-machine legality (which
    /// `EffectState` may settle which terminal — §5.2 rule 4, §5.3's
    /// cancelled-needs-proof) is the funnel's transition validation, which
    /// sees dispatch evidence this record alone does not carry.
    pub fn settle(&mut self, terminal: EffectTerminal) -> Result<(), EffectError> {
        if let Some(existing) = self.terminal {
            return Err(EffectError::TerminalAlreadySet { existing });
        }
        if terminal == EffectTerminal::Succeeded
            && let Some(t) = self
                .targets
                .iter()
                .find(|t| t.state != TargetState::Applied)
        {
            return Err(EffectError::SucceededWithUnappliedTarget {
                target_id: t.target_id.clone(),
            });
        }
        if terminal == EffectTerminal::Failed
            && let Some(t) = self
                .targets
                .iter()
                .find(|t| t.state == TargetState::Unknown)
        {
            return Err(EffectError::FailedWithUnknownTarget {
                target_id: t.target_id.clone(),
            });
        }
        self.terminal = Some(terminal);
        self.state = EffectState::Settled;
        Ok(())
    }

    /// §4.3 settlement classification over the recorded targets: any
    /// `Unknown` target forces `Unknown`; all `Applied` permits `Succeeded`;
    /// any other mix is `Failed` with the partial record.
    ///
    /// An EMPTY target set classifies `Unknown`, never a vacuous success:
    /// no observations is no evidence (§9 step 6 — never a synthetic
    /// success), and the post-hoc-enumeration shape records targets only at
    /// observation, so a kill between dispatch and observation reaches this
    /// classifier with zero rows. An effect that lawfully settles succeeded
    /// without target rows (process wait status, model-call finalization,
    /// §9.7) does so on that evidence through `settle`, not through this
    /// classifier.
    pub fn classify_targets(&self) -> EffectTerminal {
        if self.targets.is_empty() || self.targets.iter().any(|t| t.state == TargetState::Unknown) {
            EffectTerminal::Unknown
        } else if self.targets.iter().all(|t| t.state == TargetState::Applied) {
            EffectTerminal::Succeeded
        } else {
            EffectTerminal::Failed
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn intent(targets: Vec<TargetObservation>) -> EffectIntent {
        let digest = Digest::of_bytes(b"req");
        EffectIntent {
            effect_intent_id: Uuid7::mint(1, 1),
            version: 1,
            unit_id: "u-test".into(),
            attempt_epoch: AttemptEpoch(1),
            stamp: Stamp(0),
            authority_epoch: AuthorityEpoch(1),
            adapter_id: "fake".into(),
            adapter_version: "0".into(),
            operation_key: EffectIntent::derive_operation_key(
                &Uuid7::mint(1, 3).to_string(),
                Uuid7::mint(1, 4),
                &digest,
            ),
            logical_operation_id: Uuid7::mint(1, 4),
            request_digest: digest.clone(),
            retry_class: RetryClass::SafeWithOperationKey,
            reversibility_class: ReversibilityClass::Bufferable,
            approval_ref: None,
            policy_snapshot_digest: digest,
            state: EffectState::Prepared,
            terminal: None,
            target_enumeration: TargetEnumeration::PostHoc,
            targets,
            decomposable: false,
            parent_effect_intent_id: None,
            compensation_intent_id: None,
            dispatched_at: None,
            settled_at: None,
            next_reconcile_at: None,
        }
    }

    fn target(id: &str, state: TargetState) -> TargetObservation {
        TargetObservation {
            target_id: id.into(),
            expected_digest: None,
            pre_digest: None,
            observed_digest: None,
            state,
            observed_at: None,
        }
    }

    #[test]
    fn operation_key_ignores_every_epoch_axis() {
        let (w, l) = (Uuid7::mint(5, 7).to_string(), Uuid7::mint(5, 8));
        let d = Digest::of_bytes(b"same request");
        // Same logical operation from two different attempts/epochs: the key
        // inputs simply do not include those axes.
        let a = EffectIntent::derive_operation_key(&w, l, &d);
        let b = EffectIntent::derive_operation_key(&w, l, &d);
        assert_eq!(a, b);
        assert!(a.starts_with("blake3:"));
        // Any input change changes the key.
        assert_ne!(
            a,
            EffectIntent::derive_operation_key(&w, l, &Digest::of_bytes(b"other"))
        );
        assert_ne!(
            a,
            EffectIntent::derive_operation_key(&w, Uuid7::mint(5, 9), &d)
        );
    }

    #[test]
    fn terminal_is_write_once() {
        let mut i = intent(vec![]);
        i.settle(EffectTerminal::Failed).unwrap();
        assert_eq!(
            i.settle(EffectTerminal::Succeeded),
            Err(EffectError::TerminalAlreadySet {
                existing: EffectTerminal::Failed
            })
        );
        assert_eq!(i.state, EffectState::Settled);
    }

    #[test]
    fn success_requires_every_target_applied() {
        let mut i = intent(vec![
            target("a", TargetState::Applied),
            target("b", TargetState::NotApplied),
        ]);
        assert_eq!(
            i.settle(EffectTerminal::Succeeded),
            Err(EffectError::SucceededWithUnappliedTarget {
                target_id: "b".into()
            })
        );
        // The lawful settlement for the 3-of-5 shape is Failed with the
        // partial record intact (row A5's assertion shape).
        assert_eq!(i.classify_targets(), EffectTerminal::Failed);
        i.settle(EffectTerminal::Failed).unwrap();
        assert_eq!(i.targets.len(), 2);
    }

    #[test]
    fn any_unknown_target_forces_unknown() {
        let i = intent(vec![
            target("a", TargetState::Applied),
            target("b", TargetState::Unknown),
        ]);
        assert_eq!(i.classify_targets(), EffectTerminal::Unknown);
    }

    #[test]
    fn empty_target_set_never_classifies_success() {
        // No observations is no evidence — the post-hoc shape at kill time.
        let i = intent(vec![]);
        assert_eq!(i.classify_targets(), EffectTerminal::Unknown);
        // settle(Succeeded) with zero targets stays lawful for the §9.7
        // non-target evidence classes; the CALLER carries that burden.
        let mut i = intent(vec![]);
        i.settle(EffectTerminal::Succeeded).unwrap();
    }

    #[test]
    fn failed_may_not_mask_an_unknown_target() {
        let mut i = intent(vec![
            target("a", TargetState::Applied),
            target("b", TargetState::Unknown),
        ]);
        assert_eq!(
            i.settle(EffectTerminal::Failed),
            Err(EffectError::FailedWithUnknownTarget {
                target_id: "b".into()
            })
        );
        // The lawful settlement is the classifier's answer.
        i.settle(EffectTerminal::Unknown).unwrap();
    }

    #[test]
    fn operation_key_golden_vector_and_full_input_sensitivity() {
        // Pinned so a derivation change (input reorder, separator, dropped
        // input) cannot ship silently — the key is the cross-restart
        // idempotency anchor (row A13).
        let w = Uuid7::mint(1, 1).to_string();
        let l = Uuid7::mint(2, 2);
        let d = Digest::of_bytes(b"fixed request");
        assert_eq!(
            EffectIntent::derive_operation_key(&w, l, &d),
            "blake3:3d833f813676a1feb677ce3e74ece71d42862ba1fd8b834297faeeaf5dc49881"
        );
        // work_item_id sensitivity (the axis the other test misses).
        assert_ne!(
            EffectIntent::derive_operation_key(&Uuid7::mint(9, 9).to_string(), l, &d),
            EffectIntent::derive_operation_key(&w, l, &d)
        );
    }

    #[test]
    fn all_equal_digests_classify_applied() {
        // Idempotent no-op write: expected == pre == observed satisfies both
        // §4.3 conditions at once; Applied wins and is pinned here so an arm
        // reorder cannot silently flip it to NotApplied.
        let d = Digest::of_bytes(b"same");
        let mut t = TargetObservation {
            target_id: "f".into(),
            expected_digest: Some(d.clone()),
            pre_digest: Some(d.clone()),
            observed_digest: Some(d),
            state: TargetState::Unknown,
            observed_at: None,
        };
        t.classify_declared();
        assert_eq!(t.state, TargetState::Applied);
    }

    #[test]
    fn declared_target_classification_is_exact_match_only() {
        let exp = Digest::of_bytes(b"new");
        let pre = Digest::of_bytes(b"old");
        let mut t = TargetObservation {
            target_id: "f".into(),
            expected_digest: Some(exp.clone()),
            pre_digest: Some(pre.clone()),
            observed_digest: Some(exp),
            state: TargetState::Unknown,
            observed_at: None,
        };
        t.classify_declared();
        assert_eq!(t.state, TargetState::Applied);
        t.observed_digest = Some(pre);
        t.classify_declared();
        assert_eq!(t.state, TargetState::NotApplied);
        t.observed_digest = Some(Digest::of_bytes(b"torn"));
        t.classify_declared();
        assert_eq!(t.state, TargetState::PartiallyApplied);
        t.observed_digest = None;
        t.classify_declared();
        assert_eq!(t.state, TargetState::Unknown);
    }
}
