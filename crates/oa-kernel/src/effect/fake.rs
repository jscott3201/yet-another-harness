//! Fake-effect backend: the scripted external world (MILE-001 Component 2).
//!
//! Plays the target's side of the §4.2 lifecycle with zero real side
//! effects. Three resolution modes make the uncertain-effect obligations
//! exercisable without a real process: an outcome the target reports
//! immediately, an outcome that never arrives (forcing `unknown` across a
//! simulated restart), and an outcome deliberately withheld until an
//! explicit reconcile step (obligation 5, rows A6/A13/A16).
//!
//! The backend outlives simulated kernel kills — tests hold it across the
//! restart precisely because the external world does not reboot with the
//! kernel — and it logs every dispatch it observes, which is how row A13
//! asserts "exactly one operation key across dispatch, kill, restart, and
//! reconcile" from the target's point of view.

use super::{EffectIntent, EffectTerminal, TargetState};
use crate::ids::Uuid7;
use std::collections::BTreeMap;

/// How the scripted target resolves one logical operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ScriptedOutcome {
    /// Target reports this terminal as soon as it is asked.
    Immediate(EffectTerminal),
    /// Compound effect: per-target outcomes the observer must classify
    /// (§4.3) — the 3-of-5 shape of row A5 scripts here.
    ImmediateTargets(Vec<(String, TargetState)>),
    /// No outcome, ever: dispatch evidence with nothing behind it.
    Never,
    /// Hidden until [`FakeEffectBackend::reconcile`]; queries before that
    /// see [`Observation::Pending`].
    Withheld(EffectTerminal),
}

/// What a query of the target returns. `Unrecognized` is the real-world
/// "no such operation" answer — the target never received a dispatch under
/// that key — and is distinct from `Pending` (received, outcome not yet
/// determinable) because §5.3 makes proved-never-dispatched the case where
/// `cancelled` is lawful, and a kernel that regenerated its operation key
/// (the A13 defect) must see this answer, not a plausible pending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observation {
    Terminal(EffectTerminal),
    PerTarget(Vec<(String, TargetState)>),
    Pending,
    Unrecognized,
}

/// One dispatch as the target saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchRecord {
    pub operation_key: String,
    pub effect_intent_id: Uuid7,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FakeEffectError {
    /// Dispatch of an operation key no script covers — a test-authoring
    /// error surfaced loudly rather than an invented outcome.
    UnscriptedKey(String),
}

#[derive(Default)]
pub struct FakeEffectBackend {
    scripts: BTreeMap<String, ScriptedOutcome>,
    revealed: BTreeMap<String, bool>,
    dispatches: Vec<DispatchRecord>,
}

impl FakeEffectBackend {
    pub fn new() -> Self {
        Self::default()
    }

    /// Scripting a key resets any earlier reveal: a re-scripted withheld
    /// outcome is hidden again until its own explicit reconcile, so a
    /// backend reused across a simulated restart cannot leak the new
    /// outcome through the old reveal.
    pub fn script(&mut self, operation_key: impl Into<String>, outcome: ScriptedOutcome) {
        let key = operation_key.into();
        self.revealed.remove(&key);
        self.scripts.insert(key, outcome);
    }

    /// The target observes a dispatch. Recording is unconditional — a real
    /// target saw the request whether or not the harness anticipated it, and
    /// the A13 dispatch log must show a regenerated key, not hide it. The
    /// unscripted case still fails loudly after recording.
    pub fn dispatch(&mut self, intent: &EffectIntent) -> Result<(), FakeEffectError> {
        self.dispatches.push(DispatchRecord {
            operation_key: intent.operation_key.clone(),
            effect_intent_id: intent.effect_intent_id,
        });
        if !self.scripts.contains_key(&intent.operation_key) {
            return Err(FakeEffectError::UnscriptedKey(intent.operation_key.clone()));
        }
        Ok(())
    }

    /// Authoritative target query (§4.2 step 3 / §9 step 5's
    /// query-before-retry). A key never dispatched — scripted or not — is
    /// [`Observation::Unrecognized`]; withheld outcomes stay
    /// [`Observation::Pending`] until reconciled; `Never` stays pending
    /// forever.
    pub fn observe(&self, operation_key: &str) -> Observation {
        if self.dispatch_count(operation_key) == 0 {
            return Observation::Unrecognized;
        }
        match self.scripts.get(operation_key) {
            Some(ScriptedOutcome::Immediate(t)) => Observation::Terminal(*t),
            Some(ScriptedOutcome::ImmediateTargets(targets)) => {
                Observation::PerTarget(targets.clone())
            }
            Some(ScriptedOutcome::Withheld(t)) => {
                if self.revealed.get(operation_key).copied().unwrap_or(false) {
                    Observation::Terminal(*t)
                } else {
                    Observation::Pending
                }
            }
            Some(ScriptedOutcome::Never) => Observation::Pending,
            None => Observation::Unrecognized,
        }
    }

    /// The explicit reconcile step: reveals a withheld outcome — but only
    /// for an operation the target actually received; reconciling an
    /// undispatched key answers `Unrecognized` like any other query. A
    /// `Never` script stays pending — the permanently-unknown shape that
    /// must surface to the owner (§9 step 8).
    pub fn reconcile(&mut self, operation_key: &str) -> Observation {
        if self.dispatch_count(operation_key) > 0
            && let Some(ScriptedOutcome::Withheld(_)) = self.scripts.get(operation_key)
        {
            self.revealed.insert(operation_key.to_owned(), true);
        }
        self.observe(operation_key)
    }

    pub fn dispatches(&self) -> &[DispatchRecord] {
        &self.dispatches
    }

    pub fn dispatch_count(&self, operation_key: &str) -> usize {
        self.dispatches
            .iter()
            .filter(|d| d.operation_key == operation_key)
            .count()
    }

    /// Distinct operation keys this target ever saw — row A13's assertion is
    /// that one logical operation contributes exactly one entry here no
    /// matter how many kills and re-dispatches happened around it.
    pub fn distinct_keys(&self) -> Vec<&str> {
        let mut keys: Vec<&str> = self
            .dispatches
            .iter()
            .map(|d| d.operation_key.as_str())
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::TargetEnumeration;
    use crate::effect::{EffectState, RetryClass, ReversibilityClass};
    use crate::ids::{AttemptEpoch, AuthorityEpoch, Digest, Stamp};

    fn intent_with_key(key: &str, epoch: u64) -> EffectIntent {
        let digest = Digest::of_bytes(b"req");
        EffectIntent {
            effect_intent_id: Uuid7::mint(epoch, epoch as u128),
            version: 1,
            unit_id: "u-test".into(),
            attempt_epoch: AttemptEpoch(epoch),
            stamp: Stamp(0),
            authority_epoch: AuthorityEpoch(1),
            adapter_id: "fake".into(),
            adapter_version: "0".into(),
            operation_key: key.into(),
            logical_operation_id: Uuid7::mint(1, 4),
            request_digest: digest.clone(),
            retry_class: RetryClass::SafeWithOperationKey,
            reversibility_class: ReversibilityClass::Bufferable,
            approval_ref: None,
            policy_snapshot_digest: digest,
            state: EffectState::Prepared,
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

    #[test]
    fn all_five_terminal_states_are_scriptable() {
        let mut backend = FakeEffectBackend::new();
        for (i, t) in [
            EffectTerminal::Succeeded,
            EffectTerminal::Failed,
            EffectTerminal::Cancelled,
            EffectTerminal::Compensated,
            EffectTerminal::Unknown,
        ]
        .into_iter()
        .enumerate()
        {
            let key = format!("k{i}");
            backend.script(&key, ScriptedOutcome::Immediate(t));
            backend.dispatch(&intent_with_key(&key, 1)).unwrap();
            assert_eq!(backend.observe(&key), Observation::Terminal(t));
        }
    }

    #[test]
    fn undispatched_operations_are_unrecognized_not_pending() {
        let mut backend = FakeEffectBackend::new();
        // No script at all.
        assert_eq!(backend.observe("ghost"), Observation::Unrecognized);
        assert_eq!(backend.reconcile("ghost"), Observation::Unrecognized);
        // Scripted but the target never received a dispatch: same answer,
        // and the withheld outcome stays sealed.
        backend.script("k", ScriptedOutcome::Withheld(EffectTerminal::Succeeded));
        assert_eq!(backend.observe("k"), Observation::Unrecognized);
        assert_eq!(backend.reconcile("k"), Observation::Unrecognized);
        backend.dispatch(&intent_with_key("k", 1)).unwrap();
        assert_eq!(backend.observe("k"), Observation::Pending);
    }

    #[test]
    fn rescripting_a_key_reseals_its_outcome() {
        let mut backend = FakeEffectBackend::new();
        backend.script("k", ScriptedOutcome::Withheld(EffectTerminal::Succeeded));
        backend.dispatch(&intent_with_key("k", 1)).unwrap();
        assert_eq!(
            backend.reconcile("k"),
            Observation::Terminal(EffectTerminal::Succeeded)
        );
        // Simulated-restart re-script: the new withheld outcome must be
        // hidden until its own reconcile, not leak through the old reveal.
        backend.script("k", ScriptedOutcome::Withheld(EffectTerminal::Failed));
        assert_eq!(backend.observe("k"), Observation::Pending);
        assert_eq!(
            backend.reconcile("k"),
            Observation::Terminal(EffectTerminal::Failed)
        );
    }

    #[test]
    fn withheld_outcome_is_pending_until_the_explicit_reconcile() {
        let mut backend = FakeEffectBackend::new();
        backend.script("k", ScriptedOutcome::Withheld(EffectTerminal::Succeeded));
        backend.dispatch(&intent_with_key("k", 1)).unwrap();
        assert_eq!(backend.observe("k"), Observation::Pending);
        assert_eq!(
            backend.reconcile("k"),
            Observation::Terminal(EffectTerminal::Succeeded)
        );
        assert_eq!(
            backend.observe("k"),
            Observation::Terminal(EffectTerminal::Succeeded)
        );
    }

    #[test]
    fn never_resolving_effect_stays_pending_through_reconcile() {
        let mut backend = FakeEffectBackend::new();
        backend.script("k", ScriptedOutcome::Never);
        backend.dispatch(&intent_with_key("k", 1)).unwrap();
        assert_eq!(backend.reconcile("k"), Observation::Pending);
    }

    #[test]
    fn redispatch_across_epochs_observes_one_operation_key() {
        // The A13 shape from the target's side: kill and rework mint new
        // attempt epochs, but the logical operation's key never changes.
        let mut backend = FakeEffectBackend::new();
        backend.script(
            "stable",
            ScriptedOutcome::Withheld(EffectTerminal::Succeeded),
        );
        backend.dispatch(&intent_with_key("stable", 1)).unwrap();
        backend.dispatch(&intent_with_key("stable", 2)).unwrap();
        assert_eq!(backend.dispatch_count("stable"), 2);
        assert_eq!(backend.distinct_keys(), vec!["stable"]);
    }

    #[test]
    fn compound_per_target_outcomes_script_the_a5_shape() {
        let mut backend = FakeEffectBackend::new();
        backend.script(
            "compound",
            ScriptedOutcome::ImmediateTargets(vec![
                ("t1".into(), TargetState::Applied),
                ("t2".into(), TargetState::Applied),
                ("t3".into(), TargetState::Applied),
                ("t4".into(), TargetState::NotApplied),
                ("t5".into(), TargetState::NotApplied),
            ]),
        );
        backend.dispatch(&intent_with_key("compound", 1)).unwrap();
        let Observation::PerTarget(targets) = backend.observe("compound") else {
            panic!("expected per-target observation");
        };
        assert_eq!(targets.len(), 5);
        assert_eq!(
            targets
                .iter()
                .filter(|(_, s)| *s == TargetState::Applied)
                .count(),
            3
        );
    }

    #[test]
    fn unscripted_dispatch_is_loud_and_still_recorded() {
        let mut backend = FakeEffectBackend::new();
        assert_eq!(
            backend.dispatch(&intent_with_key("ghost", 1)),
            Err(FakeEffectError::UnscriptedKey("ghost".into()))
        );
        // The target saw the request: the A13 log must show a regenerated
        // key even when the harness did not anticipate it.
        assert_eq!(backend.dispatch_count("ghost"), 1);
        assert_eq!(backend.distinct_keys(), vec!["ghost"]);
    }

    #[test]
    fn compound_children_attribute_through_the_dispatch_log() {
        // Component 2's second record shape: sub-effects under one logical
        // parent, each child dispatched under its own key, attribution by
        // effect_intent_id in the dispatch log.
        let mut backend = FakeEffectBackend::new();
        backend.script(
            "child-a",
            ScriptedOutcome::Immediate(EffectTerminal::Succeeded),
        );
        backend.script(
            "child-b",
            ScriptedOutcome::Immediate(EffectTerminal::Failed),
        );
        let parent = Uuid7::mint(9, 9);
        let mut a = intent_with_key("child-a", 1);
        a.parent_effect_intent_id = Some(parent);
        let mut b = intent_with_key("child-b", 2);
        b.parent_effect_intent_id = Some(parent);
        backend.dispatch(&a).unwrap();
        backend.dispatch(&b).unwrap();
        assert_eq!(backend.distinct_keys(), vec!["child-a", "child-b"]);
        let ids: Vec<Uuid7> = backend
            .dispatches()
            .iter()
            .map(|d| d.effect_intent_id)
            .collect();
        assert_eq!(ids, vec![a.effect_intent_id, b.effect_intent_id]);
        assert_eq!(
            backend.observe("child-a"),
            Observation::Terminal(EffectTerminal::Succeeded)
        );
        assert_eq!(
            backend.observe("child-b"),
            Observation::Terminal(EffectTerminal::Failed)
        );
    }
}
