//! Trial-plan derivation (EXP-001 §9's arithmetic, §10's replay obligation).

use crate::schema::{Arm, Cell};

/// Per-trial seeds derive from the root seed and the trial's identity, so one
/// failing trial replays alone without re-running its predecessors.
pub fn trial_seed(root: u64, arm: &Arm, cell: Cell, rep: u32) -> u64 {
    let key = format!("{root}/{}/{:?}/{rep}", arm.label(), cell);
    let hash = blake3::hash(key.as_bytes());
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::KillPoint;

    #[test]
    fn trial_seeds_are_stable_and_distinct() {
        let arm = Arm::ALL[0];
        let a = trial_seed(7, &arm, Cell::Kill(KillPoint::PreSeal), 0);
        let b = trial_seed(7, &arm, Cell::Kill(KillPoint::PreSeal), 0);
        let c = trial_seed(7, &arm, Cell::Kill(KillPoint::PreSeal), 1);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
