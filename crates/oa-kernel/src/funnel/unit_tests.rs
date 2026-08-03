//! Funnel unit tests needing struct-private access (poison latch, mint
//! discipline).
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
        principal_kind: PrincipalKind::Daemon,
        expected_version: None,
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
