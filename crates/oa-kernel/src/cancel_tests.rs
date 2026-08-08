//! Unit coverage for the §5 rules that are pure functions of a scope. The
//! funnel-side protocol (durable-before-signal, rule 4's admission gate,
//! §5.3's reconciling path) is proven against the store in the integration
//! suite, not here.

use super::*;

/// Ids are opaque strings at this boundary; minting them through `Uuid7`
/// keeps the fixtures in the shape the funnel actually sees.
fn id(n: u64) -> String {
    Uuid7::mint(1_700_000_000_000, n as u128).to_string()
}

fn member(kind: CancelKind, n: u64) -> MemberInput {
    MemberInput {
        member_kind: kind,
        member_id: id(n),
        attachment: Attachment::Attached,
    }
}

#[test]
fn freeze_orders_leaf_first() {
    let root = id(1);
    let scope = CancelScope::freeze(
        CancelKind::Run,
        root.clone(),
        vec![
            MemberInput {
                member_kind: CancelKind::Run,
                member_id: root.clone(),
                attachment: Attachment::Attached,
            },
            member(CancelKind::EffectIntent, 40),
            member(CancelKind::ExecutionUnit, 20),
            member(CancelKind::Attempt, 30),
        ],
    )
    .expect("scope freezes");

    let kinds: Vec<CancelKind> = scope.members().iter().map(|m| m.member_kind).collect();
    assert_eq!(
        kinds,
        vec![
            CancelKind::EffectIntent,
            CancelKind::Attempt,
            CancelKind::ExecutionUnit,
            CancelKind::Run,
        ],
        "delivery must reach leaves before the parents that own them (§5.2 rule 3)"
    );
    let indices: Vec<u32> = scope.members().iter().map(|m| m.order_index).collect();
    assert_eq!(indices, vec![0, 1, 2, 3], "order_index is dense from 0");
}

#[test]
fn freeze_is_order_independent() {
    let root = id(1);
    let forward = vec![
        member(CancelKind::EffectIntent, 40),
        member(CancelKind::EffectIntent, 41),
        member(CancelKind::Attempt, 30),
    ];
    let mut reversed = forward.clone();
    reversed.reverse();

    let a = CancelScope::freeze(CancelKind::Run, root.clone(), forward).expect("freezes");
    let b = CancelScope::freeze(CancelKind::Run, root.clone(), reversed).expect("freezes");
    assert_eq!(
        a, b,
        "a frozen scope must be replayable from the journal regardless of \
         the order the caller happened to enumerate members in"
    );
}

#[test]
fn canonical_member_lookup_crosses_kind_partitions() {
    let root = id(1);
    let scope = CancelScope::freeze(
        CancelKind::Run,
        root.clone(),
        vec![
            member(CancelKind::EffectIntent, 40),
            member(CancelKind::Attempt, 30),
            member(CancelKind::ExecutionUnit, 20),
            MemberInput {
                member_kind: CancelKind::Run,
                member_id: root.clone(),
                attachment: Attachment::Attached,
            },
        ],
    )
    .expect("scope freezes");

    for (member_id, kind) in [
        (id(40), CancelKind::EffectIntent),
        (id(30), CancelKind::Attempt),
        (id(20), CancelKind::ExecutionUnit),
        (root, CancelKind::Run),
    ] {
        assert_eq!(scope.member(&member_id).unwrap().member_kind, kind);
    }
    assert!(scope.member(&id(99)).is_none());
}

#[test]
fn freeze_rejects_a_duplicate_member() {
    let root = id(1);
    let dup = member(CancelKind::Attempt, 30);
    let err = CancelScope::freeze(CancelKind::Run, root, vec![dup.clone(), dup])
        .expect_err("duplicate member is rejected");
    assert_eq!(err, ScopeError::DuplicateMember { member_id: id(30) });
}

#[test]
fn freeze_rejects_a_member_the_root_cannot_own() {
    // An attempt root cannot own a unit: that is a sibling subtree, and
    // freezing it would claim authority the request does not have.
    let err = CancelScope::freeze(
        CancelKind::Attempt,
        id(1),
        vec![member(CancelKind::ExecutionUnit, 20)],
    )
    .expect_err("out-of-subtree member is rejected");
    assert_eq!(
        err,
        ScopeError::NotUnderRoot {
            member_kind: CancelKind::ExecutionUnit,
            member_id: id(20),
        }
    );
}

#[test]
fn freeze_rejects_an_oversized_scope() {
    let proposed: Vec<MemberInput> = (0..=MAX_SCOPE_MEMBERS as u64)
        .map(|n| member(CancelKind::EffectIntent, n + 1000))
        .collect();
    let err =
        CancelScope::freeze(CancelKind::Run, id(1), proposed).expect_err("oversized is rejected");
    assert_eq!(
        err,
        ScopeError::TooLarge {
            count: MAX_SCOPE_MEMBERS + 1
        },
        "an oversized scope is refused whole; truncating it would strand the \
         dropped members in exactly the unrecorded state obligation 4 forbids"
    );
}

#[test]
fn a_kind_does_not_contain_itself() {
    assert!(!CancelKind::ExecutionUnit.contains(CancelKind::ExecutionUnit));
    assert!(CancelKind::Run.contains(CancelKind::EffectIntent));
    assert!(!CancelKind::EffectIntent.contains(CancelKind::Run));
}

fn request(policy: CancelPolicy, scope: CancelScope) -> CancelRequest {
    CancelRequest {
        // The request id stays a minted Uuid7 — it is kernel-minted, unlike
        // the member ids the caller supplies.
        cancel_request_id: Uuid7::mint(1_700_000_000_000, 9),
        reason: CancelReason::OwnerRequest,
        policy,
        scope,
        status: CancelStatus::Requested,
        requested_at: 1_700_000_000_000,
        requested_by_kind: "owner".to_owned(),
    }
}

#[test]
fn root_only_does_not_reach_descendants() {
    let root = id(1);
    let scope = CancelScope::freeze(
        CancelKind::ExecutionUnit,
        root.clone(),
        vec![
            MemberInput {
                member_kind: CancelKind::ExecutionUnit,
                member_id: root.clone(),
                attachment: Attachment::Attached,
            },
            member(CancelKind::Attempt, 30),
        ],
    )
    .expect("freezes");

    let req = request(CancelPolicy::RootOnly, scope.clone());
    assert!(req.governs(&root));
    assert!(
        !req.governs(&id(30)),
        "the frozen scope records the tree; the policy decides who inherits"
    );

    let cascade = request(CancelPolicy::AttachedCascade, scope);
    assert!(cascade.governs(&id(30)));
}

#[test]
fn cascade_does_not_reach_a_detached_member() {
    let root = id(1);
    let scope = CancelScope::freeze(
        CancelKind::ExecutionUnit,
        root.clone(),
        vec![MemberInput {
            member_kind: CancelKind::Attempt,
            member_id: id(30),
            attachment: Attachment::Detached,
        }],
    )
    .expect("freezes");
    let req = request(CancelPolicy::AttachedCascade, scope);
    assert!(
        !req.governs(&id(30)),
        "detached actions carry their own owner and cancellation policy (§5.2 rule 5)"
    );
}

#[test]
fn an_unresponsive_member_does_not_discharge() {
    assert!(!DeliveryOutcome::Unresponsive.is_discharged());
    for outcome in [
        DeliveryOutcome::ObservedStopped,
        DeliveryOutcome::AlreadyTerminal,
        DeliveryOutcome::DetachedDeclined,
    ] {
        assert!(outcome.is_discharged(), "{outcome:?} should discharge");
    }
}
