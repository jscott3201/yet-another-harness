use super::*;
use oa_kernel::protocol::ClientMessage;

#[test]
fn lookup_is_read_only_completed_while_submission_replays() {
    let (_dir, adapter) = adapter();
    let open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    let completed = adapter.submit(&open).unwrap();
    let event_count = adapter.resume(0).unwrap().len();

    let looked_up = adapter
        .get_receipt(open.scope.clone(), open.command_id.clone())
        .unwrap();
    assert_eq!(looked_up.outcome, ReceiptOutcome::Completed);
    assert_eq!(looked_up.state_version, completed.state_version);
    assert_eq!(looked_up.event_cursors, completed.event_cursors);
    assert_eq!(looked_up.result, completed.result);
    assert_eq!(adapter.resume(0).unwrap().len(), event_count);
    assert_eq!(
        adapter.submit(&open).unwrap().outcome,
        ReceiptOutcome::Replayed
    );
}

#[test]
fn lookup_addresses_the_full_scoped_idempotency_key() {
    let (_dir, adapter) = adapter();
    let first = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-one" }),
        None,
    );
    let mut second = command(
        2,
        CommandType::RunOpen,
        "run",
        "run-2",
        json!({ "run_id": "run-2", "goal_work_item_id": "wi-two" }),
        None,
    );
    second.command_id = first.command_id.clone();
    second.request_digest = request_digest(&second).unwrap().to_string();
    adapter.submit(&first).unwrap();
    adapter.submit(&second).unwrap();

    let first_receipt = adapter
        .get_receipt(first.scope.clone(), first.command_id.clone())
        .unwrap();
    let second_receipt = adapter
        .get_receipt(second.scope.clone(), second.command_id.clone())
        .unwrap();
    assert_eq!(first_receipt.event_cursors, [DecimalU64::new(1)]);
    assert_eq!(second_receipt.event_cursors, [DecimalU64::new(2)]);
    assert_eq!(first_receipt.result.unwrap()["run_id"], "run-1");
    assert_eq!(second_receipt.result.unwrap()["run_id"], "run-2");
}

#[test]
fn lookup_projects_persisted_rejections_and_missing_receipts() {
    let (_dir, adapter) = adapter();
    let mut open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    open.protocol_version = BoundedU32::new(2);
    open.request_digest = request_digest(&open).unwrap().to_string();
    let rejected = adapter.submit(&open).unwrap();
    let looked_up = adapter
        .get_receipt(open.scope.clone(), open.command_id.clone())
        .unwrap();
    assert_eq!(looked_up.outcome, ReceiptOutcome::Rejected);
    assert_eq!(looked_up.error.unwrap().kind, rejected.error.unwrap().kind);

    let error = adapter
        .get_receipt(
            Scope {
                scope_kind: ScopeKind::Run,
                scope_id: "missing-run".into(),
            },
            "missing-command",
        )
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::NotFound);
}

#[test]
fn raw_lookup_validates_project_and_identifiers() {
    let (_dir, adapter) = adapter();
    for message in [
        ClientMessage::GetReceipt {
            project_id: "other".into(),
            scope: Scope {
                scope_kind: ScopeKind::Run,
                scope_id: "run-1".into(),
            },
            command_id: "command-1".into(),
        },
        ClientMessage::GetReceipt {
            project_id: "p-1".into(),
            scope: Scope {
                scope_kind: ScopeKind::Run,
                scope_id: "bad/id".into(),
            },
            command_id: "command-1".into(),
        },
    ] {
        let response: ServerMessage =
            serde_json::from_slice(&adapter.handle_json(&serde_json::to_vec(&message).unwrap()))
                .unwrap();
        let ServerMessage::Error(error) = response else {
            panic!("invalid receipt lookup must return an error")
        };
        assert_eq!(error.kind, ErrorKind::InvalidRequest);
    }
}

#[test]
fn malformed_command_address_is_rejected_without_persisting_a_receipt() {
    let (dir, adapter) = adapter();
    let mut open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    open.command_id = "bad/id".into();
    open.request_digest = request_digest(&open).unwrap().to_string();
    let rejected = adapter.submit(&open).unwrap();
    assert_eq!(rejected.error.unwrap().kind, ErrorKind::InvalidRequest);
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    let missing = adapter
        .get_receipt(
            Scope {
                scope_kind: ScopeKind::Run,
                scope_id: "run-1".into(),
            },
            "bad-id",
        )
        .unwrap_err();
    assert_eq!(missing.kind, ErrorKind::NotFound);
}

#[test]
fn foreign_project_rejection_does_not_poison_recovery() {
    let (dir, adapter) = adapter();
    let mut work_item = command(
        1,
        CommandType::WorkItemCreate,
        "work_item",
        "wi-1",
        json!({
            "work_item_id": "wi-1",
            "acceptance_contract_digest": Digest::of_bytes(b"contract"),
            "declared_write_scope": ["src/"]
        }),
        None,
    );
    work_item.scope.scope_id = "p-2".into();
    work_item.protocol_version = BoundedU32::new(2);
    work_item.request_digest = request_digest(&work_item).unwrap().to_string();
    assert_eq!(
        adapter.submit(&work_item).unwrap().error.unwrap().kind,
        ErrorKind::InvalidRequest
    );
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    assert_eq!(recovered.project_id(), "p-1");
}

#[test]
fn dispatch_lookup_never_discloses_or_reactivates_credentials() {
    let (dir, adapter) = adapter();
    let dispatch = seed_dispatch(&adapter);
    let submitted = adapter.submit(&dispatch).unwrap();
    let token = submitted.result.unwrap()["attempt_token"]
        .as_str()
        .unwrap()
        .to_owned();

    let lookup = adapter
        .get_receipt(dispatch.scope.clone(), dispatch.command_id.clone())
        .unwrap();
    let result = lookup.result.unwrap();
    assert_eq!(result["unit_id"], "u-1");
    for field in [
        "attempt_token",
        "attempt_epoch",
        "stamp",
        "authority_epoch",
        "holder_id",
        "token_nonce",
    ] {
        assert!(!result.contains_key(field));
    }
    let replay = adapter.submit(&dispatch).unwrap();
    assert_eq!(replay.outcome, ReceiptOutcome::Replayed);
    assert!(replay.result.unwrap().contains_key("attempt_token"));

    drop(adapter);
    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    let lookup = adapter
        .get_receipt(dispatch.scope.clone(), dispatch.command_id.clone())
        .unwrap();
    assert!(!lookup.result.unwrap().contains_key("attempt_token"));

    let mut progress = command(
        5,
        CommandType::ProgressReport,
        "unit",
        "u-1",
        json!({ "unit_id": "u-1" }),
        Some(2),
    );
    progress.authority_epoch = Some(DecimalU64::new(2));
    progress.attempt_token = Some(token);
    progress.request_digest = request_digest(&progress).unwrap().to_string();
    assert_eq!(
        adapter.submit(&progress).unwrap().error.unwrap().kind,
        ErrorKind::FenceRejected
    );
}

fn seed_dispatch(adapter: &InProcessAdapter) -> Command {
    for command in [
        command(
            1,
            CommandType::RunOpen,
            "run",
            "run-1",
            json!({ "run_id": "run-1", "goal_work_item_id": "wi-1" }),
            None,
        ),
        command(
            2,
            CommandType::WorkItemCreate,
            "work_item",
            "wi-1",
            json!({
                "work_item_id": "wi-1",
                "acceptance_contract_digest": Digest::of_bytes(b"contract"),
                "declared_write_scope": ["src/"]
            }),
            None,
        ),
        command(
            3,
            CommandType::UnitAdmit,
            "unit",
            "u-1",
            json!({ "unit_id": "u-1", "work_item_id": "wi-1", "run_id": "run-1" }),
            None,
        ),
    ] {
        adapter.submit(&command).unwrap();
    }
    command(
        4,
        CommandType::UnitDispatch,
        "unit",
        "u-1",
        json!({ "unit_id": "u-1", "holder_id": "holder-1" }),
        Some(1),
    )
}
