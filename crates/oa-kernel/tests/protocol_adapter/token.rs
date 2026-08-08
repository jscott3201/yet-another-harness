use super::*;

#[test]
fn foreign_unit_token_does_not_reserve_the_target_receipt_key() {
    let (_dir, adapter) = adapter();
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
        command(
            4,
            CommandType::UnitAdmit,
            "unit",
            "u-2",
            json!({ "unit_id": "u-2", "work_item_id": "wi-1", "run_id": "run-1" }),
            None,
        ),
    ] {
        assert_eq!(
            adapter.submit(&command).unwrap().outcome,
            ReceiptOutcome::Completed
        );
    }
    let token = |seq, unit_id, holder_id| {
        adapter
            .submit(&command(
                seq,
                CommandType::UnitDispatch,
                "unit",
                unit_id,
                json!({ "unit_id": unit_id, "holder_id": holder_id }),
                Some(1),
            ))
            .unwrap()
            .result
            .unwrap()["attempt_token"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let token_1 = token(5, "u-1", "holder-1");
    let token_2 = token(6, "u-2", "holder-2");
    let mut progress = command(
        7,
        CommandType::ProgressReport,
        "unit",
        "u-2",
        json!({ "unit_id": "u-2" }),
        Some(2),
    );
    progress.attempt_token = Some(token_1);
    assert_eq!(
        adapter.submit(&progress).unwrap().error.unwrap().kind,
        ErrorKind::FenceRejected
    );

    progress.attempt_token = Some(token_2);
    assert_eq!(
        adapter.submit(&progress).unwrap().outcome,
        ReceiptOutcome::Completed
    );
}

#[test]
fn malformed_foreign_unit_envelope_does_not_reserve_the_target_key() {
    let (_dir, adapter) = adapter();
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
        command(
            4,
            CommandType::UnitAdmit,
            "unit",
            "u-2",
            json!({ "unit_id": "u-2", "work_item_id": "wi-1", "run_id": "run-1" }),
            None,
        ),
    ] {
        assert_eq!(
            adapter.submit(&command).unwrap().outcome,
            ReceiptOutcome::Completed
        );
    }
    let token = |seq, unit_id, holder_id| {
        adapter
            .submit(&command(
                seq,
                CommandType::UnitDispatch,
                "unit",
                unit_id,
                json!({ "unit_id": unit_id, "holder_id": holder_id }),
                Some(1),
            ))
            .unwrap()
            .result
            .unwrap()["attempt_token"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let token_1 = token(5, "u-1", "holder-1");
    let token_2 = token(6, "u-2", "holder-2");
    let mut malformed = command(
        7,
        CommandType::ProgressReport,
        "unit",
        "u-1",
        json!({ "unit_id": "u-1" }),
        Some(2),
    );
    malformed.scope.scope_id = "u-2".into();
    malformed.protocol_version = BoundedU32::new(2);
    malformed.attempt_token = Some(token_1);
    assert_eq!(
        adapter.submit(&malformed).unwrap().error.unwrap().kind,
        ErrorKind::FenceRejected
    );

    let mut progress = command(
        7,
        CommandType::ProgressReport,
        "unit",
        "u-2",
        json!({ "unit_id": "u-2" }),
        Some(2),
    );
    progress.attempt_token = Some(token_2);
    assert_eq!(
        adapter.submit(&progress).unwrap().outcome,
        ReceiptOutcome::Completed
    );
}
