use serde_json::{Value, json};
use yah_kernel::funnel::Funnel;
use yah_kernel::ids::{AuthorityEpoch, Digest, Uuid7};
use yah_kernel::protocol::{
    BoundedU32, Command, CommandBody, DEFAULT_DURABLE_QUEUE_CAPACITY, DecimalU64, ErrorKind,
    ExpectedVersion, InProcessAdapter, ReceiptOutcome, Rfc3339Timestamp, Scope, ScopeKind,
    ServerMessage, SubscriptionPoll, Target, request_digest,
};
use yah_kernel::store::Store;

#[path = "protocol_adapter/receipt.rs"]
mod receipt;
#[path = "protocol_adapter/subscription.rs"]
mod subscription;
#[path = "protocol_adapter/token.rs"]
mod token;
#[path = "protocol_adapter/wire.rs"]
mod wire;

#[derive(Clone, Copy)]
enum CommandType {
    RunOpen,
    WorkItemCreate,
    UnitAdmit,
    UnitDispatch,
    ProgressReport,
}

fn adapter() -> (tempfile::TempDir, InProcessAdapter) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::create_project(dir.path(), "adapter-a", "p-1").unwrap();
    (
        dir,
        InProcessAdapter::new(Funnel::new(store, 1_000).unwrap(), "p-1").unwrap(),
    )
}

fn command(
    seq: u128,
    kind: CommandType,
    aggregate_kind: &str,
    aggregate_id: &str,
    payload: Value,
    expected: Option<u64>,
) -> Command {
    let mut command = Command {
        protocol_version: BoundedU32::new(1),
        command_id: Uuid7::mint(1, seq).to_string(),
        scope: scope_for(kind, aggregate_id),
        payload_schema_version: BoundedU32::new(1),
        target: Target {
            aggregate_kind: aggregate_kind.into(),
            aggregate_id: aggregate_id.into(),
        },
        expected_versions: expected
            .map(|version| {
                vec![ExpectedVersion {
                    aggregate_kind: aggregate_kind.into(),
                    aggregate_id: aggregate_id.into(),
                    version: DecimalU64::new(version),
                }]
            })
            .unwrap_or_default(),
        attempt_token: None,
        authority_epoch: Some(DecimalU64::new(AuthorityEpoch(1).0)),
        deadline: None,
        causation_id: None,
        correlation_id: None,
        trace_context: None,
        body: command_body(kind, payload),
        request_digest: Digest::of_bytes(b"placeholder").to_string(),
    };
    command.request_digest = request_digest(&command).unwrap().to_string();
    command
}

fn scope_for(kind: CommandType, aggregate_id: &str) -> Scope {
    match kind {
        CommandType::RunOpen => Scope {
            scope_kind: ScopeKind::Run,
            scope_id: aggregate_id.into(),
        },
        CommandType::WorkItemCreate => Scope {
            scope_kind: ScopeKind::Project,
            scope_id: "p-1".into(),
        },
        _ => Scope {
            scope_kind: ScopeKind::Unit,
            scope_id: aggregate_id.into(),
        },
    }
}

fn command_body(kind: CommandType, payload: Value) -> CommandBody {
    let command_type = match kind {
        CommandType::RunOpen => "run.open",
        CommandType::WorkItemCreate => "work_item.create",
        CommandType::UnitAdmit => "unit.admit",
        CommandType::UnitDispatch => "unit.dispatch",
        CommandType::ProgressReport => "unit.progress_report",
    };
    serde_json::from_value(json!({ "command_type": command_type, "payload": payload })).unwrap()
}

fn append_events(adapter: &InProcessAdapter, count: u128) {
    for index in 1..=count {
        let run_id = format!("run-{index}");
        let open = command(
            index,
            CommandType::RunOpen,
            "run",
            &run_id,
            json!({ "run_id": run_id, "goal_work_item_id": "wi-goal" }),
            None,
        );
        assert_eq!(
            adapter.submit(&open).unwrap().outcome,
            ReceiptOutcome::Completed
        );
    }
}

#[test]
fn forced_round_trip_submits_replays_and_resumes_durable_events() {
    let (_dir, adapter) = adapter();
    let open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );

    let first = adapter.submit(&open).unwrap();
    assert_eq!(first.outcome, ReceiptOutcome::Completed);
    assert_eq!(first.event_cursors, [DecimalU64::new(1)]);
    assert_eq!(
        adapter.submit(&open).unwrap().outcome,
        ReceiptOutcome::Replayed
    );

    let events = adapter.resume(0).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_kind, "run.opened");
    assert_eq!(events[0].cursor, DecimalU64::new(1));
    assert_eq!(events[0].payload["goal_work_item_id"], "wi-goal");
}

#[test]
fn digest_mismatch_rejects_before_the_funnel() {
    let (_dir, adapter) = adapter();
    let mut open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    let CommandBody::RunOpen(payload) = &mut open.body else {
        panic!("run-open payload")
    };
    payload.goal_work_item_id = "changed".into();

    let receipt = adapter.submit(&open).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    assert!(adapter.resume(0).unwrap().is_empty());
}

#[test]
fn digest_mismatch_cannot_replay_a_completed_receipt() {
    let (_dir, adapter) = adapter();
    let open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    assert_eq!(
        adapter.submit(&open).unwrap().outcome,
        ReceiptOutcome::Completed
    );

    let mut altered = open.clone();
    let CommandBody::RunOpen(payload) = &mut altered.body else {
        unreachable!()
    };
    payload.goal_work_item_id = "changed".into();
    let rejected = adapter.submit(&altered).unwrap();
    assert_eq!(rejected.outcome, ReceiptOutcome::Rejected);
    assert_eq!(rejected.error.unwrap().kind, ErrorKind::InvalidRequest);
    assert!(rejected.result.is_none());

    let mut malformed = open.clone();
    malformed.request_digest = "not-a-digest".into();
    let rejected = adapter.submit(&malformed).unwrap();
    assert_eq!(rejected.outcome, ReceiptOutcome::Rejected);
    assert_eq!(rejected.error.unwrap().kind, ErrorKind::InvalidRequest);
    assert!(rejected.result.is_none());
}

#[test]
fn additive_payload_fields_are_ignored_but_principal_is_rejected() {
    let (_dir, adapter) = adapter();
    let mut additive = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({
            "run_id": "run-1",
            "goal_work_item_id": "wi-goal",
            "future_hint": "ignored"
        }),
        None,
    );
    additive.request_digest = request_digest(&additive).unwrap().to_string();
    assert_eq!(
        adapter.submit(&additive).unwrap().outcome,
        ReceiptOutcome::Completed
    );

    let principal = command(
        2,
        CommandType::RunOpen,
        "run",
        "run-2",
        json!({
            "run_id": "run-2",
            "goal_work_item_id": "wi-goal",
            "principal": "owner"
        }),
        None,
    );
    let receipt = adapter.submit(&principal).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    assert_eq!(
        receipt.error.unwrap().kind,
        yah_kernel::protocol::ErrorKind::InvalidRequest
    );
    assert_eq!(
        adapter.submit(&principal).unwrap().outcome,
        ReceiptOutcome::Rejected
    );
}

#[test]
fn oversized_semantic_event_fails_closed_until_artifact_indirection_exists() {
    let (dir, adapter) = adapter();
    let paths: Vec<String> = (0..1_000)
        .map(|index| format!("src/generated/path-{index:04}.rs"))
        .collect();
    let work_item = command(
        1,
        CommandType::WorkItemCreate,
        "work_item",
        "wi-large",
        json!({
            "work_item_id": "wi-large",
            "acceptance_contract_digest": Digest::of_bytes(b"contract"),
            "declared_write_scope": paths,
        }),
        None,
    );

    let receipt = adapter.submit(&work_item).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    assert_eq!(
        adapter.submit(&work_item).unwrap().outcome,
        ReceiptOutcome::Rejected
    );
    assert_eq!(receipt.error.unwrap().kind, ErrorKind::PayloadTooLarge);
    let replay = adapter.submit(&work_item).unwrap();
    assert_eq!(replay.outcome, ReceiptOutcome::Rejected);
    assert_eq!(replay.error.unwrap().kind, ErrorKind::PayloadTooLarge);
    assert!(adapter.resume(0).unwrap().is_empty());
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    let mut current = work_item;
    current.authority_epoch = Some(DecimalU64::new(2));
    let replay = adapter.submit(&current).unwrap();
    assert_eq!(replay.outcome, ReceiptOutcome::Rejected);
    assert_eq!(replay.error.unwrap().kind, ErrorKind::PayloadTooLarge);
}

#[test]
fn authority_command_carrying_a_token_is_an_invalid_envelope() {
    let (_dir, adapter) = adapter();
    let mut open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    open.attempt_token = Some("unknown".into());

    let receipt = adapter.submit(&open).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    assert_eq!(
        receipt.error.unwrap().kind,
        yah_kernel::protocol::ErrorKind::InvalidRequest
    );
}

#[test]
fn authority_envelope_rejection_replays_after_recovery() {
    let (dir, adapter) = adapter();
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
    assert_eq!(
        adapter.submit(&open).unwrap().outcome,
        ReceiptOutcome::Rejected
    );
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    open.authority_epoch = Some(DecimalU64::new(2));
    let replay = adapter.submit(&open).unwrap();
    assert_eq!(replay.outcome, ReceiptOutcome::Rejected);
    assert_eq!(replay.error.unwrap().kind, ErrorKind::InvalidRequest);
}

#[test]
fn stale_authority_epoch_does_not_replay_or_reserve_a_receipt() {
    let (dir, adapter) = adapter();
    let open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    let stale = adapter.submit(&open).unwrap();
    assert_eq!(stale.outcome, ReceiptOutcome::Rejected);
    assert_eq!(stale.error.unwrap().kind, ErrorKind::FenceRejected);

    let mut current = open;
    current.authority_epoch = Some(DecimalU64::new(2));
    assert_eq!(
        adapter.submit(&current).unwrap().outcome,
        ReceiptOutcome::Completed
    );
}

#[test]
fn canonical_deadline_round_trips_as_observational_intent() {
    let (_dir, adapter) = adapter();
    let mut open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    open.deadline = Some(Rfc3339Timestamp::new("2026-08-07T12:00:00.000Z".into()).unwrap());

    let receipt = adapter.submit(&open).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Completed);
}

#[test]
fn future_cursor_is_rejected_instead_of_silently_starving() {
    let (_dir, adapter) = adapter();
    let request = serde_json::to_vec(&yah_kernel::protocol::ClientMessage::Subscribe {
        project_id: "p-1".into(),
        after_cursor: DecimalU64::new(1),
    })
    .unwrap();
    let response: yah_kernel::protocol::ServerMessage =
        serde_json::from_slice(&adapter.handle_json(&request)).unwrap();
    let yah_kernel::protocol::ServerMessage::Error(error) = response else {
        panic!("future cursor must be rejected")
    };
    assert_eq!(error.kind, yah_kernel::protocol::ErrorKind::InvalidRequest);
}

#[test]
fn command_scope_must_match_the_control_graph_project() {
    let (_dir, adapter) = adapter();
    let mut open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    open.scope.scope_id = "p-2".into();
    open.scope.scope_kind = ScopeKind::Project;

    let receipt = adapter.submit(&open).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    assert!(adapter.resume(0).unwrap().is_empty());
}

#[test]
fn work_item_create_rejects_a_non_project_receipt_scope() {
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
    work_item.scope = Scope {
        scope_kind: ScopeKind::Run,
        scope_id: "run-false".into(),
    };
    let receipt = adapter.submit(&work_item).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    assert_eq!(
        adapter
            .get_receipt(work_item.scope.clone(), work_item.command_id.clone())
            .unwrap()
            .outcome,
        ReceiptOutcome::Rejected
    );
    drop(adapter);

    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
    assert_eq!(
        adapter
            .get_receipt(work_item.scope, work_item.command_id)
            .unwrap()
            .outcome,
        ReceiptOutcome::Rejected
    );
}

#[test]
fn same_command_id_in_different_scope_cannot_claim_another_receipt_cursors() {
    let (_dir, adapter) = adapter();
    let first = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    assert_eq!(
        adapter.submit(&first).unwrap().event_cursors,
        [DecimalU64::new(1)]
    );

    let mut second = command(
        2,
        CommandType::RunOpen,
        "run",
        "run-2",
        json!({ "run_id": "run-2", "goal_work_item_id": "wi-goal" }),
        None,
    );
    second.command_id = first.command_id.clone();
    second.request_digest = request_digest(&second).unwrap().to_string();
    assert_eq!(
        adapter.submit(&second).unwrap().event_cursors,
        [DecimalU64::new(2)]
    );
    assert_eq!(
        adapter.submit(&first).unwrap().event_cursors,
        [DecimalU64::new(1)]
    );

    let mut conflict = first.clone();
    let CommandBody::RunOpen(payload) = &mut conflict.body else {
        unreachable!()
    };
    payload.goal_work_item_id = "different".into();
    conflict.request_digest = request_digest(&conflict).unwrap().to_string();
    let conflict = adapter.submit(&conflict).unwrap();
    assert_eq!(conflict.outcome, ReceiptOutcome::Rejected);
    assert_eq!(conflict.error.unwrap().kind, ErrorKind::IdempotencyConflict);
    assert!(conflict.event_cursors.is_empty());
}

#[test]
fn progress_extensions_are_rejected_before_the_semantic_journal() {
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
    ] {
        assert_eq!(
            adapter.submit(&command).unwrap().outcome,
            ReceiptOutcome::Completed
        );
    }
    let dispatch = command(
        4,
        CommandType::UnitDispatch,
        "unit",
        "u-1",
        json!({ "unit_id": "u-1", "holder_id": "holder-1" }),
        Some(1),
    );
    let token = adapter.submit(&dispatch).unwrap().result.unwrap()["attempt_token"]
        .as_str()
        .unwrap()
        .to_owned();
    let dispatch_event = adapter.resume(3).unwrap().remove(0);
    assert_eq!(dispatch_event.payload["attempt_epoch"], "1");
    let progress = command(
        5,
        CommandType::ProgressReport,
        "unit",
        "u-1",
        json!({ "unit_id": "u-1" }),
        Some(2),
    );
    let mut value = serde_json::to_value(yah_kernel::protocol::ClientMessage::Command(Box::new(
        progress,
    )))
    .unwrap();
    let message = value["message"].as_object_mut().unwrap();
    message.insert("attempt_token".into(), Value::String(token));
    message["payload"]["note"] = Value::String("x".repeat(17 * 1024));
    let response: ServerMessage =
        serde_json::from_slice(&adapter.handle_json(&serde_json::to_vec(&value).unwrap())).unwrap();
    let ServerMessage::Error(error) = response else {
        panic!("progress extension must fail during typed decode")
    };
    assert_eq!(error.kind, ErrorKind::InvalidRequest);
    assert_eq!(adapter.resume(0).unwrap().len(), 4);
}

#[test]
fn decimal_u64_rejects_noncanonical_and_out_of_range_strings() {
    assert_eq!(
        serde_json::from_str::<DecimalU64>(r#""0""#).unwrap(),
        DecimalU64::new(0)
    );
    assert_eq!(
        serde_json::from_str::<DecimalU64>(&format!(r#""{}""#, u64::MAX)).unwrap(),
        DecimalU64::new(u64::MAX)
    );
    for invalid in [r#""01""#, r#""-1""#, r#""18446744073709551616""#] {
        assert!(serde_json::from_str::<DecimalU64>(invalid).is_err());
    }
}

#[test]
fn checked_in_schema_and_typescript_match_rust_types() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    yah_kernel::protocol::generate::check_checked_in(&root).unwrap();
}

#[test]
fn opaque_holder_token_is_discarded_on_authority_takeover() {
    let (dir, adapter) = adapter();
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
        assert_eq!(
            adapter.submit(&command).unwrap().outcome,
            ReceiptOutcome::Completed
        );
    }
    let dispatch = command(
        4,
        CommandType::UnitDispatch,
        "unit",
        "u-1",
        json!({ "unit_id": "u-1", "holder_id": "holder-1" }),
        Some(1),
    );
    let receipt = adapter.submit(&dispatch).unwrap();
    let result = receipt.result.unwrap();
    assert!(!result.contains_key("token_nonce"));
    let token = result["attempt_token"].as_str().unwrap().to_owned();
    assert_eq!(token.len(), 64);

    drop(adapter);
    let recovered = Store::recover(dir.path(), "adapter-b").unwrap();
    let adapter = InProcessAdapter::new(Funnel::new(recovered, 2_000).unwrap(), "p-1").unwrap();
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

    let receipt = adapter.submit(&progress).unwrap();
    assert_eq!(receipt.outcome, ReceiptOutcome::Rejected);
    let error = receipt.error.unwrap();
    assert_eq!(error.kind, yah_kernel::protocol::ErrorKind::FenceRejected);
    assert!(error.message.contains("stale or unresolvable"));
}
