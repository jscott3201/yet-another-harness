use super::*;

#[test]
fn request_digest_has_an_independent_canonical_vector() {
    let command = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    assert_eq!(
        request_digest(&command).unwrap().to_string(),
        "blake3:000fa9399587ac96b172410d84e0112dbe37e7a1f908fe4b01efbc4ca33e79e7"
    );
    let mut observational = command.clone();
    observational.deadline =
        Some(Rfc3339Timestamp::new("2026-08-07T12:00:00.000Z".into()).unwrap());
    observational.trace_context = Some("00-observational".into());
    assert_eq!(
        request_digest(&observational).unwrap(),
        request_digest(&command).unwrap()
    );
}

#[test]
fn raw_json_rejects_duplicate_names_and_unsafe_integers() {
    let (_dir, adapter) = adapter();
    for bytes in [
        br#"{"kind":"resume","kind":"resume","message":{"project_id":"p-1","after_cursor":"0"}}"#.as_slice(),
        br#"{"kind":"resume","message":{"project_id":"p-1","project_id":"other","after_cursor":"0"}}"#.as_slice(),
        br#"{"kind":"get_receipt","message":{"project_id":"p-1","project_id":"other","scope":{"scope_kind":"run","scope_id":"run-1"},"command_id":"command-1"}}"#.as_slice(),
        br#"{"kind":"get_receipt","message":{"project_id":"p-1","scope":{"scope_kind":"run","scope_id":"run-1","scope_id":"run-2"},"command_id":"command-1"}}"#.as_slice(),
        br#"{"kind":"command","message":{"protocol_version":1,"command_id":"c-1","scope":{"scope_kind":"run","scope_id":"run-1"},"payload_schema_version":1,"target":{"aggregate_kind":"run","aggregate_id":"run-1"},"expected_versions":[],"authority_epoch":"1","command_type":"run.open","payload":{"run_id":"run-1","goal_work_item_id":"wi-1","future_number":9007199254740992},"request_digest":"blake3:0000000000000000000000000000000000000000000000000000000000000000"}}"#.as_slice(),
        br#"{"kind":"command","message":{"protocol_version":1,"command_id":"c-1","scope":{"scope_kind":"run","scope_id":"run-1"},"payload_schema_version":1,"target":{"aggregate_kind":"run","aggregate_id":"run-1"},"expected_versions":[],"authority_epoch":"1","command_type":"run.open","payload":{"run_id":"run-1","goal_work_item_id":"wi-1","future_number":9007199254740992.0},"request_digest":"blake3:0000000000000000000000000000000000000000000000000000000000000000"}}"#.as_slice(),
        br#"{"kind":"command","message":{"protocol_version":1,"command_id":"c-1","scope":{"scope_kind":"run","scope_id":"run-1"},"payload_schema_version":1,"target":{"aggregate_kind":"run","aggregate_id":"run-1"},"expected_versions":[],"authority_epoch":"1","command_type":"run.open","payload":{"run_id":"run-1","goal_work_item_id":"wi-1","future_number":-9.007199254740992e15},"request_digest":"blake3:0000000000000000000000000000000000000000000000000000000000000000"}}"#.as_slice(),
    ] {
        let response: ServerMessage = serde_json::from_slice(&adapter.handle_json(bytes)).unwrap();
        let ServerMessage::Error(error) = response else {
            panic!("invalid raw JSON must return a protocol error")
        };
        assert_eq!(error.kind, ErrorKind::InvalidRequest);
    }
}

#[test]
fn raw_payload_limit_counts_encoded_bytes() {
    let (_dir, adapter) = adapter();
    let open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    let mut value =
        serde_json::to_value(oa_kernel::protocol::ClientMessage::Command(Box::new(open))).unwrap();
    value["message"]["payload"]["padding"] = Value::String("x".repeat(256 * 1024));
    let response: ServerMessage =
        serde_json::from_slice(&adapter.handle_json(&serde_json::to_vec_pretty(&value).unwrap()))
            .unwrap();
    let ServerMessage::Error(error) = response else {
        panic!("raw oversized payload must fail before typed decode")
    };
    assert_eq!(error.kind, ErrorKind::PayloadTooLarge);
}

#[test]
fn raw_payload_and_command_limits_count_escape_bytes() {
    let (_dir, adapter) = adapter();
    let open = command(
        1,
        CommandType::RunOpen,
        "run",
        "run-1",
        json!({ "run_id": "run-1", "goal_work_item_id": "wi-goal" }),
        None,
    );
    let mut value = serde_json::to_value(&open).unwrap();
    value["payload"]["padding"] = Value::String("PAYLOAD_SENTINEL".into());
    let command_json = serde_json::to_string(&value).unwrap().replace(
        "\"PAYLOAD_SENTINEL\"",
        &format!("\"{}\"", "\\u0061".repeat(44_000)),
    );
    let frame = format!(r#"{{"kind":"command","message":{command_json}}}"#);
    let response: ServerMessage =
        serde_json::from_slice(&adapter.handle_json(frame.as_bytes())).unwrap();
    let ServerMessage::Error(error) = response else {
        panic!("escape-expanded raw payload must be rejected")
    };
    assert_eq!(error.kind, ErrorKind::PayloadTooLarge);

    let mut value = serde_json::to_value(&open).unwrap();
    value["padding"] = Value::String("COMMAND_SENTINEL".into());
    let command_json = serde_json::to_string(&value).unwrap().replace(
        "\"COMMAND_SENTINEL\"",
        &format!("\"{}\"", "\\u0061".repeat(175_000)),
    );
    let frame = format!(r#"{{"kind":"command","message":{command_json}}}"#);
    let response: ServerMessage =
        serde_json::from_slice(&adapter.handle_json(frame.as_bytes())).unwrap();
    let ServerMessage::Error(error) = response else {
        panic!("escape-expanded raw command must be rejected")
    };
    assert_eq!(error.kind, ErrorKind::PayloadTooLarge);
}

#[test]
fn non_command_frames_have_a_preparse_size_limit() {
    let (_dir, adapter) = adapter();
    let frame = format!(
        r#"{{"kind":"resume","message":{{"project_id":"p-1","after_cursor":"0","padding":"{}"}}}}"#,
        "x".repeat(oa_kernel::protocol::MAX_CONTROL_FRAME_BYTES)
    );
    let response: ServerMessage =
        serde_json::from_slice(&adapter.handle_json(frame.as_bytes())).unwrap();
    let ServerMessage::Error(error) = response else {
        panic!("oversized control frame must be rejected")
    };
    assert_eq!(error.kind, ErrorKind::PayloadTooLarge);
}
