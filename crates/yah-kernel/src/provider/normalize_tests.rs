//! Unit tests for the normalizer (child module of `normalize`, split
//! out to honor the per-file LOC cap).

use super::*;

fn feed_all(dialect: Dialect, chunks: &[&str]) -> (Normalizer, Vec<StepOutcome>) {
    let mut n = Normalizer::new(dialect);
    let outcomes = chunks.iter().map(|c| n.feed(c)).collect();
    (n, outcomes)
}

fn defect(outcome: &StepOutcome) -> &StreamDefect {
    match outcome {
        StepOutcome::Defect(d) => d,
        other => panic!("expected defect, got {other:?}"),
    }
}

const A_START: &str = r#"{"type":"message_start","message":{"id":"m","model":"x","usage":{"input_tokens":1,"cache_read_input_tokens":0}}}"#;
const A_STOP: &str = r#"{"type":"message_stop"}"#;

fn a_delta_stop(reason: &str) -> String {
    format!(
        r#"{{"type":"message_delta","delta":{{"stop_reason":"{reason}"}},"usage":{{"output_tokens":1}}}}"#
    )
}

#[test]
fn anthropic_stop_reason_mapping_covers_the_closed_enum() {
    for (wire, want) in [
        ("max_tokens", FinishReason::Length),
        ("stop_sequence", FinishReason::Stop),
        ("refusal", FinishReason::ContentFilter),
    ] {
        let (_, outcomes) = feed_all(
            Dialect::AnthropicMessages,
            &[A_START, &a_delta_stop(wire), A_STOP],
        );
        let StepOutcome::Events(events) = &outcomes[2] else {
            panic!("{wire}: expected events, got {:?}", outcomes[2]);
        };
        assert_eq!(
            events.last(),
            Some(&NormalizedEvent::Finished {
                finish_reason: want
            }),
            "stop_reason {wire}"
        );
    }
    // Unknown member of the closed stop-reason vocabulary fails closed.
    let (_, outcomes) = feed_all(
        Dialect::AnthropicMessages,
        &[A_START, &a_delta_stop("pause_turn"), A_STOP],
    );
    assert!(matches!(
        defect(&outcomes[2]),
        StreamDefect::Malformed { .. }
    ));
}

#[test]
fn responses_incomplete_maps_length_and_content_filter() {
    for (wire, want) in [
        ("max_output_tokens", FinishReason::Length),
        ("content_filter", FinishReason::ContentFilter),
    ] {
        let incomplete = format!(
            r#"{{"type":"response.incomplete","sequence_number":2,"response":{{"status":"incomplete","incomplete_details":{{"reason":"{wire}"}},"usage":{{"input_tokens":1,"output_tokens":1}}}}}}"#
        );
        let (n, outcomes) = feed_all(
            Dialect::Responses,
            &[
                r#"{"type":"response.created","sequence_number":1,"response":{"id":"r","model":"x"}}"#,
                &incomplete,
            ],
        );
        let StepOutcome::Events(events) = &outcomes[1] else {
            panic!("{wire}: expected events, got {:?}", outcomes[1]);
        };
        assert_eq!(
            events.last(),
            Some(&NormalizedEvent::Finished {
                finish_reason: want
            }),
            "incomplete reason {wire}"
        );
        assert!(n.is_terminal());
    }
}

#[test]
fn missing_model_yields_absent_served_evidence() {
    let (_, outcomes) = feed_all(
        Dialect::AnthropicMessages,
        &[r#"{"type":"message_start","message":{"id":"m","usage":{"input_tokens":1}}}"#],
    );
    let StepOutcome::Events(events) = &outcomes[0] else {
        panic!("expected events");
    };
    assert_eq!(
        events[0],
        NormalizedEvent::StreamStart {
            served_model_id: None,
            served_model_evidence: ServedModelEvidence::Absent,
        }
    );
}

#[test]
fn zero_argument_tool_call_normalizes_to_empty_object() {
    // Anthropic: tool_use block closed with no input_json_delta at all.
    let (_, outcomes) = feed_all(
        Dialect::AnthropicMessages,
        &[
            A_START,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t1","name":"get_time"}}"#,
            r#"{"type":"content_block_stop","index":0}"#,
        ],
    );
    let StepOutcome::Events(events) = &outcomes[2] else {
        panic!("expected events, got {:?}", outcomes[2]);
    };
    assert_eq!(
        events[0],
        NormalizedEvent::ToolCallComplete {
            tool_call_id: "t1".to_owned(),
            arguments_json: "{}".to_owned(),
        }
    );
    // Responses: done carries "{}" with no streamed fragments.
    let (_, outcomes) = feed_all(
        Dialect::Responses,
        &[
            r#"{"type":"response.created","sequence_number":1,"response":{"id":"r","model":"x"}}"#,
            r#"{"type":"response.output_item.added","sequence_number":2,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"get_time"}}"#,
            r#"{"type":"response.output_item.done","sequence_number":3,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"get_time","arguments":"{}"}}"#,
        ],
    );
    let StepOutcome::Events(events) = &outcomes[2] else {
        panic!("expected events, got {:?}", outcomes[2]);
    };
    assert_eq!(
        events[0],
        NormalizedEvent::ToolCallComplete {
            tool_call_id: "c1".to_owned(),
            arguments_json: "{}".to_owned(),
        }
    );
}

#[test]
fn message_typed_done_for_tool_item_is_malformed() {
    let (_, outcomes) = feed_all(
        Dialect::Responses,
        &[
            r#"{"type":"response.created","sequence_number":1,"response":{"id":"r","model":"x"}}"#,
            r#"{"type":"response.output_item.added","sequence_number":2,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"f"}}"#,
            r#"{"type":"response.output_item.done","sequence_number":3,"item":{"type":"message","id":"i1"}}"#,
        ],
    );
    assert!(matches!(
        defect(&outcomes[2]),
        StreamDefect::Malformed { .. }
    ));
}

#[test]
fn reused_call_id_is_an_ordering_violation() {
    let (_, outcomes) = feed_all(
        Dialect::Responses,
        &[
            r#"{"type":"response.created","sequence_number":1,"response":{"id":"r","model":"x"}}"#,
            r#"{"type":"response.output_item.added","sequence_number":2,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"f"}}"#,
            r#"{"type":"response.output_item.added","sequence_number":3,"item":{"type":"function_call","id":"i2","call_id":"c1","name":"f"}}"#,
        ],
    );
    assert!(matches!(
        defect(&outcomes[2]),
        StreamDefect::OrderingViolation { .. }
    ));
}

#[test]
fn known_type_with_wrong_shape_and_missing_tag_are_malformed() {
    let (_, outcomes) = feed_all(
        Dialect::AnthropicMessages,
        &[r#"{"type":"content_block_delta","index":0}"#],
    );
    assert!(matches!(
        defect(&outcomes[0]),
        StreamDefect::Malformed { .. }
    ));
    let (_, outcomes) = feed_all(Dialect::Responses, &[r#"{"no_type":1}"#]);
    assert!(matches!(
        defect(&outcomes[0]),
        StreamDefect::Malformed { .. }
    ));
}

#[test]
fn sequence_gaps_are_tolerated_by_design() {
    // A quarantined unknown-type event consumes a sequence number, so a
    // gap must pass — see check_sequence.
    let (n, outcomes) = feed_all(
        Dialect::Responses,
        &[
            r#"{"type":"response.created","sequence_number":1,"response":{"id":"r","model":"x"}}"#,
            r#"{"type":"response.completed","sequence_number":5,"response":{"status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}"#,
        ],
    );
    assert!(matches!(&outcomes[1], StepOutcome::Events(_)));
    assert!(n.is_terminal());
}
