//! Dialect → normalized-event state machines.
//!
//! Failure discipline (the `malformed_event` and `out_of_order_event` rows):
//! an unrecognized event type is quarantined and the stream continues — the
//! read-side fail-open rule of ADR-002 P9.4 applied at the provider seam. A
//! chunk that is not JSON, a known type with a wrong shape, unparseable tool
//! arguments, or an unknown member of a closed wire vocabulary poisons the
//! stream with a typed [`StreamDefect`]; every later feed returns that
//! defect. Nothing here panics on wire input.

use super::dialect::{
    self, AnthropicContentBlock, AnthropicDelta, AnthropicEvent, ResponsesEvent, ResponsesItem,
    WireParse,
};
use super::{Dialect, FinishReason, NormalizedEvent, ServedModelEvidence, Usage};
use crate::error::ProviderErrorKind;
use std::collections::BTreeMap;

/// Terminal stream failure. `Malformed` covers byte/shape/vocabulary damage;
/// `OrderingViolation` covers structurally valid events arriving where the
/// stream grammar forbids them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamDefect {
    Malformed { detail: String },
    OrderingViolation { detail: String },
}

#[derive(Debug, PartialEq, Eq)]
pub enum StepOutcome {
    Events(Vec<NormalizedEvent>),
    /// Unknown event type: surfaced, counted, stream unchanged (P9.4).
    Quarantined {
        event_type: String,
    },
    Defect(StreamDefect),
}

pub struct Normalizer {
    dialect: Dialect,
    poisoned: Option<StreamDefect>,
    terminal: bool,
    quarantined: u64,
    anthropic: AnthropicState,
    responses: ResponsesState,
}

#[derive(Default)]
struct AnthropicState {
    started: bool,
    blocks: BTreeMap<u32, Block>,
    usage: Usage,
    stop_reason: Option<String>,
}

#[derive(Default)]
struct ResponsesState {
    created: bool,
    last_seq: Option<u64>,
    items: BTreeMap<String, Item>,
    saw_tool_item: bool,
}

struct Block {
    kind: BlockKind,
    stopped: bool,
}

enum BlockKind {
    Text,
    Tool { id: String, buf: String },
}

struct Item {
    kind: ItemKind,
    done: bool,
}

enum ItemKind {
    Message,
    Tool { call_id: String, buf: String },
}

impl Normalizer {
    pub fn new(dialect: Dialect) -> Self {
        Normalizer {
            dialect,
            poisoned: None,
            terminal: false,
            quarantined: 0,
            anthropic: AnthropicState::default(),
            responses: ResponsesState::default(),
        }
    }

    /// True once the stream saw its single terminal event. A stream that
    /// ends without one is truncated: the kernel maps that to an unproven
    /// model-call outcome (ADR-001 §9 proof rules), not to success.
    pub fn is_terminal(&self) -> bool {
        self.terminal
    }

    pub fn quarantined_count(&self) -> u64 {
        self.quarantined
    }

    pub fn feed(&mut self, chunk: &str) -> StepOutcome {
        if let Some(defect) = &self.poisoned {
            return StepOutcome::Defect(defect.clone());
        }
        let outcome = match self.dialect {
            Dialect::AnthropicMessages => match dialect::parse_anthropic(chunk) {
                WireParse::Malformed(detail) => Err(StreamDefect::Malformed { detail }),
                WireParse::UnknownType(event_type) => return self.quarantine(event_type),
                WireParse::Event(event) => self.feed_anthropic(event),
            },
            Dialect::Responses => match dialect::parse_responses(chunk) {
                WireParse::Malformed(detail) => Err(StreamDefect::Malformed { detail }),
                WireParse::UnknownType(event_type) => return self.quarantine(event_type),
                WireParse::Event(event) => self.feed_responses(event),
            },
        };
        match outcome {
            Ok(events) => StepOutcome::Events(events),
            Err(defect) => {
                self.poisoned = Some(defect.clone());
                StepOutcome::Defect(defect)
            }
        }
    }

    fn quarantine(&mut self, event_type: String) -> StepOutcome {
        self.quarantined += 1;
        StepOutcome::Quarantined { event_type }
    }

    fn reject_after_terminal(&self) -> Result<(), StreamDefect> {
        if self.terminal {
            return Err(StreamDefect::OrderingViolation {
                detail: "event after terminal".to_owned(),
            });
        }
        Ok(())
    }

    // ------------------------------------------------------------ Anthropic

    fn feed_anthropic(
        &mut self,
        event: AnthropicEvent,
    ) -> Result<Vec<NormalizedEvent>, StreamDefect> {
        self.reject_after_terminal()?;
        let st = &mut self.anthropic;
        match event {
            AnthropicEvent::MessageStart { message } => {
                if st.started {
                    return ordering("second message_start");
                }
                st.started = true;
                st.usage.input_tokens = message.usage.input_tokens;
                st.usage.cached_input_tokens = message.usage.cache_read_input_tokens;
                let evidence = if message.model.is_some() {
                    ServedModelEvidence::ResponseField
                } else {
                    ServedModelEvidence::Absent
                };
                Ok(vec![NormalizedEvent::StreamStart {
                    served_model_id: message.model,
                    served_model_evidence: evidence,
                }])
            }
            AnthropicEvent::ContentBlockStart {
                index,
                content_block,
            } => {
                if !st.started {
                    return ordering("content_block_start before message_start");
                }
                if st.blocks.contains_key(&index) {
                    return ordering(&format!("content block index {index} reused"));
                }
                match content_block {
                    AnthropicContentBlock::Text {} => {
                        st.blocks.insert(
                            index,
                            Block {
                                kind: BlockKind::Text,
                                stopped: false,
                            },
                        );
                        Ok(vec![])
                    }
                    AnthropicContentBlock::ToolUse { id, name } => {
                        st.blocks.insert(
                            index,
                            Block {
                                kind: BlockKind::Tool {
                                    id: id.clone(),
                                    buf: String::new(),
                                },
                                stopped: false,
                            },
                        );
                        Ok(vec![NormalizedEvent::ToolCallStart {
                            tool_call_id: id,
                            tool_name: name,
                        }])
                    }
                }
            }
            AnthropicEvent::ContentBlockDelta { index, delta } => {
                let Some(block) = st.blocks.get_mut(&index) else {
                    return ordering(&format!("delta for unopened block {index}"));
                };
                if block.stopped {
                    return ordering(&format!("delta after content_block_stop for block {index}"));
                }
                match (&mut block.kind, delta) {
                    (BlockKind::Text, AnthropicDelta::TextDelta { text }) => {
                        Ok(vec![NormalizedEvent::TextDelta { text }])
                    }
                    (
                        BlockKind::Tool { id, buf },
                        AnthropicDelta::InputJsonDelta { partial_json },
                    ) => {
                        buf.push_str(&partial_json);
                        Ok(vec![NormalizedEvent::ToolCallArgumentsDelta {
                            tool_call_id: id.clone(),
                            fragment: partial_json,
                        }])
                    }
                    _ => malformed(&format!("delta kind does not match block {index} kind")),
                }
            }
            AnthropicEvent::ContentBlockStop { index } => {
                let Some(block) = st.blocks.get_mut(&index) else {
                    return ordering(&format!("content_block_stop for unopened block {index}"));
                };
                if block.stopped {
                    return ordering(&format!("second content_block_stop for block {index}"));
                }
                block.stopped = true;
                match &block.kind {
                    BlockKind::Text => Ok(vec![]),
                    BlockKind::Tool { id, buf } => {
                        // A zero-argument tool call streams no input_json_delta
                        // at all; the wire meaning of an empty accumulation is
                        // an empty argument object.
                        let args = if buf.is_empty() { "{}" } else { buf.as_str() };
                        if serde_json::from_str::<serde_json::Value>(args).is_err() {
                            return malformed(&format!(
                                "tool call {id} closed with unparseable arguments"
                            ));
                        }
                        Ok(vec![NormalizedEvent::ToolCallComplete {
                            tool_call_id: id.clone(),
                            arguments_json: args.to_owned(),
                        }])
                    }
                }
            }
            AnthropicEvent::MessageDelta { delta, usage } => {
                if !st.started {
                    return ordering("message_delta before message_start");
                }
                if let Some(reason) = delta.stop_reason {
                    st.stop_reason = Some(reason);
                }
                // The wire reports cumulative output tokens; latest wins.
                st.usage.output_tokens = usage.output_tokens;
                Ok(vec![])
            }
            AnthropicEvent::MessageStop {} => {
                if !st.started {
                    return ordering("message_stop before message_start");
                }
                if let Some((index, _)) = st.blocks.iter().find(|(_, b)| !b.stopped) {
                    return ordering(&format!("message_stop with open content block {index}"));
                }
                let reason = match st.stop_reason.as_deref() {
                    Some("end_turn") | Some("stop_sequence") => FinishReason::Stop,
                    Some("max_tokens") => FinishReason::Length,
                    Some("tool_use") => FinishReason::ToolCall,
                    Some("refusal") => FinishReason::ContentFilter,
                    Some(other) => {
                        return malformed(&format!("unknown stop_reason {other:?}"));
                    }
                    None => return malformed("message_stop without a prior stop_reason"),
                };
                self.terminal = true;
                Ok(vec![
                    NormalizedEvent::Usage(st.usage),
                    NormalizedEvent::Finished {
                        finish_reason: reason,
                    },
                ])
            }
            AnthropicEvent::Error { error } => {
                let kind = match error.error_type.as_str() {
                    "rate_limit_error" => ProviderErrorKind::RateLimit,
                    "overloaded_error" | "api_error" => ProviderErrorKind::ServerError,
                    // Fake-dialect placeholder: the real error registry has no
                    // not_supported_error — live capability failures arrive as
                    // invalid_request_error. The R5 provider ADR owns the real
                    // mapping; this arm exists so the R5.2 path is exercisable
                    // model-free (see dialect.rs module doc).
                    "not_supported_error" => ProviderErrorKind::UnsupportedCapability,
                    // The real endpoint reports context exhaustion as an
                    // invalid_request_error with a documented message prefix;
                    // the mapping is re-verified at the R5 provider ADR.
                    "invalid_request_error" if error.message.starts_with("prompt is too long") => {
                        ProviderErrorKind::ContextLengthExceeded
                    }
                    "invalid_request_error" => ProviderErrorKind::InvalidRequest,
                    other => return malformed(&format!("unknown error type {other:?}")),
                };
                self.terminal = true;
                Ok(vec![NormalizedEvent::ProviderError {
                    error_kind: kind,
                    message: error.message,
                }])
            }
        }
    }

    // ------------------------------------------------------------ Responses

    fn feed_responses(
        &mut self,
        event: ResponsesEvent,
    ) -> Result<Vec<NormalizedEvent>, StreamDefect> {
        self.reject_after_terminal()?;
        self.check_sequence(&event)?;
        let st = &mut self.responses;
        match event {
            ResponsesEvent::Created { response, .. } => {
                if st.created {
                    return ordering("second response.created");
                }
                st.created = true;
                let evidence = if response.model.is_some() {
                    ServedModelEvidence::ResponseField
                } else {
                    ServedModelEvidence::Absent
                };
                Ok(vec![NormalizedEvent::StreamStart {
                    served_model_id: response.model,
                    served_model_evidence: evidence,
                }])
            }
            ResponsesEvent::OutputItemAdded { item, .. } => {
                if !st.created {
                    return ordering("output_item.added before response.created");
                }
                match item {
                    ResponsesItem::Message { id } => {
                        if st.items.contains_key(&id) {
                            return ordering(&format!("item id {id} reused"));
                        }
                        st.items.insert(
                            id,
                            Item {
                                kind: ItemKind::Message,
                                done: false,
                            },
                        );
                        Ok(vec![])
                    }
                    ResponsesItem::FunctionCall {
                        id, call_id, name, ..
                    } => {
                        if st.items.contains_key(&id) {
                            return ordering(&format!("item id {id} reused"));
                        }
                        // Downstream consumers key tool state on call_id, so a
                        // reused call_id conflates two independent calls even
                        // when the item ids differ.
                        if st.items.values().any(|i| {
                            matches!(&i.kind, ItemKind::Tool { call_id: c, .. } if *c == call_id)
                        }) {
                            return ordering(&format!("call_id {call_id} reused"));
                        }
                        st.saw_tool_item = true;
                        st.items.insert(
                            id,
                            Item {
                                kind: ItemKind::Tool {
                                    call_id: call_id.clone(),
                                    buf: String::new(),
                                },
                                done: false,
                            },
                        );
                        Ok(vec![NormalizedEvent::ToolCallStart {
                            tool_call_id: call_id,
                            tool_name: name,
                        }])
                    }
                }
            }
            ResponsesEvent::OutputTextDelta { item_id, delta, .. } => {
                let Some(item) = st.items.get(&item_id) else {
                    return ordering(&format!("text delta for unknown item {item_id}"));
                };
                if item.done {
                    return ordering(&format!("text delta after item {item_id} done"));
                }
                match item.kind {
                    ItemKind::Message => Ok(vec![NormalizedEvent::TextDelta { text: delta }]),
                    ItemKind::Tool { .. } => {
                        malformed(&format!("text delta into function_call item {item_id}"))
                    }
                }
            }
            ResponsesEvent::FunctionCallArgumentsDelta { item_id, delta, .. } => {
                let Some(item) = st.items.get_mut(&item_id) else {
                    return ordering(&format!("arguments delta for unknown item {item_id}"));
                };
                if item.done {
                    return ordering(&format!("arguments delta after item {item_id} done"));
                }
                match &mut item.kind {
                    ItemKind::Tool { call_id, buf } => {
                        buf.push_str(&delta);
                        Ok(vec![NormalizedEvent::ToolCallArgumentsDelta {
                            tool_call_id: call_id.clone(),
                            fragment: delta,
                        }])
                    }
                    ItemKind::Message => {
                        malformed(&format!("arguments delta into message item {item_id}"))
                    }
                }
            }
            ResponsesEvent::OutputItemDone { item, .. } => match item {
                ResponsesItem::Message { id } => {
                    let Some(entry) = st.items.get_mut(&id) else {
                        return ordering(&format!("done for unknown item {id}"));
                    };
                    if entry.done {
                        return ordering(&format!("second done for item {id}"));
                    }
                    // Kind must match in BOTH directions; accepting a
                    // message-typed done for a tool item would drop the tool
                    // call silently — the exact failure R5.1 typing exists to
                    // prevent.
                    let ItemKind::Message = &entry.kind else {
                        return malformed(&format!("message done for function_call item {id}"));
                    };
                    entry.done = true;
                    Ok(vec![])
                }
                ResponsesItem::FunctionCall { id, arguments, .. } => {
                    let Some(entry) = st.items.get_mut(&id) else {
                        return ordering(&format!("done for unknown item {id}"));
                    };
                    if entry.done {
                        return ordering(&format!("second done for item {id}"));
                    }
                    let ItemKind::Tool { call_id, buf } = &entry.kind else {
                        return malformed(&format!("function_call done for message item {id}"));
                    };
                    // The done item carries the full argument string; when
                    // fragments were streamed it must agree with them, or the
                    // stream is lying to one of its two readers. A
                    // zero-argument call streams no fragments at all — the
                    // done item's string (or an empty object) is then the
                    // whole truth.
                    let final_args = match arguments {
                        Some(args) => {
                            if !buf.is_empty() && args != *buf {
                                return malformed(&format!(
                                    "item {id} done arguments disagree with accumulated fragments"
                                ));
                            }
                            args
                        }
                        None if buf.is_empty() => "{}".to_owned(),
                        None => buf.clone(),
                    };
                    if serde_json::from_str::<serde_json::Value>(&final_args).is_err() {
                        return malformed(&format!(
                            "tool call {call_id} closed with unparseable arguments"
                        ));
                    }
                    let event = NormalizedEvent::ToolCallComplete {
                        tool_call_id: call_id.clone(),
                        arguments_json: final_args,
                    };
                    entry.done = true;
                    Ok(vec![event])
                }
            },
            ResponsesEvent::Completed { response, .. } => {
                if !st.created {
                    return ordering("response.completed before response.created");
                }
                if response.status != "completed" {
                    return malformed(&format!(
                        "response.completed with status {:?}",
                        response.status
                    ));
                }
                if let Some((id, _)) = st.items.iter().find(|(_, i)| !i.done) {
                    return ordering(&format!("response.completed with open item {id}"));
                }
                let reason = if st.saw_tool_item {
                    FinishReason::ToolCall
                } else {
                    FinishReason::Stop
                };
                self.terminal = true;
                Ok(vec![
                    NormalizedEvent::Usage(map_responses_usage(&response.usage)),
                    NormalizedEvent::Finished {
                        finish_reason: reason,
                    },
                ])
            }
            ResponsesEvent::Incomplete { response, .. } => {
                if !st.created {
                    return ordering("response.incomplete before response.created");
                }
                let reason = match response
                    .incomplete_details
                    .as_ref()
                    .map(|d| d.reason.as_str())
                {
                    Some("max_output_tokens") => FinishReason::Length,
                    Some("content_filter") => FinishReason::ContentFilter,
                    Some(other) => {
                        return malformed(&format!("unknown incomplete reason {other:?}"));
                    }
                    None => return malformed("response.incomplete without incomplete_details"),
                };
                self.terminal = true;
                Ok(vec![
                    NormalizedEvent::Usage(map_responses_usage(&response.usage)),
                    NormalizedEvent::Finished {
                        finish_reason: reason,
                    },
                ])
            }
            ResponsesEvent::Failed { response, .. } => {
                let kind = match response.error.code.as_str() {
                    "rate_limit_exceeded" => ProviderErrorKind::RateLimit,
                    "invalid_request" => ProviderErrorKind::InvalidRequest,
                    "context_length_exceeded" => ProviderErrorKind::ContextLengthExceeded,
                    "server_error" => ProviderErrorKind::ServerError,
                    "unsupported_parameter" => ProviderErrorKind::UnsupportedCapability,
                    other => return malformed(&format!("unknown error code {other:?}")),
                };
                self.terminal = true;
                Ok(vec![NormalizedEvent::ProviderError {
                    error_kind: kind,
                    message: response.error.message,
                }])
            }
        }
    }

    /// `sequence_number` must be strictly increasing — an equal value is the
    /// duplicate-delta case of the `out_of_order_event` row. Gaps are
    /// deliberately tolerated: a quarantined unknown-type event consumes a
    /// sequence number without ever reaching this check, so contiguity
    /// enforcement would poison every stream P9.4 requires us to survive.
    fn check_sequence(&mut self, event: &ResponsesEvent) -> Result<(), StreamDefect> {
        let seq = match event {
            ResponsesEvent::Created {
                sequence_number, ..
            }
            | ResponsesEvent::OutputItemAdded {
                sequence_number, ..
            }
            | ResponsesEvent::OutputTextDelta {
                sequence_number, ..
            }
            | ResponsesEvent::FunctionCallArgumentsDelta {
                sequence_number, ..
            }
            | ResponsesEvent::OutputItemDone {
                sequence_number, ..
            }
            | ResponsesEvent::Completed {
                sequence_number, ..
            }
            | ResponsesEvent::Incomplete {
                sequence_number, ..
            }
            | ResponsesEvent::Failed {
                sequence_number, ..
            } => *sequence_number,
        };
        let st = &mut self.responses;
        if let Some(last) = st.last_seq
            && seq <= last
        {
            return Err(StreamDefect::OrderingViolation {
                detail: format!("sequence_number {seq} not greater than {last}"),
            });
        }
        st.last_seq = Some(seq);
        Ok(())
    }
}

fn map_responses_usage(u: &dialect::ResponsesUsage) -> Usage {
    Usage {
        input_tokens: u.input_tokens,
        cached_input_tokens: u.input_tokens_details.cached_tokens,
        output_tokens: u.output_tokens,
        reasoning_tokens: u.output_tokens_details.reasoning_tokens,
    }
}

fn ordering(detail: &str) -> Result<Vec<NormalizedEvent>, StreamDefect> {
    Err(StreamDefect::OrderingViolation {
        detail: detail.to_owned(),
    })
}

fn malformed(detail: &str) -> Result<Vec<NormalizedEvent>, StreamDefect> {
    Err(StreamDefect::Malformed {
        detail: detail.to_owned(),
    })
}

#[cfg(test)]
mod tests {
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
}
