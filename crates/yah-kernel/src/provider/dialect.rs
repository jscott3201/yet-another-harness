//! Wire-dialect event shapes for the two R15 arms.
//!
//! These are deliberately a subset of each real API: enough surface for the
//! 13-row fixture catalog to exercise every kernel obligation. Live-endpoint
//! fidelity (full event registries, header evidence, version pinning) is the
//! R5.4 provider ADR's obligation, re-cited against primary provider
//! documentation there — see the A12 note in ADR-001 §4.4.
//!
//! Parsing is two-stage on purpose: a chunk that is not JSON, or that decodes
//! to a known type with a wrong shape, is [`WireParse::Malformed`]; valid
//! JSON with an unrecognized `type` tag is [`WireParse::UnknownType`]. The
//! normalizer quarantines the second and fails the stream on the first
//! (`malformed_event` row: reject or quarantine, never crash).

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub enum WireParse<E> {
    Event(E),
    UnknownType(String),
    Malformed(String),
}

fn parse_tagged<E: serde::de::DeserializeOwned>(chunk: &str, known: &[&str]) -> WireParse<E> {
    let value: Value = match serde_json::from_str(chunk) {
        Ok(v) => v,
        Err(e) => return WireParse::Malformed(format!("not JSON: {e}")),
    };
    let Some(tag) = value.get("type").and_then(Value::as_str).map(str::to_owned) else {
        return WireParse::Malformed("missing string `type` tag".to_owned());
    };
    if !known.contains(&tag.as_str()) {
        return WireParse::UnknownType(tag);
    }
    match serde_json::from_value(value) {
        Ok(event) => WireParse::Event(event),
        Err(e) => WireParse::Malformed(format!("known type {tag:?} with wrong shape: {e}")),
    }
}

// ---------------------------------------------------------------- Anthropic

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicEvent {
    MessageStart {
        message: AnthropicMessageStart,
    },
    ContentBlockStart {
        index: u32,
        content_block: AnthropicContentBlock,
    },
    ContentBlockDelta {
        index: u32,
        delta: AnthropicDelta,
    },
    ContentBlockStop {
        index: u32,
    },
    MessageDelta {
        delta: AnthropicMessageDeltaBody,
        usage: AnthropicOutputUsage,
    },
    MessageStop {},
    Error {
        error: AnthropicError,
    },
}

const ANTHROPIC_TYPES: &[&str] = &[
    "message_start",
    "content_block_start",
    "content_block_delta",
    "content_block_stop",
    "message_delta",
    "message_stop",
    "error",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnthropicMessageStart {
    pub id: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub usage: AnthropicInputUsage,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AnthropicInputUsage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AnthropicOutputUsage {
    #[serde(default)]
    pub output_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicContentBlock {
    Text {},
    ToolUse { id: String, name: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AnthropicDelta {
    TextDelta { text: String },
    InputJsonDelta { partial_json: String },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AnthropicMessageDeltaBody {
    #[serde(default)]
    pub stop_reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AnthropicError {
    /// Real wire values the mapping handles: `rate_limit_error`,
    /// `invalid_request_error`, `overloaded_error`, `api_error`.
    /// `not_supported_error` is a fake-dialect placeholder — the real
    /// registry has no such type and reports capability failures as
    /// `invalid_request_error`; the R5 provider ADR owns the live mapping.
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

pub fn parse_anthropic(chunk: &str) -> WireParse<AnthropicEvent> {
    parse_tagged(chunk, ANTHROPIC_TYPES)
}

// ---------------------------------------------------------------- Responses

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResponsesEvent {
    #[serde(rename = "response.created")]
    Created {
        sequence_number: u64,
        response: ResponsesResponseHead,
    },
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded {
        sequence_number: u64,
        item: ResponsesItem,
    },
    #[serde(rename = "response.output_text.delta")]
    OutputTextDelta {
        sequence_number: u64,
        item_id: String,
        delta: String,
    },
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionCallArgumentsDelta {
        sequence_number: u64,
        item_id: String,
        delta: String,
    },
    #[serde(rename = "response.output_item.done")]
    OutputItemDone {
        sequence_number: u64,
        item: ResponsesItem,
    },
    #[serde(rename = "response.completed")]
    Completed {
        sequence_number: u64,
        response: ResponsesResponseTail,
    },
    #[serde(rename = "response.incomplete")]
    Incomplete {
        sequence_number: u64,
        response: ResponsesResponseTail,
    },
    #[serde(rename = "response.failed")]
    Failed {
        sequence_number: u64,
        response: ResponsesFailure,
    },
}

const RESPONSES_TYPES: &[&str] = &[
    "response.created",
    "response.output_item.added",
    "response.output_text.delta",
    "response.function_call_arguments.delta",
    "response.output_item.done",
    "response.completed",
    "response.incomplete",
    "response.failed",
];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesResponseHead {
    pub id: String,
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ResponsesItem {
    #[serde(rename = "message")]
    Message { id: String },
    #[serde(rename = "function_call")]
    FunctionCall {
        id: String,
        call_id: String,
        name: String,
        #[serde(default)]
        arguments: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesResponseTail {
    pub status: String,
    #[serde(default)]
    pub incomplete_details: Option<ResponsesIncompleteDetails>,
    pub usage: ResponsesUsage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesIncompleteDetails {
    pub reason: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesUsage {
    pub input_tokens: u64,
    #[serde(default)]
    pub input_tokens_details: ResponsesInputDetails,
    pub output_tokens: u64,
    #[serde(default)]
    pub output_tokens_details: ResponsesOutputDetails,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResponsesInputDetails {
    #[serde(default)]
    pub cached_tokens: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ResponsesOutputDetails {
    #[serde(default)]
    pub reasoning_tokens: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesFailure {
    pub error: ResponsesError,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResponsesError {
    /// Wire values seen in the dialect mapping: `rate_limit_exceeded`,
    /// `invalid_request`, `context_length_exceeded`, `server_error`,
    /// `unsupported_parameter`.
    pub code: String,
    pub message: String,
}

pub fn parse_responses(chunk: &str) -> WireParse<ResponsesEvent> {
    parse_tagged(chunk, RESPONSES_TYPES)
}
