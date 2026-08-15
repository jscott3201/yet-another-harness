//! Normalized provider events (R5.1) and the deterministic fake provider
//! that emits them (MILE-001 Component 1).
//!
//! The kernel owns these shapes; provider payloads are never authoritative
//! (R5). A dialect module maps one wire format onto them and nothing
//! downstream of [`normalize`] may depend on which dialect produced a
//! stream — that indifference is what the R15 two-arm catalog proves.

pub mod dialect;
pub mod fake;
pub mod normalize;

use crate::error::ProviderErrorKind;
use serde::{Deserialize, Serialize};

/// Which wire dialect a stream arrived in. Closed: v0 ships exactly the two
/// R15 arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Dialect {
    Responses,
    AnthropicMessages,
}

/// ADR-001 §4.4 `ModelCallEffect.finish_reason`, closed. `Interrupted` is
/// kernel-assigned on cancellation; no dialect maps to it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    Length,
    ToolCall,
    ContentFilter,
    Error,
    Interrupted,
}

/// ADR-001 §4.4: served-model identity is read from the RESPONSE, never the
/// request — router-backed endpoints pick the serving model per request.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServedModelEvidence {
    ResponseField,
    ResponseHeader,
    Absent,
}

/// Token accounting in kernel field names. The kernel records what the
/// provider reported without reinterpreting provider-specific field names
/// (`usage_accounting` row); the dialect mapping is where wire names die.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub cached_input_tokens: u64,
    pub output_tokens: u64,
    pub reasoning_tokens: u64,
}

/// One normalized event. The stream grammar the normalizer enforces:
/// `StreamStart` first, then deltas and tool-call events, then at most one
/// terminal (`Finished` or `ProviderError`); nothing follows a terminal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NormalizedEvent {
    StreamStart {
        served_model_id: Option<String>,
        served_model_evidence: ServedModelEvidence,
    },
    TextDelta {
        text: String,
    },
    ToolCallStart {
        tool_call_id: String,
        tool_name: String,
    },
    /// A fragment of the call's argument JSON, split wherever the wire split
    /// it — mid-token, mid-escape. Reassembly is the kernel's job.
    ToolCallArgumentsDelta {
        tool_call_id: String,
        fragment: String,
    },
    /// Emitted only when the accumulated fragments parse as one JSON value;
    /// unparseable arguments at block close are a stream defect, not a
    /// completed call.
    ToolCallComplete {
        tool_call_id: String,
        arguments_json: String,
    },
    Usage(Usage),
    Finished {
        finish_reason: FinishReason,
    },
    /// Typed terminal failure (R5.1: a typed error, not a raw payload).
    ProviderError {
        error_kind: ProviderErrorKind,
        message: String,
    },
}
