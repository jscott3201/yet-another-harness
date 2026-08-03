//! Deterministic fake provider: replays a fixture's scripted wire chunks.
//!
//! Fixtures are provider-owned test data (MILE-001 Component 1), embedded at
//! compile time so a fixture run has no filesystem, wall-clock, or network
//! dependency. Replay is byte-for-byte: a chunk authored as a JSON object is
//! serialized once per delivery through `serde_json` (BTreeMap key order —
//! deterministic), and a chunk authored as a raw string is delivered
//! verbatim, which is how the `malformed_event` row ships bytes that are not
//! JSON.
//!
//! Time model: one chunk delivery is one tick. A `control` action with
//! `at_chunk = k` is observed in the same tick as chunk `k` — that is what
//! lets `cancellation_race` put cancel and natural completion in one tick.

use super::{Dialect, FinishReason, ServedModelEvidence, Usage};
use crate::error::ProviderErrorKind;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashSet;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// Unique across the catalog; variants extend the row name
    /// (`typed_error_rate_limit`).
    pub fixture_id: String,
    /// The MILE-001 card row this fixture covers. The coverage test closes
    /// over these.
    pub scenario_id: String,
    pub dialect: Dialect,
    pub description: String,
    /// Scripted context window for the `context_limit_truncation` row.
    /// Unread today: the no-silent-shortening half of that row needs the
    /// kernel's request seam (task 5), which will correlate this window with
    /// the dispatched input size.
    #[serde(default)]
    pub context_window_tokens: Option<u64>,
    pub turns: Vec<Turn>,
    #[serde(default)]
    pub control: Vec<ControlAction>,
    /// One entry per turn, asserted by the catalog test against the turn's
    /// final attempt.
    pub expect: Vec<Expect>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Turn {
    /// Successive answers to retries of this turn. A stream request past the
    /// last scripted attempt replays the last one — the `retry_ceiling` row
    /// scripts more attempts than the ceiling allows precisely to prove the
    /// kernel stops asking.
    pub attempts: Vec<Attempt>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attempt {
    pub chunks: Vec<ChunkSpec>,
}

/// A wire chunk: structured JSON serialized at delivery, or raw bytes
/// delivered verbatim.
#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum ChunkSpec {
    Raw(String),
    Json(Value),
}

impl ChunkSpec {
    pub fn render(&self) -> String {
        match self {
            ChunkSpec::Raw(s) => s.clone(),
            ChunkSpec::Json(v) => v.to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlAction {
    pub turn: usize,
    pub at_chunk: usize,
    pub action: ControlKind,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlKind {
    Cancel,
}

/// Declarative assertions for one turn. Every field is optional; the catalog
/// test asserts exactly the fields present.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    pub text: Option<String>,
    pub tool_calls: Option<Vec<ExpectToolCall>>,
    pub finish_reason: Option<FinishReason>,
    pub usage: Option<Usage>,
    pub error_kind: Option<ProviderErrorKind>,
    /// `"malformed"` or `"ordering_violation"` — the stream must poison with
    /// this defect class.
    pub defect: Option<String>,
    pub quarantined: Option<u64>,
    /// Exact quarantined event-type strings, in arrival order — pins the
    /// surfaced half of the P9.4 obligation, not just the count.
    pub quarantined_types: Option<Vec<String>>,
    pub served_model_id: Option<String>,
    pub served_model_evidence: Option<ServedModelEvidence>,
    /// Tool calls that were started but MUST resolve cancelled (never
    /// completed, never dropped) when the cancel tick lands — the
    /// `cancellation_mid_tool_call` row's observable.
    pub cancelled_tool_calls: Option<Vec<String>>,
    /// Attempts the driver may request for this turn under the retry rule —
    /// the `retry_ceiling` row asserts this equals the ceiling while the
    /// script deliberately carries one more attempt.
    pub attempts: Option<u64>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpectToolCall {
    pub tool_call_id: String,
    pub tool_name: String,
    /// Compared as parsed JSON, not bytes — argument equality must not
    /// depend on chunk boundaries (`fragmented_stream` row).
    pub arguments: Value,
}

/// One delivery tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickDelivery {
    pub chunk: String,
    /// A cancel control action lands in this tick.
    pub cancel: bool,
}

pub struct FakeProvider {
    fixture: Fixture,
    attempts_served: Vec<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum FixtureError {
    NoSuchTurn(usize),
    /// A turn with zero scripted attempts — load_catalog rejects these, but
    /// Fixture fields are pub, so hand-built fixtures must fail here rather
    /// than underflow in the saturation index.
    EmptyTurn(usize),
}

impl FakeProvider {
    pub fn new(fixture: Fixture) -> Self {
        let turns = fixture.turns.len();
        FakeProvider {
            fixture,
            attempts_served: vec![0; turns],
        }
    }

    pub fn fixture(&self) -> &Fixture {
        &self.fixture
    }

    /// Opens the next scripted attempt for `turn`. Repeated calls advance
    /// through the scripted attempts and saturate on the last.
    pub fn open_stream(&mut self, turn: usize) -> Result<Vec<TickDelivery>, FixtureError> {
        let script = self
            .fixture
            .turns
            .get(turn)
            .ok_or(FixtureError::NoSuchTurn(turn))?;
        if script.attempts.is_empty() {
            return Err(FixtureError::EmptyTurn(turn));
        }
        let served = &mut self.attempts_served[turn];
        let attempt = &script.attempts[(*served).min(script.attempts.len() - 1)];
        *served += 1;
        let ticks =
            attempt
                .chunks
                .iter()
                .enumerate()
                .map(|(i, chunk)| TickDelivery {
                    chunk: chunk.render(),
                    cancel: self.fixture.control.iter().any(|c| {
                        c.turn == turn && c.at_chunk == i && c.action == ControlKind::Cancel
                    }),
                })
                .collect();
        Ok(ticks)
    }

    /// How many attempts have been requested for `turn` so far — the
    /// `retry_ceiling` obligation asserts this stops at the ceiling.
    pub fn attempts_served(&self, turn: usize) -> usize {
        self.attempts_served.get(turn).copied().unwrap_or(0)
    }
}

macro_rules! catalog_files {
    ($($path:literal),+ $(,)?) => {
        &[$(($path, include_str!(concat!("../../fixtures/", $path)))),+]
    };
}

/// Every embedded fixture as `(path, bytes)`. The catalog test decodes each
/// one and closes coverage over the 13 scenario rows in both dialects.
pub const CATALOG_FILES: &[(&str, &str)] = catalog_files![
    "anthropic_messages/fragmented_stream.json",
    "anthropic_messages/tool_call_single.json",
    "anthropic_messages/tool_call_parallel.json",
    "anthropic_messages/tool_call_sequential_dependent.json",
    "anthropic_messages/typed_error_rate_limit.json",
    "anthropic_messages/typed_error_invalid_request.json",
    "anthropic_messages/typed_error_context_length_exceeded.json",
    "anthropic_messages/typed_error_server_error.json",
    "anthropic_messages/typed_error_unsupported_capability.json",
    "anthropic_messages/retry_ceiling.json",
    "anthropic_messages/cancellation_mid_stream.json",
    "anthropic_messages/cancellation_mid_tool_call.json",
    "anthropic_messages/cancellation_race.json",
    "anthropic_messages/usage_accounting.json",
    "anthropic_messages/context_limit_truncation.json",
    "anthropic_messages/malformed_event.json",
    "anthropic_messages/out_of_order_event.json",
    "anthropic_messages/out_of_order_event_duplicate.json",
    "responses/fragmented_stream.json",
    "responses/tool_call_single.json",
    "responses/tool_call_parallel.json",
    "responses/tool_call_sequential_dependent.json",
    "responses/typed_error_rate_limit.json",
    "responses/typed_error_invalid_request.json",
    "responses/typed_error_context_length_exceeded.json",
    "responses/typed_error_server_error.json",
    "responses/typed_error_unsupported_capability.json",
    "responses/retry_ceiling.json",
    "responses/cancellation_mid_stream.json",
    "responses/cancellation_mid_tool_call.json",
    "responses/cancellation_race.json",
    "responses/usage_accounting.json",
    "responses/context_limit_truncation.json",
    "responses/malformed_event.json",
    "responses/out_of_order_event.json",
    "responses/out_of_order_event_after_terminal.json",
];

/// The card's closed scenario catalog. A fixture whose `scenario_id` is not
/// here is a catalog error; a row here with no fixture in some dialect fails
/// coverage.
pub const SCENARIO_ROWS: &[&str] = &[
    "fragmented_stream",
    "tool_call_single",
    "tool_call_parallel",
    "tool_call_sequential_dependent",
    "typed_error",
    "retry_ceiling",
    "cancellation_mid_stream",
    "cancellation_mid_tool_call",
    "cancellation_race",
    "usage_accounting",
    "context_limit_truncation",
    "malformed_event",
    "out_of_order_event",
];

pub fn load_catalog() -> Result<Vec<Fixture>, String> {
    let mut fixtures = Vec::new();
    let mut seen = HashSet::new();
    for (path, bytes) in CATALOG_FILES {
        let fixture: Fixture = serde_json::from_str(bytes)
            .map_err(|e| format!("fixture {path} does not decode: {e}"))?;
        if fixture.turns.is_empty() || fixture.turns.iter().any(|t| t.attempts.is_empty()) {
            return Err(format!("fixture {path} has an empty turn or attempt list"));
        }
        if fixture.expect.len() != fixture.turns.len() {
            return Err(format!(
                "fixture {path}: {} expect blocks for {} turns",
                fixture.expect.len(),
                fixture.turns.len()
            ));
        }
        if !SCENARIO_ROWS.contains(&fixture.scenario_id.as_str()) {
            return Err(format!(
                "fixture {path} names unknown scenario {:?}",
                fixture.scenario_id
            ));
        }
        if !seen.insert((fixture.dialect, fixture.fixture_id.clone())) {
            return Err(format!("duplicate fixture id {:?}", fixture.fixture_id));
        }
        fixtures.push(fixture);
    }
    Ok(fixtures)
}
