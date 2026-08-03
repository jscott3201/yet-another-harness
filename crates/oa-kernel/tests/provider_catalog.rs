//! Component-1 catalog gate: every MILE-001 scenario row has a fixture in
//! both dialect arms, every fixture's declared expectations hold, and replay
//! is deterministic.
//!
//! The driver here is the cancel-observing, retry-disciplined consumer the
//! kernel run loop will later be: it stops feeding after a cancel tick,
//! resolves started-but-incomplete tool calls to cancelled, assigns
//! `Interrupted` when cancellation preempts the wire terminal, and retries a
//! transient provider error only up to the R25c ceiling. Every turn is also
//! held to the normalized stream grammar (StreamStart first, at most one
//! terminal, nothing after it, Usage before Finished) and to the poison
//! latch (a defect repeats verbatim on every later feed).

use oa_kernel::error::ProviderErrorKind;
use oa_kernel::provider::fake::{
    CATALOG_FILES, FakeProvider, Fixture, SCENARIO_ROWS, load_catalog,
};
use oa_kernel::provider::normalize::{Normalizer, StepOutcome, StreamDefect};
use oa_kernel::provider::{FinishReason, NormalizedEvent, ServedModelEvidence, Usage};
use std::collections::BTreeSet;

/// Retry ceiling ratified by R25c (MILE-001 open parameters).
const RETRY_CEILING: usize = 3;

/// Everything one turn's final attempt produced, in assertable form.
#[derive(Debug, Default, PartialEq)]
struct TurnObservation {
    events: Vec<NormalizedEvent>,
    text: String,
    tool_calls: Vec<(String, String, serde_json::Value)>,
    cancelled_tool_calls: Vec<String>,
    finish_reason: Option<FinishReason>,
    usage: Option<Usage>,
    error_kind: Option<ProviderErrorKind>,
    defect: Option<StreamDefect>,
    quarantined: u64,
    quarantined_types: Vec<String>,
    served_model_id: Option<String>,
    served_model_evidence: Option<ServedModelEvidence>,
    cancel_observed: bool,
    attempts_served: u64,
}

fn run_turn(provider: &mut FakeProvider, turn: usize) -> TurnObservation {
    let ticks = provider.open_stream(turn).expect("scripted turn exists");
    let dialect = provider.fixture().dialect;
    let fixture_id = provider.fixture().fixture_id.clone();
    let mut normalizer = Normalizer::new(dialect);
    let mut obs = TurnObservation::default();
    // (tool_call_id, tool_name) in start order; completion tracked by id.
    let mut started_tools: Vec<(String, String)> = Vec::new();
    let mut completed_tools: BTreeSet<String> = BTreeSet::new();
    let mut poisoned: Option<StreamDefect> = None;
    for tick in ticks {
        match normalizer.feed(&tick.chunk) {
            StepOutcome::Events(events) => {
                assert!(
                    poisoned.is_none(),
                    "{fixture_id}: events emitted after stream poison"
                );
                for event in events {
                    obs.events.push(event.clone());
                    match event {
                        NormalizedEvent::StreamStart {
                            served_model_id,
                            served_model_evidence,
                        } => {
                            obs.served_model_id = served_model_id;
                            obs.served_model_evidence = Some(served_model_evidence);
                        }
                        NormalizedEvent::TextDelta { text } => obs.text.push_str(&text),
                        NormalizedEvent::ToolCallStart {
                            tool_call_id,
                            tool_name,
                        } => started_tools.push((tool_call_id, tool_name)),
                        NormalizedEvent::ToolCallComplete {
                            tool_call_id,
                            arguments_json,
                        } => {
                            let name = started_tools
                                .iter()
                                .find(|(id, _)| *id == tool_call_id)
                                .map(|(_, n)| n.clone())
                                .expect("complete only after start");
                            let args: serde_json::Value =
                                serde_json::from_str(&arguments_json).expect("complete args parse");
                            completed_tools.insert(tool_call_id.clone());
                            obs.tool_calls.push((tool_call_id, name, args));
                        }
                        NormalizedEvent::Usage(usage) => obs.usage = Some(usage),
                        NormalizedEvent::Finished { finish_reason } => {
                            obs.finish_reason = Some(finish_reason)
                        }
                        NormalizedEvent::ProviderError { error_kind, .. } => {
                            obs.error_kind = Some(error_kind)
                        }
                        NormalizedEvent::ToolCallArgumentsDelta { .. } => {}
                    }
                }
            }
            StepOutcome::Quarantined { event_type } => obs.quarantined_types.push(event_type),
            StepOutcome::Defect(defect) => match &poisoned {
                None => {
                    poisoned = Some(defect.clone());
                    obs.defect = Some(defect);
                }
                // The latch must return the FIRST defect verbatim, forever.
                Some(first) => assert_eq!(
                    &defect, first,
                    "{fixture_id}: poison latch returned a different defect"
                ),
            },
        }
        // Cancel is observed at the END of its tick: the same-tick chunk
        // landed (it was already in flight), nothing after it may.
        if tick.cancel {
            obs.cancel_observed = true;
            break;
        }
    }
    obs.quarantined = normalizer.quarantined_count();
    if obs.cancel_observed {
        // Started-but-incomplete tool calls resolve to cancelled — never
        // silently dropped (cancellation_mid_tool_call row).
        obs.cancelled_tool_calls = started_tools
            .iter()
            .map(|(id, _)| id.clone())
            .filter(|id| !completed_tools.contains(id))
            .collect();
        // Interrupted is consumer-assigned, and only when the wire terminal
        // did not land in the cancel tick — the race row keeps its single
        // natural terminal.
        if obs.finish_reason.is_none() && obs.error_kind.is_none() {
            obs.finish_reason = Some(FinishReason::Interrupted);
        }
    }
    assert_stream_grammar(&fixture_id, &obs, &normalizer);
    obs
}

/// The normalized grammar from provider/mod.rs, held for every fixture:
/// StreamStart only first, at most one terminal, nothing after it, Usage
/// before Finished, and `is_terminal` telling the truth.
fn assert_stream_grammar(fixture_id: &str, obs: &TurnObservation, normalizer: &Normalizer) {
    let is_start = |e: &NormalizedEvent| matches!(e, NormalizedEvent::StreamStart { .. });
    let is_terminal = |e: &NormalizedEvent| {
        matches!(
            e,
            NormalizedEvent::Finished { .. } | NormalizedEvent::ProviderError { .. }
        )
    };
    let starts = obs.events.iter().filter(|e| is_start(e)).count();
    assert!(starts <= 1, "{fixture_id}: more than one StreamStart");
    if starts == 1 {
        assert!(
            is_start(&obs.events[0]),
            "{fixture_id}: StreamStart is not the first event"
        );
    }
    let terminals: Vec<usize> = obs
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| is_terminal(e))
        .map(|(i, _)| i)
        .collect();
    assert!(
        terminals.len() <= 1,
        "{fixture_id}: double terminal at events {terminals:?}"
    );
    if let Some(&t) = terminals.first() {
        assert_eq!(
            t,
            obs.events.len() - 1,
            "{fixture_id}: events after the terminal"
        );
    }
    if let (Some(u), Some(&f)) = (
        obs.events
            .iter()
            .position(|e| matches!(e, NormalizedEvent::Usage(_))),
        terminals.first(),
    ) {
        assert!(u < f, "{fixture_id}: Usage after the terminal");
    }
    assert_eq!(
        normalizer.is_terminal(),
        !terminals.is_empty(),
        "{fixture_id}: is_terminal disagrees with the emitted events"
    );
}

/// Drives every turn under the kernel retry rule: retry only a transient
/// provider error, and never past the ceiling.
fn run_fixture(fixture: &Fixture) -> Vec<TurnObservation> {
    let mut provider = FakeProvider::new(fixture.clone());
    let mut per_turn = Vec::new();
    for t in 0..fixture.turns.len() {
        let mut last;
        loop {
            last = run_turn(&mut provider, t);
            let transient = last.error_kind.map(|k| k.transient()).unwrap_or(false);
            if !transient || provider.attempts_served(t) >= RETRY_CEILING {
                break;
            }
        }
        last.attempts_served = provider.attempts_served(t) as u64;
        per_turn.push(last);
    }
    per_turn
}

#[test]
fn catalog_covers_every_row_in_both_dialects() {
    let catalog = load_catalog().expect("catalog decodes");
    for dialect in [
        oa_kernel::provider::Dialect::AnthropicMessages,
        oa_kernel::provider::Dialect::Responses,
    ] {
        let rows: BTreeSet<&str> = catalog
            .iter()
            .filter(|f| f.dialect == dialect)
            .map(|f| f.scenario_id.as_str())
            .collect();
        let expected: BTreeSet<&str> = SCENARIO_ROWS.iter().copied().collect();
        assert_eq!(rows, expected, "scenario coverage for {dialect:?}");

        // The typed_error row demands one fixture per error class.
        let variants: BTreeSet<&str> = catalog
            .iter()
            .filter(|f| f.dialect == dialect && f.scenario_id == "typed_error")
            .map(|f| f.fixture_id.as_str())
            .collect();
        let expected_variants: BTreeSet<&str> = [
            "typed_error_rate_limit",
            "typed_error_invalid_request",
            "typed_error_context_length_exceeded",
            "typed_error_server_error",
            "typed_error_unsupported_capability",
        ]
        .into_iter()
        .collect();
        assert_eq!(
            variants, expected_variants,
            "typed_error variants for {dialect:?}"
        );
    }
}

#[test]
fn catalog_files_match_the_fixtures_directory() {
    // A fixture on disk but absent from CATALOG_FILES would ship untested
    // with no signal; deletion is caught at compile time by include_str!.
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    let mut on_disk = BTreeSet::new();
    for dialect_dir in std::fs::read_dir(&root).expect("fixtures dir") {
        let dialect_dir = dialect_dir.unwrap().path();
        for file in std::fs::read_dir(&dialect_dir).expect("dialect dir") {
            let path = file.unwrap().path();
            on_disk.insert(format!(
                "{}/{}",
                dialect_dir.file_name().unwrap().to_str().unwrap(),
                path.file_name().unwrap().to_str().unwrap()
            ));
        }
    }
    let registered: BTreeSet<String> = CATALOG_FILES.iter().map(|(p, _)| (*p).to_owned()).collect();
    assert_eq!(on_disk, registered, "fixtures on disk vs CATALOG_FILES");
}

#[test]
fn control_actions_reference_scripted_chunks() {
    for fixture in load_catalog().expect("catalog decodes") {
        for action in &fixture.control {
            let turn = fixture
                .turns
                .get(action.turn)
                .unwrap_or_else(|| panic!("{}: control names missing turn", fixture.fixture_id));
            for attempt in &turn.attempts {
                assert!(
                    action.at_chunk < attempt.chunks.len(),
                    "{}: control at_chunk {} past attempt length {}",
                    fixture.fixture_id,
                    action.at_chunk,
                    attempt.chunks.len()
                );
            }
        }
    }
}

#[test]
fn every_fixture_meets_its_declared_expectations() {
    for fixture in load_catalog().expect("catalog decodes") {
        let observed = run_fixture(&fixture);
        for (t, (obs, expect)) in observed.iter().zip(&fixture.expect).enumerate() {
            let ctx = format!("{} [{:?}] turn {t}", fixture.fixture_id, fixture.dialect);
            if let Some(text) = &expect.text {
                assert_eq!(&obs.text, text, "{ctx}: text");
            }
            if let Some(calls) = &expect.tool_calls {
                let got: Vec<_> = obs
                    .tool_calls
                    .iter()
                    .map(|(id, name, args)| (id.as_str(), name.as_str(), args))
                    .collect();
                let want: Vec<_> = calls
                    .iter()
                    .map(|c| (c.tool_call_id.as_str(), c.tool_name.as_str(), &c.arguments))
                    .collect();
                assert_eq!(got, want, "{ctx}: tool calls");
            }
            if let Some(cancelled) = &expect.cancelled_tool_calls {
                assert_eq!(
                    &obs.cancelled_tool_calls, cancelled,
                    "{ctx}: cancelled tool calls"
                );
            }
            if let Some(reason) = expect.finish_reason {
                assert_eq!(obs.finish_reason, Some(reason), "{ctx}: finish reason");
            }
            if let Some(usage) = expect.usage {
                assert_eq!(obs.usage, Some(usage), "{ctx}: usage");
            }
            if let Some(kind) = expect.error_kind {
                assert_eq!(obs.error_kind, Some(kind), "{ctx}: error kind");
            }
            match expect.defect.as_deref() {
                Some("malformed") => {
                    assert!(
                        matches!(obs.defect, Some(StreamDefect::Malformed { .. })),
                        "{ctx}: expected malformed defect, got {:?}",
                        obs.defect
                    );
                }
                Some("ordering_violation") => {
                    assert!(
                        matches!(obs.defect, Some(StreamDefect::OrderingViolation { .. })),
                        "{ctx}: expected ordering violation, got {:?}",
                        obs.defect
                    );
                }
                Some(other) => panic!("{ctx}: unknown expected defect {other:?}"),
                None => {
                    assert_eq!(obs.defect, None, "{ctx}: unexpected stream defect");
                }
            }
            if let Some(n) = expect.quarantined {
                assert_eq!(obs.quarantined, n, "{ctx}: quarantined count");
            }
            if let Some(types) = &expect.quarantined_types {
                assert_eq!(&obs.quarantined_types, types, "{ctx}: quarantined types");
            }
            if let Some(model) = &expect.served_model_id {
                assert_eq!(
                    obs.served_model_id.as_ref(),
                    Some(model),
                    "{ctx}: served model"
                );
            }
            if let Some(evidence) = expect.served_model_evidence {
                assert_eq!(
                    obs.served_model_evidence,
                    Some(evidence),
                    "{ctx}: served model evidence"
                );
            }
            if let Some(attempts) = expect.attempts {
                assert_eq!(obs.attempts_served, attempts, "{ctx}: attempts served");
            }
        }
    }
}

#[test]
fn retry_ceiling_never_requests_the_extra_scripted_attempt() {
    for fixture in load_catalog().expect("catalog decodes") {
        if fixture.scenario_id != "retry_ceiling" {
            continue;
        }
        assert!(
            fixture.turns[0].attempts.len() > RETRY_CEILING,
            "{}: the fixture must script past the ceiling to prove restraint",
            fixture.fixture_id
        );
        let mut provider = FakeProvider::new(fixture.clone());
        let mut last = run_turn(&mut provider, 0);
        while last.error_kind.map(|k| k.transient()).unwrap_or(false)
            && provider.attempts_served(0) < RETRY_CEILING
        {
            last = run_turn(&mut provider, 0);
        }
        assert_eq!(
            provider.attempts_served(0),
            RETRY_CEILING,
            "{}: attempts requested",
            fixture.fixture_id
        );
        assert_eq!(
            last.error_kind,
            Some(ProviderErrorKind::ServerError),
            "{}: ceiling exit must carry the terminal typed error",
            fixture.fixture_id
        );
    }
}

#[test]
fn replay_is_deterministic() {
    for fixture in load_catalog().expect("catalog decodes") {
        let first = run_fixture(&fixture);
        let second = run_fixture(&fixture);
        assert_eq!(first, second, "{}: replay diverged", fixture.fixture_id);
    }
}
