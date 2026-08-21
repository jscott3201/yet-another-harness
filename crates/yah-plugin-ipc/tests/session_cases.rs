//! The named boundary cases of the session reference model: the
//! protocol's load-bearing behaviors, each pinned as an explicit trace
//! the model and production must agree on. The generated corpus and the
//! replayable pinned traces live in `session_model.rs`.

#[path = "support/adapter.rs"]
mod adapter;
#[path = "support/model.rs"]
mod model;
#[path = "support/model_facts.rs"]
mod model_facts;
#[path = "support/model_host.rs"]
mod model_host;

use adapter::Adapter;
use model::ModelSession;
use model_facts::{Action, EventFact, Kind, WOutcome};

fn compare_trace(name: &str, budget: Option<u64>, actions: &[Action]) -> Result<(), String> {
    let mut model = ModelSession::new(budget);
    let mut adapter = Adapter::new(budget);
    for (step, action) in actions.iter().enumerate() {
        model.apply(action);
        adapter.apply(action);
        let adapter_err = adapter.pending_err();
        let (model_wire, model_events) = model.collect();
        let (actual_wire, mut actual_events) = adapter.drain();
        if let Some(err_name) = adapter_err {
            actual_events.push(EventFact::AppErr(err_name));
        }
        let mismatch = |what: String| -> String {
            format!(
                "{name} diverged at step {step} ({what})\naction: {:?}\ntrace json: {}\nmodel wire: {model_wire:?}\nactual wire: {actual_wire:?}\nmodel events: {model_events:?}\nactual events: {actual_events:?}",
                action,
                serde_json::to_string(&actions).expect("traces serialize"),
            )
        };
        if model_wire != actual_wire {
            return Err(mismatch("queued frames".into()));
        }
        if model_events != actual_events {
            return Err(mismatch("events".into()));
        }
        let model_gauges = (
            model.closed(),
            model.live_handles(),
            model.retired_operations(),
            model.in_flight(),
            model.pending_releases(),
        );
        if model_gauges != adapter.gauges() {
            return Err(mismatch(format!("gauges {model_gauges:?}")));
        }
    }
    Ok(())
}

fn case(name: &str, budget: Option<u64>, actions: Vec<Action>) {
    if let Err(divergence) = compare_trace(name, budget, &actions) {
        panic!("{divergence}");
    }
}

fn hello() -> Action {
    Action::Hello
}

#[test]
fn named_cases_match_the_model() {
    // Legal handshake, one call each way, clean goodbye.
    case(
        "handshake_calls_goodbye",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
            Action::AnswerWorkerCall {
                id: 1,
                outcome: WOutcome::Ok,
            },
            Action::Goodbye,
        ],
    );
    // A second hello is fatal.
    case(
        "second_hello_is_fatal",
        None,
        vec![
            hello(),
            Action::HelloAgain,
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
        ],
    );
    // Unknown required feature fails closed at negotiation.
    case(
        "unknown_required_feature",
        None,
        vec![Action::HelloUnknownRequired],
    );
    // A decodable non-hello first frame is refused by name.
    case("non_hello_first_frame", None, vec![Action::NonHelloFirst]);
    // Host and worker id spaces are independent: id 1 in flight in both
    // directions at once, settled independently.
    case(
        "independent_id_spaces",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
            Action::AnswerWorkerCall {
                id: 1,
                outcome: WOutcome::Ok,
            },
        ],
    );
    // A duplicate in-flight worker call id is fatal; a late terminal for
    // a settled host call is a tolerated race.
    case(
        "duplicate_and_late_terminal",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 2,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            Action::WorkerCall {
                id: 2,
                stream: false,
            },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
        ],
    );
    // A terminal racing local deadline settlement: the deadline decides,
    // the late terminal is tolerated, the session lives.
    case(
        "terminal_races_deadline",
        None,
        vec![
            hello(),
            Action::HostCall {
                deadline_ms: Some(10),
            },
            Action::Tick { now_ms: 11 },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
        ],
    );
    // The lifetime bound: at the budget new admissions are refused and no
    // id is spent; one past it the same holds while already-admitted work
    // still completes.
    case(
        "lifetime_bound_exact_and_one_past",
        Some(2),
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::AnswerWorkerCall {
                id: 1,
                outcome: WOutcome::Ok,
            },
            Action::WorkerCall {
                id: 2,
                stream: false,
            },
            Action::AnswerWorkerCall {
                id: 2,
                outcome: WOutcome::Ok,
            },
            Action::WorkerCall {
                id: 3,
                stream: false,
            },
            Action::WorkerCall {
                id: 4,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            Action::Mint { id: 3 },
        ],
    );
    // Stream order family: data before open, second open, sequence gap,
    // data after the final item.
    case(
        "stream_order_family",
        None,
        vec![
            hello(),
            Action::HostCall { deadline_ms: None },
            Action::StreamData {
                id: 1,
                seq: 0,
                more: true,
                lossless: true,
                dropped: 0,
            },
            Action::StreamOpen { id: 1, credit: 2 },
            Action::StreamOpen { id: 1, credit: 2 },
            Action::StreamData {
                id: 1,
                seq: 0,
                more: true,
                lossless: true,
                dropped: 0,
            },
            Action::StreamData {
                id: 1,
                seq: 5,
                more: true,
                lossless: true,
                dropped: 0,
            },
            Action::StreamData {
                id: 1,
                seq: 1,
                more: false,
                lossless: true,
                dropped: 0,
            },
            Action::StreamData {
                id: 1,
                seq: 2,
                more: false,
                lossless: true,
                dropped: 0,
            },
        ],
    );
    // Credit family: zero credit at open, widening to the exact ceiling
    // and one past, lossless overdraw, lossy drop monotonicity.
    case(
        "credit_family",
        None,
        vec![
            hello(),
            Action::HostCall { deadline_ms: None },
            Action::StreamOpen { id: 1, credit: 0 },
            Action::StreamOpen {
                id: 1,
                credit: 1024,
            },
            Action::Credit {
                id: 1,
                additional: 0,
            },
            Action::Credit {
                id: 1,
                additional: 1,
            },
            Action::StreamData {
                id: 1,
                seq: 0,
                more: true,
                lossless: true,
                dropped: 3,
            },
            Action::StreamData {
                id: 1,
                seq: 1,
                more: true,
                lossless: true,
                dropped: 2,
            },
            Action::StreamData {
                id: 1,
                seq: 2,
                more: true,
                lossless: true,
                dropped: 2,
            },
            Action::Credit {
                id: 1,
                additional: 1023,
            },
            Action::Credit {
                id: 1,
                additional: 1024,
            },
            Action::StreamData {
                id: 1,
                seq: 3,
                more: true,
                lossless: true,
                dropped: 2,
            },
            Action::StreamData {
                id: 1,
                seq: 4,
                more: true,
                lossless: true,
                dropped: 2,
            },
        ],
    );
    // Handle family: never-held, wrong kind, double release,
    // unsolicited and wrong-kind acks, release crossing reclaim exactly
    // once, repeated worker offer ids.
    case(
        "handle_family",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::Mint { id: 1 },
            Action::Release {
                handle: 9,
                kind: Kind::Capability,
            },
            Action::Release {
                handle: 1,
                kind: Kind::Artifact,
            },
            Action::Release {
                handle: 1,
                kind: Kind::Capability,
            },
            Action::Release {
                handle: 1,
                kind: Kind::Capability,
            },
            Action::ReleaseAck {
                handle: 7,
                kind: Kind::Artifact,
            },
            // A worker call whose err terminal reclaims its mint, then a
            // release racing that reclaim: acked once, fatal twice.
            Action::WorkerCall {
                id: 2,
                stream: false,
            },
            Action::Mint { id: 2 },
            Action::AnswerWorkerCall {
                id: 2,
                outcome: WOutcome::ErrInternal,
            },
            Action::Release {
                handle: 2,
                kind: Kind::Capability,
            },
            Action::Release {
                handle: 2,
                kind: Kind::Capability,
            },
        ],
    );
    // Repeated worker offer id: the second spilled offer naming a spent
    // handle id is fatal.
    case(
        "repeated_worker_offer_id",
        None,
        vec![
            hello(),
            Action::HostCall { deadline_ms: None },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 3,
                    bytes: 10,
                },
            },
            Action::HostCall { deadline_ms: None },
            Action::WorkerReply {
                id: 2,
                outcome: WOutcome::Spilled {
                    handle: 3,
                    bytes: 10,
                },
            },
        ],
    );
    // Artifact read ranges: unknown handle, wrong kind, zero length.
    case(
        "artifact_read_ranges",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::OfferArtifact { id: 1, bytes: 8 },
            Action::ArtifactRead {
                id: 2,
                handle: 9,
                ok_range: true,
            },
            Action::ArtifactRead {
                id: 2,
                handle: 1,
                ok_range: false,
            },
            Action::ArtifactRead {
                id: 2,
                handle: 1,
                ok_range: true,
            },
        ],
    );
    // Loss classification: goodbye settles in-flight work cancelled
    // without reconciliation; a clean disconnect settles it
    // outcome-unknown with reconciliation required; a disconnect after a
    // fatal changes nothing.
    case(
        "goodbye_vs_disconnect",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            Action::Goodbye,
        ],
    );
    case(
        "disconnect_is_outcome_unknown",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            Action::Eof,
        ],
    );
    // Host-side stream production: open, items, credit spend, the
    // worker's window, and drops riding the next item.
    case(
        "host_stream_production",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: true,
            },
            Action::HostOpenStream { id: 1, credit: 2 },
            Action::HostStreamItem {
                id: 1,
                lossless: true,
                more: true,
            },
            Action::HostNoteDrops { id: 1, dropped: 4 },
            Action::HostStreamItem {
                id: 1,
                lossless: true,
                more: true,
            },
            Action::HostStreamItem {
                id: 1,
                lossless: false,
                more: false,
            },
            Action::HostStreamItem {
                id: 1,
                lossless: false,
                more: false,
            },
            Action::AnswerWorkerCall {
                id: 1,
                outcome: WOutcome::Ok,
            },
        ],
    );
    // Worker-side streaming into a host call, muted by a stream cancel:
    // items still validate and spend, delivery stops, terminal lands.
    case(
        "stream_cancel_mutes_delivery",
        None,
        vec![
            hello(),
            Action::HostCall { deadline_ms: None },
            Action::StreamOpen { id: 1, credit: 4 },
            Action::HostCancel {
                id: 1,
                target_call: false,
            },
            Action::StreamData {
                id: 1,
                seq: 0,
                more: true,
                lossless: true,
                dropped: 0,
            },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
        ],
    );
    // Host release of a worker-offered handle: pending, ack, spent;
    // double release is the application's bug to hear about.
    case(
        "host_release_lifecycle",
        None,
        vec![
            hello(),
            Action::HostCall { deadline_ms: None },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 5,
                    bytes: 10,
                },
            },
            Action::HostRelease {
                handle: 5,
                kind: Kind::Artifact,
            },
            Action::HostRelease {
                handle: 5,
                kind: Kind::Artifact,
            },
            Action::ReleaseAck {
                handle: 5,
                kind: Kind::Artifact,
            },
            Action::HostRelease {
                handle: 5,
                kind: Kind::Artifact,
            },
        ],
    );
    // Cancel targets: call cancel is advisory and delivered; a cancel
    // naming an unknown worker call is silent.
    case(
        "cancel_targets",
        None,
        vec![
            hello(),
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::WorkerCancel {
                id: 1,
                target_call: true,
            },
            Action::WorkerCancel {
                id: 9,
                target_call: true,
            },
            Action::AnswerWorkerCall {
                id: 1,
                outcome: WOutcome::Cancelled,
            },
        ],
    );
}

#[test]
fn named_budget_cases_match_the_model() {
    // Late spilled offers riding a settled host call are the one
    // worker-input path that mints a new correlation entry per frame:
    // below the budget each spends its fresh handle id; at the budget
    // the spend is refused, the race stays tolerated, and the offer's
    // handle is not remembered — so a second late offer naming the same
    // fresh handle is also tolerated, while a repeat of a REMEMBERED
    // handle is still the reuse fault.
    case(
        "late_offers_stop_spending_at_the_budget",
        Some(3),
        vec![
            Action::Hello,
            Action::HostCall { deadline_ms: None },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Ok,
            },
            // Retired operations: 1 (the settled host call). Two late
            // offers spend ids 2 and 3, reaching the budget.
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 2,
                    bytes: 10,
                },
            },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 3,
                    bytes: 10,
                },
            },
            // At the budget: fresh handles are not remembered, and the
            // same handle twice is equally tolerated.
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 4,
                    bytes: 10,
                },
            },
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 4,
                    bytes: 10,
                },
            },
            // A repeat of a remembered handle is still the reuse fault.
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 2,
                    bytes: 10,
                },
            },
        ],
    );
    // The offer and release SessionRetired gates: at the budget the host
    // application cannot mint new artifact memory or ask for new
    // worker-side retirements.
    case(
        "host_admissions_stop_at_the_budget",
        Some(3),
        vec![
            Action::Hello,
            // A worker call stays in flight across the budget boundary,
            // so the offer/mint gates are reachable after it fills.
            Action::WorkerCall {
                id: 1,
                stream: false,
            },
            Action::HostCall { deadline_ms: None },
            // The spilled terminal retires two entries: the host call id
            // and the offered handle (2 of the 3-slot budget).
            Action::WorkerReply {
                id: 1,
                outcome: WOutcome::Spilled {
                    handle: 2,
                    bytes: 10,
                },
            },
            Action::WorkerCall {
                id: 2,
                stream: false,
            },
            // The answer fills the budget (3 of 3).
            Action::AnswerWorkerCall {
                id: 1,
                outcome: WOutcome::Ok,
            },
            // Every new host admission is now SessionRetired: the offer
            // and mint on the still-in-flight call, the release of the
            // remembered worker handle, and a new host call.
            Action::OfferArtifact { id: 2, bytes: 8 },
            Action::Mint { id: 2 },
            Action::HostRelease {
                handle: 2,
                kind: Kind::Artifact,
            },
            Action::HostCall { deadline_ms: None },
        ],
    );
}
