//! The session reference model, compared against production over
//! deterministic generated traces and named boundary cases.
//!
//! Every action is applied to both the model (`support/model.rs`, written
//! from the protocol doc) and a real [`HostSession`] (through
//! `support/adapter.rs`), and the observable facts must agree after every
//! step: queued frame classes, event classes, and the public gauges. A
//! mismatch names the step, the action, and both fact vectors; its action
//! list serializes to JSON and pins under `tests/corpus/session_traces/`
//! as an ordinary replayable regression.

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

/// Apply the trace to both sides, comparing observable facts after every
/// action. The error message carries the full JSON trace so a failure is
/// replayable without this harness.
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

fn hello() -> Action {
    Action::Hello
}

/// xorshift64*: the generator's whole randomness, seeded deterministically
/// so the corpus is reproducible from the pinned seeds.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// Generate one bounded trace. Ids stay in tiny spaces so collisions —
/// the interesting cases — happen often; roughly a third of traces carry
/// a small correlation budget so the bound is exercised against arbitrary
/// orderings, not just the named cases.
fn generate(rng: &mut Rng) -> (Option<u64>, Vec<Action>) {
    let budget = if rng.below(3) == 0 {
        Some(4 + rng.below(20))
    } else {
        None
    };
    let mut actions = vec![hello()];
    let mut worker_ids: Vec<u64> = Vec::new();
    let mut next_worker_id = 1u64;
    let mut offered: Vec<u64> = Vec::new();
    let mut minted: Vec<u64> = Vec::new();
    let mut host_calls: Vec<u64> = Vec::new();
    let mut next_handle = 1u64;
    let mut now = 0u64;
    let len = 8 + rng.below(33);
    for _ in 0..len {
        let pick = rng.below(20);
        let action = match pick {
            0..=3 => {
                let id = next_worker_id;
                next_worker_id += 1;
                worker_ids.push(id);
                Action::WorkerCall {
                    id,
                    stream: rng.below(2) == 0,
                }
            }
            4..=6 => {
                if worker_ids.is_empty() {
                    Action::Tick { now_ms: now }
                } else {
                    let id = worker_ids[rng.below(worker_ids.len() as u64) as usize];
                    let outcome = match rng.below(5) {
                        0 => WOutcome::Ok,
                        1 => WOutcome::ErrInternal,
                        2 => WOutcome::Cancelled,
                        3 => WOutcome::ErrUnknownCall,
                        _ => {
                            let handle = next_handle;
                            next_handle += 1;
                            offered.push(handle);
                            WOutcome::Spilled { handle, bytes: 10 }
                        }
                    };
                    Action::WorkerReply { id, outcome }
                }
            }
            7..=8 => {
                let id = 1 + rng.below(6);
                if rng.below(2) == 0 && !host_calls.is_empty() {
                    let host_id = host_calls[rng.below(host_calls.len() as u64) as usize];
                    Action::StreamOpen {
                        id: host_id,
                        credit: 1 + rng.below(4) as u32,
                    }
                } else {
                    Action::StreamOpen {
                        id,
                        credit: rng.below(6) as u32,
                    }
                }
            }
            9..=10 => {
                let id = 1 + rng.below(6);
                Action::StreamData {
                    id,
                    seq: rng.below(4),
                    more: rng.below(4) != 0,
                    lossless: rng.below(2) == 0,
                    dropped: rng.below(4),
                }
            }
            11 => match rng.below(3) {
                0 => Action::ArtifactRead {
                    id: 1 + rng.below(6),
                    handle: 1 + rng.below(if next_handle > 1 { next_handle } else { 2 }),
                    ok_range: rng.below(2) == 0,
                },
                _ => {
                    let id = 1 + rng.below(6);
                    Action::Credit {
                        id,
                        additional: rng.below(6) as u32,
                    }
                }
            },
            12 => {
                if offered.is_empty() {
                    Action::Tick { now_ms: now }
                } else {
                    let handle = offered[rng.below(offered.len() as u64) as usize];
                    Action::HostRelease {
                        handle,
                        kind: if rng.below(4) == 0 {
                            Kind::Capability
                        } else {
                            Kind::Artifact
                        },
                    }
                }
            }
            13 => {
                let handle = 1 + rng.below(if next_handle > 1 { next_handle } else { 2 });
                Action::ReleaseAck {
                    handle,
                    kind: if rng.below(4) == 0 {
                        Kind::Capability
                    } else {
                        Kind::Artifact
                    },
                }
            }
            14 => {
                let handle = 1 + rng.below(if next_handle > 1 { next_handle } else { 2 });
                Action::Release {
                    handle,
                    kind: if rng.below(4) == 0 {
                        Kind::Capability
                    } else {
                        Kind::Artifact
                    },
                }
            }
            15 => {
                if minted.is_empty() && offered.is_empty() {
                    Action::Tick { now_ms: now }
                } else {
                    let pool = minted
                        .iter()
                        .chain(offered.iter())
                        .copied()
                        .collect::<Vec<_>>();
                    let handle = pool[rng.below(pool.len() as u64) as usize];
                    Action::Release {
                        handle,
                        kind: if rng.below(4) == 0 {
                            Kind::Capability
                        } else {
                            Kind::Artifact
                        },
                    }
                }
            }
            16 => {
                let deadline = if rng.below(2) == 0 {
                    None
                } else {
                    Some((1 + rng.below(20)) as u32)
                };
                host_calls.push(0); // placeholder; the session mints real ids
                Action::HostCall {
                    deadline_ms: deadline,
                }
            }
            17 => {
                now += rng.below(30);
                Action::Tick { now_ms: now }
            }
            18 => {
                if worker_ids.is_empty() {
                    Action::Tick { now_ms: now }
                } else {
                    let id = worker_ids[rng.below(worker_ids.len() as u64) as usize];
                    if rng.below(2) == 0 {
                        worker_ids.retain(|k| *k != id);
                        Action::AnswerWorkerCall {
                            id,
                            outcome: WOutcome::Ok,
                        }
                    } else {
                        let handle = next_handle;
                        next_handle += 1;
                        minted.push(handle);
                        Action::Mint { id }
                    }
                }
            }
            _ => {
                if rng.below(6) == 0 {
                    Action::Goodbye
                } else {
                    Action::Eof
                }
            }
        };
        actions.push(action);
    }
    (budget, actions)
}

const GENERATOR_SEEDS: &[u64] = &[
    0x5eed_0001,
    0x5eed_0002,
    0x5eed_0003,
    0x5eed_0004,
    0x5eed_0005,
    0x5eed_0006,
    0x5eed_0007,
    0x5eed_0008,
    0x5eed_0009,
    0x5eed_000a,
];
const TRACES_PER_SEED: u64 = 250;
/// The generator's own cap; the assertion below pins it via the corpus.
#[expect(dead_code)]
const MAX_ACTIONS_PER_TRACE: usize = 41;

#[test]
fn generated_traces_match_the_model() {
    let mut run = 0usize;
    for seed in GENERATOR_SEEDS {
        let mut rng = Rng::new(*seed);
        for index in 0..TRACES_PER_SEED {
            let (budget, actions) = generate(&mut rng);
            run += 1;
            if let Err(divergence) = compare_trace(
                &format!("generated seed {seed:#x} trace {index}"),
                budget,
                &actions,
            ) {
                panic!(
                    "{divergence}\npromote: save the trace json under \
                     tests/corpus/session_traces/ and add a named regression for the \
                     minimized divergence"
                );
            }
        }
    }
    assert!(
        run >= 2_500,
        "the corpus must not silently shrink: {run} traces"
    );
}

/// Pinned replayable regressions: each file under
/// `tests/corpus/session_traces/` is a serialized trace that once
/// diverged (or pins a boundary), rerun here without the generator.
#[test]
fn pinned_traces_replay_clean() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus/session_traces");
    let mut names: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("trace corpus is checked in")
        .map(|entry| entry.expect("readable").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "json"))
        .collect();
    names.sort();
    assert!(
        names.len() >= 3,
        "the pinned trace corpus must not silently shrink"
    );
    for path in names {
        let raw = std::fs::read_to_string(&path).expect("trace is readable");
        let trace: PinnedTrace = serde_json::from_str(&raw).expect("trace deserializes");
        let name = path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Err(divergence) = compare_trace(&name, trace.budget, &trace.actions) {
            panic!("pinned regression {name} diverged: {divergence}");
        }
    }
}

#[derive(serde::Deserialize)]
struct PinnedTrace {
    budget: Option<u64>,
    actions: Vec<Action>,
}
