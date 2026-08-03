//! EXP-001 gate harness (G02): Selene fan-in and crash recovery.
//!
//! One binary, several roles. `run` orchestrates: it spawns this same binary
//! as `worker` processes, kills them at the cell's injection point with
//! SIGKILL (an in-process panic would let destructors and buffered writes run
//! — mercy a real crash doesn't grant), reopens the store, audits, and
//! classifies. `contend`/`race` are the takeover cell's claimants. `audit`
//! rescoring an existing trial directory never writes to its store.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use exp001_harness::manifest::Manifest;
use exp001_harness::orchestrate::{run_gate, scan_wal, RunConfig};
use exp001_harness::plan::trial_seed;
use exp001_harness::schema::{Arm, Batching, Cell, CommandKind};
use exp001_harness::sidecar::read_confirmed;
use exp001_harness::store::Store;
use exp001_harness::worker::{self, KillDirective, KillMode, WorkerConfig};
use exp001_harness::audit;

#[derive(Parser)]
#[command(name = "exp001", about = "EXP-001 Selene fan-in/recovery gate (G02)")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Print the run manifest that a gate run would embed (EXP-001 §3).
    Manifest {
        #[arg(long, default_value = "/Users/justin/Development/selene-db")]
        selene_dir: String,
        #[arg(long, default_value = ".")]
        open_agent_dir: String,
        #[arg(long, default_value_t = 0)]
        seed: u64,
    },
    /// Enumerate the full trial plan for a seed.
    Plan {
        #[arg(long, default_value_t = 0)]
        seed: u64,
        #[arg(long, default_value_t = 20)]
        reps: u32,
    },
    /// Execute the gate: all arms × cells × reps, or a filtered slice.
    Run {
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 0)]
        seed: u64,
        /// R25c default: 20 repetitions per cell.
        #[arg(long, default_value_t = 20)]
        reps: u32,
        /// Comma-separated arm labels (e.g. "w2-batchOFF,w8-batchON"); empty = all six.
        #[arg(long, default_value = "")]
        arms: String,
        /// Comma-separated cell names (KillPoint or cell variants); empty = all ten.
        #[arg(long, default_value = "")]
        cells: String,
        /// Max sampling attempts per qualifying rep for timing cells.
        #[arg(long, default_value_t = 40)]
        attempt_cap: u32,
    },
    /// Internal: workload process spawned (and killed) by `run`.
    Worker {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        sidecar: PathBuf,
        #[arg(long)]
        trial_seed: u64,
        #[arg(long)]
        writers: u32,
        #[arg(long)]
        batching: String,
        #[arg(long)]
        steps: u32,
        #[arg(long, default_value_t = false)]
        create: bool,
        #[arg(long)]
        kill_mode: Option<String>,
        #[arg(long, default_value_t = 0)]
        kill_at: u64,
        #[arg(long)]
        kill_kind: Option<String>,
        #[arg(long, default_value_t = 0)]
        timer_us: u64,
    },
    /// Internal: live-contention claimant for the takeover cell.
    Contend {
        #[arg(long)]
        dir: PathBuf,
    },
    /// Internal: post-death racing claimant for the takeover cell.
    Race {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        racer: u32,
        #[arg(long, default_value_t = 1500)]
        hold_ms: u64,
    },
    /// Rescore an existing trial directory (never writes to its store).
    Audit {
        #[arg(long)]
        dir: PathBuf,
        #[arg(long)]
        sidecar: PathBuf,
    },
}

fn parse_batching(s: &str) -> Batching {
    match s {
        "on" => Batching::DefaultBound,
        _ => Batching::Off,
    }
}

fn parse_kind(s: &str) -> CommandKind {
    match s {
        "Dispatch" => CommandKind::Dispatch,
        "LeaseRenewal" => CommandKind::LeaseRenewal,
        "ProgressRollup" => CommandKind::ProgressRollup,
        "ToolCompletion" => CommandKind::ToolCompletion,
        "ReviewEvidence" => CommandKind::ReviewEvidence,
        "Cancellation" => CommandKind::Cancellation,
        "OwnerDecision" => CommandKind::OwnerDecision,
        other => panic!("unknown command kind {other}"),
    }
}

fn parse_cells(s: &str) -> Vec<Cell> {
    if s.is_empty() {
        return Cell::all().collect();
    }
    s.split(',')
        .map(|name| {
            Cell::all()
                .find(|c| {
                    let n = match c {
                        Cell::Kill(k) => format!("{k:?}"),
                        other => format!("{other:?}"),
                    };
                    n.eq_ignore_ascii_case(name)
                })
                .unwrap_or_else(|| panic!("unknown cell {name}"))
        })
        .collect()
}

fn parse_arms(s: &str) -> Vec<Arm> {
    if s.is_empty() {
        return Arm::ALL.to_vec();
    }
    s.split(',')
        .map(|label| {
            Arm::ALL
                .into_iter()
                .find(|a| a.label().eq_ignore_ascii_case(label))
                .unwrap_or_else(|| panic!("unknown arm {label}"))
        })
        .collect()
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Manifest { selene_dir, open_agent_dir, seed } => {
            let m = Manifest::capture(&selene_dir, &open_agent_dir, seed, 20, 50);
            println!("{}", serde_json::to_string_pretty(&m).expect("manifest serializes"));
        }
        Cmd::Plan { seed, reps } => {
            let mut trials = 0u32;
            for arm in Arm::ALL {
                for cell in Cell::all() {
                    for rep in 0..reps {
                        println!(
                            "{}\t{:?}\trep{rep}\tseed={:#018x}",
                            arm.label(),
                            cell,
                            trial_seed(seed, &arm, cell, rep)
                        );
                        trials += 1;
                    }
                }
            }
            eprintln!(
                "{trials} trials ({} arms x {} cells x {reps} reps)",
                Arm::ALL.len(),
                Cell::all().count()
            );
        }
        Cmd::Run { out, seed, reps, arms, cells, attempt_cap } => {
            let cfg = RunConfig {
                out,
                root_seed: seed,
                reps,
                arms: parse_arms(&arms),
                cells: parse_cells(&cells),
                attempt_cap,
            };
            match run_gate(&cfg) {
                Ok(summary) => {
                    println!("{}", serde_json::to_string_pretty(&summary).expect("summary"));
                    if summary.failed > 0 || !summary.shortfalls.is_empty() {
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("gate run failed: {e}");
                    std::process::exit(2);
                }
            }
        }
        Cmd::Worker {
            dir, sidecar, trial_seed, writers, batching, steps, create,
            kill_mode, kill_at, kill_kind, timer_us,
        } => {
            let kill = kill_mode.map(|m| KillDirective {
                mode: match m.as_str() {
                    "before-commit" => KillMode::BeforeCommit,
                    "after-commit" => KillMode::AfterCommit,
                    "response-window" => KillMode::ResponseWindow,
                    "timer" => KillMode::Timer,
                    other => panic!("unknown kill mode {other}"),
                },
                at_eligible: kill_at,
                only_kind: kill_kind.as_deref().map(parse_kind),
                timer_us,
            });
            let cfg = WorkerConfig {
                dir,
                sidecar,
                trial_seed,
                writers,
                batching: parse_batching(&batching),
                steps,
                create,
                kill,
            };
            if let Err(e) = worker::run(&cfg) {
                eprintln!("worker: {e}");
                std::process::exit(1);
            }
        }
        Cmd::Contend { dir } => std::process::exit(worker::contend(&dir)),
        Cmd::Race { dir, racer, hold_ms } => std::process::exit(worker::race(&dir, racer, hold_ms)),
        Cmd::Audit { dir, sidecar } => {
            let scan = scan_wal(&Store::wal_path(&dir));
            let confirmed = read_confirmed(&sidecar).expect("sidecar readable");
            let store = Store::recover(&dir).expect("store recovers");
            let report = audit::score(&store.audit_snapshot(), &confirmed);
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "wal": scan, "audit": report,
                }))
                .expect("report serializes")
            );
            if !report.violations.is_empty() {
                std::process::exit(1);
            }
        }
    }
}
