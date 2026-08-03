//! EXP-001 gate harness (G02): Selene fan-in and crash recovery.
//!
//! One binary, three roles. `run` orchestrates cells: it spawns this same
//! binary as `worker` processes, kills them at the cell's injection point with
//! SIGKILL (an in-process panic would let destructors and buffered writes run
//! — mercy a real crash doesn't grant), reopens the store, and audits.
//! `audit` alone never writes.
//!
//! Kill orchestration is not wired yet: `run`/`worker`/`audit` exit with an
//! honest error until it lands. `plan` and `manifest` are complete and
//! deterministic; the funnel and the non-kill cells live in the library and
//! run under `cargo test`.

use clap::{Parser, Subcommand};
use exp001_harness::plan::trial_seed;
use exp001_harness::schema::{Arm, Cell};
use exp001_harness::manifest::Manifest;

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
    /// Enumerate the full trial plan for a seed: every (arm, cell, rep) with
    /// its derived per-trial seed. Same seed, same plan, byte for byte.
    Plan {
        #[arg(long, default_value_t = 0)]
        seed: u64,
        /// R25c default: 20 repetitions per cell.
        #[arg(long, default_value_t = 20)]
        reps: u32,
    },
    /// Execute the gate (orchestrator role). Not wired yet.
    Run,
    /// Internal: workload process spawned and killed by `run`.
    Worker,
    /// Reopen a store directory and score the HARD bars against it.
    Audit,
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
        Cmd::Run | Cmd::Worker | Cmd::Audit => {
            eprintln!(
                "not wired: kill orchestration pending (OA Queue: EXP-001 harness implementation)"
            );
            std::process::exit(2);
        }
    }
}
