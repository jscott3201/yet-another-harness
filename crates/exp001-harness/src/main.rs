//! EXP-001 gate harness (G02): Selene fan-in and crash recovery.
//!
//! One binary, three roles. `run` orchestrates cells: it spawns this same
//! binary as `worker` processes, kills them at the cell's injection point with
//! SIGKILL (an in-process panic would let destructors and buffered writes run
//! — mercy a real crash doesn't grant), reopens the store, and audits.
//! `audit` alone never writes.
//!
//! Store integration is not wired yet: `run`/`worker`/`audit` exit with an
//! honest error until the selene-db dependency lands. `plan` and `manifest`
//! are complete and deterministic.

mod manifest;
mod schema;

use clap::{Parser, Subcommand};
use schema::{Arm, Cell};

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
    /// Execute the gate (orchestrator role). Not wired until selene-db lands.
    Run,
    /// Internal: workload process spawned and killed by `run`.
    Worker,
    /// Reopen a store directory and score the HARD bars against it.
    Audit,
}

/// Per-trial seeds derive from the root seed and the trial's identity, so one
/// failing trial replays alone without re-running its predecessors (§10's
/// replay-exactly obligation).
fn trial_seed(root: u64, arm: &Arm, cell: Cell, rep: u32) -> u64 {
    let key = format!("{root}/{}/{:?}/{rep}", arm.label(), cell);
    let hash = blake3::hash(key.as_bytes());
    u64::from_le_bytes(hash.as_bytes()[..8].try_into().unwrap())
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Manifest { selene_dir, open_agent_dir, seed } => {
            let m = manifest::Manifest::capture(&selene_dir, &open_agent_dir, seed, 20, 50);
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
            eprintln!("{trials} trials ({} arms x {} cells x {reps} reps)", Arm::ALL.len(), Cell::all().count());
        }
        Cmd::Run | Cmd::Worker | Cmd::Audit => {
            eprintln!(
                "not wired: selene-db integration pending (OA Queue: EXP-001 harness implementation)"
            );
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trial_seeds_are_stable_and_distinct() {
        let arm = Arm::ALL[0];
        let a = trial_seed(7, &arm, Cell::Kill(schema::KillPoint::PreSeal), 0);
        let b = trial_seed(7, &arm, Cell::Kill(schema::KillPoint::PreSeal), 0);
        let c = trial_seed(7, &arm, Cell::Kill(schema::KillPoint::PreSeal), 1);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
