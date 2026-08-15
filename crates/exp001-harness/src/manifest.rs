//! Run manifest (EXP-001 §3): record, do not assume.
//!
//! Every gate run embeds this in its artifact directory. Numbers without a
//! manifest are not portable evidence — the card compares against a recorded
//! reference machine (Apple M5, 10 cores, 16 GiB, rustc 1.95.0) and requires
//! the actual run machine alongside.

use serde::{Deserialize, Serialize};
use std::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub selene_db_sha: String,
    // Preserve the original EXP-001 evidence key across the project rename.
    #[serde(rename = "open_agent_sha")]
    pub harness_sha: String,
    pub rustc_version: String,
    pub cpu_model: String,
    pub physical_cores: String,
    pub logical_cores: String,
    pub ram_bytes: String,
    pub os_build: String,
    pub seed: u64,
    /// R25c: kill-cell repetitions (default 20) and latency samples per arm
    /// (default 50), recorded so a run with overridden values is
    /// distinguishable from a defaults run.
    pub reps_per_cell: u32,
    pub samples_per_arm: u32,
}

/// A probe that fails records "unrecorded", never a guess (§3). The gate
/// scorer treats "unrecorded" machine fields as blocking for MEASURED metrics
/// and irrelevant for HARD-bar correctness cells.
fn probe(cmd: &str, args: &[&str]) -> String {
    Command::new(cmd)
        .args(args)
        .output()
        .ok()
        .filter(|o| o.status.success())
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unrecorded".to_string())
}

/// HEAD SHA with an explicit `-dirty` suffix when the tree has uncommitted
/// changes — a bare SHA over a dirty tree is a false provenance claim.
fn git_head(dir: &str) -> String {
    let sha = probe("git", &["-C", dir, "rev-parse", "HEAD"]);
    if sha == "unrecorded" {
        return sha;
    }
    let status = probe("git", &["-C", dir, "status", "--porcelain"]);
    if status == "unrecorded" || status.is_empty() {
        sha
    } else {
        format!("{sha}-dirty")
    }
}

impl Manifest {
    pub fn capture(
        selene_dir: &str,
        harness_dir: &str,
        seed: u64,
        reps_per_cell: u32,
        samples_per_arm: u32,
    ) -> Manifest {
        Manifest {
            selene_db_sha: git_head(selene_dir),
            harness_sha: git_head(harness_dir),
            rustc_version: probe("rustc", &["--version"]),
            // sysctl keys are macOS-specific; the gate machine is a Mac per
            // R25c ("actual daemon machine"). Other hosts record "unrecorded".
            cpu_model: probe("sysctl", &["-n", "machdep.cpu.brand_string"]),
            physical_cores: probe("sysctl", &["-n", "hw.physicalcpu"]),
            logical_cores: probe("sysctl", &["-n", "hw.logicalcpu"]),
            ram_bytes: probe("sysctl", &["-n", "hw.memsize"]),
            os_build: probe("sw_vers", &["-buildVersion"]),
            seed,
            reps_per_cell,
            samples_per_arm,
        }
    }
}
