//! MEASURED metrics (EXP-001 §8) — recorded, not gated.
//!
//! Wall-clock is legal here: this is measurement, not spec-stream input, and
//! the trial seeds still come from the plan. A provisional-envelope breach
//! does not fail the gate; it forces an owner ruling before the gate is
//! scored PASS (§8), so the verdict here is `within_envelope`, never
//! pass/fail.
//!
//! Reference points come from selene-db BENCHMARKS.md:1902-1903 — 1000
//! WAL-backed commits (10 updates each) at 1/8/32 threads. Those are totals,
//! not percentiles, and do not model this schema; the card treats them as
//! order-of-magnitude anchors only. The w2 arms borrow the 1-thread anchor
//! (nearest recorded), noted in the output.

use std::path::Path;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::schema::{Arm, Batching};
use crate::store::{ApplyError, Store};
use crate::workload::{UnitPool, WriterStream};

pub const TOTAL_COMMITS: u32 = 1_000;
/// First fraction of samples discarded as warm-up (§9).
const WARMUP_FRACTION: f64 = 0.1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArmMeasurement {
    pub arm: String,
    pub commits: u32,
    pub samples_after_warmup: usize,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub p999_us: u64,
    pub throughput_commits_per_s: f64,
    pub total_s: f64,
    pub recovery_replay_s: f64,
    pub recovered_wal_entries: u64,
    /// 3× the per-commit mean derived from the reference total (µs); the §8
    /// provisional envelope. Reference anchor named alongside.
    pub envelope_p95_us: u64,
    pub reference_anchor: String,
    pub p95_within_envelope: bool,
    pub replay_within_10s: bool,
    /// Queue-wait, lock-hold, fsync and checkpoint internals from
    /// validation/04's item-5 list are not exposed by the selene API at the
    /// pinned SHA — recorded as an honest gap, not fabricated.
    pub internals: &'static str,
}

/// Reference totals in seconds for 1000 commits (BENCHMARKS.md:1902-1903).
fn reference_total_s(writers: u32, batching: Batching) -> (f64, &'static str) {
    match (writers, batching) {
        (2, Batching::Off) => (
            4.71,
            "1-thread batchOFF 4.71s (nearest anchor; no 2-thread row)",
        ),
        (2, Batching::DefaultBound) => (
            4.57,
            "1-thread batchON 4.57s (nearest anchor; no 2-thread row)",
        ),
        (8, Batching::Off) => (3.86, "8-thread batchOFF 3.86s"),
        (8, Batching::DefaultBound) => (0.953, "8-thread batchON 953ms"),
        (32, Batching::Off) => (3.83, "32-thread batchOFF 3.83s"),
        (32, Batching::DefaultBound) => (0.269, "32-thread batchON 269ms"),
        _ => (4.71, "1-thread batchOFF 4.71s (default anchor)"),
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

pub fn measure_arm(dir: &Path, arm: Arm, seed: u64) -> Result<ArmMeasurement, String> {
    let store_dir = dir.join(arm.label());
    std::fs::create_dir_all(&store_dir).map_err(|e| e.to_string())?;
    let store = std::sync::Arc::new(
        Store::create(&store_dir, arm.batching).map_err(|e| format!("create: {e}"))?,
    );
    let pool = std::sync::Arc::new(std::sync::Mutex::new(UnitPool::new(
        (arm.writers.max(2) * 4) as usize,
    )));
    let per_writer = TOTAL_COMMITS / arm.writers;
    let started = Instant::now();

    let mut all_samples: Vec<u64> = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for w in 0..arm.writers {
            let store = std::sync::Arc::clone(&store);
            let pool = std::sync::Arc::clone(&pool);
            handles.push(scope.spawn(move || {
                let mut stream = WriterStream::new(seed, w);
                let mut samples = Vec::with_capacity(per_writer as usize);
                let mut done = 0u32;
                while done < per_writer {
                    let spec = { stream.next_spec(&mut pool.lock().expect("pool")) };
                    let Some(spec) = spec else {
                        std::thread::yield_now();
                        continue;
                    };
                    let t0 = Instant::now();
                    match store.apply(&spec) {
                        Ok(_) => {
                            samples.push(t0.elapsed().as_micros() as u64);
                            pool.lock().expect("pool").release(spec.slot, &spec, true);
                            done += 1;
                        }
                        Err(ApplyError::Rejected(_)) => {
                            // Planned injections still consume wall time but
                            // are not commit samples.
                            pool.lock().expect("pool").release(spec.slot, &spec, false);
                        }
                        Err(e) => panic!("bench apply: {e:?}"),
                    }
                }
                samples
            }));
        }
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("writer"))
            .collect()
    });
    let total_s = started.elapsed().as_secs_f64();
    let commits = all_samples.len() as u32;

    // Warm-up discard is per the §9 convention; samples arrive unordered
    // across writers, so the discard approximates "earliest" by count only.
    let warmup = (all_samples.len() as f64 * WARMUP_FRACTION) as usize;
    all_samples.sort_unstable();
    let kept = &all_samples[warmup..];

    drop(store); // clean close; committer joins.
    let t0 = Instant::now();
    let recovered = Store::recover(&store_dir).map_err(|e| format!("recover: {e}"))?;
    let recovery_replay_s = t0.elapsed().as_secs_f64();
    drop(recovered);
    let scan = crate::orchestrate::scan_wal(&Store::wal_path(&store_dir));

    let (ref_total, anchor) = reference_total_s(arm.writers, arm.batching);
    let envelope_p95_us = (3.0 * ref_total / f64::from(TOTAL_COMMITS) * 1e6) as u64;
    let p95_us = percentile(kept, 0.95);

    Ok(ArmMeasurement {
        arm: arm.label(),
        commits,
        samples_after_warmup: kept.len(),
        p50_us: percentile(kept, 0.50),
        p95_us,
        p99_us: percentile(kept, 0.99),
        p999_us: percentile(kept, 0.999),
        throughput_commits_per_s: f64::from(commits) / total_s,
        total_s,
        recovery_replay_s,
        recovered_wal_entries: scan.entries,
        envelope_p95_us,
        reference_anchor: anchor.to_string(),
        p95_within_envelope: p95_us <= envelope_p95_us,
        replay_within_10s: recovery_replay_s < 10.0,
        internals: "queue-wait/lock-hold/fsync/checkpoint durations not exposed by selene API at pinned SHA",
    })
}

pub fn run_bench(out: &Path, seed: u64) -> Result<Vec<ArmMeasurement>, String> {
    std::fs::create_dir_all(out).map_err(|e| e.to_string())?;
    let mut results = Vec::new();
    for arm in Arm::ALL {
        eprintln!("measuring {}...", arm.label());
        let m = measure_arm(out, arm, seed)?;
        eprintln!(
            "  p50={}us p95={}us (envelope {}us, within={}) throughput={:.0}/s replay={:.2}s",
            m.p50_us,
            m.p95_us,
            m.envelope_p95_us,
            m.p95_within_envelope,
            m.throughput_commits_per_s,
            m.recovery_replay_s
        );
        results.push(m);
    }
    std::fs::write(
        out.join("measured.json"),
        serde_json::to_vec_pretty(&results).expect("measured"),
    )
    .map_err(|e| e.to_string())?;
    Ok(results)
}
