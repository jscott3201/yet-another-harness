//! Worker role: the process the orchestrator kills (EXP-001 §6).
//!
//! Runs N writer threads over one store and dies by SIGKILL — its own, at an
//! exact directive point, or the orchestrator's on a timer. Self-kill via
//! `libc::kill(getpid(), SIGKILL)` is the precision path: no destructor, no
//! committer join, no buffered-write mercy, and no cross-process signal
//! latency blurring which commit the kill landed on.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::schema::{Batching, CommandKind};
use crate::sidecar::{Confirmed, Sidecar};
use crate::store::{ApplyError, Rejection, Store};
use crate::workload::ExpectedRejection;
use crate::workload::{UnitPool, WriterStream};

/// Where the kill lands relative to the Nth eligible spec (global count
/// across all writer threads; the interleaving picks which spec is Nth, and
/// post-hoc classification reads the evidence, per §10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KillMode {
    /// SIGKILL before `apply` is entered: nothing of the spec may survive.
    BeforeCommit,
    /// SIGKILL after `apply` returned and the sidecar line landed: the
    /// commit is confirmed and must fully survive.
    AfterCommit,
    /// SIGKILL after `apply` returned, sidecar deliberately skipped: the
    /// §6 post-publish-pre-response window.
    ResponseWindow,
    /// Arm a killer thread sleeping `timer_us`, then enter `apply`: the kill
    /// lands somewhere inside the commit pipeline; classification decides
    /// which window it hit.
    Timer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillDirective {
    pub mode: KillMode,
    /// 1-based index over eligible specs.
    pub at_eligible: u64,
    /// Restrict eligibility to one command kind (artifact and mid-effect
    /// cells target ReviewEvidence / Dispatch commits specifically).
    pub only_kind: Option<CommandKind>,
    /// Killer-thread sleep for `Timer` mode, microseconds.
    pub timer_us: u64,
}

pub struct WorkerConfig {
    pub dir: std::path::PathBuf,
    pub sidecar: std::path::PathBuf,
    pub trial_seed: u64,
    pub writers: u32,
    pub batching: Batching,
    /// Specs each writer thread attempts before a clean exit (kill trials
    /// set this high enough that the directive always fires first).
    pub steps: u32,
    pub create: bool,
    pub kill: Option<KillDirective>,
}

fn sigkill_self() -> ! {
    // SAFETY: kill(2) with our own pid and SIGKILL; no memory is touched and
    // the process does not continue past this call.
    unsafe {
        libc::kill(libc::getpid(), libc::SIGKILL);
    }
    unreachable!("SIGKILL did not deliver");
}

/// Run the workload. Returns only on clean completion; a firing directive
/// never returns.
pub fn run(cfg: &WorkerConfig) -> Result<(), String> {
    let store = if cfg.create {
        Store::create(&cfg.dir, cfg.batching).map_err(|e| format!("create: {e}"))?
    } else {
        Store::recover(&cfg.dir).map_err(|e| format!("recover: {e}"))?
    };
    let store = Arc::new(store);
    let sidecar = Arc::new(Sidecar::create(&cfg.sidecar).map_err(|e| format!("sidecar: {e}"))?);
    let pool = Arc::new(Mutex::new(UnitPool::new((cfg.writers.max(2) * 4) as usize)));
    let eligible_seen = Arc::new(AtomicU64::new(0));

    std::thread::scope(|scope| {
        for w in 0..cfg.writers {
            let store = Arc::clone(&store);
            let sidecar = Arc::clone(&sidecar);
            let pool = Arc::clone(&pool);
            let eligible_seen = Arc::clone(&eligible_seen);
            let kill = cfg.kill.clone();
            scope.spawn(move || {
                let mut stream = WriterStream::new(cfg.trial_seed, w);
                let mut done = 0u32;
                while done < cfg.steps {
                    let spec = {
                        let mut p = pool.lock().expect("pool lock");
                        stream.next_spec(&mut p)
                    };
                    let Some(spec) = spec else {
                        std::thread::yield_now();
                        continue;
                    };

                    let directive_fires = kill.as_ref().is_some_and(|k| {
                        let eligible = k.only_kind.is_none_or(|kind| spec.kind == kind);
                        eligible
                            && eligible_seen.fetch_add(1, Ordering::SeqCst) + 1 == k.at_eligible
                    });

                    if directive_fires {
                        let k = kill.as_ref().expect("directive fired");
                        match k.mode {
                            KillMode::BeforeCommit => sigkill_self(),
                            KillMode::Timer => {
                                let us = k.timer_us;
                                std::thread::spawn(move || {
                                    std::thread::sleep(std::time::Duration::from_micros(us));
                                    sigkill_self();
                                });
                                // fall through into apply; the timer races the
                                // commit pipeline.
                            }
                            KillMode::AfterCommit | KillMode::ResponseWindow => {}
                        }
                    }

                    match store.apply(&spec) {
                        Ok(applied) => {
                            let stale_accepted = spec.expect_reject.is_some();
                            if directive_fires
                                && kill
                                    .as_ref()
                                    .is_some_and(|k| k.mode == KillMode::ResponseWindow)
                            {
                                sigkill_self();
                            }
                            sidecar
                                .append(&Confirmed {
                                    spec: spec.clone(),
                                    generation: applied.generation,
                                    durable_at: applied.durable_at,
                                    stale_accepted,
                                })
                                .expect("sidecar append");
                            if directive_fires
                                && kill
                                    .as_ref()
                                    .is_some_and(|k| k.mode == KillMode::AfterCommit)
                            {
                                sigkill_self();
                            }
                            pool.lock()
                                .expect("pool lock")
                                .release(spec.slot, &spec, true);
                        }
                        Err(ApplyError::Rejected(r)) => {
                            // Only the PLANNED rejection kind is legal here: an
                            // unplanned or wrong-kind rejection is a harness
                            // defect, and accepting it would let a bar-6 cell
                            // pass without ever exercising its path.
                            let planned_ok = matches!(
                                (spec.expect_reject, &r),
                                (
                                    Some(ExpectedRejection::StaleLease),
                                    Rejection::StaleLease { .. }
                                ) | (
                                    Some(ExpectedRejection::StaleHolder),
                                    Rejection::StaleHolder { .. }
                                )
                            );
                            if !planned_ok {
                                panic!("unplanned funnel rejection {r:?} for {spec:?}");
                            }
                            pool.lock()
                                .expect("pool lock")
                                .release(spec.slot, &spec, false);
                        }
                        Err(ApplyError::Harness(msg)) => panic!("harness incoherence: {msg}"),
                        Err(ApplyError::Graph(e)) => {
                            // A poisoned committer after a real durable failure
                            // ends the trial loudly; the orchestrator reads the
                            // exit code, never a swallowed error.
                            panic!("store error in worker: {e}");
                        }
                    }
                    done += 1;
                }
            });
        }
    });
    Ok(())
}

/// Contend mode: try to open a held store once. Prints a marker the
/// orchestrator parses; exit code 1 means the lock failed to hold.
pub fn contend(dir: &std::path::Path) -> i32 {
    match Store::recover(dir) {
        Err(e) if matches!(&e, selene_graph::GraphError::Persist(p) if matches!(p, crate::store::PersistError::WriterLockHeld)) =>
        {
            println!("CONTEND:HELD");
            0
        }
        Err(e) => {
            println!("CONTEND:ERROR:{e}");
            1
        }
        Ok(_) => {
            println!("CONTEND:OPENED");
            1
        }
    }
}

/// Race mode: one of two claimants after the holder dies. The winner applies
/// three fresh dispatches (reserved unit-id range) and holds the store briefly
/// so the loser's attempt lands inside the hold window.
pub fn race(dir: &std::path::Path, racer: u32, hold_ms: u64) -> i32 {
    match Store::recover(dir) {
        Err(e) if matches!(&e, selene_graph::GraphError::Persist(p) if matches!(p, crate::store::PersistError::WriterLockHeld)) =>
        {
            println!("RACE:HELD");
            0
        }
        Err(e) => {
            println!("RACE:ERROR:{e}");
            1
        }
        Ok(store) => {
            let mut pool = UnitPool::new(4);
            // Reserved id range keeps racer units disjoint from workload units.
            for s in &mut pool.slots {
                s.unit_id |= 1 << 40 | u64::from(racer) << 32;
            }
            // Writer number offset keeps racer command/event ids disjoint
            // from every workload writer's bit-packed id range.
            let mut stream = WriterStream::new(0x0ACE_5EED ^ u64::from(racer), 1000 + racer);
            let mut applied = 0;
            while applied < 3 {
                let Some(spec) = stream.next_spec(&mut pool) else {
                    break;
                };
                match store.apply(&spec) {
                    Ok(_) => {
                        pool.release(spec.slot, &spec, true);
                        applied += 1;
                    }
                    Err(ApplyError::Rejected(_)) => pool.release(spec.slot, &spec, false),
                    Err(ApplyError::Harness(msg)) => {
                        println!("RACE:ERROR:{msg}");
                        return 1;
                    }
                    Err(ApplyError::Graph(e)) => {
                        println!("RACE:ERROR:{e}");
                        return 1;
                    }
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(hold_ms));
            println!("RACE:WON:{applied}");
            0
        }
    }
}
