//! Orchestrator role (EXP-001 §6, §9, §10): spawn workers, kill them, reopen,
//! audit, classify, and retain evidence.
//!
//! Exact kill points self-position in the worker; the two timing-sampled
//! cells (post-seal and cursor-abort) fire a seeded timer into the commit
//! pipeline and classify the landing post-hoc from the evidence — sidecar
//! tail vs. WAL tail — resampling until the cell's quota of qualifying
//! landings is met. §10 anticipates exactly this: the retained artifacts must
//! reconstruct "which pipeline stage a kill landed in."

use std::path::{Path, PathBuf};
use std::process::Command;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};

use crate::audit::{self, AuditReport};
use crate::schema::{Arm, Batching, Cell, CommandKind, KillPoint};
use crate::sidecar::{Confirmed, read_confirmed};
use crate::store::{Store, TypeViolation, poll_wal};
use crate::worker::{KillDirective, KillMode};
use crate::workload::{UnitPool, WriterStream};
use selene_graph::GraphError;
use selene_persist::PersistError;

/// Pre-recovery WAL scan: the only view of tornness — recovery truncates it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalScan {
    pub entries: u64,
    pub last_seq: u64,
    pub torn: bool,
    /// Error text when iteration stopped on something other than a clean end
    /// or a torn tail (e.g. a checksum mismatch from the corruption drill).
    pub stop_error: Option<String>,
}

pub fn scan_wal(wal_path: &Path) -> WalScan {
    let mut scan = WalScan {
        entries: 0,
        last_seq: 0,
        torn: false,
        stop_error: None,
    };
    let reader = match selene_persist::WalReader::open(wal_path) {
        Ok(r) => r,
        Err(e) => {
            scan.stop_error = Some(format!("open: {e}"));
            return scan;
        }
    };
    let stream = match reader.iterate(|_| true) {
        Ok(s) => s,
        Err(e) => {
            scan.stop_error = Some(format!("iterate: {e}"));
            return scan;
        }
    };
    for entry in stream {
        match entry {
            Ok(view) => {
                scan.entries += 1;
                scan.last_seq = view.header.sequence;
            }
            Err(PersistError::TruncatedEntry { .. }) => {
                scan.torn = true;
                break;
            }
            Err(e) => {
                scan.stop_error = Some(e.to_string());
                break;
            }
        }
    }
    scan
}

/// Where the evidence says a timing-sampled kill landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Landing {
    /// Directive was exact (non-timer modes) — landing is by construction.
    Exact,
    /// Nothing beyond the sidecar's confirmations reached the WAL.
    PreAppend,
    /// The WAL tail is torn: an append began and never became durable.
    TornTail,
    /// Durable entries exist beyond the last confirmation — the kill landed
    /// after durability, before the response.
    DurableUnconfirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrialVerdict {
    pub arm: String,
    pub cell: String,
    pub rep: u32,
    pub attempt: u32,
    pub trial_seed: u64,
    pub directive: Option<KillDirective>,
    pub worker_signal: Option<i32>,
    pub wal_pre: WalScan,
    pub landing: Landing,
    /// Landing did not qualify for this cell; trial resampled, not scored.
    pub disqualified: bool,
    pub audit: AuditReport,
    /// Cell-specific check failures (empty = cell conditions held).
    pub cell_failures: Vec<String>,
    pub pass: bool,
}

pub struct RunConfig {
    pub out: PathBuf,
    pub root_seed: u64,
    pub reps: u32,
    pub arms: Vec<Arm>,
    pub cells: Vec<Cell>,
    /// Max sampling attempts per qualifying rep for timing cells.
    pub attempt_cap: u32,
}

fn worker_cmd(exe: &Path, dir: &Path, sidecar: &Path, seed: u64, arm: &Arm, steps: u32) -> Command {
    let mut c = Command::new(exe);
    c.arg("worker")
        .arg("--dir")
        .arg(dir)
        .arg("--sidecar")
        .arg(sidecar)
        .arg("--trial-seed")
        .arg(seed.to_string())
        .arg("--writers")
        .arg(arm.writers.to_string())
        .arg("--batching")
        .arg(match arm.batching {
            Batching::Off => "off",
            Batching::DefaultBound => "on",
        })
        .arg("--steps")
        .arg(steps.to_string())
        .arg("--create");
    c
}

fn append_directive(c: &mut Command, d: &KillDirective) {
    c.arg("--kill-mode").arg(match d.mode {
        KillMode::BeforeCommit => "before-commit",
        KillMode::AfterCommit => "after-commit",
        KillMode::ResponseWindow => "response-window",
        KillMode::Timer => "timer",
    });
    c.arg("--kill-at").arg(d.at_eligible.to_string());
    if let Some(k) = d.only_kind {
        c.arg("--kill-kind").arg(format!("{k:?}"));
    }
    c.arg("--timer-us").arg(d.timer_us.to_string());
}

/// Directive for one attempt of a kill cell. Seeded: same trial seed, same
/// directive.
fn directive_for(cell: KillPoint, rng: &mut ChaCha20Rng, phase_post: bool) -> KillDirective {
    let at_any = rng.random_range(20..80u64);
    let at_kinded = rng.random_range(2..8u64);
    match cell {
        KillPoint::PreSeal => KillDirective {
            mode: KillMode::BeforeCommit,
            at_eligible: at_any,
            only_kind: None,
            timer_us: 0,
        },
        KillPoint::PostSealPrePublish | KillPoint::CursorAllocAbort => KillDirective {
            mode: KillMode::Timer,
            at_eligible: at_any,
            only_kind: None,
            timer_us: rng.random_range(30..2_500),
        },
        KillPoint::PostPublishPreResponse => KillDirective {
            mode: KillMode::ResponseWindow,
            at_eligible: at_any,
            only_kind: None,
            timer_us: 0,
        },
        KillPoint::ArtifactPublication => KillDirective {
            mode: if phase_post {
                KillMode::AfterCommit
            } else {
                KillMode::BeforeCommit
            },
            at_eligible: at_kinded,
            only_kind: Some(CommandKind::ReviewEvidence),
            timer_us: 0,
        },
        KillPoint::MidEffectIntent => KillDirective {
            mode: KillMode::AfterCommit,
            at_eligible: at_kinded,
            only_kind: Some(CommandKind::Dispatch),
            timer_us: 0,
        },
        KillPoint::WriterTakeover => unreachable!("takeover has its own choreography"),
    }
}

fn classify(directive: &KillDirective, confirmed: &[Confirmed], scan: &WalScan) -> Landing {
    if directive.mode != KillMode::Timer {
        return Landing::Exact;
    }
    let max_confirmed = confirmed
        .iter()
        .filter_map(|c| c.durable_at)
        .max()
        .unwrap_or(0);
    if scan.torn {
        Landing::TornTail
    } else if scan.last_seq > max_confirmed {
        Landing::DurableUnconfirmed
    } else {
        Landing::PreAppend
    }
}

/// Which landings score for which timing cell. Post-seal accepts both
/// inside-commit landings. Cursor-abort scores EVERY timer landing: SIGKILL
/// cannot tear a single small write() (the kernel keeps written bytes after
/// process death), and with concurrent writers some commit is almost always
/// durable-unconfirmed at kill time, so landing-based gating would starve the
/// cell. Its falsifiable content is the R26b continuity assertion set run in
/// its cell check; the landing is recorded per trial for coverage honesty.
/// The fsync-loss half of the abort case needs power-loss testing, outside
/// this gate's process-kill derivation (validation/01:392).
fn qualifies(cell: KillPoint, landing: Landing) -> bool {
    match cell {
        KillPoint::PostSealPrePublish => {
            matches!(landing, Landing::TornTail | Landing::DurableUnconfirmed)
        }
        KillPoint::CursorAllocAbort => true,
        _ => true,
    }
}

pub struct TrialCtx<'a> {
    pub exe: &'a Path,
    pub out: &'a Path,
    pub arm: Arm,
    pub rep: u32,
    pub trial_seed: u64,
}

fn run_worker_to_kill(
    ctx: &TrialCtx<'_>,
    dir: &Path,
    sidecar: &Path,
    directive: &KillDirective,
) -> Result<Option<i32>, String> {
    // Steps sized just past the directive: after the kill fires the rest is
    // moot, and a timing miss (clean exit) stays cheap to resample.
    let steps = (directive.at_eligible as u32 * 8 / ctx.arm.writers.max(1)).max(30) + 40;
    let mut cmd = worker_cmd(ctx.exe, dir, sidecar, ctx.trial_seed, &ctx.arm, steps);
    append_directive(&mut cmd, directive);
    let out = cmd.output().map_err(|e| format!("spawn worker: {e}"))?;
    use std::os::unix::process::ExitStatusExt;
    Ok(out.status.signal())
}

/// One attempt at one kill-cell trial. `phase_post` selects the artifact
/// cell's post-publication injection.
pub fn kill_trial(
    ctx: &TrialCtx<'_>,
    cell: KillPoint,
    attempt: u32,
    phase_post: bool,
) -> Result<TrialVerdict, String> {
    let name = if cell == KillPoint::ArtifactPublication {
        format!("{cell:?}-{}", if phase_post { "post" } else { "pre" })
    } else {
        format!("{cell:?}")
    };
    let trial_dir = ctx
        .out
        .join(ctx.arm.label())
        .join(&name)
        .join(format!("rep{}-a{attempt}", ctx.rep));
    std::fs::create_dir_all(&trial_dir).map_err(|e| e.to_string())?;
    let store_dir = trial_dir.join("store");
    std::fs::create_dir_all(&store_dir).map_err(|e| e.to_string())?;
    let sidecar_path = trial_dir.join("sidecar.jsonl");

    let mut rng = ChaCha20Rng::seed_from_u64(ctx.trial_seed ^ u64::from(attempt) << 32);
    let directive = directive_for(cell, &mut rng, phase_post);
    let signal = run_worker_to_kill(ctx, &store_dir, &sidecar_path, &directive)?;

    let confirmed = read_confirmed(&sidecar_path).map_err(|e| format!("sidecar: {e}"))?;
    let wal_pre = scan_wal(&Store::wal_path(&store_dir));
    let landing = classify(&directive, &confirmed, &wal_pre);
    let disqualified = signal.is_none() || !qualifies(cell, landing);

    let mut cell_failures = Vec::new();
    let (audit_report, pass) = if disqualified {
        (
            AuditReport {
                violations: vec![],
                nonterminal_intents: vec![],
                units_checked: 0,
                events_checked: 0,
                confirmed_checked: 0,
            },
            false,
        )
    } else {
        let store = Store::recover(&store_dir).map_err(|e| format!("recover: {e}"))?;
        let snap = store.audit_snapshot();
        let report = audit::score(&snap, &confirmed);

        match cell {
            KillPoint::MidEffectIntent => {
                // The directive thread's SIGKILL races other threads' sidecar
                // appends, so the targeted commit is the last confirmed of the
                // targeted KIND, not the last line.
                match confirmed
                    .iter()
                    .rev()
                    .find(|c| c.spec.kind == CommandKind::Dispatch)
                {
                    Some(last) => {
                        let intent = last.spec.effect.as_ref().expect("dispatch carries intent");
                        if !report.nonterminal_intents.contains(&intent.intent_id) {
                            cell_failures.push(format!(
                                "killed dispatch intent {} not classified nonterminal",
                                intent.intent_id
                            ));
                        }
                    }
                    None => cell_failures.push("no confirmed Dispatch in sidecar".into()),
                }
            }
            KillPoint::ArtifactPublication if phase_post => match confirmed
                .iter()
                .rev()
                .find(|c| c.spec.kind == CommandKind::ReviewEvidence)
            {
                Some(last) => {
                    let digest = last
                        .spec
                        .artifact_ref
                        .clone()
                        .expect("review carries artifact");
                    if !snap.artifacts.contains(&digest) {
                        cell_failures
                            .push(format!("confirmed artifact {digest} missing after reopen"));
                    }
                    match snap.units.get(&last.spec.unit_id) {
                        Some(u) if u.artifact_ref.as_deref() == Some(digest.as_str()) => {}
                        _ => cell_failures.push(format!(
                            "unit {} does not reference confirmed artifact",
                            last.spec.unit_id
                        )),
                    }
                }
                None => cell_failures.push("no confirmed ReviewEvidence in sidecar".into()),
            },
            KillPoint::PostPublishPreResponse => {
                // Durable-unconfirmed commits are identifiable from the store
                // alone; each must accept no replay of its receipt claim.
                let confirmed_events: std::collections::HashSet<u64> = confirmed
                    .iter()
                    .map(|c| c.spec.events[0].event_id)
                    .collect();
                let mut probed = 0;
                for (event_id, row) in &snap.events {
                    if confirmed_events.contains(event_id) {
                        continue;
                    }
                    let command_id = event_id >> 8;
                    let key = format!("{}/{}", row.aggregate_id, command_id);
                    match store.probe_duplicate_receipt(&key) {
                        Err(GraphError::TypeViolation(
                            TypeViolation::UniquePropertyDuplicate { .. },
                        )) => probed += 1,
                        Err(e) => {
                            cell_failures.push(format!("replay probe {key}: wrong error {e}"))
                        }
                        Ok(()) => cell_failures.push(format!(
                            "replay probe {key}: duplicate receipt ACCEPTED — re-execution possible"
                        )),
                    }
                }
                if probed == 0 && cell_failures.is_empty() {
                    cell_failures.push("no durable-unconfirmed commit found to probe".into());
                }
            }
            KillPoint::CursorAllocAbort => {
                // R26b continuity form: every confirmation sits at or below
                // the recovered frontier, and a post-recovery commit proceeds
                // cleanly (sequence reuse is legal; dangling references are
                // not — the loss/agreement bars above catch those).
                let max_confirmed = confirmed
                    .iter()
                    .filter_map(|c| c.durable_at)
                    .max()
                    .unwrap_or(0);
                let post = scan_wal(&Store::wal_path(&store_dir));
                if max_confirmed > post.last_seq {
                    cell_failures.push(format!(
                        "confirmed durable_at {max_confirmed} beyond recovered frontier {}",
                        post.last_seq
                    ));
                }
                let mut pool = UnitPool::new(2);
                for s in &mut pool.slots {
                    s.unit_id |= 1 << 41;
                }
                // Writer number offset keeps probe command/event ids disjoint
                // from every workload writer's bit-packed range (same rule as
                // the takeover racers).
                let mut stream = WriterStream::new(ctx.trial_seed ^ 0xC0117111017, 2000);
                let spec = stream.next_spec(&mut pool).expect("free slot");
                match store.apply(&spec) {
                    Ok(applied) => {
                        if applied.durable_at.unwrap_or(0) <= max_confirmed {
                            cell_failures
                                .push("post-recovery commit did not advance the cursor".into());
                        }
                    }
                    Err(e) => cell_failures.push(format!("post-recovery commit failed: {e:?}")),
                }
            }
            _ => {}
        }

        let pass = report.violations.is_empty() && cell_failures.is_empty();
        (report, pass)
    };

    Ok(TrialVerdict {
        arm: ctx.arm.label(),
        cell: name,
        rep: ctx.rep,
        attempt,
        trial_seed: ctx.trial_seed,
        directive: Some(directive),
        worker_signal: signal,
        wal_pre,
        landing,
        disqualified,
        audit: audit_report,
        cell_failures,
        pass,
    })
}

// ---------------------------------------------------------------------------
// In-process concurrent driver for the non-kill cells: same claim/apply/
// release protocol as the worker, no kill, per-apply callback for the watch
// cell's watermark bookkeeping.

pub fn drive_concurrent(
    store: &std::sync::Arc<Store>,
    trial_seed: u64,
    writers: u32,
    steps_per_writer: u32,
    mut on_applied: impl FnMut(&crate::workload::CommitSpec, &crate::store::Applied) + Send,
) -> Result<u64, String> {
    let pool = std::sync::Arc::new(std::sync::Mutex::new(UnitPool::new(
        (writers.max(2) * 4) as usize,
    )));
    let (tx, rx) =
        std::sync::mpsc::channel::<(crate::workload::CommitSpec, crate::store::Applied)>();
    let applied_count = std::thread::scope(|scope| -> Result<u64, String> {
        for w in 0..writers {
            let store = std::sync::Arc::clone(store);
            let pool = std::sync::Arc::clone(&pool);
            let tx = tx.clone();
            scope.spawn(move || {
                let mut stream = WriterStream::new(trial_seed, w);
                let mut done = 0u32;
                while done < steps_per_writer {
                    let spec = { stream.next_spec(&mut pool.lock().expect("pool")) };
                    let Some(spec) = spec else {
                        std::thread::yield_now();
                        continue;
                    };
                    match store.apply(&spec) {
                        Ok(applied) => {
                            tx.send((spec.clone(), applied)).expect("collector alive");
                            pool.lock().expect("pool").release(spec.slot, &spec, true);
                        }
                        Err(crate::store::ApplyError::Rejected(r)) => {
                            let planned_ok = matches!(
                                (spec.expect_reject, &r),
                                (
                                    Some(crate::workload::ExpectedRejection::StaleLease),
                                    crate::store::Rejection::StaleLease { .. }
                                ) | (
                                    Some(crate::workload::ExpectedRejection::StaleHolder),
                                    crate::store::Rejection::StaleHolder { .. }
                                )
                            );
                            if !planned_ok {
                                panic!("unplanned funnel rejection {r:?} for {spec:?}");
                            }
                            pool.lock().expect("pool").release(spec.slot, &spec, false);
                        }
                        Err(crate::store::ApplyError::Harness(msg)) => {
                            panic!("harness incoherence: {msg}")
                        }
                        Err(crate::store::ApplyError::Graph(e)) => panic!("store error: {e}"),
                    }
                    done += 1;
                }
            });
        }
        drop(tx);
        let mut n = 0u64;
        while let Ok((spec, applied)) = rx.recv() {
            on_applied(&spec, &applied);
            n += 1;
        }
        Ok(n)
    })?;
    Ok(applied_count)
}

fn fresh_cell_dirs(
    ctx: &TrialCtx<'_>,
    name: &str,
    attempt: u32,
) -> Result<(PathBuf, PathBuf), String> {
    let trial_dir = ctx
        .out
        .join(ctx.arm.label())
        .join(name)
        .join(format!("rep{}-a{attempt}", ctx.rep));
    std::fs::create_dir_all(&trial_dir).map_err(|e| e.to_string())?;
    let store_dir = trial_dir.join("store");
    std::fs::create_dir_all(&store_dir).map_err(|e| e.to_string())?;
    Ok((trial_dir, store_dir))
}

fn base_verdict(ctx: &TrialCtx<'_>, name: &str, attempt: u32) -> TrialVerdict {
    TrialVerdict {
        arm: ctx.arm.label(),
        cell: name.to_string(),
        rep: ctx.rep,
        attempt,
        trial_seed: ctx.trial_seed,
        directive: None,
        worker_signal: None,
        wal_pre: WalScan {
            entries: 0,
            last_seq: 0,
            torn: false,
            stop_error: None,
        },
        landing: Landing::Exact,
        disqualified: false,
        audit: AuditReport {
            violations: vec![],
            nonterminal_intents: vec![],
            units_checked: 0,
            events_checked: 0,
            confirmed_checked: 0,
        },
        cell_failures: Vec::new(),
        pass: false,
    }
}

mod cells;
pub use cells::*;

// Gate loop.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GateSummary {
    pub scored: u32,
    pub passed: u32,
    pub failed: u32,
    pub resampled_attempts: u32,
    /// Cells that missed their quota within the attempt cap — reported, never
    /// silently dropped.
    pub shortfalls: Vec<String>,
}

fn persist_verdict(out: &Path, v: &TrialVerdict) -> Result<(), String> {
    let dir = out.join(&v.arm).join(&v.cell);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let path = dir.join(format!("rep{}-verdict.json", v.rep));
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(v).expect("verdict serializes"),
    )
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// R25c bounded retention: passing trials keep store bytes only for the first
/// and last rep of each cell; sidecars and verdicts are always kept. Failing
/// trials keep everything.
fn apply_retention(out: &Path, v: &TrialVerdict, reps: u32) {
    if !v.pass || v.rep == 0 || v.rep == reps - 1 {
        return;
    }
    let store_dir = out
        .join(&v.arm)
        .join(&v.cell)
        .join(format!("rep{}-a{}", v.rep, v.attempt))
        .join("store");
    let _ = std::fs::remove_dir_all(store_dir);
}

pub fn run_gate(cfg: &RunConfig) -> Result<GateSummary, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&cfg.out).map_err(|e| e.to_string())?;
    let manifest =
        crate::manifest::Manifest::capture("../selene-db", ".", cfg.root_seed, cfg.reps, 50);
    std::fs::write(
        cfg.out.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("manifest"),
    )
    .map_err(|e| e.to_string())?;

    let mut summary = GateSummary {
        scored: 0,
        passed: 0,
        failed: 0,
        resampled_attempts: 0,
        shortfalls: vec![],
    };

    for arm in &cfg.arms {
        for cell in &cfg.cells {
            for rep in 0..cfg.reps {
                let trial_seed = crate::plan::trial_seed(cfg.root_seed, arm, *cell, rep);
                let ctx = TrialCtx {
                    exe: &exe,
                    out: &cfg.out,
                    arm: *arm,
                    rep,
                    trial_seed,
                };
                let verdicts: Vec<TrialVerdict> = match cell {
                    Cell::Kill(KillPoint::WriterTakeover) => {
                        let mut got = None;
                        for attempt in 0..cfg.attempt_cap {
                            let v = takeover_trial(&ctx, attempt)?;
                            if v.disqualified {
                                summary.resampled_attempts += 1;
                                continue;
                            }
                            got = Some(v);
                            break;
                        }
                        match got {
                            Some(v) => vec![v],
                            None => {
                                summary
                                    .shortfalls
                                    .push(format!("{}/WriterTakeover/rep{rep}", arm.label()));
                                vec![]
                            }
                        }
                    }
                    Cell::Kill(KillPoint::ArtifactPublication) => {
                        let mut out = Vec::new();
                        for phase_post in [false, true] {
                            let v = kill_trial(
                                &ctx,
                                KillPoint::ArtifactPublication,
                                u32::from(phase_post),
                                phase_post,
                            )?;
                            out.push(v);
                        }
                        out
                    }
                    Cell::Kill(k) => {
                        let mut got = None;
                        for attempt in 0..cfg.attempt_cap {
                            let v = kill_trial(&ctx, *k, attempt, false)?;
                            if v.disqualified {
                                summary.resampled_attempts += 1;
                                continue;
                            }
                            got = Some(v);
                            break;
                        }
                        match got {
                            Some(v) => vec![v],
                            None => {
                                summary
                                    .shortfalls
                                    .push(format!("{}/{k:?}/rep{rep}", arm.label()));
                                vec![]
                            }
                        }
                    }
                    Cell::JournalImmutability => vec![journal_cell_trial(&ctx)?],
                    Cell::SnapshotWatchHandoff => vec![watch_cell_trial(&ctx)?],
                    Cell::CorruptionDrill => vec![corruption_trial(&ctx, rep % 3)?],
                };
                for v in verdicts {
                    summary.scored += 1;
                    if v.pass {
                        summary.passed += 1;
                    } else {
                        summary.failed += 1;
                        eprintln!(
                            "FAIL {}/{}/rep{}: {:?} {:?}",
                            v.arm, v.cell, v.rep, v.audit.violations, v.cell_failures
                        );
                    }
                    persist_verdict(&cfg.out, &v)?;
                    apply_retention(&cfg.out, &v, cfg.reps);
                }
            }
        }
    }
    std::fs::write(
        cfg.out.join("summary.json"),
        serde_json::to_vec_pretty(&summary).expect("summary"),
    )
    .map_err(|e| e.to_string())?;
    Ok(summary)
}
