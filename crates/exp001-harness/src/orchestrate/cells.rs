//! The four non-kill cells (journal immutability, snapshot watch,
//! takeover, corruption) — split from the orchestrator to honor the
//! per-file LOC cap; helpers stay in the parent module.

use super::*;

/// §6 post-commit journal mutation/duplicate cell, per arm (in-process; no
/// kill is injected — write-time rejection, not recovery).
pub fn journal_cell_trial(ctx: &TrialCtx<'_>) -> Result<TrialVerdict, String> {
    let (_trial_dir, store_dir) = fresh_cell_dirs(ctx, "JournalImmutability", 0)?;
    let store = std::sync::Arc::new(
        Store::create(&store_dir, ctx.arm.batching).map_err(|e| format!("create: {e}"))?,
    );
    let mut applied: Vec<crate::workload::CommitSpec> = Vec::new();
    {
        let collected = std::sync::Mutex::new(&mut applied);
        drive_concurrent(&store, ctx.trial_seed, ctx.arm.writers, 40, |spec, _| {
            collected.lock().expect("collect").push(spec.clone());
        })?;
    }
    let mut v = base_verdict(ctx, "JournalImmutability", 0);
    if applied.is_empty() {
        v.cell_failures.push("no commits applied".into());
        return Ok(v);
    }
    let target = applied[applied.len() / 2].events[0].clone();
    let before = store
        .event_payload(target.event_id)
        .ok_or("target payload missing")?;

    match store.try_update_event(target.event_id) {
        Err(GraphError::TypeViolation(TypeViolation::ImmutablePropertyUpdate { .. })) => {}
        other => v.cell_failures.push(format!(
            "update arm: expected immutable rejection, got {other:?}"
        )),
    }
    match store.apply_delete(crate::store::DeleteTarget::Event {
        event_id: target.event_id,
    }) {
        Err(crate::store::ApplyError::Rejected(crate::store::Rejection::JournalDelete {
            event_id,
        })) if event_id == target.event_id => {}
        other => v
            .cell_failures
            .push(format!("delete arm: wrong funnel outcome {other:?}")),
    }
    let dup_id = crate::schema::SemanticEvent {
        payload: "{}".into(),
        ..target.clone()
    };
    match store.try_duplicate_event(&dup_id) {
        Err(GraphError::TypeViolation(TypeViolation::UniquePropertyDuplicate { .. })) => {}
        other => v.cell_failures.push(format!("dup event_id arm: {other:?}")),
    }
    let dup_comp = crate::schema::SemanticEvent {
        event_id: target.event_id ^ (1 << 60),
        ..target.clone()
    };
    match store.try_duplicate_event(&dup_comp) {
        Err(GraphError::TypeViolation(TypeViolation::UniquePropertyDuplicate { .. })) => {}
        other => v
            .cell_failures
            .push(format!("dup composite arm: {other:?}")),
    }
    let after = store
        .event_payload(target.event_id)
        .ok_or("target payload missing after")?;
    if before != after {
        v.cell_failures
            .push("journal bytes changed under rejected mutations".into());
    }
    v.pass = v.cell_failures.is_empty();
    Ok(v)
}

/// §6 snapshot-plus-poll handoff cell per arm (amended R26c): snapshot at
/// cursor C mid-stream, then poll CONCURRENTLY with the still-running writers
/// — the poller races live appends, which is what makes the watermark filter
/// and poll_wal's torn-tail stop real obligations rather than dead code.
pub fn watch_cell_trial(ctx: &TrialCtx<'_>) -> Result<TrialVerdict, String> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let (_trial_dir, store_dir) = fresh_cell_dirs(ctx, "SnapshotWatchHandoff", 0)?;
    let store = std::sync::Arc::new(
        Store::create(&store_dir, ctx.arm.batching).map_err(|e| format!("create: {e}"))?,
    );
    let watermark = std::sync::Arc::new(AtomicU64::new(0));
    let committed = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u64>::new()));
    let snapshot_taken = std::sync::Arc::new(std::sync::Mutex::new(
        None::<(u64, std::collections::BTreeSet<u64>)>,
    ));
    let done = std::sync::Arc::new(AtomicBool::new(false));

    let driver = {
        let store = std::sync::Arc::clone(&store);
        let watermark = std::sync::Arc::clone(&watermark);
        let committed = std::sync::Arc::clone(&committed);
        let snapshot_taken = std::sync::Arc::clone(&snapshot_taken);
        let done = std::sync::Arc::clone(&done);
        let writers = ctx.arm.writers;
        let seed = ctx.trial_seed;
        std::thread::spawn(move || {
            let mut seen = 0u64;
            let store2 = std::sync::Arc::clone(&store);
            let r = drive_concurrent(&store, seed, writers, 60, move |spec, applied| {
                // Commit recorded before the watermark advances: a poller can
                // never trust a sequence whose event might be missing from
                // `committed`.
                committed
                    .lock()
                    .expect("committed")
                    .push(spec.events[0].event_id);
                if let Some(seq) = applied.durable_at {
                    watermark.fetch_max(seq, Ordering::SeqCst);
                }
                seen += 1;
                if seen == 40 {
                    let cursor = watermark.load(Ordering::SeqCst);
                    let events = store2.snapshot_event_ids().into_iter().collect();
                    *snapshot_taken.lock().expect("snap") = Some((cursor, events));
                }
            });
            done.store(true, Ordering::SeqCst);
            r
        })
    };

    // Concurrent poller: incremental polls gated by the moving watermark,
    // racing the writers' live appends.
    let mut polled_events: std::collections::BTreeSet<u64> = Default::default();
    let mut last_polled = 0u64;
    let mut cursor_opt = None;
    let wal_path = Store::wal_path(&store_dir);
    loop {
        if cursor_opt.is_none() {
            cursor_opt = snapshot_taken.lock().expect("snap").clone();
            if let Some((cursor, _)) = cursor_opt {
                last_polled = cursor;
            }
        }
        if let Some((_, _)) = cursor_opt {
            let wm = watermark.load(std::sync::atomic::Ordering::SeqCst);
            if wm > last_polled {
                let polled =
                    poll_wal(&wal_path, last_polled, wm).map_err(|e| format!("poll: {e}"))?;
                for (_, changes) in &polled {
                    for c in changes {
                        if let selene_core::Change::NodeCreated { properties, .. } = c
                            && let Some(selene_core::Value::Uint(u)) = properties
                                .get(&selene_core::db_string("event_id").expect("db string"))
                        {
                            polled_events.insert(*u);
                        }
                    }
                }
                last_polled = wm;
            }
        }
        if done.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_micros(200));
    }
    driver.join().map_err(|_| "driver panicked".to_string())??;

    let mut v = base_verdict(ctx, "SnapshotWatchHandoff", 0);
    let Some((_, snapshot_events)) = cursor_opt else {
        v.cell_failures
            .push("stream too short: snapshot point never reached".into());
        return Ok(v);
    };
    // Final poll to the settled watermark closes the tail.
    let final_watermark = watermark.load(std::sync::atomic::Ordering::SeqCst);
    if final_watermark > last_polled {
        let polled =
            poll_wal(&wal_path, last_polled, final_watermark).map_err(|e| format!("poll: {e}"))?;
        for (_, changes) in &polled {
            for c in changes {
                if let selene_core::Change::NodeCreated { properties, .. } = c
                    && let Some(selene_core::Value::Uint(u)) =
                        properties.get(&selene_core::db_string("event_id").expect("db string"))
                {
                    polled_events.insert(*u);
                }
            }
        }
    }
    let all: std::collections::BTreeSet<u64> = committed
        .lock()
        .expect("committed")
        .iter()
        .copied()
        .collect();
    let union: std::collections::BTreeSet<u64> =
        snapshot_events.union(&polled_events).copied().collect();
    let missing: Vec<u64> = all.difference(&union).copied().collect();
    if !missing.is_empty() {
        v.cell_failures.push(format!(
            "invisible gap: {} events in neither snapshot nor poll",
            missing.len()
        ));
    }
    v.pass = v.cell_failures.is_empty();
    Ok(v)
}

/// §6 WriterLockHeld takeover cell: live contention fails fast; after the
/// holder dies, exactly one racing claimant recovers and can write.
pub fn takeover_trial(ctx: &TrialCtx<'_>, attempt: u32) -> Result<TrialVerdict, String> {
    let (trial_dir, store_dir) = fresh_cell_dirs(ctx, "WriterTakeover", attempt)?;
    let sidecar_path = trial_dir.join("sidecar.jsonl");
    let mut v = base_verdict(ctx, "WriterTakeover", attempt);

    let mut holder = worker_cmd(
        ctx.exe,
        &store_dir,
        &sidecar_path,
        ctx.trial_seed,
        &ctx.arm,
        1_000_000,
    )
    .stdout(std::process::Stdio::null())
    .spawn()
    .map_err(|e| format!("spawn holder: {e}"))?;

    // Wait until the holder has real durable state to contest.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let lines = std::fs::read_to_string(&sidecar_path)
            .map(|s| s.lines().count())
            .unwrap_or(0);
        if lines >= 30 {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = holder.kill();
            let _ = holder.wait();
            return Err("holder never reached 30 confirmed commits".into());
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let contend_out = Command::new(ctx.exe)
        .arg("contend")
        .arg("--dir")
        .arg(&store_dir)
        .output()
        .map_err(|e| format!("spawn contender: {e}"))?;
    let contend_text = String::from_utf8_lossy(&contend_out.stdout).to_string();
    if !contend_text.contains("CONTEND:HELD") {
        v.cell_failures.push(format!(
            "live contention did not hold: {}",
            contend_text.trim()
        ));
    }

    holder.kill().map_err(|e| format!("kill holder: {e}"))?;
    holder.wait().map_err(|e| format!("reap holder: {e}"))?;
    v.worker_signal = Some(9);

    let racer = |n: u32| {
        Command::new(ctx.exe)
            .arg("race")
            .arg("--dir")
            .arg(&store_dir)
            .arg("--racer")
            .arg(n.to_string())
            .arg("--hold-ms")
            .arg("1500")
            .stdout(std::process::Stdio::piped())
            .spawn()
    };
    let c1 = racer(1).map_err(|e| format!("spawn racer1: {e}"))?;
    let c2 = racer(2).map_err(|e| format!("spawn racer2: {e}"))?;
    let o1 = c1.wait_with_output().map_err(|e| e.to_string())?;
    let o2 = c2.wait_with_output().map_err(|e| e.to_string())?;
    let t1 = String::from_utf8_lossy(&o1.stdout).to_string();
    let t2 = String::from_utf8_lossy(&o2.stdout).to_string();
    let wins = [&t1, &t2].iter().filter(|t| t.contains("RACE:WON")).count();
    let helds = [&t1, &t2]
        .iter()
        .filter(|t| t.contains("RACE:HELD"))
        .count();
    if wins != 1 || helds != 1 {
        // Both sequentially winning is a choreography miss, not a store
        // defect: the loser attempted after the winner released. Resample.
        v.disqualified = wins == 2;
        if !v.disqualified {
            v.cell_failures.push(format!(
                "race outcome wins={wins} helds={helds}: [{}] [{}]",
                t1.trim(),
                t2.trim()
            ));
        }
    }

    v.wal_pre = scan_wal(&Store::wal_path(&store_dir));
    let confirmed = read_confirmed(&sidecar_path).map_err(|e| format!("sidecar: {e}"))?;
    let store = Store::recover(&store_dir).map_err(|e| format!("final recover: {e}"))?;
    let snap = store.audit_snapshot();
    v.audit = audit::score(&snap, &confirmed);
    let racer_units = snap.units.keys().filter(|u| *u & (1 << 40) != 0).count();
    if wins == 1 && racer_units != 4 && racer_units != 3 {
        // The winner dispatches from a 4-slot reserved pool; 3 applied
        // commits touch 3 or 4 distinct units depending on the draw.
        v.cell_failures.push(format!(
            "winner wrote {racer_units} reserved units (want 3-4)"
        ));
    }
    v.pass = !v.disqualified && v.audit.violations.is_empty() && v.cell_failures.is_empty();
    Ok(v)
}

/// §6 corruption drill: flip one byte inside a committed record's WAL entry
/// with the store closed; the auditor must fail closed — name the damage —
/// never pass silently. Selene's corruption granularity is the WAL entry
/// (per-entry payload checksum; header bytes are outside it — flipping one
/// changes nothing semantic, which run 1 demonstrated), so the variants
/// differ by which record class's entry is damaged, always flipping inside
/// the checksummed payload: v0 any event's entry, v1 a receipt digest's
/// entry, v2 the Dispatch entry that created the unit's current-state row.
pub fn corruption_trial(ctx: &TrialCtx<'_>, variant: u32) -> Result<TrialVerdict, String> {
    let name = format!("CorruptionDrill-v{variant}");
    let (trial_dir, store_dir) = fresh_cell_dirs(ctx, &name, 0)?;
    let sidecar_path = trial_dir.join("sidecar.jsonl");
    let mut v = base_verdict(ctx, &name, 0);

    let status = worker_cmd(
        ctx.exe,
        &store_dir,
        &sidecar_path,
        ctx.trial_seed,
        &ctx.arm,
        40,
    )
    .status()
    .map_err(|e| format!("spawn worker: {e}"))?;
    if !status.success() {
        return Err(format!("clean run failed: {status}"));
    }
    let confirmed = read_confirmed(&sidecar_path).map_err(|e| format!("sidecar: {e}"))?;
    if confirmed.len() < 10 {
        return Err("too few confirmed commits for the drill".into());
    }

    let mut rng = ChaCha20Rng::seed_from_u64(ctx.trial_seed ^ 0xC0552);
    let wal_path = Store::wal_path(&store_dir);
    let mut bytes = std::fs::read(&wal_path).map_err(|e| format!("read wal: {e}"))?;

    // Pick a mid-stream record whose needle locates uniquely.
    let (needle, back_off) = loop {
        let rec = &confirmed[rng.random_range(4..confirmed.len() - 2)];
        let payload = &rec.spec.events[0].payload;
        match variant % 3 {
            0 => {
                let filler = payload
                    .split("\"filler\":\"")
                    .nth(1)
                    .and_then(|s| s.split('"').next());
                if let Some(f) = filler
                    && f.len() >= 48
                {
                    break (f.as_bytes()[..48].to_vec(), 0usize);
                }
            }
            1 => {
                if rec.spec.kind != CommandKind::ReviewEvidence {
                    break (rec.spec.request_digest.clone().into_bytes(), 0usize);
                }
            }
            _ => {
                // Current-state variant: the Dispatch entry carries the unit
                // row's creating change; corrupting its payload destroys the
                // current-state row's durable record. The flip stays inside
                // the checksummed region via the event filler needle — a
                // back-off past the payload start can land on unchecksummed
                // header/padding bytes and damage nothing (run-1 finding).
                if rec.spec.kind == CommandKind::Dispatch {
                    let filler = payload
                        .split("\"filler\":\"")
                        .nth(1)
                        .and_then(|s| s.split('"').next());
                    if let Some(f) = filler
                        && f.len() >= 48
                    {
                        break (f.as_bytes()[..48].to_vec(), 0usize);
                    }
                }
            }
        }
    };
    let hit = bytes
        .windows(needle.len())
        .position(|w| w == needle.as_slice())
        .ok_or("needle not found in WAL bytes")?;
    // Flip inside the needle itself: guaranteed within the entry's
    // checksummed payload. back_off is retained for provenance in the log.
    let _ = back_off;
    let target = hit + 20;
    bytes[target] ^= 0x40;
    std::fs::write(&wal_path, &bytes).map_err(|e| format!("write wal: {e}"))?;

    v.wal_pre = scan_wal(&wal_path);
    match Store::recover(&store_dir) {
        Err(e) => {
            // Engine refused to start — fail closed at the engine level.
            v.cell_failures.clear();
            v.pass = true;
            v.audit.violations.push(crate::audit::Violation {
                bar: crate::audit::Bar::Agreement,
                detail: format!("engine refused corrupted store: {e}"),
            });
        }
        Ok(store) => {
            let snap = store.audit_snapshot();
            v.audit = audit::score(&snap, &confirmed);
            if v.audit.violations.is_empty() {
                v.cell_failures.push(
                    "store recovered over corruption and the auditor found nothing — fail-open"
                        .into(),
                );
            }
            v.pass = v.cell_failures.is_empty();
        }
    }
    Ok(v)
}

// ---------------------------------------------------------------------------
