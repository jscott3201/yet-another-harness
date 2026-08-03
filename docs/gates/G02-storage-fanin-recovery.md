# G02 — storage fan-in and crash recovery: PASS

Ruled 2026-08-03. This gate was the falsification test for the runtime's
storage truth model: can the embedded store (Selene) express a current-state
compare-and-set, a command-receipt claim, and an immutable journal append as
one durable atomic commit — under process kill at every commit-pipeline
stage, writer-takeover contests, and on-disk corruption. A reproducible
hard-bar failure would have invalidated the design, not delayed it.

## Result

All hard bars held with **zero violations across 1,440 scored trials**
(1,320 in the main run, 120 corruption-drill re-runs after a
drill-targeting fix, described below). WAL replay on reopen: ~0.01 s
against a 10 s bar.

| Bar | Definition | Result |
|---|---|---|
| Atomicity | No partial write-set visible after any kill: state, journal event, receipt, effect and artifact rows land together or not at all | 0 violations |
| Committed-transition loss | Every commit confirmed durable before the kill is fully present after reopen, byte-identical | 0 violations |
| Receipt/state/event agreement | Every unit at version V has journal versions exactly 1..V, each with exactly one receipt; no orphans either direction | 0 violations |
| Journal immutability | Updates reject typed (store-enforced immutability), deletes reject typed (funnel dispatch), duplicate ids and duplicate composite keys reject at commit | 0 violations |
| Stale lease acceptance | Injected superseded-epoch and wrong-holder commands all rejected typed; zero accepted | 0 violations |
| Recovery to consistent state | Every kill point in every repetition reached an auditor-clean state | 100% |

## Matrix

Six workload arms (2/8/32 writer threads × fsync-per-commit vs bounded group
commit) × ten cells × 20 repetitions. Cells: kill before commit, kill inside
the commit pipeline (timer-sampled, landing classified post-hoc from the
sidecar tail vs WAL tail), kill after durability but before the response
(with a duplicate-claim replay probe proving re-execution is impossible),
kill around artifact publication (before and after separately), kill after
an effect intent commits but before its terminal observation, a live
writer-lock contest plus a two-claimant recovery race after the holder dies
(exactly one winner in every trial), a cursor-continuity check after aborted
appends, journal mutation/duplicate attempts, a snapshot-plus-poll handoff
racing live appends (no invisible gap), and a byte-flip corruption drill
scored on whether the auditor fails closed.

Environment (recorded, not assumed): Apple M5, 10 cores, 16 GiB, macOS
25G70, rustc 1.97.1, selene-db at `9a61de124ffa`, seed 1. Full manifests,
per-trial verdicts, sidecars, and failing-trial stores are retained in the
run directories.

## Honest limits and observations

- **SIGKILL cannot tear a small `write()`.** The kernel keeps written bytes
  after process death, so torn WAL tails are essentially unproducible under
  process-kill with entry-sized appends. Timer-sampled kills landed
  overwhelmingly in the durable-but-unacknowledged window (landing
  distribution retained per trial). The fsync-loss half of abort semantics
  needs power-loss testing, which is out of this gate's scope.
- **Two run-1 drill trials scored a false "fail-open".** The corruption
  drill's third variant backed off past the payload start and flipped an
  unchecksummed header/padding byte — damaging nothing, so recovery and the
  auditor were both correct. The variant was retargeted to flip inside the
  checksummed payload of the entry carrying the unit's current-state row,
  and all 120 drill trials re-ran clean. Incidental observation: WAL entry
  header bytes sit outside checksum coverage, and mid-file structural
  corruption is handled as a torn tail (the suffix silently truncates at
  engine level — the harness auditor catches the loss).
- **The store has no watch API and no append-only flag.** The
  snapshot-handoff cell runs over WAL polling gated by a durability
  watermark (an unfiltered poller can observe appended-but-unflushed
  entries), and journal-delete rejection is enforced at the harness's write
  funnel, the only code path that touches the store. Both are recorded
  upstream as feature requests; the subscription layer will be kernel-owned.
- **Measured latency vs the pre-registered envelope.** Four of six arms
  breached the drafted envelope, which divided reference totals into a
  per-commit mean and compared it against queue-inclusive latency — a
  quantity that grows with writer count even as throughput beats the
  reference (batchOFF arms ran 1.09–1.28× reference throughput). The
  envelope was replaced by owner ruling with regression bars set from this
  run's numbers; the group-commit arms' throughput deltas (0.88×/0.54× at
  8/32 writers) are attributed to this schema's heavier commits and stay
  under investigation.

## What this does and does not close

Closed: the storage-substrate atomicity/recovery questions this gate
existed to answer, including cursor-allocation continuity and the
split-claimant recovery contest. Not closed: uniqueness constraints beyond
those exercised, a continuously-running state auditor service, retention /
backup / erasure, clock semantics, approval invalidation, scheduler
admission and fairness, and operator recovery for permanently-unknown
effects. The 8- and 32-writer arms are capacity evidence only — the
runtime's admission cap of two concurrent writers is a separate,
already-made design decision that this gate neither reopens nor ratifies.

Next gate: G01, the model-free kernel milestone.
