# Development

## Workspace Requirements

- Rust 1.97.1, pinned by `rust-toolchain.toml`.
- cargo-nextest 0.9.143, installed from a checksum-verified upstream release by
  `scripts/install-nextest.sh`.
- The `wasm32-unknown-unknown` target, for the Rust example guest. Also pinned
  by `rust-toolchain.toml`, so rustup installs it and no lane adds a step for
  it; it is listed here as something the build depends on, not as a chore.
- Node 26, installed yourself. Any 26.x works. The floor is the
  `^22.20 || ^24.12 || >=25` range `@napi-rs/lzma` brings into `jco`'s
  dependency tree — `jco` itself declares no `engines` — and npm only *warns*
  on a dependency engines mismatch, so an older Node fails later and less
  clearly than it would if npm refused. This is the repository's one non-Rust
  toolchain and the one requirement here that nothing in the repository
  installs for you on a development machine; it is needed only to build the
  example guests, see [Example Guests](#example-guests).
- Network access on the first build so Cargo can fetch the exact public Selene
  Git revision pinned in `Cargo.toml` and `Cargo.lock`, and so `npm ci` can
  fetch the TypeScript guest's locked dependency tree from the npm registry.

## Fast Checks

```bash
bash scripts/install-nextest.sh
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
bash scripts/test.sh
```

`scripts/test.sh` runs the native macOS test binaries with Nextest and then
runs Rust doctests separately, because Nextest does not execute doctests. The
repository profile uses four test workers, starts the two known long-running
subscription cases early, detects leaked child processes, and bounds slow and
whole-suite execution.

Filter a fast loop directly with Nextest:

```bash
cargo nextest run --locked --workspace -E 'test(subscription::)'
```

Install the tracked local hooks once per clone:

```bash
bash scripts/install-hooks.sh
```

The pre-commit hook rejects unstaged tracked changes and untracked files, then
checks formatting, generated protocol artifacts, the staged 700-line source
cap, and the complete tracked tree for accidental secrets. The pre-push hook
runs locked Clippy, the non-fail-fast Nextest CI profile, and doctests. A retry
collects diagnostic evidence but `flaky-result = "fail"` prevents it from
turning a flaky test green.

## Native and Container Tests

The ordinary developer loop stays native on macOS. Hosted Rust CI runs in a
digest-pinned Rust 1.97.1 Bookworm job container, which gives the crash,
process, and filesystem tests a reproducible native Linux environment. Cargo
registry and Git sources are cached; `target/` is deliberately rebuilt from
scratch.

Run the same Linux environment and commands locally with Docker:

```bash
bash scripts/container-test.sh
```

The multi-architecture image resolves to native `linux/arm64` on Apple Silicon
and native `linux/amd64` on GitHub's hosted runner. The default uses clean,
disposable build volumes; pass `--reuse-target` for a faster iterative Linux
loop. Set
`YAH_CONTAINER_PLATFORM=linux/amd64` only for an explicit emulation check; it
is not a meaningful performance baseline on Apple Silicon. Named Docker
volumes retain downloaded Cargo sources, the Nextest binary, and the Node
toolchain without creating root-owned files in the checkout.

The image carries Rust and nothing else, so the container installs the lane's
two non-image tools itself — `scripts/install-nextest.sh` and, because Debian
Bookworm's packaged Node is v18, `scripts/install-node.sh`.
Both are pinned and checksum-verified, and both land in the reused
`yah-tools-*` volume, so only the first run per architecture pays for them.
Hosted CI installs the same two through its own steps. `install-node.sh` covers
Linux only: it exists for this container, not as a way to install Node on a
machine you work on.

CI writes JUnit to `target/nextest/ci/junit.xml` and uploads it even when tests
fail. The current suite is about twenty seconds, so build archives and test
partitions would add overhead. Revisit Nextest archive plus `slice:m/n`
partitioning when test execution, rather than compilation, takes several
minutes.

## Fuzzing the Worker Protocol Boundary

The byte-facing half of the worker protocol — the incremental frame decoder
and strict JSON admission in `crates/yah-plugin-ipc` — has an isolated
cargo-fuzz package at `crates/yah-plugin-ipc/fuzz/`. The package is its own
Cargo workspace: nightly Rust, libFuzzer, and every fuzz-only dependency
stay out of the repository workspace, the production crates, and the normal
developer loop. Nothing in the stable gate needs any of them.

Tooling, pinned by what the recorded evidence ran:

- `rustup toolchain install nightly-2026-08-15` — the fuzz toolchain. The
  stable gate never uses it.
- `cargo install cargo-fuzz --locked --version 0.13.2`
- `libfuzzer-sys 0.4`, from the fuzz package's own lockfile.

Four targets, each independently runnable:

```bash
cd crates/yah-plugin-ipc/fuzz
# A: the incremental frame decoder under arbitrary bytes and chunkings
rustup run nightly-2026-08-15 cargo fuzz run incremental_decode \
  fuzz/corpus/incremental_decode ../tests/corpus -- -max_len=65536 -max_total_time=30
# B: strict JSON admission under arbitrary bytes
rustup run nightly-2026-08-15 cargo fuzz run strict_json_admission \
  fuzz/corpus/strict_json_admission ../tests/corpus -- -max_len=65536 -max_total_time=30
# C: admitted-frame round trip (well-typed frames, arbitrary chunkings)
rustup run nightly-2026-08-15 cargo fuzz run frame_round_trip \
  fuzz/corpus/frame_round_trip ../tests/corpus -- -max_len=65536 -max_total_time=30
# D: the stateful host session under arbitrary bounded action traces
rustup run nightly-2026-08-15 cargo fuzz run session_actions \
  fuzz/corpus/session_actions ../tests/corpus -- -max_len=4096 -max_total_time=30
```

The second directory argument is the shared seed corpus,
`crates/yah-plugin-ipc/tests/corpus/`, read-only to the fuzzer; new inputs
land in the gitignored `fuzz/corpus/<target>/`. A longer local campaign is
the same command with `-max_total_time=600` or `-runs=10000000`.

`cargo fuzz` builds with AddressSanitizer by default, and the recorded
evidence ran with it. Keep that default; a sanitizer run needs the nightly
toolchain named above and nothing else.

Corpus discipline: every seed in `tests/corpus/` carries a class prefix
(`frame-clean-`, `frame-poison-too-large-`, `json-duplicate-`, …) that the
stable `corpus_inventory` test verifies against the current implementation,
so a behavior change that reclassifies a seed fails the normal gate, and
two seeds with identical bytes fail the duplicate check. Promotion
workflow: when a fuzz campaign finds a crash or a semantic divergence,
minimize it (`cargo fuzz tmin <target> <artifact>`), add the minimized
input to `tests/corpus/` under the class its correct classification gives
it, fix or narrow the claim, and add the ordinary deterministic regression
test. A retained fuzz artifact is never the only proof of a fix.

The stable gate is deterministic and does not run the fuzzer. A short
smoke campaign exercises the boundary and has already caught one real
defect (a one-ULP float-parsing divergence, fixed by enabling
`float_roundtrip`); it does not come close to proving parser correctness,
and no documentation claims otherwise.

## Session Model and Hostile-Process Pressure

The worker session's state rules are pinned twice over in the normal
stable gate. `crates/yah-plugin-ipc/tests/session_model.rs` compares a
real `HostSession` against an independent reference model — authored from
`docs/plugin-worker-protocol.md`, not from the session code — after every
step of named boundary cases and 2,500 seeded generated traces (ten
pinned seeds, 250 traces each, at most 41 actions per trace). A mismatch
names the step and prints the full JSON trace; traces that once diverged
are pinned under `tests/corpus/session_traces/` and replayed as ordinary
regressions, no generator involved.

The process lane's hostile-pressure suite,
`crates/yah-plugin-proc/tests/hostile_pressure.rs`, drives the same
guarantees through a real socketpair and a real child: floods, command
back-pressure, partial input, trickled goodbyes, and deactivation under
combined pressure. The suite is part of the crate's ordinary test run.

Before merging a change to the session or pump, run the hostile-process
suite thirty consecutive times in the pinned Linux container — zero
flakes or leaked processes is the bar. The recorded evidence path copies
the exact tree into a volume-backed container (this machine's Docker
Desktop cannot stack a volume over a bind mount — a local virtiofs
defect, verified against a pristine `main` — so the checkout is copied
rather than mounted), runs the container under `--init` so reaped
orphans are not mistaken for survivors, and executes the suite thirty
times against the same build:

```bash
IMAGE=rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97
docker volume create yah-stress-ws
docker run -d --init --name yah-stress -v yah-stress-ws:/ws "$IMAGE" sleep infinity
tar --exclude=./target --exclude=./.git --no-xattrs -cf - . | docker cp - yah-stress:/ws
docker exec yah-stress sh -c 'cd /ws && bash scripts/install-nextest.sh'
for i in $(seq 1 30); do
  docker exec yah-stress sh -c 'cd /ws && cargo nextest run --locked -p yah-plugin-proc'
done
docker rm -f yah-stress && docker volume rm yah-stress-ws
```

Hosted CI still runs the canonical read-only lane on every push; the
copy-in path trades mount strictness for the ability to run locally, on
the same digest image, toolchain, and commands.

## Example Guests

Two example plugins implement the same `yah:plugin@0.1.0` world, one in Rust
and one in TypeScript, and the `yah-plugin-wasm` example tests assert they
answer identically. Build both:

```bash
bash scripts/build-guests.sh
```

`scripts/ci-rust.sh` runs this before the test lane, so the full gate and
container run cover it; run it by hand only when iterating on a guest.

Nothing it produces is committed (DEC-038): the artifacts are built from source
at gate time, which is also the only way the TypeScript component could be
reviewed at all, since it is 12 MB of compiled SpiderMonkey. Every byte it
writes lands under `target/`, and nothing under `examples/` is touched — the
container lane mounts the repository readonly, so a build that wrote next to
its sources would pass hosted CI and fail the local parity command.

### The TypeScript component is not byte-reproducible

`jco componentize` does not produce a stable artifact. Measured on 2026-08-16,
four builds of identical input with one pinned `jco` on one machine gave four
different components — 12,493,085, 12,493,116, 12,493,124 and 12,493,178 bytes
— and the same input in the Linux container gave a fifth, 12,491,867. Two of
the four differ in 2,564,053 bytes: 14 in the first megabyte, which are length
prefixes shifting, and the rest concentrated in the trailing four megabytes
that hold the embedded JavaScript engine snapshot.

The Rust guest is the control: its core module came out at 17,951 bytes on
every one of those builds, macOS and Linux alike. That is size rather than
content, so it is not a reproducibility claim for the Rust guest — but it does
place the instability in the JavaScript toolchain rather than in building
guests as such.

So the artifact cannot be pinned by checksum the way `scripts/install-node.sh`
and `scripts/install-nextest.sh` pin their downloads. What *is* pinned is the
input: `package-lock.json` fixes the `jco` version and its whole dependency
tree, and `npm ci` installs exactly that. The component is then verified by
behaviour rather than by hash — the example tests drive both guests through the
same corpus of inputs and require the same answers — which is the guarantee
that actually matters for a conformance example, and the only one available
here.

## Full Gate

Run before opening or merging a pull request:

```bash
bash scripts/full-gate.sh
```

The script starts with `cargo clean` so a prior build cannot hide deleted or
stale generated code, then runs the same canonical Rust lane as container CI.

## 700-Line Review Cap

`.github/scripts/check-file-size.sh` rejects a reviewable file over 700 lines.
For code and configuration it counts non-empty, non-comment lines; for docs,
schemas, assets, and licenses it counts every non-empty line. It covers tracked
and unignored new files. Package-manager lockfiles, build output, and vendored
code are excluded.

The same script runs in the pre-commit hook, the full local gate, and CI. Split
a file along a real module or responsibility boundary rather than adding an
exception.

## Protocol Changes

Two protocol type sets generate checked-in artifacts: the kernel protocol
(`crates/yah-kernel/src/protocol/` → `generated/protocol/`) and the worker
protocol (`crates/yah-plugin-ipc/src/types.rs` →
`generated/worker-protocol/`). After editing either:

```bash
cargo run --locked --manifest-path tools/protocol-codegen/Cargo.toml
```

Commit the Rust change and its generated artifacts together — six files
across the two sets; the gate's `--check` run rejects a stale one. Add or
update a golden or behavioral test that demonstrates the compatibility change.

## Pull Request Checklist

- [ ] The change is scoped to one reviewable responsibility.
- [ ] Every source file remains within the 700-line cap.
- [ ] `bash scripts/full-gate.sh` passes from a clean build.
- [ ] Generated protocol artifacts match the Rust source types.
- [ ] Tests cover success, rejection, replay, and restart behavior where
      applicable.
- [ ] `README.md` still describes the project accurately for a new user.
- [ ] Relevant pages under `docs/` reflect behavior, status, and known gaps.
- [ ] No completed gate or production capability is claimed without its
      evidence report.

Documentation review is required for every pull request. A code-only change may
legitimately need no prose edit, but the pull request should state that the
README and docs were checked.
