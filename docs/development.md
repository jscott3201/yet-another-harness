# Development

## Workspace Requirements

- Rust 1.97.1, pinned by `rust-toolchain.toml`.
- cargo-nextest 0.9.143, installed from a checksum-verified upstream release by
  `scripts/install-nextest.sh`.
- Network access on the first build so Cargo can fetch the exact public Selene
  Git revision pinned in `Cargo.toml` and `Cargo.lock`.

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
volumes retain downloaded Cargo sources and the Nextest binary without creating
root-owned files in the checkout.

CI writes JUnit to `target/nextest/ci/junit.xml` and uploads it even when tests
fail. The current suite is about twenty seconds, so build archives and test
partitions would add overhead. Revisit Nextest archive plus `slice:m/n`
partitioning when test execution, rather than compilation, takes several
minutes.

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

After editing protocol Rust types:

```bash
cargo run --locked -p yah-kernel --bin generate-protocol
```

Commit the Rust change and all three generated artifacts together. Add or
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
