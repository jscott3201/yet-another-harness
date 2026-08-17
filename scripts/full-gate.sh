#!/usr/bin/env bash
# Per-increment gate: a FROM-SCRATCH verification. Run it at every boundary
# where work leaves the fast loop — before a commit lands on main, and
# between PRs (the analog of open-control's development -> main release
# gate). Owner instruction 2026-08-03: the clean is not optional at a PR
# boundary, so consecutive PRs never share build state.
#
# Why the clean: the per-commit hooks build incrementally, and incremental
# artifacts can keep a build green that would not reproduce — a deleted
# module whose object file lingers, a stale proc-macro expansion, a fixture
# no longer generated. Anything that only passes because of cached state is
# a defect the next clone would hit first. Between PRs the same trap has a
# second form: PR N's artifacts can mask a break PR N+1 introduces, and the
# two only diverge once someone builds from a clean tree.
#
# Cost: this rebuilds the whole workspace including the pinned Selene Git
# dependency, so it is minutes, not seconds. Use the hooks for the fast loop
# and this before a PR/commit that lands.
#
# Invoke the script — do not reconstruct its steps as an inline shell chain.
# A gate command piped into anything reports the pipe's exit status, not its
# own, which is how a broken build once got past this gate (prior defect log).
#
# Usage: bash scripts/full-gate.sh

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

echo "==> cargo clean"
cargo clean
cargo clean --manifest-path tools/protocol-codegen/Cargo.toml
# The example guest is outside the workspace and keeps its own target
# directory, so a workspace clean would leave its artifacts behind - exactly
# the stale state this gate exists to rule out.
cargo clean --manifest-path examples/guests/rust-example/Cargo.toml

echo "==> file-size cap"
bash .github/scripts/check-file-size.sh

echo "==> no-secret scan"
bash .github/scripts/check-no-secrets.sh

echo "==> canonical Rust CI lane (from scratch)"
bash scripts/ci-rust.sh

echo
echo "FULL GATE GREEN"
