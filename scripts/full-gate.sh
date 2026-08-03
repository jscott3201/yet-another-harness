#!/usr/bin/env bash
# Per-increment gate: a FROM-SCRATCH verification, run before a commit lands
# on main (the analog of open-control's development -> main release gate).
#
# Why the clean: the per-commit hooks build incrementally, and incremental
# artifacts can keep a build green that would not reproduce — a deleted
# module whose object file lingers, a stale proc-macro expansion, a fixture
# no longer generated. Anything that only passes because of cached state is
# a defect the next clone would hit first.
#
# Cost: this rebuilds the whole workspace including the selene-db path
# dependency, so it is minutes, not seconds. Use the hooks for the fast loop
# and this before a PR/commit that lands.
#
# Usage: bash scripts/full-gate.sh

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

echo "==> cargo clean"
cargo clean

echo "==> fmt"
cargo fmt --all --check

echo "==> file-size cap"
bash .github/scripts/check-file-size.sh

echo "==> no-secret scan"
bash .github/scripts/check-no-secrets.sh

echo "==> clippy (-D warnings, all targets, from scratch)"
cargo clippy --workspace --all-targets -- -D warnings

echo "==> tests (from scratch)"
cargo test --workspace

echo
echo "FULL GATE GREEN"
