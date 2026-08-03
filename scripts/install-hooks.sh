#!/usr/bin/env bash
# One-time local setup: point git at the tracked .githooks/ directory.
# Run once per clone:  bash scripts/install-hooks.sh
#
# .githooks/ is version-controlled (unlike .git/hooks/), so every clone
# shares the same gates. Split mirrors CI (ported from open-control):
#   pre-commit -> cargo fmt --check + file-size cap + no-secret scan   (fast)
#   pre-push   -> cargo clippy -D warnings + cargo test --workspace
# The pre-push compile gates stand in for remote CI until selene-db stops
# being a local-only path dependency.
#
# Escape hatches: `git commit/push --no-verify` (once) or
# `export OA_SKIP_HOOKS=1` (whole shell session). Empty, 0, and false do not skip.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

git config core.hooksPath .githooks
chmod +x .githooks/pre-commit .githooks/pre-push 2>/dev/null || true
chmod +x .github/scripts/*.sh 2>/dev/null || true

echo "core.hooksPath -> .githooks"
echo "  pre-commit: cargo fmt --check + file-size cap + no-secret scan"
echo "  pre-push:   cargo clippy -D warnings + cargo test --workspace"
echo "Skip once: --no-verify   |   skip session: export OA_SKIP_HOOKS=1"
