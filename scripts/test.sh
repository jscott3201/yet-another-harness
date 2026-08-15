#!/usr/bin/env bash
# Run the workspace with Nextest, then retain Cargo's separate doctest gate.

set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

readonly expected_version="0.9.143"
profile="${1:-default}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$profile" in
  default | ci) ;;
  *)
    echo "usage: bash scripts/test.sh [default|ci] [nextest arguments...]" >&2
    exit 2
    ;;
esac

installed_version="$(cargo nextest --version 2>/dev/null || true)"
case "$installed_version" in
  "cargo-nextest ${expected_version} "*) ;;
  *)
    echo "cargo-nextest ${expected_version} is required" >&2
    echo "run: bash scripts/install-nextest.sh" >&2
    exit 1
    ;;
esac

echo "==> nextest workspace (profile: ${profile})"
cargo nextest run --locked --workspace --profile "$profile" "$@"

# Nextest intentionally does not execute rustdoc tests.
echo "==> workspace doctests"
cargo test --locked --workspace --doc
