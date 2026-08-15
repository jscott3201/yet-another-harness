#!/usr/bin/env bash
# Canonical Rust CI lane. Hosted CI and the local container wrapper call this.

set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

echo "==> workspace format"
cargo fmt --all --check

echo "==> protocol generator format"
cargo fmt --manifest-path tools/protocol-codegen/Cargo.toml -- --check

echo "==> protocol generator clippy"
cargo clippy --locked --manifest-path tools/protocol-codegen/Cargo.toml -- -D warnings

echo "==> generated protocol artifacts"
cargo run --locked --manifest-path tools/protocol-codegen/Cargo.toml -- --check

echo "==> workspace clippy"
cargo clippy --locked --workspace --all-targets -- -D warnings

bash scripts/test.sh ci
