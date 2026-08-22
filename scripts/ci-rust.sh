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

echo "==> TypeScript worker codec"
bash scripts/test-typescript-sdk.sh

echo "==> workspace clippy"
cargo clippy --locked --workspace --all-targets -- -D warnings

# Before the tests, because the example-plugin cases load what it builds and
# DEC-038 forbids committing the artifacts. This is where the lane acquires its
# dependency on Node and on an npm registry; `npm ci` installs the committed
# lockfile exactly.
bash scripts/build-guests.sh

bash scripts/test.sh ci
