#!/usr/bin/env bash
# Build the example guest plugins from source into `target/guests`.
#
# DEC-038: guests are source like everything else, so nothing here is committed
# as a binary. The gate runs this before the test lane, and the example tests
# fail with a pointer back to this script if its outputs are missing.
#
# Two toolchains, because the point of the pair is that neither language is
# privileged by the world:
#
#   Rust — `cargo build --target wasm32-unknown-unknown` produces a *core*
#   module. Turning it into a component is `wit_component::ComponentEncoder`,
#   which the test does rather than this script, because that crate is already
#   in the workspace and a CLI for it would be a third toolchain to install.
#
#   TypeScript — `jco componentize` produces a component directly, and needs
#   Node. `npm ci` installs from the committed lockfile: an exact tree, not
#   whatever the registry resolves today.
#
# Usage: bash scripts/build-guests.sh

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

out="target/guests"
mkdir -p "$out"

echo "==> rust example guest (clippy)"
# The guest is outside the workspace, so the workspace lane never sees it. An
# example that does not pass the repository's own lint bar is not an example.
cargo clippy --release \
  --target wasm32-unknown-unknown \
  --manifest-path examples/guests/rust-example/Cargo.toml \
  --all-targets -- -D warnings

echo "==> rust example guest (core module)"
cargo build --release \
  --target wasm32-unknown-unknown \
  --manifest-path examples/guests/rust-example/Cargo.toml
cp examples/guests/rust-example/target/wasm32-unknown-unknown/release/yah_guest_rust_example.wasm \
  "$out/rust-example.core.wasm"

echo "==> typescript example guest (component)"
(
  cd examples/guests/ts-example
  npm ci --no-audit --no-fund
  npm run build
)
cp examples/guests/ts-example/guest.component.wasm "$out/ts-example.component.wasm"

echo
ls -l "$out"
echo "GUESTS BUILT"
