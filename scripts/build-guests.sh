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
# Every byte this script writes lands under `target/`, and nothing under
# `examples/` is touched. That is not tidiness: `scripts/container-test.sh`
# bind-mounts the repository *readonly* so a container can never mutate the
# host tree, and supplies writable volumes only under `target/`. A build that
# wrote its outputs next to its sources would pass in CI — where the checkout
# is writable — and fail the documented local parity command, which is the
# failure mode the container lane exists to catch.
#
# Usage: bash scripts/build-guests.sh

set -euo pipefail
# Derived from this script's own location, as `ci-rust.sh`, `container-test.sh`
# and `test.sh` do, and deliberately not from `git rev-parse --show-toplevel`.
# (`full-gate.sh` and `install-hooks.sh` still use git, and may: neither runs
# inside the CI container.) Hosted CI checks out as
# one uid and runs the job container as root, so git refuses the repository as
# dubiously owned and exits 128. That was already happening before this script
# needed the value: the old `cd "$(git rev-parse ...)"` swallowed it, because
# `cd ""` is a successful no-op, and the script carried on from the directory
# `ci-rust.sh` had already set. Nothing here needs git.
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

# Preflight rather than a confusing failure three commands in. The Rust half
# needs only what the workspace already needs; the TypeScript half needs Node,
# which is this lane's one non-Rust dependency.
for tool in node npm; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "FAIL: $tool is required to build the TypeScript example guest." >&2
    echo "      Node 26; the dependency floor is ^22.20 || ^24.12 || >=25, and npm" >&2
    echo "      only warns when it is unmet. See docs/development.md." >&2
    exit 2
  fi
done

out="target/guests"
rust_build="target/guests/rust-example.build"
ts_build="target/guests/ts-example.build"
mkdir -p "$out"

echo "==> rust example guest (clippy)"
# The guest is outside the workspace, so the workspace lane never sees it. An
# example that does not pass the repository's own lint bar is not an example.
#
# `--target-dir` on every cargo invocation, including this one: clippy writes
# artifacts too, and the default would be `examples/guests/rust-example/target`.
cargo clippy --locked --release \
  --target wasm32-unknown-unknown \
  --target-dir "$rust_build" \
  --manifest-path examples/guests/rust-example/Cargo.toml \
  --all-targets -- -D warnings

echo "==> rust example guest (format)"
# The guest is outside the workspace, so `cargo fmt --all` never reaches it.
# `--check` reports rather than rewrites, so this one writes nothing anywhere.
cargo fmt --check --manifest-path examples/guests/rust-example/Cargo.toml

echo "==> rust example guest (core module)"
# `--locked`, like every other cargo invocation in scripts/. Without it the
# guest's committed lockfile is a starting point rather than a pin: cargo
# re-resolves against the registry and silently writes a new lock mid-gate.
cargo build --locked --release \
  --target wasm32-unknown-unknown \
  --target-dir "$rust_build" \
  --manifest-path examples/guests/rust-example/Cargo.toml
cp "$rust_build/wasm32-unknown-unknown/release/yah_guest_rust_example.wasm" \
  "$out/rust-example.core.wasm"

echo "==> typescript example guest (component)"
# Staged rather than built in place, because `npm ci` materialises a ~250 MB
# `node_modules` and `jco` emits a 12 MB component — three writes into a tree
# the container lane mounts readonly. Copying three small text files is the
# cheaper half of that trade, and `npm ci` deletes and reinstalls
# `node_modules` on every run regardless, so staging costs no install time.
#
# Wiped first so a renamed or deleted source cannot linger here and be built
# instead of what the repository actually has. The file list is explicit and
# short; a source file added without being added here fails loudly at the
# import, which is the failure mode to want from a stale-input guard.
rm -rf "$ts_build"
mkdir -p "$ts_build"
cp examples/guests/ts-example/package.json \
  examples/guests/ts-example/package-lock.json \
  examples/guests/ts-example/guest.ts \
  "$ts_build/"
(
  cd "$ts_build"
  npm ci --no-audit --no-fund
  # The two paths the build script parameterises. Absolute, because the
  # staging directory does not sit at the depth the in-place default assumes —
  # and a developer running `npm run build` in `examples/guests/ts-example`
  # still gets the defaults, which is why they remain defaults.
  YAH_GUEST_WIT_DIR="$repo_root/crates/yah-plugin-wasm/wit" \
    YAH_GUEST_OUT="$repo_root/$out/ts-example.component.wasm" \
    npm run build
)

echo
ls -l "$out"
echo "GUESTS BUILT"
