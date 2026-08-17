#!/usr/bin/env bash
# Install the pinned Node release the TypeScript example guest is built with.
#
# This exists for `scripts/container-test.sh`. The canonical Rust CI lane runs
# in `rust:1.97.1-bookworm`, which carries no Node at all, and Debian bookworm's
# apt Node is v18 — below `jco`'s own `engines` floor of `^22.20 || ^24.12 ||
# >=25`, so it fails at install rather than at build. Hosted CI gets Node from
# `actions/setup-node`; this is the local parity run's equivalent.
#
# Developers on macOS and Linux workstations are not expected to run this: use
# whatever Node manager you already have, per docs/development.md. Nothing here
# is a supported way to install Node on a machine you work on.
#
# The version is pinned and the archive is checksummed for the same reason
# scripts/install-nextest.sh does both: a gate that silently changes toolchain
# under you is not a gate.

set -euo pipefail

# Kept in step with `node-version` in the ci workflow's setup-node step. That
# one floats within a major and this one cannot, so they agree on the major and
# hosted CI is the lane that would notice a 26.x regression first.
readonly node_version="26.7.0"
readonly release_base="https://nodejs.org/dist/v${node_version}"

install_root="${YAH_NODE_INSTALL_ROOT:-${HOME}/.local}"
readonly node_home="${install_root}/node"

if [[ "$("${node_home}/bin/node" --version 2>/dev/null || true)" == "v${node_version}" ]]; then
  echo "node ${node_version} is already installed at ${node_home}"
  exit 0
fi

case "$(uname -s):$(uname -m)" in
  Linux:x86_64 | Linux:amd64)
    target="linux-x64"
    expected_sha="bd6b6c31e377bad9ad579bed72e5bc11f4c879ac9452ad51d30e646ea3d828df"
    ;;
  Linux:aarch64 | Linux:arm64)
    target="linux-arm64"
    expected_sha="925aa6157dd37542d0d7f2e28b7bf61e7b39284411210b0498bc3788db4aef68"
    ;;
  *)
    echo "this installer covers the Linux CI container only, not $(uname -s) $(uname -m)" >&2
    echo "install Node ${node_version%%.*} however you normally do; see docs/development.md" >&2
    exit 1
    ;;
esac

for required_command in curl tar; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "required command is missing: $required_command" >&2
    exit 1
  fi
done

node_tmp="$(mktemp -d)"
trap 'rm -rf "$node_tmp"' EXIT

archive="${node_tmp}/node.tar.gz"
url="${release_base}/node-v${node_version}-${target}.tar.gz"
echo "downloading node ${node_version} for ${target}"
curl --proto '=https' --tlsv1.2 --fail --show-error --location --retry 3 \
  --output "$archive" "$url"

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha="$(sha256sum "$archive" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual_sha="$(shasum -a 256 "$archive" | awk '{print $1}')"
else
  echo "sha256sum or shasum is required to verify node" >&2
  exit 1
fi

if [[ "$actual_sha" != "$expected_sha" ]]; then
  echo "node archive checksum mismatch" >&2
  echo "expected: $expected_sha" >&2
  echo "actual:   $actual_sha" >&2
  exit 1
fi

tar -xzf "$archive" -C "$node_tmp"

# The whole tree, not just `bin/node`: `bin/npm` is a relative symlink into
# `lib/node_modules/npm`, so npm only resolves when the layout around it
# survives. Replaced rather than merged, so a version change cannot leave a
# half-old tree behind in the reused container volume.
rm -rf "$node_home"
mkdir -p "$install_root"
mv "${node_tmp}/node-v${node_version}-${target}" "$node_home"
echo "installed node ${node_version} to ${node_home}"
