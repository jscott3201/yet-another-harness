#!/usr/bin/env bash
# Run the canonical Rust CI lane in the same multi-arch image used by GitHub.

set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

readonly ci_image="rust:1.97.1-bookworm@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97"

reuse_target=false
case "${1:-}" in
  "") ;;
  --reuse-target) reuse_target=true ;;
  *)
    echo "usage: bash scripts/container-test.sh [--reuse-target]" >&2
    exit 2
    ;;
esac
if [[ $# -gt 1 ]]; then
  echo "usage: bash scripts/container-test.sh [--reuse-target]" >&2
  exit 2
fi

if [[ -n "${YAH_CONTAINER_PLATFORM:-}" ]]; then
  platform="$YAH_CONTAINER_PLATFORM"
else
  case "$(uname -m)" in
    arm64 | aarch64) platform="linux/arm64" ;;
    x86_64 | amd64) platform="linux/amd64" ;;
    *)
      echo "cannot select a native Linux container for $(uname -m)" >&2
      exit 1
      ;;
  esac
fi

case "$platform" in
  linux/arm64) cache_arch="arm64" ;;
  linux/amd64) cache_arch="amd64" ;;
  *)
    echo "YAH_CONTAINER_PLATFORM must be linux/arm64 or linux/amd64" >&2
    exit 2
    ;;
esac

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required for the Linux parity run" >&2
  exit 1
fi

if [[ "$reuse_target" == true ]]; then
  target_mounts=(
    --mount "type=volume,source=yah-target-${cache_arch},target=/workspace/target"
    --mount "type=volume,source=yah-codegen-target-${cache_arch},target=/workspace/tools/protocol-codegen/target"
  )
  target_mode="reused target caches"
else
  # Docker removes these anonymous build volumes with the --rm container.
  target_mounts=(
    --mount "type=volume,target=/workspace/target"
    --mount "type=volume,target=/workspace/tools/protocol-codegen/target"
  )
  target_mode="clean target volumes"
fi

# The image is Rust and nothing else. Hosted CI adds the lane's two non-image
# tools with dedicated steps - cargo-nextest and, for the TypeScript example
# guest, Node - so parity means adding them here too. Not identically: hosted CI
# shares `scripts/install-nextest.sh`, but takes Node from `actions/setup-node`
# pinned only to a major, where this pins an exact version. Both land in the
# reused /opt/yah-tools volume, so only the first run per architecture pays.
# shellcheck disable=SC2016  # `${PATH}` must expand in the container, not here.
readonly container_lane='
set -euo pipefail
export YAH_NEXTEST_INSTALL_ROOT=/opt/yah-tools
export YAH_NODE_INSTALL_ROOT=/opt/yah-tools
export PATH="/opt/yah-tools/bin:/opt/yah-tools/node/bin:${PATH}"
bash scripts/install-nextest.sh
bash scripts/install-node.sh
bash scripts/ci-rust.sh
'

echo "==> running canonical Rust CI lane in ${platform} (${target_mode})"
docker run --rm --init --pull=missing --platform "$platform" \
  --mount "type=bind,source=${PWD},target=/workspace,readonly" \
  --mount "type=volume,source=yah-cargo-registry-${cache_arch},target=/usr/local/cargo/registry" \
  --mount "type=volume,source=yah-cargo-git-${cache_arch},target=/usr/local/cargo/git" \
  --mount "type=volume,source=yah-tools-${cache_arch},target=/opt/yah-tools" \
  "${target_mounts[@]}" \
  --workdir /workspace \
  --env CARGO_INCREMENTAL=0 \
  --env CARGO_TERM_COLOR=always \
  --env RUST_BACKTRACE=1 \
  "$ci_image" \
  bash -c "$container_lane"
