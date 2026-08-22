#!/usr/bin/env bash
# Test the source-level worker codec from a writable staging tree. The Linux
# parity lane bind-mounts the checkout read-only, so dependency installation
# and all package-manager state belong under target/.

set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repo_root"

for tool in node npm; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "FAIL: $tool is required to test the TypeScript worker codec." >&2
    exit 2
  fi
done

node_major="$(node -p 'process.versions.node.split(".")[0]')"
case "$node_major" in
  24 | 26) ;;
  *)
    echo "FAIL: the TypeScript worker codec gate requires Node 24 or 26; found $(node --version)." >&2
    exit 2
    ;;
esac

stage="$repo_root/target/typescript-sdk"
package_stage="$stage/sdk/typescript"
rm -rf "$stage"
mkdir -p "$package_stage" "$stage/generated/worker-protocol"
cp sdk/typescript/package.json sdk/typescript/package-lock.json sdk/typescript/tsconfig.json "$package_stage/"
cp -R sdk/typescript/src sdk/typescript/test "$package_stage/"
cp generated/worker-protocol/protocol.ts \
  generated/worker-protocol/host.schema.json \
  generated/worker-protocol/worker.schema.json \
  "$stage/generated/worker-protocol/"

echo "==> TypeScript worker codec ($(node --version))"
(
  cd "$package_stage"
  npm ci --ignore-scripts --no-audit --no-fund
  npm run typecheck
  YAH_REPO_ROOT="$repo_root" npm test
)
