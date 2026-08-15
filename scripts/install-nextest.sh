#!/usr/bin/env bash
# Install the exact cargo-nextest release used by local gates and CI.

set -euo pipefail

readonly nextest_version="0.9.143"
readonly release_base="https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${nextest_version}"

installed_version="$(cargo nextest --version 2>/dev/null || true)"
case "$installed_version" in
  "cargo-nextest ${nextest_version} "*)
    echo "cargo-nextest ${nextest_version} is already installed"
    exit 0
    ;;
esac

case "$(uname -s):$(uname -m)" in
  Darwin:*)
    target="universal-apple-darwin"
    expected_sha="4830d430411148d17602a75cc880bfb4dc8dac153dea59a48a2ef4cc93577f07"
    ;;
  Linux:x86_64 | Linux:amd64)
    target="x86_64-unknown-linux-gnu"
    expected_sha="66786b9abe23920d022a182d1416b1bbc8130dd4872a9553d76985a1708dcd1e"
    ;;
  Linux:aarch64 | Linux:arm64)
    target="aarch64-unknown-linux-gnu"
    expected_sha="2a64b3566a92508550a7ab29c3e8db25472ca37730ecb4d22100b6aa440c2a68"
    ;;
  *)
    echo "unsupported cargo-nextest host: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

for required_command in curl tar; do
  if ! command -v "$required_command" >/dev/null 2>&1; then
    echo "required command is missing: $required_command" >&2
    exit 1
  fi
done

nextest_tmp="$(mktemp -d)"
trap 'rm -rf "$nextest_tmp"' EXIT

archive="${nextest_tmp}/cargo-nextest.tar.gz"
url="${release_base}/cargo-nextest-${nextest_version}-${target}.tar.gz"
echo "downloading cargo-nextest ${nextest_version} for ${target}"
curl --proto '=https' --tlsv1.2 --fail --show-error --location --retry 3 \
  --output "$archive" "$url"

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha="$(sha256sum "$archive" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual_sha="$(shasum -a 256 "$archive" | awk '{print $1}')"
else
  echo "sha256sum or shasum is required to verify cargo-nextest" >&2
  exit 1
fi

if [[ "$actual_sha" != "$expected_sha" ]]; then
  echo "cargo-nextest archive checksum mismatch" >&2
  echo "expected: $expected_sha" >&2
  echo "actual:   $actual_sha" >&2
  exit 1
fi

tar -xzf "$archive" -C "$nextest_tmp"
install_root="${YAH_NEXTEST_INSTALL_ROOT:-${CARGO_HOME:-${HOME}/.cargo}}"
mkdir -p "${install_root}/bin"
install -m 0755 "${nextest_tmp}/cargo-nextest" "${install_root}/bin/cargo-nextest"
echo "installed cargo-nextest ${nextest_version} to ${install_root}/bin"
