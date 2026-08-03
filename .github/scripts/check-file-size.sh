#!/usr/bin/env bash
# 700-line cap per source file (ported from open-control). Counts non-empty,
# non-comment lines in Rust sources; flags any that exceed the cap. Covers
# tracked AND new-but-unignored files, so a file added this session is caught
# by the full gate rather than surviving until it is staged. Runs from repo
# root. Compatible with macOS bash 3.x (no mapfile).

set -euo pipefail

CAP=700
violations=0

while IFS= read -r f; do
  [ -f "$f" ] || continue
  loc=$(grep -cvE '^\s*(//.*)?$' "$f" || true)
  if [ "$loc" -gt "$CAP" ]; then
    echo "FAIL: $f has $loc LOC (cap: $CAP)"
    violations=$((violations + 1))
  fi
done < <({ git ls-files '*.rs'; git ls-files --others --exclude-standard '*.rs'; } 2>/dev/null \
  | sort -u | grep -v -E '^(target|generated|out)/' || true)

if [ "$violations" -gt 0 ]; then
  echo
  echo "Refactor or split files exceeding the $CAP LOC cap."
  exit 1
fi

echo "OK: all .rs files within the $CAP LOC cap."
