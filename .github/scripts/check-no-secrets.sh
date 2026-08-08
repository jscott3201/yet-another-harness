#!/usr/bin/env bash
# Baseline no-secret scan (ported from open-control). Greps tracked files for
# common secret-shaped patterns. The `_*/` planning corpus and `*-id.json`
# identity files are gitignored (never tracked) so they are out of scope by
# construction — this catches the noisy floor before a push makes it public.

set -euo pipefail

violations=0
CACHED=false
[ "${1:-}" = "--cached" ] && CACHED=true

scan() {
  local pattern="$1"
  local label="$2"
  local grep_args=(-nE)
  $CACHED && grep_args+=(--cached)
  local output
  local status
  set +e
  output=$(git grep "${grep_args[@]}" -e "$pattern" 2>&1)
  status=$?
  set -e
  case "$status" in
    0)
      echo "FAIL: matched $label pattern in tracked files:"
      printf '%s\n' "$output"
      violations=$((violations + 1))
      ;;
    1) ;;
    *)
      echo "FAIL: secret scanner could not inspect the repository:"
      printf '%s\n' "$output"
      exit 2
      ;;
  esac
}

scan 'AKIA[0-9A-Z]{16}' 'AWS access key id'
scan '-----BEGIN (RSA|EC|OPENSSH|PGP) PRIVATE KEY-----' 'private key block'
scan 'xox[abpr]-[A-Za-z0-9-]{10,}' 'Slack token'
scan 'gh[pousr]_[A-Za-z0-9]{36,}' 'GitHub token'
scan 'sk-[A-Za-z0-9_-]{20,}' 'API token (sk- prefix)'

# Project-specific leak classes: full UUIDs (agent / session identity ids,
# the *-id.json class), developer home-directory paths, and Aionforge env-var
# names. Each pattern matches a SHAPE, never a literal sensitive value, so
# this scanner never re-leaks what it detects.
scan '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}' 'UUID (possible agent / session identity)'
scan '/(Users|home)/[A-Za-z0-9._-]+/' 'absolute developer home-directory path'
scan 'AIONFORGE_[A-Z][A-Z_]+' 'Aionforge environment variable name'

if [ "$violations" -gt 0 ]; then
  echo
  echo "Remove secrets from the working tree, rotate compromised credentials, and rewrite history."
  exit 1
fi

echo "OK: baseline no-secret scan clean."
