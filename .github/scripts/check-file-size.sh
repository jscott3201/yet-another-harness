#!/usr/bin/env bash
# 700-line cap per reviewable source file (ported from open-control). Covers
# tracked and new-but-unignored code, scripts, config, and generated protocol
# artifacts. Dependency lockfiles are excluded because their layout is owned
# by package managers. Runs from repo root; compatible with macOS bash 3.x.

set -euo pipefail

CAP=700
violations=0
CACHED=false
[ "${1:-}" = "--cached" ] && CACHED=true

count_blob() {
  local f="$1"
  local content
  if $CACHED; then
    content=$(git show ":$f")
  else
    content=$(<"$f")
  fi
  case "$f" in
    *.rs | *.ts | *.tsx | *.js | *.jsx | *.wit)
      printf '%s\n' "$content" | grep -cvE '^\s*(//.*)?$' || true
      ;;
    *.wat)
      printf '%s\n' "$content" | grep -cvE '^\s*(;;.*)?$' || true
      ;;
    *.sh | *.toml | *.yml | *.yaml)
      printf '%s\n' "$content" | grep -cvE '^\s*(#.*)?$' || true
      ;;
    *)
      printf '%s\n' "$content" | grep -cvE '^\s*$' || true
      ;;
  esac
}

if $CACHED; then
  files=$(git diff --cached --name-only --diff-filter=ACMR) || {
    echo "FAIL: could not enumerate staged files"
    exit 2
  }
else
  tracked=$(git ls-files '*.rs' '*.ts' '*.tsx' '*.js' '*.jsx' '*.sh' '*.toml' '*.json' '*.yml' '*.yaml' '*.wit' '*.wat' '*.md' '*.svg' '*.css' '*.html' 'LICENSE*') || {
    echo "FAIL: could not enumerate tracked files"
    exit 2
  }
  untracked=$(git ls-files --others --exclude-standard \
    '*.rs' '*.ts' '*.tsx' '*.js' '*.jsx' '*.sh' '*.toml' '*.json' '*.yml' '*.yaml' '*.wit' '*.wat' '*.md' '*.svg' '*.css' '*.html' 'LICENSE*') || {
    echo "FAIL: could not enumerate untracked files"
    exit 2
  }
  files=$(printf '%s\n%s\n' "$tracked" "$untracked" | sort -u)
fi

while IFS= read -r f; do
  case "$f" in
    *.rs | *.ts | *.tsx | *.js | *.jsx | *.sh | *.toml | *.json | *.yml | *.yaml | *.wit | *.wat | *.md | *.svg | *.css | *.html | LICENSE*) ;;
    *) continue ;;
  esac
  $CACHED || [ -f "$f" ] || continue
  loc=$(count_blob "$f")
  if [ "$loc" -gt "$CAP" ]; then
    echo "FAIL: $f has $loc LOC (cap: $CAP)"
    violations=$((violations + 1))
  fi
done < <(printf '%s\n' "$files" \
  | grep -v -E '(^|/)(package-lock\.json|pnpm-lock\.yaml)$|^(target|out|vendor)/' || true)

if [ "$violations" -gt 0 ]; then
  echo
  echo "Refactor or split files exceeding the $CAP LOC cap."
  exit 1
fi

echo "OK: all reviewable files within the $CAP LOC cap."
