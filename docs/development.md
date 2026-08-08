# Development

## Workspace Requirements

- Rust 1.97.1, pinned by `rust-toolchain.toml`.
- Network access on the first build so Cargo can fetch the exact public Selene
  Git revision pinned in `Cargo.toml` and `Cargo.lock`.

## Fast Checks

```bash
cargo fmt --all --check
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
```

Install the tracked local hooks once per clone:

```bash
bash scripts/install-hooks.sh
```

The pre-commit hook rejects unstaged tracked changes and untracked files, then checks
formatting, generated protocol artifacts, the staged 700-line source cap, and
the complete tracked tree for accidental secrets. The pre-push hook runs locked
Clippy and the full locked test workspace. Hosted CI repeats the workspace
gates.

## Full Gate

Run before opening or merging a pull request:

```bash
bash scripts/full-gate.sh
```

The script starts with `cargo clean` so a prior build cannot hide deleted or
stale generated code.

## 700-Line Review Cap

`.github/scripts/check-file-size.sh` rejects a reviewable file over 700 lines.
For code and configuration it counts non-empty, non-comment lines; for docs,
schemas, assets, and licenses it counts every non-empty line. It covers tracked
and unignored new files. Package-manager lockfiles, build output, and vendored
code are excluded.

The same script runs in the pre-commit hook, the full local gate, and CI. Split
a file along a real module or responsibility boundary rather than adding an
exception.

## Protocol Changes

After editing protocol Rust types:

```bash
cargo run --locked -p oa-kernel --bin generate-protocol
```

Commit the Rust change and all three generated artifacts together. Add or
update a golden or behavioral test that demonstrates the compatibility change.

## Pull Request Checklist

- [ ] The change is scoped to one reviewable responsibility.
- [ ] Every source file remains within the 700-line cap.
- [ ] `bash scripts/full-gate.sh` passes from a clean build.
- [ ] Generated protocol artifacts match the Rust source types.
- [ ] Tests cover success, rejection, replay, and restart behavior where
      applicable.
- [ ] `README.md` still describes the project accurately for a new user.
- [ ] Relevant pages under `docs/` reflect behavior, status, and known gaps.
- [ ] No completed gate or production capability is claimed without its
      evidence report.

Documentation review is required for every pull request. A code-only change may
legitimately need no prose edit, but the pull request should state that the
README and docs were checked.
