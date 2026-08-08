# Open Agent

Open Agent is an experimental, local-first runtime for supervising coding
agents. It is designed to preserve a trustworthy record across retries,
crashes, cancellation, and external tool calls.

**Status: pre-0.1 and not ready for use.** There is no release, installable
daemon, live model connection, or production execution backend yet. Current
work is building and testing the model-free kernel first.

## Why Open Agent

Coding agents can edit files and call tools, but long-running work also needs
answers to harder questions:

- Did this command commit before the process crashed?
- Is this worker still authorized to publish a result?
- Did an external action succeed, fail, or become uncertain?
- Can a retry safely continue without repeating an effect?
- Which result is still current and authorized to advance state?

Open Agent treats those answers as durable runtime state instead of inferring
them from a surviving process or chat transcript.

## Current Progress

- Storage fan-in and crash recovery gate: passed across 1,440 scored trials.
- Model-free kernel gate: in progress.
- Implemented foundations: command receipts, semantic journal, fencing,
  cancellation records, effect reconciliation state and parking, provider
  normalization, and the first in-process JSON protocol slice.
- Not implemented: usable daemon, CLI, live providers, real tool execution,
  sandboxing, MCP lifecycle, or network adapters.

See [project status](docs/project-status.md) for the detailed gate table and
[architecture](docs/architecture.md) for the runtime model.

## Repository

| Path | Contents |
|---|---|
| `crates/oa-kernel/` | Model-free kernel and Adapter 1 work |
| `crates/exp001-harness/` | Storage and recovery evidence harness |
| `generated/protocol/` | Checked-in client/server JSON Schemas and TypeScript bindings |
| `docs/` | Architecture, protocol, development, and gate reports |

The workspace pins Selene to an exact public Git revision. Cargo fetches that
revision during the first build.

## Development

The pinned toolchain is Rust 1.97.1:

```bash
cargo test --locked --workspace
```

Run the complete local gate before a pull request:

```bash
bash scripts/full-gate.sh
```

Source files are capped at 700 reviewable lines. Generated protocol artifacts,
formatting, Clippy, tests, and documentation updates are part of the pull
request checklist. See [development](docs/development.md) for details.

## Documentation

- [Documentation index](docs/README.md)
- [Project status](docs/project-status.md)
- [Architecture](docs/architecture.md)
- [Application protocol](docs/protocol.md)
- [Development and pull requests](docs/development.md)
- [G02 storage evidence](docs/gates/G02-storage-fanin-recovery.md)

## License

Licensed under either the [Apache License, Version 2.0](LICENSE-APACHE) or the
[MIT License](LICENSE-MIT), at your option.
