# Open Agent Documentation

Open Agent is under active design and implementation. These pages describe the
code that exists, its tested boundaries, and the work still required before a
usable release.

## Start Here

| Document | Purpose |
|---|---|
| [Project status](project-status.md) | Current gates, implemented components, and missing product surfaces |
| [Architecture](architecture.md) | Runtime truth model, authority boundaries, and recovery design |
| [Application protocol](protocol.md) | Adapter 1 wire shapes, generated artifacts, and remaining conformance work |
| [Development](development.md) | Workspace setup, local gates, file-size policy, and pull request checklist |

## Evidence

Gate reports record completed experiments and their limits. A report is
evidence for the named gate only; it does not imply that Open Agent is ready for
production use.

- [G02: storage fan-in and crash recovery](gates/G02-storage-fanin-recovery.md)

## Generated Reference

The protocol types in `crates/oa-kernel/src/protocol/` generate three checked-in
artifacts:

- `generated/protocol/client.schema.json`
- `generated/protocol/server.schema.json`
- `generated/protocol/protocol.ts`

Run `cargo run --locked -p oa-kernel --bin generate-protocol` after changing
protocol types. The local gate rejects stale generated files.
