# Open Agent Documentation

Open Agent is under active redesign and implementation. These pages separate
the tested reliability kernel that exists today from the Rust-native,
graph-backed, plugin-extensible target harness now being designed and spiked.

## Start Here

| Document | Purpose |
|---|---|
| [Project status](project-status.md) | Current evidence, reusable kernel foundation, and unimplemented pivot work |
| [Architecture](architecture.md) | Target composition, graph, plugin, effect, sandbox, and recovery boundaries |
| [Application protocol](protocol.md) | Current Adapter 1 experiment; not the future plugin SDK or a stable public protocol |
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
