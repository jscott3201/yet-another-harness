# Yet Another Harness Documentation

Yet Another Harness (YAH) is under active redesign and implementation. These
pages separate the tested reliability kernel and initial live composition core
from the Rust-native, graph-backed, plugin-extensible target harness still being
proved.

## Start Here

| Document | Purpose |
|---|---|
| [Project status](project-status.md) | Current evidence, reusable kernel foundation, and unimplemented pivot work |
| [Architecture](architecture.md) | Target composition, graph, plugin, effect, sandbox, and recovery boundaries |
| [Plugin manifest](plugin-manifest.md) | Implemented manifest v1 identities, entrypoints, requests, revision values, and authority limits |
| [Plugin driver](plugin-driver.md) | Implemented exact-activation driver preparation, readiness, cancellation, cleanup, and health contract |
| [Plugin driver conformance](plugin-driver-conformance.md) | Reusable host-side driver cases, trusted fixture contract, evidence, and explicit non-certification limits |
| [Plugin capabilities](plugin-capabilities.md) | Implemented activation-scoped effective grants, exact typed registrations, mediated handles, and revocation limits |
| [Local plugin authoring](plugin-authoring.md) | Runnable trusted built-in consumer, example-only host capability, exact-grant lifecycle, and explicit limits |
| [Wasm plugin contract](wasm-plugin-contract.md) | Provisional WIT world, the Wasmtime driver that executes it, version axes, static import semantics, and runtime limits |
| [Worker process wire protocol](plugin-worker-protocol.md) | Framed strict-JSON protocol v1 for future Node/CPython workers: handshake, calls, streams, cancellation, artifact spill, and resource handles |
| [Application protocol](protocol.md) | Current Adapter 1 experiment; not the future plugin SDK or a stable public protocol |
| [Development](development.md) | Workspace setup, local gates, file-size policy, and pull request checklist |

## Evidence

Gate reports record completed experiments and their limits. A report is
evidence for the named gate only; it does not imply that YAH is ready for
production use.

- [G02: storage fan-in and crash recovery](gates/G02-storage-fanin-recovery.md)

## Generated Reference

The protocol types in `crates/yah-kernel/src/protocol/` generate three checked-in
artifacts:

- `generated/protocol/client.schema.json`
- `generated/protocol/server.schema.json`
- `generated/protocol/protocol.ts`

The worker protocol types in `crates/yah-plugin-ipc/src/types.rs` generate
three more:

- `generated/worker-protocol/worker.schema.json`
- `generated/worker-protocol/host.schema.json`
- `generated/worker-protocol/protocol.ts`

Run `cargo run --locked --manifest-path tools/protocol-codegen/Cargo.toml`
after changing either set. The local gate rejects stale generated files.
