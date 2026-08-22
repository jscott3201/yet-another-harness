# TypeScript Worker Wire Codec

`@yet-another-harness/worker-codec` is a private, ESM-only source package. It
implements the worker side of the protocol byte boundary:

- `FrameDecoder` and `encodeFrame` handle the four-byte big-endian envelope;
- `decodeHostMessage` admits UTF-8 and information-preserving JSON, then
  validates the host direction against the checked-in Rust-generated schema;
- `encodeWorkerMessage` rejects JavaScript values that JSON would alter,
  validates the worker direction, and returns unframed payload bytes; and
- protocol types, version, limits, and defaults are re-exported from
  `generated/worker-protocol/protocol.ts`.

The codec enforces the outer frame limit and raw control-frame limit. Payload
class limits, negotiated ceilings, handshake state, call lifecycle, streams,
cancellation, artifact reads and digest checks, capability handles and release,
fd-3 IO, process supervision, sandboxing, and handler APIs belong to later
layers. There is no authored Node worker or published npm package.

The current gate runs Node 26.7.0. Node 24 and 26 are the intended support
lines, but the cross-version matrix has not run yet.

From the repository root, run the staged package gate:

```bash
bash scripts/test-typescript-sdk.sh
```

For a focused source-tree loop:

```bash
cd sdk/typescript
npm ci --ignore-scripts --no-audit --no-fund
npm run typecheck
npm test
```

`scripts/test-typescript-sdk.sh` stages the package under `target/` so the same
command works when the repository is mounted read-only in the Linux parity
container. Its corpus test reads `crates/yah-plugin-ipc/tests/corpus` directly;
the corpus is not copied into this package.
