import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_WIRE_LIMITS,
  FRAME_PREFIX_BYTES,
  encodeFrame,
  FrameDecoder,
} from "../src/index.ts";
import { expectCodecError } from "./support.ts";

const textEncoder = new TextEncoder();

function decodeChunks(chunks: readonly Uint8Array[]): Uint8Array[] {
  const decoder = new FrameDecoder();
  const frames: Uint8Array[] = [];
  for (const chunk of chunks) {
    decoder.push(chunk, (frame) => frames.push(frame));
  }
  assert.deepEqual(decoder.finish(), { kind: "clean" });
  return frames;
}

test("every prefix and payload split produces the same representative frames", () => {
  const first = textEncoder.encode('{"frame":"goodbye","reason":"one"}');
  const second = textEncoder.encode('{"frame":"goodbye","reason":"two"}');
  const stream = new Uint8Array(encodeFrame(first).length + encodeFrame(second).length);
  stream.set(encodeFrame(first));
  stream.set(encodeFrame(second), encodeFrame(first).length);

  for (let split = 0; split <= stream.length; split += 1) {
    assert.deepEqual(decodeChunks([stream.subarray(0, split), stream.subarray(split)]), [first, second]);
  }
  assert.deepEqual(decodeChunks([stream]), [first, second]);
  assert.deepEqual(
    decodeChunks(Array.from(stream, (byte) => Uint8Array.of(byte))),
    [first, second],
  );
});

test("the outer bound is admitted and zero and one-over declarations poison", () => {
  const maximal = new Uint8Array(DEFAULT_WIRE_LIMITS.max_frame_bytes);
  assert.deepEqual(decodeChunks([encodeFrame(maximal)]), [maximal]);
  expectCodecError("FRAME_EMPTY", () => encodeFrame(new Uint8Array()));
  expectCodecError("FRAME_TOO_LARGE", () =>
    encodeFrame(new Uint8Array(DEFAULT_WIRE_LIMITS.max_frame_bytes + 1)),
  );

  for (const [prefix, code] of [
    [Uint8Array.of(0, 0, 0, 0), "FRAME_EMPTY"],
    [prefixFor(DEFAULT_WIRE_LIMITS.max_frame_bytes + 1), "FRAME_TOO_LARGE"],
  ] as const) {
    for (let split = 0; split <= FRAME_PREFIX_BYTES; split += 1) {
      const decoder = new FrameDecoder();
      let error;
      if (split < FRAME_PREFIX_BYTES) {
        decoder.push(prefix.subarray(0, split), assertNoFrame);
        error = expectCodecError(code, () =>
          decoder.push(prefix.subarray(split), assertNoFrame),
        );
      } else {
        error = expectCodecError(code, () => decoder.push(prefix, assertNoFrame));
      }
      assert.equal(decoder.retainedBytes, 0);
      assert.equal(decoder.poisoned, true);
      assert.strictEqual(
        expectCodecError(code, () =>
          decoder.push(encodeFrame(Uint8Array.of(1)), assertNoFrame),
        ),
        error,
      );
      assert.equal(decoder.retainedBytes, 0);
      assert.strictEqual(expectCodecError(code, () => decoder.finish()), error);
    }
  }
});

test("EOF distinguishes clean input, a short prefix, and a short payload", () => {
  assert.deepEqual(new FrameDecoder().finish(), { kind: "clean" });
  for (let have = 1; have < FRAME_PREFIX_BYTES; have += 1) {
    const decoder = new FrameDecoder();
    decoder.push(Uint8Array.of(0, 0, 5).subarray(0, have), assertNoFrame);
    assert.deepEqual(decoder.finish(), { kind: "truncated-prefix", have });
    assert.equal(decoder.retainedBytes, have);
  }

  const decoder = new FrameDecoder();
  decoder.push(Uint8Array.of(0, 0, 0, 5, 1, 2), assertNoFrame);
  assert.deepEqual(decoder.finish(), { kind: "truncated-payload", declared: 5, have: 2 });
  assert.equal(decoder.retainedBytes, 6);
  const frames: Uint8Array[] = [];
  decoder.push(Uint8Array.of(3, 4, 5), (frame) => frames.push(frame));
  assert.deepEqual(frames, [Uint8Array.of(1, 2, 3, 4, 5)]);
  assert.equal(decoder.retainedBytes, 0);
  assert.deepEqual(decoder.finish(), { kind: "clean" });
});

test("a declaration does not create logical payload retention", () => {
  const decoder = new FrameDecoder();
  decoder.push(prefixFor(DEFAULT_WIRE_LIMITS.max_frame_bytes), assertNoFrame);
  assert.equal(decoder.retainedBytes, 4);
  decoder.push(new Uint8Array(17), assertNoFrame);
  assert.equal(decoder.retainedBytes, 21);
});

test("a valid frame coalesced before poison is emitted before the terminal error", () => {
  const payload = Uint8Array.of(1, 2, 3);
  const bytes = new Uint8Array(encodeFrame(payload).byteLength + FRAME_PREFIX_BYTES);
  bytes.set(encodeFrame(payload));
  const frames: Uint8Array[] = [];
  const decoder = new FrameDecoder();
  expectCodecError("FRAME_EMPTY", () =>
    decoder.push(bytes, (frame) => frames.push(frame)),
  );
  assert.deepEqual(frames, [payload]);
  assert.equal(decoder.retainedBytes, 0);
});

function prefixFor(length: number): Uint8Array {
  return Uint8Array.of(length >>> 24, length >>> 16, length >>> 8, length);
}

function assertNoFrame(): never {
  return assert.fail("an incomplete or poisoned prefix cannot emit a frame");
}
