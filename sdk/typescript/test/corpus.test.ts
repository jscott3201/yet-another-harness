import assert from "node:assert/strict";
import { readdirSync, readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import test from "node:test";

import { FrameDecoder, WireCodecError } from "../src/index.ts";
import { parseWireJson } from "../src/json.ts";

const corpusDirectory = process.env.YAH_REPO_ROOT
  ? new URL("crates/yah-plugin-ipc/tests/corpus/", pathToFileURL(`${process.env.YAH_REPO_ROOT}/`))
  : new URL("../../../crates/yah-plugin-ipc/tests/corpus/", import.meta.url);

test("the shared Rust byte corpus keeps its frame and raw-JSON classifications", () => {
  const names = readdirSync(corpusDirectory)
    .filter((name) => name.startsWith("frame-") || name.startsWith("json-"))
    .sort();
  assert(names.length >= 25, "the shared corpus must not silently shrink");

  for (const name of names) {
    const bytes = new Uint8Array(readFileSync(new URL(name, corpusDirectory)));
    if (name.startsWith("frame-")) {
      assertFrameClass(name, bytes);
    } else {
      assertJsonClass(name, bytes);
    }
  }
});

function assertFrameClass(name: string, bytes: Uint8Array): void {
  const decoder = new FrameDecoder();
  try {
    const frames: Uint8Array[] = [];
    decoder.push(bytes, (frame) => frames.push(frame));
    const end = decoder.finish();
    if (name.startsWith("frame-clean-")) {
      assert(frames.length > 0, name);
      assert.equal(end.kind, "clean", name);
    } else if (name.startsWith("frame-truncated-prefix-")) {
      assert.equal(end.kind, "truncated-prefix", name);
    } else if (name.startsWith("frame-truncated-frame-")) {
      assert.equal(end.kind, "truncated-payload", name);
    } else {
      assert.fail(`${name} should poison`);
    }
  } catch (error: unknown) {
    assert(error instanceof WireCodecError, name);
    if (name.startsWith("frame-poison-empty-")) {
      assert.equal(error.code, "FRAME_EMPTY", name);
    } else if (name.startsWith("frame-poison-too-large-")) {
      assert.equal(error.code, "FRAME_TOO_LARGE", name);
    } else {
      throw error;
    }
    assert.equal(decoder.retainedBytes, 0, name);
  }
}

function assertJsonClass(name: string, bytes: Uint8Array): void {
  let code = "ok";
  try {
    parseWireJson(bytes);
  } catch (error: unknown) {
    assert(error instanceof WireCodecError, name);
    code = error.code;
  }
  if (name.startsWith("json-ok-")) {
    assert.equal(code, "ok", name);
  } else if (name.startsWith("json-duplicate-")) {
    assert.equal(code, "DUPLICATE_MEMBER", name);
  } else if (name.startsWith("json-unsafe-integer-")) {
    assert.equal(code, "UNSAFE_INTEGER", name);
  } else if (name.startsWith("json-syntax-invalid-utf8")) {
    assert.equal(code, "INVALID_UTF8", name);
  } else if (name.startsWith("json-syntax-")) {
    assert.equal(code, "INVALID_JSON", name);
  } else {
    assert.fail(`unknown corpus class: ${name}`);
  }
}
