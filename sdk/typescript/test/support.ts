import assert from "node:assert/strict";

import { WireCodecError, type WireCodecErrorCode } from "../src/index.ts";

export function expectCodecError(
  code: WireCodecErrorCode,
  operation: () => unknown,
): WireCodecError {
  let caught: unknown;
  try {
    operation();
  } catch (error: unknown) {
    caught = error;
  }
  assert(caught instanceof WireCodecError, `expected WireCodecError ${code}`);
  assert.equal(caught.code, code);
  assert(caught.message.length <= 96, "SDK diagnostics stay bounded");
  return caught;
}
