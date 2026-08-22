import assert from "node:assert/strict";
import test from "node:test";

import { MAX_WIRE_ID, type HostMessage, type WorkerMessage } from "../src/index.ts";
import { assertHostMessage, assertWorkerMessage } from "../src/validation.ts";
import { expectCodecError } from "./support.ts";

const workerHello = (sdkName = "sdk"): WorkerMessage => ({
  frame: "hello",
  protocol_versions: [1],
  sdk_name: sdkName,
  sdk_version: "0.0.0",
  features: [],
  required_features: [],
});

const hostGoodbye: HostMessage = { frame: "goodbye", reason: "done" };

test("the generated Draft 2020-12 schemas compile and validate each direction", () => {
  assert.doesNotThrow(() => assertHostMessage(hostGoodbye));
  assert.doesNotThrow(() => assertWorkerMessage(workerHello()));
  expectCodecError("INVALID_HOST_MESSAGE", () => assertHostMessage(workerHello()));
  expectCodecError("INVALID_WORKER_MESSAGE", () =>
    assertWorkerMessage({
      frame: "accept",
      protocol_version: 1,
      features: [],
      limits: {},
      ceilings: {},
    }),
  );
});

test("unknown fields, tags, enum values, and missing fields remain closed", () => {
  for (const value of [
    { ...workerHello(), extra: true },
    { ...workerHello(), frame: "future" },
    { frame: "cancel", call_id: 1, target: "later" },
    { frame: "goodbye" },
  ]) {
    expectCodecError("INVALID_WORKER_MESSAGE", () => assertWorkerMessage(value));
  }
});

test("id and uint32 formats enforce their exact numeric bounds", () => {
  for (const callId of [1, MAX_WIRE_ID]) {
    assert.doesNotThrow(() =>
      assertWorkerMessage({ frame: "cancel", call_id: callId, target: "call" }),
    );
  }
  for (const callId of [0, MAX_WIRE_ID + 1, 1.5]) {
    expectCodecError("INVALID_WORKER_MESSAGE", () =>
      assertWorkerMessage({ frame: "cancel", call_id: callId, target: "call" }),
    );
  }
  assert.doesNotThrow(() =>
    assertWorkerMessage({ frame: "credit", call_id: 1, additional: 0xffff_ffff }),
  );
  expectCodecError("INVALID_WORKER_MESSAGE", () =>
    assertWorkerMessage({ frame: "credit", call_id: 1, additional: 0x1_0000_0000 }),
  );
});

test("schema string lengths count Unicode scalar values", () => {
  assert.doesNotThrow(() => assertWorkerMessage(workerHello("😀".repeat(64))));
  expectCodecError("INVALID_WORKER_MESSAGE", () =>
    assertWorkerMessage(workerHello("😀".repeat(65))),
  );
  expectCodecError("INVALID_WORKER_MESSAGE", () => assertWorkerMessage(workerHello("")));
});

test("validation does not remove or rewrite rejected data", () => {
  const value = { ...workerHello(), unknown: "retained" };
  expectCodecError("INVALID_WORKER_MESSAGE", () => assertWorkerMessage(value));
  assert.equal(value.unknown, "retained");
});
