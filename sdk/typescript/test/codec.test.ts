import assert from "node:assert/strict";
import test from "node:test";

import {
  DEFAULT_CEILINGS,
  DEFAULT_WIRE_LIMITS,
  PROTOCOL_VERSION,
  decodeHostMessage,
  encodeWorkerMessage,
  type WorkerMessage,
} from "../src/index.ts";
import { expectCodecError } from "./support.ts";

const encoder = new TextEncoder();

test("host payload admission is directional and preserves arbitrary JSON payload data", () => {
  const payload = encoder.encode(
    '{"frame":"call","call_id":1,"method":"m","deadline_ms":null,"stream":false,"payload":{"__proto__":5,"n":0.1}}',
  );
  const decoded = decodeHostMessage(payload);
  assert.equal(decoded.frame, "call");
  if (decoded.frame === "call") {
    assert.equal(Object.hasOwn(decoded.payload as object, "__proto__"), true);
    assert.equal((decoded.payload as Record<string, unknown>).__proto__, 5);
    assert.equal((decoded.payload as Record<string, unknown>).n, 0.1);
  }
  expectCodecError("INVALID_HOST_MESSAGE", () =>
    decodeHostMessage(
      encoder.encode(
        '{"frame":"hello","protocol_versions":[1],"sdk_name":"x","sdk_version":"x","features":[],"required_features":[]}',
      ),
    ),
  );
});

test("raw host and worker control frames enforce the generated bound exactly", () => {
  const hostAtBound = hostAcceptPayload(DEFAULT_WIRE_LIMITS.max_control_frame_bytes);
  assert.equal(hostAtBound.byteLength, DEFAULT_WIRE_LIMITS.max_control_frame_bytes);
  assert.equal(decodeHostMessage(hostAtBound).frame, "accept");
  expectCodecError("CONTROL_FRAME_TOO_LARGE", () =>
    decodeHostMessage(hostAcceptPayload(DEFAULT_WIRE_LIMITS.max_control_frame_bytes + 1)),
  );

  const workerAtBound = workerHelloPayload(DEFAULT_WIRE_LIMITS.max_control_frame_bytes);
  assert.equal(workerAtBound.byteLength, DEFAULT_WIRE_LIMITS.max_control_frame_bytes);
  expectCodecError("CONTROL_FRAME_TOO_LARGE", () =>
    workerHelloPayload(DEFAULT_WIRE_LIMITS.max_control_frame_bytes + 1),
  );
});

test("outer payload bounds apply even when framing helpers are bypassed", () => {
  expectCodecError("FRAME_EMPTY", () => decodeHostMessage(new Uint8Array()));
  expectCodecError("FRAME_TOO_LARGE", () =>
    decodeHostMessage(new Uint8Array(DEFAULT_WIRE_LIMITS.max_frame_bytes + 1)),
  );
});

test("outbound encoding refuses every JavaScript value that JSON would mutate", () => {
  const cyclic: Record<string, unknown> = {};
  cyclic.self = cyclic;
  const sparse = new Array(1);
  const arrayGetter = Object.defineProperty([], "0", {
    configurable: true,
    enumerable: true,
    get: () => 1,
  });
  const withSymbolKey = { ok: true, [Symbol("hidden")]: true };
  const withGetter = Object.defineProperty({}, "x", { enumerable: true, get: () => 1 });
  for (const [value, code] of [
    [Number.NaN, "NON_FINITE_NUMBER"],
    [Number.POSITIVE_INFINITY, "NON_FINITE_NUMBER"],
    [9_007_199_254_740_992, "UNSAFE_INTEGER"],
    [1n, "INVALID_OUTBOUND_VALUE"],
    [undefined, "INVALID_OUTBOUND_VALUE"],
    [() => 1, "INVALID_OUTBOUND_VALUE"],
    [Symbol("value"), "INVALID_OUTBOUND_VALUE"],
    [cyclic, "CYCLIC_OUTBOUND_VALUE"],
    [sparse, "INVALID_OUTBOUND_VALUE"],
    [arrayGetter, "INVALID_OUTBOUND_VALUE"],
    [withSymbolKey, "INVALID_OUTBOUND_VALUE"],
    [withGetter, "INVALID_OUTBOUND_VALUE"],
    [new Date(0), "INVALID_OUTBOUND_VALUE"],
    ["\ud800", "INVALID_UNICODE"],
  ] as const) {
    expectCodecError(code, () => encodeWorkerMessage(workerCall(value) as WorkerMessage));
  }
});

test("outbound schema failure does not strip unknown properties", () => {
  const message = { frame: "goodbye", reason: "done", unknown: "retained" };
  expectCodecError("INVALID_WORKER_MESSAGE", () =>
    encodeWorkerMessage(message as unknown as WorkerMessage),
  );
  assert.equal(message.unknown, "retained");
});

test("data frames are not assigned session payload-class bounds by this codec", () => {
  const message = workerCall("x".repeat(DEFAULT_WIRE_LIMITS.max_control_frame_bytes));
  const payload = encodeWorkerMessage(message as WorkerMessage);
  assert(payload.byteLength > DEFAULT_WIRE_LIMITS.max_control_frame_bytes);
  assert(payload.byteLength < DEFAULT_WIRE_LIMITS.max_frame_bytes);
});

test("outbound negative zero keeps its sign on the wire", () => {
  const payload = encodeWorkerMessage(workerCall(-0) as WorkerMessage);
  assert.equal(new TextDecoder().decode(payload).endsWith('"payload":-0}'), true);
});

function workerCall(payload: unknown): Record<string, unknown> {
  return {
    frame: "call",
    call_id: 1,
    method: "m",
    deadline_ms: null,
    stream: false,
    payload,
  };
}

function workerHelloPayload(targetBytes: number): Uint8Array {
  const message: WorkerMessage = {
    frame: "hello",
    protocol_versions: [PROTOCOL_VERSION],
    sdk_name: "sdk",
    sdk_version: "0.0.0",
    features: [""],
    required_features: [],
  };
  const base = encodeWorkerMessage(message);
  const padding = targetBytes - base.byteLength;
  assert(padding >= 0);
  message.features = ["x".repeat(padding)];
  return encodeWorkerMessage(message);
}

function hostAcceptPayload(targetBytes: number): Uint8Array {
  const prefix = `{"frame":"accept","protocol_version":${PROTOCOL_VERSION},"features":["`;
  const suffix = `"],"limits":${wireLimitsText()},"ceilings":${ceilingsText()}}`;
  const padding = targetBytes - encoder.encode(prefix + suffix).byteLength;
  assert(padding >= 0);
  return encoder.encode(prefix + "x".repeat(padding) + suffix);
}

function wireLimitsText(): string {
  const limits = DEFAULT_WIRE_LIMITS;
  return `{"max_frame_bytes":${limits.max_frame_bytes},"max_control_frame_bytes":${limits.max_control_frame_bytes},"max_call_payload_bytes":${limits.max_call_payload_bytes},"max_inline_result_bytes":${limits.max_inline_result_bytes},"max_stream_data_bytes":${limits.max_stream_data_bytes},"max_artifact_read_bytes":${limits.max_artifact_read_bytes}}`;
}

function ceilingsText(): string {
  const ceilings = DEFAULT_CEILINGS;
  return `{"host_calls_in_flight":${ceilings.host_calls_in_flight},"worker_calls_in_flight":${ceilings.worker_calls_in_flight},"live_handles":${ceilings.live_handles},"initial_stream_credit":${ceilings.initial_stream_credit},"max_stream_credit":${ceilings.max_stream_credit}}`;
}
