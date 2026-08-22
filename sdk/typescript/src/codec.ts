import { DEFAULT_WIRE_LIMITS, type HostMessage, type WorkerMessage } from "../../../generated/worker-protocol/protocol.ts";

import { codecError } from "./errors.ts";
import { assertJsonRepresentable, parseWireJson, stringifyWireJson } from "./json.ts";
import { assertHostMessage, assertWorkerMessage } from "./validation.ts";

const dataFrameTags: ReadonlySet<string> = new Set(["call", "reply", "stream-data"]);

/** Admit one unframed host payload after UTF-8, raw-JSON, and host-schema checks. */
export function decodeHostMessage(payload: Uint8Array): HostMessage {
  assertOuterBound(payload);
  const value = parseWireJson(payload);
  assertHostMessage(value);
  assertControlBound(value.frame, payload.byteLength);
  return value;
}

/** Validate and encode one unframed worker payload without lossy JSON conversion. */
export function encodeWorkerMessage(message: WorkerMessage): Uint8Array {
  assertJsonRepresentable(message);
  assertWorkerMessage(message);
  const payload = stringifyWireJson(message);
  assertOuterBound(payload);
  assertControlBound(message.frame, payload.byteLength);
  return payload;
}

function assertOuterBound(payload: Uint8Array): void {
  if (payload.byteLength === 0) {
    throw codecError("FRAME_EMPTY");
  }
  if (payload.byteLength > DEFAULT_WIRE_LIMITS.max_frame_bytes) {
    throw codecError("FRAME_TOO_LARGE");
  }
}

function assertControlBound(frame: string, bytes: number): void {
  if (!dataFrameTags.has(frame) && bytes > DEFAULT_WIRE_LIMITS.max_control_frame_bytes) {
    throw codecError("CONTROL_FRAME_TOO_LARGE");
  }
}
