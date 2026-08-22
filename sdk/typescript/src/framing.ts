import {
  DEFAULT_WIRE_LIMITS,
  FRAME_PREFIX_BYTES,
} from "../../../generated/worker-protocol/protocol.ts";

import { WireCodecError, codecError } from "./errors.ts";

export type FrameEndOfInput =
  | Readonly<{ kind: "clean" }>
  | Readonly<{ kind: "truncated-prefix"; have: number }>
  | Readonly<{ kind: "truncated-payload"; declared: number; have: number }>;

/** Prefix one non-empty payload after checking the generated outer bound. */
export function encodeFrame(payload: Uint8Array): Uint8Array {
  if (payload.byteLength === 0) {
    throw codecError("FRAME_EMPTY");
  }
  if (payload.byteLength > DEFAULT_WIRE_LIMITS.max_frame_bytes) {
    throw codecError("FRAME_TOO_LARGE");
  }

  const frame = new Uint8Array(FRAME_PREFIX_BYTES + payload.byteLength);
  const length = payload.byteLength;
  frame[0] = length >>> 24;
  frame[1] = length >>> 16;
  frame[2] = length >>> 8;
  frame[3] = length;
  frame.set(payload, FRAME_PREFIX_BYTES);
  return frame;
}

/** Incrementally decode frames without allocating from an unfulfilled prefix. */
export class FrameDecoder {
  private prefix = new Uint8Array(FRAME_PREFIX_BYTES);
  private prefixLength = 0;
  private declared: number | undefined;
  private payloadChunks: Uint8Array[] = [];
  private payloadLength = 0;
  private poisonError: WireCodecError | undefined;

  /** Bytes held for an incomplete prefix or payload; poison resets this to zero. */
  get retainedBytes(): number {
    return this.prefixLength + this.payloadLength;
  }

  get poisoned(): boolean {
    return this.poisonError !== undefined;
  }

  /** Consume a chunk, emitting each frame before inspecting later bytes. */
  push(bytes: Uint8Array, emit: (payload: Uint8Array) => void): void {
    if (this.poisonError !== undefined) {
      throw this.poisonError;
    }

    let offset = 0;
    while (offset < bytes.byteLength) {
      if (this.declared === undefined) {
        const take = Math.min(FRAME_PREFIX_BYTES - this.prefixLength, bytes.byteLength - offset);
        this.prefix.set(bytes.subarray(offset, offset + take), this.prefixLength);
        this.prefixLength += take;
        offset += take;
        if (this.prefixLength < FRAME_PREFIX_BYTES) {
          continue;
        }

        const declared =
          (this.prefix[0]! * 0x1000000 +
            this.prefix[1]! * 0x10000 +
            this.prefix[2]! * 0x100 +
            this.prefix[3]!) >>>
          0;
        if (declared === 0) {
          throw this.poison("FRAME_EMPTY");
        }
        if (declared > DEFAULT_WIRE_LIMITS.max_frame_bytes) {
          throw this.poison("FRAME_TOO_LARGE");
        }
        this.declared = declared;
      }

      const remaining = this.declared - this.payloadLength;
      const take = Math.min(remaining, bytes.byteLength - offset);
      if (take > 0) {
        const chunk = new Uint8Array(take);
        chunk.set(bytes.subarray(offset, offset + take));
        this.payloadChunks.push(chunk);
        this.payloadLength += take;
        offset += take;
      }

      if (this.payloadLength === this.declared) {
        const payload = new Uint8Array(this.declared);
        let payloadOffset = 0;
        for (const chunk of this.payloadChunks) {
          payload.set(chunk, payloadOffset);
          payloadOffset += chunk.byteLength;
        }
        this.resetFrame();
        emit(payload);
      }
    }
  }

  /** Classify a transport close after all complete frames have been drained. */
  finish(): FrameEndOfInput {
    if (this.poisonError !== undefined) {
      throw this.poisonError;
    }
    if (this.declared !== undefined) {
      return {
        kind: "truncated-payload",
        declared: this.declared,
        have: this.payloadLength,
      };
    }
    if (this.prefixLength > 0) {
      return { kind: "truncated-prefix", have: this.prefixLength };
    }
    return { kind: "clean" };
  }

  private resetFrame(): void {
    this.prefixLength = 0;
    this.declared = undefined;
    this.payloadChunks = [];
    this.payloadLength = 0;
  }

  private poison(code: "FRAME_EMPTY" | "FRAME_TOO_LARGE"): WireCodecError {
    const error = codecError(code);
    this.prefix = new Uint8Array(FRAME_PREFIX_BYTES);
    this.resetFrame();
    this.poisonError = error;
    return error;
  }
}
