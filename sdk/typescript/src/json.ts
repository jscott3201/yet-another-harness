import { parse, stringify } from "lossless-json";

import { MAX_WIRE_ID, type JsonValue } from "../../../generated/worker-protocol/protocol.ts";

import { codecError, type WireCodecErrorCode } from "./errors.ts";

const decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const encoder = new TextEncoder();
const integerPattern = /^-?(?:0|[1-9][0-9]*)$/;
const maxIntegerDigits = String(MAX_WIRE_ID);
const negativeZeroStringifier = Object.freeze({
  test: (value: unknown): boolean => typeof value === "number" && Object.is(value, -0),
  stringify: (): string => "-0",
});

class ParseRefusal extends Error {
  readonly code: WireCodecErrorCode;

  constructor(code: WireCodecErrorCode) {
    super(code);
    this.code = code;
  }
}

interface ObjectContext {
  readonly kind: "object";
  readonly keys: Set<string>;
  expectKey: boolean;
}

interface ArrayContext {
  readonly kind: "array";
}

interface StringToken {
  readonly value: string;
  readonly end: number;
}

interface KeyScan {
  readonly rewritten: string;
  readonly protoPlaceholder: string | undefined;
}

/** Decode one UTF-8 JSON value while preserving number tokens until policy checks run. */
export function parseWireJson(bytes: Uint8Array): JsonValue {
  let text: string;
  try {
    text = decoder.decode(bytes);
  } catch {
    throw codecError("INVALID_UTF8");
  }

  try {
    const scan = scanObjectKeys(text);
    const parsed = parse(scan.rewritten, undefined, {
      parseNumber: parseNumberToken,
      onDuplicateKey: () => {
        throw new ParseRefusal("DUPLICATE_MEMBER");
      },
    });
    return normalizeParsedValue(parsed, scan.protoPlaceholder);
  } catch (error: unknown) {
    if (error instanceof ParseRefusal) {
      throw codecError(error.code);
    }
    throw codecError("INVALID_JSON");
  }
}

/** Serialize a preflighted JSON value without non-finite-to-null conversion. */
export function stringifyWireJson(value: JsonValue): Uint8Array {
  return encoder.encode(stringifyJson(value));
}

function stringifyJson(value: JsonValue): string {
  let text: string | undefined;
  try {
    text = stringify(value, undefined, undefined, [negativeZeroStringifier]);
  } catch {
    throw codecError("INVALID_OUTBOUND_VALUE");
  }
  if (text === undefined) {
    throw codecError("INVALID_OUTBOUND_VALUE");
  }
  return text;
}

/** Refuse values that serialization would mutate or strict wire admission would reject. */
export function assertJsonRepresentable(value: unknown): asserts value is JsonValue {
  const active = new WeakSet<object>();
  const work: Array<Readonly<{ value: unknown; leave: boolean }>> = [{ value, leave: false }];

  while (work.length > 0) {
    const item = work.pop()!;
    const current = item.value;
    if (item.leave) {
      active.delete(current as object);
      continue;
    }
    if (current === null || typeof current === "boolean") {
      continue;
    }
    if (typeof current === "string") {
      assertUnicodeScalarString(current, true);
      continue;
    }
    if (typeof current === "number") {
      if (!Number.isFinite(current)) {
        throw codecError("NON_FINITE_NUMBER");
      }
      if (
        Number.isInteger(current) &&
        !Number.isSafeInteger(current) &&
        integerPattern.test(stringifyJson(current))
      ) {
        throw codecError("UNSAFE_INTEGER");
      }
      continue;
    }
    if (typeof current !== "object") {
      throw codecError("INVALID_OUTBOUND_VALUE");
    }
    if (active.has(current)) {
      throw codecError("CYCLIC_OUTBOUND_VALUE");
    }
    active.add(current);
    work.push({ value: current, leave: true });

    if (Array.isArray(current)) {
      const keys = Reflect.ownKeys(current);
      if (keys.some((key) => typeof key === "symbol")) {
        throw codecError("INVALID_OUTBOUND_VALUE");
      }
      for (let index = current.length - 1; index >= 0; index -= 1) {
        const descriptor = Object.getOwnPropertyDescriptor(current, index);
        if (descriptor === undefined || !("value" in descriptor)) {
          throw codecError("INVALID_OUTBOUND_VALUE");
        }
        work.push({ value: descriptor.value, leave: false });
      }
      const expectedKeys = current.length + 1;
      if (keys.length !== expectedKeys) {
        throw codecError("INVALID_OUTBOUND_VALUE");
      }
      continue;
    }

    const prototype = Object.getPrototypeOf(current);
    if (prototype !== Object.prototype && prototype !== null) {
      throw codecError("INVALID_OUTBOUND_VALUE");
    }
    const descriptors = Object.getOwnPropertyDescriptors(current);
    for (const key of Reflect.ownKeys(descriptors)) {
      if (typeof key === "symbol") {
        throw codecError("INVALID_OUTBOUND_VALUE");
      }
      assertUnicodeScalarString(key, true);
      const descriptor = descriptors[key]!;
      if (!descriptor.enumerable || !("value" in descriptor)) {
        throw codecError("INVALID_OUTBOUND_VALUE");
      }
      work.push({ value: descriptor.value, leave: false });
    }
  }
}

function parseNumberToken(token: string): number {
  if (integerPattern.test(token)) {
    const digits = token.startsWith("-") ? token.slice(1) : token;
    if (
      digits.length > maxIntegerDigits.length ||
      (digits.length === maxIntegerDigits.length && digits > maxIntegerDigits)
    ) {
      throw new ParseRefusal("UNSAFE_INTEGER");
    }
    return Number(token);
  }
  const number = Number(token);
  if (!Number.isFinite(number)) {
    throw new ParseRefusal("NON_FINITE_NUMBER");
  }
  return number;
}

function scanObjectKeys(text: string): KeyScan {
  const stack: Array<ObjectContext | ArrayContext> = [];
  const protoRanges: Array<Readonly<{ start: number; end: number }>> = [];
  const allKeys = new Set<string>();

  let index = 0;
  while (index < text.length) {
    const character = text[index]!;
    if (character === '"') {
      const start = index;
      const token = scanString(text, index);
      const context = stack.at(-1);
      if (context?.kind === "object" && context.expectKey) {
        if (context.keys.has(token.value)) {
          throw new ParseRefusal("DUPLICATE_MEMBER");
        }
        context.keys.add(token.value);
        allKeys.add(token.value);
        context.expectKey = false;
        if (token.value === "__proto__") {
          protoRanges.push({ start, end: token.end });
        }
      }
      index = token.end;
      continue;
    }
    if (character === "{") {
      stack.push({ kind: "object", keys: new Set(), expectKey: true });
    } else if (character === "[") {
      stack.push({ kind: "array" });
    } else if (character === "}" || character === "]") {
      stack.pop();
    } else if (character === ",") {
      const context = stack.at(-1);
      if (context?.kind === "object") {
        context.expectKey = true;
      }
    }
    index += 1;
  }

  if (protoRanges.length === 0) {
    return { rewritten: text, protoPlaceholder: undefined };
  }
  let suffix = 0;
  let placeholder = `$yah_proto_${suffix}`;
  while (allKeys.has(placeholder)) {
    suffix += 1;
    placeholder = `$yah_proto_${suffix}`;
  }
  let rewritten = "";
  let cursor = 0;
  for (const range of protoRanges) {
    rewritten += text.slice(cursor, range.start) + `"${placeholder}"`;
    cursor = range.end;
  }
  rewritten += text.slice(cursor);
  return { rewritten, protoPlaceholder: placeholder };
}

function scanString(text: string, start: number): StringToken {
  let value = "";
  let index = start + 1;
  while (index < text.length) {
    const character = text[index]!;
    if (character === '"') {
      return { value, end: index + 1 };
    }
    if (character !== "\\") {
      if (character.charCodeAt(0) < 0x20) {
        throw new ParseRefusal("INVALID_JSON");
      }
      value += character;
      index += 1;
      continue;
    }

    const escaped = text[index + 1];
    const escapes: Readonly<Record<string, string>> = {
      '"': '"',
      "\\": "\\",
      "/": "/",
      b: "\b",
      f: "\f",
      n: "\n",
      r: "\r",
      t: "\t",
    };
    if (escaped !== undefined && escapes[escaped] !== undefined) {
      value += escapes[escaped];
      index += 2;
      continue;
    }
    if (escaped === "u") {
      const hex = text.slice(index + 2, index + 6);
      if (!/^[0-9a-fA-F]{4}$/.test(hex)) {
        throw new ParseRefusal("INVALID_JSON");
      }
      value += String.fromCharCode(Number.parseInt(hex, 16));
      index += 6;
      continue;
    }
    throw new ParseRefusal("INVALID_JSON");
  }
  throw new ParseRefusal("INVALID_JSON");
}

function normalizeParsedValue(value: unknown, protoPlaceholder: string | undefined): JsonValue {
  if (value === null || typeof value === "boolean" || typeof value === "number") {
    return value;
  }
  if (typeof value === "string") {
    assertUnicodeScalarString(value);
    return value;
  }
  if (typeof value !== "object") {
    throw new ParseRefusal("INVALID_JSON");
  }

  const root: JsonValue = Array.isArray(value) ? [] : {};
  const work: Array<Readonly<{ input: object; output: JsonValue[] | Record<string, JsonValue> }>> = [
    { input: value, output: root as JsonValue[] | Record<string, JsonValue> },
  ];
  while (work.length > 0) {
    const { input, output } = work.pop()!;
    const entries = Array.isArray(input)
      ? input.map((item, index) => [String(index), item] as const)
      : Object.entries(input);
    for (const [inputKey, child] of entries) {
      const key = inputKey === protoPlaceholder ? "__proto__" : inputKey;
      assertUnicodeScalarString(key);
      let normalized: JsonValue;
      if (child === null || typeof child === "boolean" || typeof child === "number") {
        normalized = child;
      } else if (typeof child === "string") {
        assertUnicodeScalarString(child);
        normalized = child;
      } else if (typeof child === "object") {
        normalized = Array.isArray(child) ? [] : {};
        work.push({
          input: child,
          output: normalized as JsonValue[] | Record<string, JsonValue>,
        });
      } else {
        throw new ParseRefusal("INVALID_JSON");
      }
      Object.defineProperty(output, key, {
        configurable: true,
        enumerable: true,
        value: normalized,
        writable: true,
      });
    }
  }
  return root;
}

function assertUnicodeScalarString(value: string, outbound = false): void {
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (!(next >= 0xdc00 && next <= 0xdfff)) {
        throw outbound ? codecError("INVALID_UNICODE") : new ParseRefusal("INVALID_UNICODE");
      }
      index += 1;
    } else if (unit >= 0xdc00 && unit <= 0xdfff) {
      throw outbound ? codecError("INVALID_UNICODE") : new ParseRefusal("INVALID_UNICODE");
    }
  }
}
