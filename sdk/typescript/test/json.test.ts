import assert from "node:assert/strict";
import test from "node:test";

import { MAX_WIRE_ID } from "../src/index.ts";
import { parseWireJson } from "../src/json.ts";
import { expectCodecError } from "./support.ts";

const encode = (text: string): Uint8Array => new TextEncoder().encode(text);

test("duplicate members are refused at all protocol-relevant positions", () => {
  for (const text of [
    '{"a":1,"a":1}',
    '{"a":1,"a":2}',
    '{"outer":{"a":1,"a":2}}',
    '[{"a":1},{"a":2,"a":3}]',
    '{"\\u0061":1,"a":2}',
  ]) {
    expectCodecError("DUPLICATE_MEMBER", () => parseWireJson(encode(text)));
  }
});

test("integer tokens stop at the generated I-JSON endpoints", () => {
  assert.equal(parseWireJson(encode(String(MAX_WIRE_ID))), MAX_WIRE_ID);
  assert.equal(parseWireJson(encode(String(-MAX_WIRE_ID))), -MAX_WIRE_ID);
  for (const text of [
    String(MAX_WIRE_ID + 1),
    String(-MAX_WIRE_ID - 1),
    "99999999999999999999",
    "-99999999999999999999",
  ]) {
    expectCodecError("UNSAFE_INTEGER", () => parseWireJson(encode(text)));
  }
});

test("float tokens use finite JavaScript numbers with pinned bit patterns", () => {
  for (const [literal, bits] of [
    ["2.5e-30", 0x39c95a5efea6b347n],
    ["2.5e-10", 0x3df12e0be826d695n],
    ["1e300", 0x7e37e43c8800759cn],
    ["0.1", 0x3fb999999999999an],
    ["5e-324", 0x1n],
    ["1.7976931348623157e308", 0x7fefffffffffffffn],
  ] as const) {
    const value = parseWireJson(encode(literal));
    assert.equal(typeof value, "number");
    assert.equal(numberBits(value as number), bits, literal);
  }
  expectCodecError("NON_FINITE_NUMBER", () => parseWireJson(encode("1e309")));
  expectCodecError("NON_FINITE_NUMBER", () => parseWireJson(encode("-1e9999")));
});

test("UTF-8, trailing input, deep input, and unpaired surrogates fail with bounded errors", () => {
  expectCodecError("INVALID_UTF8", () => parseWireJson(Uint8Array.of(0xff, 0xfe)));
  for (const text of ["", "{} {}", '"unterminated', "01", "[".repeat(200_000)]) {
    expectCodecError("INVALID_JSON", () => parseWireJson(encode(text)));
  }
  expectCodecError("INVALID_UNICODE", () => parseWireJson(encode('"\\ud800"')));
  expectCodecError("INVALID_UNICODE", () => parseWireJson(encode('{"\\udfff":1}')));
});

test("object member names that affect prototypes remain ordinary JSON data", () => {
  const value = parseWireJson(encode('{"__proto__":5,"nested":{"__proto__":{"x":1}}}'));
  assert.equal(Object.getPrototypeOf(value), Object.prototype);
  assert.equal(Object.hasOwn(value as object, "__proto__"), true);
  assert.equal((value as Record<string, unknown>).__proto__, 5);
  const nested = (value as Record<string, unknown>).nested as Record<string, unknown>;
  assert.equal(Object.hasOwn(nested, "__proto__"), true);
  assert.deepEqual(nested.__proto__, { x: 1 });
});

test("hostile member text is never copied into an SDK diagnostic", () => {
  const hostile = "secret-marker-".repeat(20_000);
  const error = expectCodecError("DUPLICATE_MEMBER", () =>
    parseWireJson(encode(`{"${hostile}":1,"${hostile}":2}`)),
  );
  assert.equal(error.message.includes("secret-marker"), false);
});

function numberBits(value: number): bigint {
  const bytes = new ArrayBuffer(8);
  new DataView(bytes).setFloat64(0, value);
  return new DataView(bytes).getBigUint64(0);
}
