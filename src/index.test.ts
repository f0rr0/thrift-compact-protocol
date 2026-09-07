import { describe, expect, test } from "bun:test";

import type { StandardSchemaV1 } from "@standard-schema/spec";

import { ThriftError, t } from "./index.ts";
import type { Infer } from "./index.ts";

describe("Facebook Thrift Compact Protocol", () => {
  test("reuses types and fields without changing IDs or optionality", () => {
    const address = t.struct({ city: t.string.field(1) });
    const home = address.field(1);
    const optionalHome = home.optional();
    const schema = t.struct({
      home,
      previous: t.list(address).field(3),
      work: address.field(2).optional(),
    });
    const value: Infer<typeof schema> = {
      home: { city: "London" },
      previous: [{ city: "Paris" }],
    };

    expect(schema.decode(schema.encode(value))).toEqual(value);
    expect(
      schema.decode(schema.encode({ ...value, work: { city: "Tokyo" } }))
    ).toEqual({
      ...value,
      work: { city: "Tokyo" },
    });
    expect(home.isOptional).toBe(false);
    expect(optionalHome.isOptional).toBe(true);
    expect(optionalHome).not.toBe(home);
    expect(optionalHome.type).toBe(address);
    expect(address.field(9).id).toBe(9);
    expect(home.id).toBe(1);
    expect(() =>
      schema.encode(
        // @ts-expect-error Required fields must remain required in the inferred input.
        { previous: [] }
      )
    ).toThrow(ThriftError);
    expect(() =>
      schema.encode({
        ...value,
        // @ts-expect-error Optional nested structs retain their property types.
        work: { city: 42 },
      })
    ).toThrow(ThriftError);
    expect(() =>
      schema.encode({
        ...value,
        // @ts-expect-error Optionality belongs to fields, not list elements.
        previous: [undefined],
      })
    ).toThrow(ThriftError);
  });

  test("matches a known compact wire vector", () => {
    const example = t.struct({
      count: t.i32.field(2).optional(),
      enabled: t.bool.field(1),
      names: t.list(t.string).field(4),
    });
    const value: Infer<typeof example> = {
      count: -1,
      enabled: true,
      names: ["a", "é"],
    };
    const bytes = Uint8Array.of(
      0x11,
      0x15,
      0x01,
      0x29,
      0x28,
      0x01,
      0x61,
      0x02,
      0xc3,
      0xa9,
      0x00
    );

    expect(example.encode(value)).toEqual(bytes);
    expect(example.decode(bytes)).toEqual(value);
  });

  test("round-trips every supported type and nested container", () => {
    const child = t.struct({ value: t.string.field(1) });
    const schema = t.struct({
      binary: t.binary.field(7),
      bools: t.list(t.bool).field(8),
      byte: t.byte.field(1),
      child: child.field(11),
      double: t.double.field(5),
      float: t.float.field(6),
      i16: t.i16.field(2),
      i32: t.i32.field(3),
      i64: t.i64.field(4),
      map: t.map(t.string, t.i64).field(10),
      nested: t.list(t.map(t.i32, child)).field(12),
      set: t.set(t.i16).field(9),
    });
    const value: Infer<typeof schema> = {
      binary: Uint8Array.of(0, 255),
      bools: [true, false],
      byte: -128,
      child: { value: "hello" },
      double: Math.PI,
      float: 1.25,
      i16: 32_767,
      i32: -2_147_483_648,
      i64: 9_223_372_036_854_775_807n,
      map: new Map([
        ["one", 1n],
        ["two", 2n],
      ]),
      nested: [new Map([[7, { value: "nested" }]])],
      set: new Set([-1, 2]),
    };

    expect(schema.decode(schema.encode(value))).toEqual(value);
  });

  test("skips unknown nested fields", () => {
    const newer = t.struct({
      extra: t.map(t.string, t.list(t.bool)).field(2),
      id: t.i32.field(1),
    });
    const older = t.struct({ id: t.i32.field(1) });

    expect(
      older.decode(
        newer.encode({
          extra: new Map([["flags", [true, false]]]),
          id: 42,
        })
      )
    ).toEqual({ id: 42 });
  });

  test("supports long collection headers", () => {
    const schema = t.struct({ values: t.list(t.byte).field(1) });
    const value = { values: Array.from({ length: 15 }, (_, index) => index) };
    const bytes = schema.encode(value);

    expect(bytes.slice(0, 3)).toEqual(Uint8Array.of(0x19, 0xf3, 0x0f));
    expect(schema.decode(bytes)).toEqual(value);
  });

  test("implements Standard Schema and rejects malformed data", () => {
    const schema = t.struct({ id: t.i32.field(1) });
    const value: StandardSchemaV1.InferOutput<typeof schema> = { id: 7 };

    expect(schema["~standard"].validate(schema.encode(value))).toEqual({
      value: { id: 7 },
    });
    expect(schema["~standard"].validate("not bytes")).toEqual({
      issues: [{ message: "Expected Uint8Array" }],
    });
    expect(() => schema.decode(Uint8Array.of(0x15, 0x02))).toThrow(ThriftError);
  });
});
