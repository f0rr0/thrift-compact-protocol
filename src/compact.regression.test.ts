/* oxlint-disable oxc/approx-constant, unicorn/no-hex-escape -- Preserve upstream numeric fixtures and malicious byte strings. */
// Adapted from Facebook FBThrift tests. Copyright (c) Meta Platforms, Inc. and affiliates.
// SPDX-License-Identifier: Apache-2.0
// See tests/UPSTREAM.md for pinned sources, adaptations, and license.
import { describe, expect, test } from "bun:test";

import { ThriftError, t } from "./index.ts";
import type { Type } from "./index.ts";

const hex = (value: string): Uint8Array =>
  Uint8Array.from(Buffer.from(value, "hex"));

const nested = t.struct({ elem: t.i64.field(1) });
const original = t.struct({
  f1: t.bool.field(1),
  f100: t.i64.field(100),
  f20000: t.double.field(20_000),
  f20030: nested.field(20_030),
  f20032: t.string.field(20_032),
  f20034: t.list(t.i32).field(20_034),
  f3: t.bool.field(3),
  f48: t.i32.field(48),
  f6: t.byte.field(6),
  f8: t.i16.field(8),
});
const originalValue = {
  f1: true,
  f100: 1600n,
  f20000: 1,
  f20030: { elem: 0n },
  f20032: "def",
  f20034: [0],
  f3: false,
  f48: 1300,
  f6: 50,
  f8: 1200,
};
const updated = t.struct({
  ...original.shape,
  f2: t.bool.field(2),
  f20020: t.string.field(20_020),
  f20031: nested.field(20_031),
  f20033: t.string.field(20_033),
  f20035: t.list(t.i32).field(20_035),
  f4: t.bool.field(4),
  f5: t.bool.field(5),
  f68: t.i32.field(68),
  f7: t.i32.field(7),
  f88: t.i32.field(88),
});
const updatedValue = {
  ...originalValue,
  f2: false,
  f20020: "abc",
  f20031: { elem: 1n },
  f20033: "ghi",
  f20035: [1],
  f4: true,
  f5: false,
  f68: 1400,
  f7: 1100,
  f88: 1500,
};

// Hand-derived from the upstream initialized structs and wire rules, not our encoder.
const originalBytes = hex(
  "1122333224e0120560a81406c801801907c0b8023ff00000000000000cfcb802160000280364656629150000"
);
const updatedBytes = hex(
  "1112121112133215981114e0120560a814058801f01505b001b817c6801907c0b8023ff000000000000008e8b80203616263ac1600001c1602001803646566180367686919150019150200"
);

describe("FBThrift upstream regression adaptations", () => {
  test("C++ ParsesOriginalViaRead: large field deltas and nested state", () => {
    expect(original.encode(originalValue)).toEqual(originalBytes);
    expect(original.decode(originalBytes)).toEqual(originalValue);
  });

  test("C++ ParsesUpdatedViaRead: skip interleaved unknown fields", () => {
    expect(updated.encode(updatedValue)).toEqual(updatedBytes);
    expect(updated.decode(updatedBytes)).toEqual(updatedValue);
    expect(original.decode(updatedBytes)).toEqual(originalValue);
  });

  test("C++ buffer boundaries: nonzero byte offsets and every truncated prefix", () => {
    for (let offset = 0; offset < 8; offset += 1) {
      const padded = new Uint8Array(updatedBytes.length + offset + 8).fill(255);
      padded.set(updatedBytes, offset);
      expect(
        original.decode(padded.subarray(offset, offset + updatedBytes.length))
      ).toEqual(originalValue);
    }
    for (let end = 0; end < updatedBytes.length; end += 1) {
      expect(() => original.decode(updatedBytes.subarray(0, end))).toThrow(
        ThriftError
      );
    }
  });

  test.each([
    [t.double, 1, "173ff000000000000000"],
    [t.double, -0, "17800000000000000000"],
    [t.double, Infinity, "177ff000000000000000"],
    [t.double, -Infinity, "17fff000000000000000"],
    [t.float, 1.25, "1d3fa0000000"],
    [t.float, -0, "1d8000000000"],
    [t.float, Infinity, "1d7f80000000"],
    [t.float, -Infinity, "1dff80000000"],
  ] as const)(
    "FB compact floating point %s %s uses big-endian bytes",
    (type, value, bytes) => {
      const schema = t.struct({ value: type.field(1) });
      expect(schema.encode({ value })).toEqual(hex(bytes));
      expect(schema.decode(hex(bytes)).value).toBe(value);
    }
  );

  test.each([
    [t.byte, [117, 0, 1, 32, 127, -128, 44]],
    [t.i16, [459, 0, 1, -1, -128, 127, 32_767, -32_768]],
    [t.i32, [459, 0, 1, -1, -128, 127, 32_767, 2_147_483_647, -2_147_483_535]],
    [
      t.i64,
      [
        459n,
        0n,
        1n,
        -1n,
        -128n,
        127n,
        32_767n,
        2_147_483_647n,
        -2_147_483_535n,
        34_359_738_481n,
        -35_184_372_088_719n,
        -9_223_372_036_854_775_808n,
        9_223_372_036_854_775_807n,
      ],
    ],
    [t.bool, [false, true, false, false, true]],
  ] satisfies [Type, unknown[]][])(
    "Rust read_write scalar containers: %s",
    (type, values) => {
      const schema = t.struct({
        list: t.list(type).field(1),
        map: t.map(t.string, type).field(2),
        set: t.set(type).field(3),
      });
      const value = {
        list: values,
        map: new Map(values.map((item, index) => [String(index), item])),
        set: new Set<number | bigint | boolean>(values),
      };
      expect(schema.decode(schema.encode(value))).toEqual(value);
    }
  );

  test.each([t.float, t.double])(
    "Rust floating point special values: %s",
    (type) => {
      const values = [
        459.3,
        0,
        -0,
        -1,
        1,
        0.5,
        0.3333,
        3.14159,
        1.537e-38,
        1.673e25,
        6.0221417e23,
        -6.0221417e23,
        Infinity,
        -Infinity,
        Number.NaN,
      ];
      const schema = t.struct({ values: t.list(type).field(1) });
      const expected = type === t.float ? values.map(Math.fround) : values;
      expect(schema.decode(schema.encode({ values })).values).toEqual(expected);
    }
  );

  test.each([
    "%0\x88\x8A\x97\xB7\xC4\x030",
    "%0\x98\xFA\xB7\xB7\xC4\xC4\x03\x01a",
    "%0\xA8\xFA\x97\xB7\xC4\xC4\x03\x01a",
  ])("Go initial-allocation attack payload is rejected: %s", (payload) => {
    expect(() =>
      t.struct({}).decode(Uint8Array.from(Buffer.from(payload, "latin1")))
    ).toThrow(ThriftError);
  });

  test.each(["191000", "1a1000", "1b010100"])(
    "Rust T29755131: stop in unknown container %s",
    (bytes) => {
      expect(() => t.struct({}).decode(hex(bytes))).toThrow(ThriftError);
    }
  );

  test("Rust skip_i32_ok: skipping consumes the integer and preserves the next field", () => {
    expect(
      t.struct({ value: t.bool.field(2) }).decode(hex("15f6011100"))
    ).toEqual({ value: true });
  });
});

describe("local wire and input regressions", () => {
  test.each([
    [t.i16, -32_768, "14ffff0300"],
    [t.i16, 32_767, "14feff0300"],
    [t.i32, -2_147_483_648, "15ffffffff0f00"],
    [t.i32, 2_147_483_647, "15feffffff0f00"],
    [t.i64, -9_223_372_036_854_775_808n, "16ffffffffffffffffff0100"],
    [t.i64, 9_223_372_036_854_775_807n, "16feffffffffffffffff0100"],
  ] satisfies [Type, unknown, string][])(
    "integer limit wire vector %s %s",
    (type, value, bytes) => {
      const schema = t.struct({ value: type.field(1) });
      expect(schema.encode({ value })).toEqual(hex(bytes));
      expect(schema.decode(hex(bytes))).toEqual({ value });
    }
  );

  test.each([0, 1, 14, 15, 16, 127, 128])(
    "collection header boundary %i",
    (length) => {
      const schema = t.struct({ values: t.list(t.bool).field(1) });
      const values = Array.from({ length }, (_, index) => index % 2 === 0);
      const header =
        length < 15
          ? [length * 16 + 1]
          : [0xf1, ...(length < 128 ? [length] : [0x80, 1])];
      const bytes = Uint8Array.from([
        0x19,
        ...header,
        ...values.map((value) => (value ? 1 : 2)),
        0,
      ]);
      expect(schema.encode({ values })).toEqual(bytes);
      expect(schema.decode(bytes)).toEqual({ values });
    }
  );

  test.each([
    [t.i16, 32_768],
    [t.i16, -32_769],
    [t.i32, 2_147_483_648],
    [t.i32, -2_147_483_649],
    [t.i64, 9_223_372_036_854_775_808n],
    [t.i64, -9_223_372_036_854_775_809n],
    [t.byte, 128],
    [t.byte, -129],
    [t.i32, 1.5],
    [t.i32, Number.NaN],
    [t.bool, 1],
  ] satisfies [Type, unknown][])(
    "rejects invalid scalar %s %s",
    (type, value) => {
      expect(() =>
        t.struct({ value: type.field(1) }).encode({ value })
      ).toThrow(ThriftError);
    }
  );

  test.each([
    [t.i32, ""],
    [t.i32, "15"],
    [t.i32, "1580"],
    [t.i32, "15808080808000"],
    [t.i16, "14ffff0700"],
    [t.i64, "16ffffffffffffffffff0200"],
    [t.string, "18056100"],
    [t.string, "1801ff00"],
    [t.list(t.bool), "19110300"],
    [t.map(t.string, t.string), "1b018000"],
    [t.i32, "1f00"],
    [t.i32, "150000ff"],
  ] satisfies [Type, string][])(
    "rejects malformed wire data %s %s",
    (type, bytes) => {
      const schema = t.struct({ value: type.field(1) });
      expect(() => schema.decode(hex(bytes))).toThrow(ThriftError);
    }
  );

  test("invalid UTF-8 and booleans are rejected in known and skipped collections", () => {
    expect(() =>
      t.struct({ value: t.string.field(1) }).decode(hex("1801ff00"))
    ).toThrow(ThriftError);
    for (const schema of [
      t.struct({}),
      t.struct({ value: t.list(t.bool).field(1) }),
    ]) {
      expect(() => schema.decode(hex("19110300"))).toThrow(ThriftError);
    }
  });

  test("unknown struct depth is bounded", () => {
    const bytes = Uint8Array.from([
      ...Array.from({ length: 100 }, () => 0x1c),
      ...Array.from({ length: 101 }, () => 0),
    ]);
    expect(() => t.struct({}).decode(bytes)).toThrow("Maximum nesting depth");
  });

  test("empty structs, containers and boolean map keys have exact encodings", () => {
    const schema = t.struct({
      list: t.list(t.i32).field(1),
      map: t.map(t.bool, t.bool).field(2),
      set: t.set(t.bool).field(3),
      struct: t.struct({}).field(4),
    });
    const value = {
      list: [],
      map: new Map([
        [true, false],
        [false, true],
      ]),
      set: new Set<boolean>(),
      struct: {},
    };
    const bytes = hex("19051b0211010202011a011c0000");
    expect(schema.encode(value)).toEqual(bytes);
    expect(schema.decode(bytes)).toEqual(value);
  });
});
