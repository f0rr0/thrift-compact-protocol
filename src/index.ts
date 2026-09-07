import type { StandardSchemaV1 } from "@standard-schema/spec";

const CompactType = {
  binary: 8,
  byte: 3,
  double: 7,
  false: 2,
  float: 13,
  i16: 4,
  i32: 5,
  i64: 6,
  list: 9,
  map: 11,
  set: 10,
  stop: 0,
  struct: 12,
  true: 1,
} as const;

type CompactType = (typeof CompactType)[keyof typeof CompactType];
type Kind =
  | "binary"
  | "bool"
  | "byte"
  | "double"
  | "float"
  | "i16"
  | "i32"
  | "i64"
  | "list"
  | "map"
  | "set"
  | "string"
  | "struct";

declare const output: unique symbol;

export interface Type<Output = unknown> {
  readonly field: (id: number) => Field<this, false>;
  readonly kind: Kind;
  readonly wireType: CompactType;
  readonly [output]: Output;
}

export type Infer<T extends Type> =
  T extends Type<infer Output> ? Output : never;

interface ListType<Item extends Type> extends Type<Infer<Item>[]> {
  readonly item: Item;
  readonly kind: "list";
}

interface SetType<Item extends Type> extends Type<Set<Infer<Item>>> {
  readonly item: Item;
  readonly kind: "set";
}

interface MapType<Key extends Type, Value extends Type> extends Type<
  Map<Infer<Key>, Infer<Value>>
> {
  readonly key: Key;
  readonly kind: "map";
  readonly value: Value;
}

export interface Field<
  Value extends Type = Type,
  Optional extends boolean = boolean,
> {
  readonly id: number;
  readonly isOptional: Optional;
  readonly type: Value;
  readonly optional: () => Field<Value, true>;
}

const createField = <Value extends Type, Optional extends boolean>(
  id: number,
  type: Value,
  isOptional: Optional
): Field<Value, Optional> => ({
  id,
  isOptional,
  optional: () => createField(id, type, true),
  type,
});

const defineField = function defineField<Value extends Type>(
  this: Value,
  id: number
): Field<Value, false> {
  return createField(id, this, false);
};

type Shape = Record<string, Field>;
type OptionalKeys<S extends Shape> = {
  [K in keyof S]-?: S[K] extends Field<Type, true> ? K : never;
}[keyof S];
type RequiredKeys<S extends Shape> = Exclude<keyof S, OptionalKeys<S>>;
type FieldOutput<F extends Field> =
  F extends Field<infer Value> ? Infer<Value> : never;
type StructOutput<S extends Shape> = {
  [
    K in keyof ({
      [K in OptionalKeys<S>]?: FieldOutput<S[K]>;
    } & {
      [K in RequiredKeys<S>]: FieldOutput<S[K]>;
    })
  ]: ({
    [K in OptionalKeys<S>]?: FieldOutput<S[K]>;
  } & {
    [K in RequiredKeys<S>]: FieldOutput<S[K]>;
  })[K];
};

const scalar = <Output>(kind: Kind, wireType: CompactType): Type<Output> =>
  ({ field: defineField, kind, wireType }) as Type<Output>;

const MAX_DEPTH = 64;
const textDecoder = new TextDecoder("utf-8", { fatal: true });
const textEncoder = new TextEncoder();

export class ThriftError extends Error {
  override readonly name = "ThriftError";
}

export class Struct<S extends Shape>
  implements
    Type<StructOutput<S>>,
    StandardSchemaV1<Uint8Array, StructOutput<S>>
{
  declare readonly [output]: StructOutput<S>;
  readonly kind = "struct";
  readonly wireType = CompactType.struct;
  readonly fields: readonly (readonly [keyof S & string, S[keyof S]])[];
  readonly shape: S;
  readonly "~standard": StandardSchemaV1.Props<Uint8Array, StructOutput<S>>;

  constructor(shape: S) {
    this.shape = shape;
    const ids = new Set<number>();
    this.fields = Object.entries(shape)
      .map(([name, field]) => {
        if (
          !Number.isInteger(field.id) ||
          field.id < -32_768 ||
          field.id > 32_767
        ) {
          throw new ThriftError(`Field ${name} has invalid i16 id ${field.id}`);
        }
        if (ids.has(field.id)) {
          throw new ThriftError(`Duplicate field id ${field.id}`);
        }
        ids.add(field.id);
        return [name, field] as const;
      })
      .toSorted(
        (left, right) => left[1].id - right[1].id
      ) as unknown as typeof this.fields;

    this["~standard"] = {
      validate: (value) => {
        if (!(value instanceof Uint8Array)) {
          return { issues: [{ message: "Expected Uint8Array" }] };
        }
        try {
          return { value: this.decode(value) };
        } catch (error) {
          return {
            issues: [
              {
                message:
                  error instanceof Error
                    ? error.message
                    : "Invalid Thrift payload",
              },
            ],
          };
        }
      },
      vendor: "thrift-compact-protocol",
      version: 1,
    };
  }

  decode(bytes: Uint8Array): StructOutput<S> {
    const reader = new Reader(bytes);
    const value = reader.struct(this, "root", 0);
    if (!reader.done) {
      throw new ThriftError(
        `Unexpected trailing data at byte ${reader.offset}`
      );
    }
    return value;
  }

  encode(value: StructOutput<S>): Uint8Array {
    const writer = new Writer();
    writer.struct(this, value, "root", 0);
    return writer.finish();
  }

  field(id: number): Field<this, false> {
    return createField(id, this, false);
  }
}

const isBool = (type: number) =>
  type === CompactType.true || type === CompactType.false;
const wireMatches = (expected: CompactType, actual: CompactType) =>
  expected === actual || (expected === CompactType.true && isBool(actual));

class Reader {
  offset = 0;
  private readonly bytes: Uint8Array;
  private lastFieldId = 0;
  private readonly fieldStack: number[] = [];
  private readonly view: DataView;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }

  get done(): boolean {
    return this.offset === this.bytes.length;
  }

  private get remaining(): number {
    return this.bytes.length - this.offset;
  }

  private require(length: number): void {
    if (length < 0 || length > this.remaining) {
      throw new ThriftError(`Unexpected end of input at byte ${this.offset}`);
    }
  }

  private byte(): number {
    this.require(1);
    const value = this.view.getUint8(this.offset);
    this.offset += 1;
    return value;
  }

  private signedByte(): number {
    this.require(1);
    const value = this.view.getInt8(this.offset);
    this.offset += 1;
    return value;
  }

  private varint(maxBytes = 10): bigint {
    let value = 0n;
    for (let shift = 0n; shift < BigInt(maxBytes * 7); shift += 7n) {
      const byte = this.byte();
      value |= BigInt(byte & 0x7f) << shift;
      if ((byte & 0x80) === 0) {
        return value;
      }
    }
    throw new ThriftError(
      `Varint exceeds ${maxBytes} bytes at byte ${this.offset}`
    );
  }

  private signed(bits: 16 | 32 | 64): bigint {
    const value = this.varint(bits === 64 ? 10 : 5);
    const decoded = (value >> 1n) ^ -(value & 1n);
    const limit = 1n << BigInt(bits - 1);
    if (decoded < -limit || decoded >= limit) {
      throw new ThriftError(
        `Decoded i${bits} is out of range at byte ${this.offset}`
      );
    }
    return decoded;
  }

  private size(label: string): number {
    const value = this.varint(5);
    if (value > BigInt(this.remaining)) {
      throw new ThriftError(
        `${label} size exceeds remaining input at byte ${this.offset}`
      );
    }
    return Number(value);
  }

  private pushStruct(): void {
    this.fieldStack.push(this.lastFieldId);
    this.lastFieldId = 0;
  }

  private popStruct(): void {
    this.lastFieldId = this.fieldStack.pop() ?? 0;
  }

  private field(): readonly [CompactType, number] | undefined {
    const header = this.byte();
    if (header === CompactType.stop) {
      return undefined;
    }
    const delta = header >> 4;
    const type = header & 0x0f;
    if (type > CompactType.float) {
      throw new ThriftError(
        `Unknown compact type ${type} at byte ${this.offset - 1}`
      );
    }
    this.lastFieldId =
      delta === 0 ? Number(this.signed(16)) : this.lastFieldId + delta;
    if (this.lastFieldId < -32_768 || this.lastFieldId > 32_767) {
      throw new ThriftError(`Field id is out of range at byte ${this.offset}`);
    }
    return [type as CompactType, this.lastFieldId];
  }

  private collectionHeader(): readonly [CompactType, number] {
    const header = this.byte();
    const type = header & 0x0f;
    const inlineSize = header >> 4;
    const size = inlineSize === 15 ? this.size("Collection") : inlineSize;
    if (type === CompactType.stop || type > CompactType.float) {
      throw new ThriftError(
        `Invalid collection type ${type} at byte ${this.offset - 1}`
      );
    }
    if (size > this.remaining) {
      throw new ThriftError(
        `Collection size exceeds remaining input at byte ${this.offset}`
      );
    }
    return [type as CompactType, size];
  }

  private mapHeader(): readonly [CompactType, CompactType, number] {
    const size = this.size("Map");
    if (size === 0) {
      return [CompactType.stop, CompactType.stop, 0];
    }
    const types = this.byte();
    const key = types >> 4;
    const value = types & 0x0f;
    if (
      key === CompactType.stop ||
      value === CompactType.stop ||
      key > CompactType.float ||
      value > CompactType.float
    ) {
      throw new ThriftError(`Invalid map type at byte ${this.offset - 1}`);
    }
    return [key as CompactType, value as CompactType, size];
  }

  private value<Value>(type: Type<Value>, path: string, depth: number): Value {
    if (depth > MAX_DEPTH) {
      throw new ThriftError(`Maximum nesting depth exceeded at ${path}`);
    }
    switch (type.kind) {
      case "bool": {
        const value = this.byte();
        if (!isBool(value)) {
          throw new ThriftError(`Invalid boolean ${value} at ${path}`);
        }
        return (value === CompactType.true) as Value;
      }
      case "byte": {
        return this.signedByte() as Value;
      }
      case "i16": {
        return Number(this.signed(16)) as Value;
      }
      case "i32": {
        return Number(this.signed(32)) as Value;
      }
      case "i64": {
        return this.signed(64) as Value;
      }
      case "double": {
        this.require(8);
        const value = this.view.getFloat64(this.offset, false);
        this.offset += 8;
        return value as Value;
      }
      case "float": {
        this.require(4);
        const value = this.view.getFloat32(this.offset, false);
        this.offset += 4;
        return value as Value;
      }
      case "binary": {
        const length = this.size("Binary");
        const value = this.bytes.slice(this.offset, this.offset + length);
        this.offset += length;
        return value as Value;
      }
      case "string": {
        const length = this.size("String");
        const bytes = this.bytes.subarray(this.offset, this.offset + length);
        this.offset += length;
        try {
          return textDecoder.decode(bytes) as Value;
        } catch {
          throw new ThriftError(`Invalid UTF-8 at ${path}`);
        }
      }
      case "list": {
        const list = type as unknown as ListType<Type>;
        const [wireType, size] = this.collectionHeader();
        if (!wireMatches(list.item.wireType, wireType)) {
          throw new ThriftError(`Unexpected list item type at ${path}`);
        }
        const value: unknown[] = [];
        for (let index = 0; index < size; index += 1) {
          value.push(this.value(list.item, `${path}[${index}]`, depth + 1));
        }
        return value as Value;
      }
      case "set": {
        const set = type as unknown as SetType<Type>;
        const [wireType, size] = this.collectionHeader();
        if (!wireMatches(set.item.wireType, wireType)) {
          throw new ThriftError(`Unexpected set item type at ${path}`);
        }
        const value = new Set<unknown>();
        for (let index = 0; index < size; index += 1) {
          value.add(this.value(set.item, `${path}[${index}]`, depth + 1));
        }
        return value as Value;
      }
      case "map": {
        const map = type as unknown as MapType<Type, Type>;
        const [keyType, valueType, size] = this.mapHeader();
        if (
          size > 0 &&
          (!wireMatches(map.key.wireType, keyType) ||
            !wireMatches(map.value.wireType, valueType))
        ) {
          throw new ThriftError(`Unexpected map entry type at ${path}`);
        }
        const value = new Map<unknown, unknown>();
        for (let index = 0; index < size; index += 1) {
          value.set(
            this.value(map.key, `${path}[${index}].key`, depth + 1),
            this.value(map.value, `${path}[${index}].value`, depth + 1)
          );
        }
        return value as Value;
      }
      case "struct": {
        return this.nestedStruct(type as Struct<Shape>, path, depth) as Value;
      }
    }
  }

  private nestedStruct(
    struct: Struct<Shape>,
    path: string,
    depth: number
  ): unknown {
    this.pushStruct();
    try {
      return this.struct(struct, path, depth + 1);
    } finally {
      this.popStruct();
    }
  }

  struct<S extends Shape>(
    struct: Struct<S>,
    path: string,
    depth: number
  ): StructOutput<S> {
    if (depth > MAX_DEPTH) {
      throw new ThriftError(`Maximum nesting depth exceeded at ${path}`);
    }
    const result: Record<string, unknown> = {};
    const fieldsById = new Map(
      struct.fields.map(([name, field]) => [field.id, [name, field] as const])
    );
    let stopped = false;
    while (!this.done) {
      const header = this.field();
      if (!header) {
        stopped = true;
        break;
      }
      const [wireType, id] = header;
      const entry = fieldsById.get(id);
      if (!entry || !wireMatches(entry[1].type.wireType, wireType)) {
        this.skip(wireType, true, depth + 1);
        continue;
      }
      const [name, field] = entry;
      result[name] =
        field.type.kind === "bool"
          ? wireType === CompactType.true
          : this.value(field.type, `${path}.${name}`, depth + 1);
    }
    if (!stopped) {
      throw new ThriftError(`Missing struct stop at ${path}`);
    }
    for (const [name, field] of struct.fields) {
      if (!field.isOptional && !(name in result)) {
        throw new ThriftError(
          `Missing required field ${path}.${name} (#${field.id})`
        );
      }
    }
    return result as StructOutput<S>;
  }

  private skip(type: CompactType, fieldBoolean: boolean, depth: number): void {
    if (depth > MAX_DEPTH) {
      throw new ThriftError(
        "Maximum nesting depth exceeded while skipping unknown field"
      );
    }
    if (isBool(type)) {
      if (!fieldBoolean) {
        const value = this.byte();
        if (!isBool(value)) {
          throw new ThriftError(
            `Invalid boolean ${value} at byte ${this.offset - 1}`
          );
        }
      }
      return;
    }
    switch (type) {
      case CompactType.byte: {
        this.require(1);
        this.offset += 1;
        return;
      }
      case CompactType.i16:
      case CompactType.i32: {
        this.varint(5);
        return;
      }
      case CompactType.i64: {
        this.varint(10);
        return;
      }
      case CompactType.double: {
        this.require(8);
        this.offset += 8;
        return;
      }
      case CompactType.float: {
        this.require(4);
        this.offset += 4;
        return;
      }
      case CompactType.binary: {
        const length = this.size("Binary");
        this.offset += length;
        return;
      }
      case CompactType.list:
      case CompactType.set: {
        const [itemType, size] = this.collectionHeader();
        for (let index = 0; index < size; index += 1) {
          this.skip(itemType, false, depth + 1);
        }
        return;
      }
      case CompactType.map: {
        const [keyType, valueType, size] = this.mapHeader();
        for (let index = 0; index < size; index += 1) {
          this.skip(keyType, false, depth + 1);
          this.skip(valueType, false, depth + 1);
        }
        return;
      }
      case CompactType.struct: {
        this.pushStruct();
        try {
          while (!this.done) {
            const field = this.field();
            if (!field) {
              return;
            }
            this.skip(field[0], true, depth + 1);
          }
          throw new ThriftError(`Missing struct stop at byte ${this.offset}`);
        } finally {
          this.popStruct();
        }
      }
      case CompactType.stop: {
        throw new ThriftError(`Unexpected stop type at byte ${this.offset}`);
      }
    }
  }
}

class Writer {
  // ponytail: number[] keeps writes simple; use a growable Uint8Array if multi-MB payloads prove hot.
  private readonly bytes: number[] = [];
  private lastFieldId = 0;
  private readonly fieldStack: number[] = [];

  finish(): Uint8Array {
    return Uint8Array.from(this.bytes);
  }

  private byte(value: number): void {
    this.bytes.push(value & 0xff);
  }

  private raw(value: Uint8Array): void {
    for (const byte of value) {
      this.bytes.push(byte);
    }
  }

  private varint(value: bigint): void {
    let remaining = value;
    do {
      const byte = Number(remaining & 0x7fn);
      remaining >>= 7n;
      this.byte(remaining === 0n ? byte : byte | 0x80);
    } while (remaining !== 0n);
  }

  private integer(value: unknown, bits: 8 | 16 | 32): number {
    if (typeof value !== "number" || !Number.isInteger(value)) {
      throw new ThriftError(`Expected i${bits}, received ${String(value)}`);
    }
    const limit = 2 ** (bits - 1);
    if (value < -limit || value >= limit) {
      throw new ThriftError(`i${bits} value ${value} is out of range`);
    }
    return value;
  }

  private signed(value: bigint, bits: 16 | 32 | 64): void {
    const limit = 1n << BigInt(bits - 1);
    if (value < -limit || value >= limit) {
      throw new ThriftError(`i${bits} value ${value} is out of range`);
    }
    this.varint((value << 1n) ^ (value >> BigInt(bits - 1)));
  }

  private checkSize(value: number, label: string): void {
    if (!Number.isSafeInteger(value) || value < 0 || value > 0x7f_ff_ff_ff) {
      throw new ThriftError(`${label} size ${value} is out of range`);
    }
  }

  private size(value: number, label: string): void {
    this.checkSize(value, label);
    this.varint(BigInt(value));
  }

  private field(id: number, type: CompactType): void {
    const delta = id - this.lastFieldId;
    if (delta > 0 && delta < 16) {
      this.byte((delta << 4) | type);
    } else {
      this.byte(type);
      this.signed(BigInt(id), 16);
    }
    this.lastFieldId = id;
  }

  private pushStruct(): void {
    this.fieldStack.push(this.lastFieldId);
    this.lastFieldId = 0;
  }

  private popStruct(): void {
    this.lastFieldId = this.fieldStack.pop() ?? 0;
  }

  private value(type: Type, value: unknown, path: string, depth: number): void {
    if (depth > MAX_DEPTH) {
      throw new ThriftError(`Maximum nesting depth exceeded at ${path}`);
    }
    switch (type.kind) {
      case "bool": {
        if (typeof value !== "boolean") {
          throw new ThriftError(`Expected boolean at ${path}`);
        }
        this.byte(value ? CompactType.true : CompactType.false);
        return;
      }
      case "byte": {
        this.byte(this.integer(value, 8));
        return;
      }
      case "i16": {
        this.signed(BigInt(this.integer(value, 16)), 16);
        return;
      }
      case "i32": {
        this.signed(BigInt(this.integer(value, 32)), 32);
        return;
      }
      case "i64": {
        if (typeof value !== "bigint") {
          throw new ThriftError(`Expected bigint at ${path}`);
        }
        this.signed(value, 64);
        return;
      }
      case "double":
      case "float": {
        if (typeof value !== "number") {
          throw new ThriftError(`Expected number at ${path}`);
        }
        const length = type.kind === "double" ? 8 : 4;
        const bytes = new Uint8Array(length);
        const view = new DataView(bytes.buffer);
        if (type.kind === "double") {
          view.setFloat64(0, value, false);
        } else {
          view.setFloat32(0, value, false);
        }
        this.raw(bytes);
        return;
      }
      case "binary": {
        if (!(value instanceof Uint8Array)) {
          throw new ThriftError(`Expected Uint8Array at ${path}`);
        }
        this.size(value.length, "Binary");
        this.raw(value);
        return;
      }
      case "string": {
        if (typeof value !== "string") {
          throw new ThriftError(`Expected string at ${path}`);
        }
        const bytes = textEncoder.encode(value);
        this.size(bytes.length, "String");
        this.raw(bytes);
        return;
      }
      case "list": {
        if (!Array.isArray(value)) {
          throw new ThriftError(`Expected array at ${path}`);
        }
        const list = type as ListType<Type>;
        this.collection(list.item.wireType, value.length);
        for (const [index, item] of value.entries()) {
          this.value(list.item, item, `${path}[${index}]`, depth + 1);
        }
        return;
      }
      case "set": {
        if (!(value instanceof Set)) {
          throw new ThriftError(`Expected Set at ${path}`);
        }
        const set = type as SetType<Type>;
        this.collection(set.item.wireType, value.size);
        let index = 0;
        for (const item of value) {
          this.value(set.item, item, `${path}[${index}]`, depth + 1);
          index += 1;
        }
        return;
      }
      case "map": {
        if (!(value instanceof Map)) {
          throw new ThriftError(`Expected Map at ${path}`);
        }
        const map = type as MapType<Type, Type>;
        this.size(value.size, "Map");
        if (value.size > 0) {
          this.byte((map.key.wireType << 4) | map.value.wireType);
          let index = 0;
          for (const [key, entryValue] of value) {
            this.value(map.key, key, `${path}[${index}].key`, depth + 1);
            this.value(
              map.value,
              entryValue,
              `${path}[${index}].value`,
              depth + 1
            );
            index += 1;
          }
        }
        return;
      }
      case "struct": {
        this.pushStruct();
        try {
          this.struct(type as Struct<Shape>, value, path, depth + 1);
        } finally {
          this.popStruct();
        }
      }
    }
  }

  private collection(itemType: CompactType, length: number): void {
    this.checkSize(length, "Collection");
    if (length < 15) {
      this.byte((length << 4) | itemType);
    } else {
      this.byte(0xf0 | itemType);
      this.varint(BigInt(length));
    }
  }

  struct<S extends Shape>(
    struct: Struct<S>,
    value: unknown,
    path: string,
    depth: number
  ): void {
    if (depth > MAX_DEPTH) {
      throw new ThriftError(`Maximum nesting depth exceeded at ${path}`);
    }
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      throw new ThriftError(`Expected object at ${path}`);
    }
    const object = value as Record<string, unknown>;
    for (const [name, descriptor] of struct.fields) {
      const fieldValue = object[name];
      if (fieldValue === undefined) {
        if (!descriptor.isOptional) {
          throw new ThriftError(
            `Missing required field ${path}.${name} (#${descriptor.id})`
          );
        }
        continue;
      }
      if (descriptor.type.kind === "bool") {
        if (typeof fieldValue !== "boolean") {
          throw new ThriftError(`Expected boolean at ${path}.${name}`);
        }
        this.field(
          descriptor.id,
          fieldValue ? CompactType.true : CompactType.false
        );
      } else {
        this.field(descriptor.id, descriptor.type.wireType);
        this.value(descriptor.type, fieldValue, `${path}.${name}`, depth + 1);
      }
    }
    this.byte(CompactType.stop);
  }
}

const bool = scalar<boolean>("bool", CompactType.true);
const byte = scalar<number>("byte", CompactType.byte);
const i16 = scalar<number>("i16", CompactType.i16);
const i32 = scalar<number>("i32", CompactType.i32);
const i64 = scalar<bigint>("i64", CompactType.i64);
const double = scalar<number>("double", CompactType.double);
const float = scalar<number>("float", CompactType.float);
const binary = scalar<Uint8Array>("binary", CompactType.binary);
const string = scalar<string>("string", CompactType.binary);

export const t = {
  binary,
  bool,
  byte,
  double,
  float,
  i16,
  i32,
  i64,
  list: <Item extends Type>(item: Item): ListType<Item> =>
    ({
      field: defineField,
      item,
      kind: "list",
      wireType: CompactType.list,
    }) as ListType<Item>,
  map: <Key extends Type, Value extends Type>(
    key: Key,
    value: Value
  ): MapType<Key, Value> =>
    ({
      field: defineField,
      key,
      kind: "map",
      value,
      wireType: CompactType.map,
    }) as MapType<Key, Value>,
  set: <Item extends Type>(item: Item): SetType<Item> =>
    ({
      field: defineField,
      item,
      kind: "set",
      wireType: CompactType.set,
    }) as SetType<Item>,
  string,
  struct: <S extends Shape>(shape: S): Struct<S> => new Struct(shape),
} as const;
