# thrift-compact-protocol

A tiny, type-safe codec for the Facebook Thrift Compact Protocol. It has no runtime dependencies beyond the type-only Standard Schema spec and works with `Uint8Array` in Bun, Node.js, browsers, and other modern JavaScript runtimes.

## Install

Requires Node.js 22+ or Bun 1.3+. CI verifies Node.js 22 and 24; releases run on Node.js 24.

```sh
bun add @f0rr0/thrift-compact-protocol
```

## Use

```ts
import { type Infer, t } from "@f0rr0/thrift-compact-protocol";

const user = t.struct({
  id: t.i64.field(1),
  name: t.string.field(2),
  active: t.bool.field(3),
  tags: t.set(t.string).field(4).optional(),
  scores: t.map(t.string, t.i32).field(5),
});

type User = Infer<typeof user>;

const value: User = {
  id: 42n,
  name: "Ada",
  active: true,
  scores: new Map([["math", 100]]),
};

const bytes: Uint8Array = user.encode(value);
const decoded: User = user.decode(bytes);
```

Every struct is also a [Standard Schema](https://standardschema.dev/) whose input is `Uint8Array` and output is the inferred object:

```ts
const result = user["~standard"].validate(bytes);
```

## Types

The schema DSL mirrors Thrift without inventing another type system:

- Scalars: `t.bool`, `t.byte`, `t.i16`, `t.i32`, `t.i64`, `t.float`, `t.double`, `t.string`, `t.binary`
- Containers: `t.list(type)`, `t.set(type)`, `t.map(key, value)`
- Fields: `type.field(id)`, `type.field(id).optional()`
- Structs: `t.struct({ ...fields })`

Field IDs are stable wire identities: match the remote Thrift schema and keep IDs unchanged when reordering or renaming properties. Every type, including a reusable struct, supports `.field(id)`. Calling `.field(id)` or `.optional()` creates a new field descriptor without changing the original. Optionality applies to the field, not to collection elements.

`i64` uses `bigint`, binary uses `Uint8Array`, and maps and sets use JavaScript's native `Map` and `Set`. Unknown fields are skipped for forward compatibility. Missing required fields, wrong wire types, invalid UTF-8, truncated payloads, out-of-range integers, and excessive nesting throw `ThriftError`.

This package implements raw struct payloads in current Facebook compact (V2): doubles and floats are big-endian, and compact type `13` is a 32-bit float. Legacy V1/Apache little-endian doubles, RPC message envelopes and Apache Thrift's newer UUID use of type `13` are out of scope. Raw structs have no version header, so both peers must agree on the dialect.

Regression tests adapt FBThrift C++, Rust and Go cases and include fixed wire vectors. See [upstream provenance and limitations](tests/UPSTREAM.md).

## Develop

```sh
mise install
bun install
hk install
bun run check
```
