## thrift-compact-protocol

Marshalling/unmarshalling between Facebook Thrift Compact Protocol and Javascript with type definitions.

## Usage

```ts
import {
  t,
  ThriftType,
  thriftReadToObject,
  thriftWriteFromObject,
} from '@f0rr0/thrift-compact-protocol'

// Describe the shape of expected Thrift data over the wire
const structDefinition = t.struct({
  foo: t.field(1, t.boolean()),
  bar: t.field(2, t.int64()).optional(),
  baz: t.field(
    4,
    t.list(
      t.struct({
        quz: t.field(1, t.binary()),
      })
    )
  ),
})

// Typescript type is inferred from the Thrift definition
type StructDefinition = t.TypeOf<typeof structDefinition>

// Parse a Thrift buffer to JS object
const javascriptObject = thriftReadToObject(
  someThriftBufferPayload,
  structDefinition
)

// Write JS object to Thrift buffer
const thriftBuffer = thriftWriteFromObject(
  {
    foo: true,
    bar: BigInt(1),
    baz: [
      {
        quz: 'string',
      },
    ],
  },
  structDefinition
)
```
