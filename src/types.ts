export enum ThriftType {
  STOP = 0,
  TRUE = 1,
  FALSE = 2,
  BYTE = 3,
  INT_16 = 4,
  INT_32 = 5,
  INT_64 = 6,
  DOUBLE = 7,
  BINARY = 8,
  LIST = 9,
  SET = 10,
  MAP = 11,
  STRUCT = 12,
  // Facebook-specific : https://github.com/facebook/fbthrift/blob/v2021.03.22.00/thrift/lib/cpp/protocol/TCompactProtocol-inl.h#L57
  FLOAT = 13,

  // internal!
  BOOLEAN = 161, //0xa1
}

export function isThriftBoolean(type: ThriftType) {
  type &= 0x0f
  return (
    type === ThriftType.TRUE ||
    type === ThriftType.FALSE ||
    type === ThriftType.BOOLEAN
  )
}

export function thriftTypeName(type: ThriftType) {
  const entry = Object.entries(ThriftType).find(([_, t]) => t == type)
  if (!entry) {
    return 'UNKNOWN'
  }
  return entry[0]
}

export type Int64 = number | bigint

export function int64ToNumber(i64: Int64): number {
  if (typeof i64 === 'number') return i64
  return Number(i64)
}

/*
 ** Zod like helpers for defining
 ** thrift types <-> typescript
 */

export namespace t {
  export type TypeOf<T extends TType<any>> = T['_output']

  interface TTypeDef {}

  abstract class TType<Output, Def extends TTypeDef = TTypeDef> {
    readonly _output!: Output
    protected readonly _def!: Def

    constructor(def: Def) {
      this._def = def
    }
  }

  abstract class TThriftType<
    Output,
    Def extends TTypeDef = TTypeDef
  > extends TType<Output, Def> {
    abstract readonly thrift_type: ThriftType
  }

  interface TBinaryTypeDef extends TTypeDef {}

  export enum TBinaryType {
    STRING = 'STRING',
    BINARY = 'BINARY',
  }
  interface TBinaryDef extends TBinaryTypeDef {
    type: TBinaryType.STRING
  }

  interface TStringDef extends TBinaryTypeDef {
    type: TBinaryType.BINARY
  }

  type ObjectType<T> = T extends TBinaryType.STRING ? TStringDef : TBinaryDef

  class TBinary<
    T extends TStringDef | TBinaryDef = TStringDef | TBinaryDef
  > extends TThriftType<
    T extends TStringDef
      ? string
      : T extends TBinaryDef
      ? Buffer
      : string | Buffer,
    T
  > {
    readonly thrift_type = ThriftType.BINARY

    get meta() {
      return {
        type: this._def.type,
      }
    }

    static create = <T extends TBinaryType = TBinaryType.STRING>(type?: T) => {
      return new TBinary<ObjectType<T>>({
        type: type || TBinaryType.STRING,
      } as any)
    }
  }

  type TInt16Def = TTypeDef
  class TInt16 extends TThriftType<number, TInt16Def> {
    readonly thrift_type = ThriftType.INT_16

    static create = (): TInt16 => {
      return new TInt16({})
    }
  }

  type TInt32Def = TTypeDef
  class TInt32 extends TThriftType<number, TInt32Def> {
    readonly thrift_type = ThriftType.INT_32

    static create = (): TInt32 => {
      return new TInt32({})
    }
  }

  type TInt64Def = TTypeDef
  class TInt64 extends TThriftType<bigint, TInt64Def> {
    readonly thrift_type = ThriftType.INT_64

    static create = (): TInt64 => {
      return new TInt64({})
    }
  }

  type TBooleanDef = TTypeDef
  class TBoolean extends TThriftType<boolean, TBooleanDef> {
    readonly thrift_type = ThriftType.BOOLEAN

    static create = (): TBoolean => {
      return new TBoolean({})
    }
  }

  type TByteDef = TTypeDef
  class TByte extends TThriftType<number, TByteDef> {
    readonly thrift_type = ThriftType.BYTE

    static create = (): TByte => {
      return new TByte({})
    }
  }

  interface TListDef<T extends TThriftTypeAny = TThriftTypeAny>
    extends TTypeDef {
    item: T
  }
  class TList<T extends TThriftTypeAny = TThriftTypeAny> extends TThriftType<
    T['_output'][],
    TListDef<T>
  > {
    readonly thrift_type = ThriftType.LIST

    get meta() {
      return {
        item: this._def.item,
      }
    }

    static create = <T extends TThriftTypeAny = TThriftTypeAny>(
      item: T
    ): TList<T> => {
      return new TList({
        item,
      })
    }
  }

  interface TSetDef<T extends TThriftTypeAny = TThriftTypeAny>
    extends TTypeDef {
    item: T
  }
  class TSet<T extends TThriftTypeAny = TThriftTypeAny> extends TThriftType<
    T['_output'][],
    TSetDef<T>
  > {
    readonly thrift_type = ThriftType.SET

    get meta() {
      return {
        item: this._def.item,
      }
    }

    static create = <T extends TThriftTypeAny = TThriftTypeAny>(
      item: T
    ): TSet<T> => {
      return new TSet({
        item,
      })
    }
  }

  interface TMapDef<
    Key extends TThriftTypeAny = TThriftTypeAny,
    Value extends TThriftTypeAny = TThriftTypeAny
  > extends TTypeDef {
    valueType: Value
    keyType: Key
  }

  class TMap<
    Key extends TBinary<TStringDef> | TInt16 | TInt32 =
      | TBinary<TStringDef>
      | TInt16
      | TInt32, // cheat here because we know we are only getting these types
    // Key extends TThriftTypeAny = TThriftTypeAny,
    Value extends TThriftTypeAny = TThriftTypeAny
  > extends TThriftType<
    { [k in Key['_output']]: Value['_output'] },
    // Map<Key['_output'], Value['_output']>,
    TMapDef<Key, Value>
  > {
    readonly thrift_type = ThriftType.MAP

    get meta() {
      return {
        thrift_type: ThriftType.MAP as const,
        key: this._def.keyType,
        value: this._def.valueType,
        isOptional: false,
      }
    }

    static create = <
      Key extends TBinary<TStringDef> | TInt16 | TInt32,
      Value extends TThriftTypeAny = TThriftTypeAny
    >(
      keyType: Key,
      valueType: Value
    ): TMap<Key, Value> => {
      return new TMap({
        valueType,
        keyType,
      })
    }
  }

  interface TFieldDef<
    Optional extends boolean = false,
    T extends TThriftTypeAny = TThriftTypeAny
  > extends TTypeDef {
    number: number
    type: T
    isOptional: Optional
  }

  class TField<
    Optional extends boolean = false,
    T extends TThriftTypeAny = TThriftTypeAny
  > extends TType<
    Optional extends false ? T['_output'] : T['_output'] | undefined,
    TFieldDef<Optional, T>
  > {
    get meta() {
      return this._def
    }

    static create = <T extends TThriftTypeAny = TThriftTypeAny>(
      number: number,
      type: T
    ): TField<false, T> => {
      return new TField({
        number,
        type,
        isOptional: false,
      })
    }

    optional: () => TField<true, T> = () =>
      new TField({ ...this._def, isOptional: true })
  }

  namespace structUtil {
    export type MergeShapes<U extends TRawShape, V extends TRawShape> = {
      [k in Exclude<keyof U, keyof V>]: U[k]
    } &
      V

    type optionalKeys<T extends object> = {
      [k in keyof T]: undefined extends T[k] ? k : never
    }[keyof T]

    type requiredKeys<T extends object> = Exclude<keyof T, optionalKeys<T>>

    export type addQuestionMarks<T extends object> = {
      [k in optionalKeys<T>]?: T[k]
    } &
      { [k in requiredKeys<T>]: T[k] }

    type identity<T> = T
    export type flatten<T extends object> = identity<{ [k in keyof T]: T[k] }>
    export const mergeShapes = <U extends TRawShape, T extends TRawShape>(
      first: U,
      second: T
    ): T & U => {
      return {
        ...first,
        ...second, // second overwrites first
      }
    }
    export const mergeStructs =
      <First extends TStruct>(first: First) =>
      <Second extends TStruct>(
        second: Second
      ): TStruct<First['meta']['shape'] & Second['meta']['shape']> => {
        const mergedShape = structUtil.mergeShapes(
          first.meta.shape,
          second.meta.shape
        )
        const merged = new TStruct({
          shape: mergedShape,
        })
        return merged
      }

    export type extend<A, B> = {
      [k in Exclude<keyof A, keyof B>]: A[k]
    } &
      { [k in keyof B]: B[k] }
  }

  interface TStructDef<T extends TRawShape = TRawShape> extends TTypeDef {
    shape: T
  }

  type baseStructOutputType<Shape extends TRawShape> = structUtil.flatten<
    structUtil.addQuestionMarks<
      {
        [k in keyof Shape]: Shape[k]['_output']
      }
    >
  >

  export type TRawShape = {
    [k: string]: TField<boolean>
  }

  type structOutputType<Shape extends TRawShape> = baseStructOutputType<Shape>

  export class TStruct<T extends TRawShape = TRawShape> extends TThriftType<
    structOutputType<T>,
    TStructDef<T>
  > {
    readonly thrift_type = ThriftType.STRUCT

    get meta() {
      return {
        shape: this._def.shape,
      }
    }

    merge = <Incoming extends TStruct>(
      merging: Incoming
    ): TStruct<structUtil.extend<T, Incoming['meta']['shape']>> => {
      const mergedShape = structUtil.mergeShapes(
        this.meta.shape,
        merging.meta.shape
      )
      const merged = new TStruct({
        shape: mergedShape,
      })
      return merged as any as TStruct<
        structUtil.extend<T, Incoming['meta']['shape']>
      >
    }

    static create = <T extends TRawShape>(shape: T): TStruct<T> => {
      return new TStruct({
        shape,
      })
    }
  }

  export const byte = TByte.create
  export const binary = TBinary.create
  export const int16 = TInt16.create
  export const int32 = TInt32.create
  export const int64 = TInt64.create
  export const boolean = TBoolean.create
  export const list = TList.create
  export const set = TSet.create
  export const map = TMap.create
  export const struct = TStruct.create
  export const field = TField.create

  export type TThriftTypeAny =
    | TBinary
    | TByte
    | TBoolean
    | TInt16
    | TInt32
    | TInt64
    | TList
    | TSet
    | TMap
    | TStruct

  export type TThriftSerializable<T extends TThriftTypeAny = TThriftTypeAny> =
    T['_output']
}
