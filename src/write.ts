import { ThriftType, thriftTypeName, t } from './types'
import { ThriftError } from './errors'

export class BufferWriter {
  _buffer: Buffer
  _cursor = 0
  _prev_field_id: number
  _stack: number[]

  public get buffer(): Buffer {
    return this._buffer
  }

  constructor() {
    this._buffer = Buffer.from([])
    this._prev_field_id = 0
    this._stack = []
    this._cursor = 0
  }

  _push_stack() {
    this._stack.push(this._prev_field_id)
    this._prev_field_id = 0
  }

  _pop_stack() {
    if (this._stack.length > 0) {
      this._prev_field_id = this._stack.pop()!
    }
  }

  public move(bytes: number): number {
    this._cursor = this._cursor + bytes
    return this._cursor - bytes
  }

  private writeBuffer(buf: Buffer) {
    this._buffer = Buffer.concat([this._buffer, buf])
    this.move(buf.length)
  }

  _write_byte(byte: number) {
    const buf = Buffer.alloc(1)
    buf.writeUInt8(byte, 0)
    this.writeBuffer(buf)
  }

  public static bigintToZigZag(n: bigint): bigint {
    return (n << BigInt(1)) ^ (n >> BigInt(63))
  }

  public static toZigZag = (n: number, bits: number) =>
    (n << 1) ^ (n >> (bits - 1))

  private _write_varint(num: number) {
    while (true) {
      let byte = num & ~0x7f
      if (byte === 0) {
        this._write_byte(num)
        break
      } else if (byte === -128) {
        this._write_byte(0)
        break
      } else {
        byte = (num & 0xff) | 0x80
        this._write_byte(byte)
        num = num >> 7
      }
    }
  }

  private _write_bigint_varint(n: bigint) {
    while (true) {
      if ((n & ~BigInt(0x7f)) === BigInt(0)) {
        this._write_byte(Number(n))
        break
      } else {
        this._write_byte(Number((n & BigInt(0x7f)) | BigInt(0x80)))
        n = n >> BigInt(7)
      }
    }
  }

  _write_word(val: number) {
    this._write_varint(BufferWriter.toZigZag(val, 16))
  }

  _write_int(val: number) {
    this._write_varint(BufferWriter.toZigZag(val, 32))
  }

  _write_long(val: number | bigint) {
    this._write_bigint_varint(
      BufferWriter.bigintToZigZag(typeof val == 'bigint' ? val : BigInt(val))
    )
  }

  _write_field_begin(field_id: number, type: ThriftType) {
    const delta = field_id - this._prev_field_id
    if (0 < delta && delta < 16) {
      this._write_byte((delta << 4) | type)
    } else {
      this._write_byte(type)
      this._write_word(field_id)
    }
    this._prev_field_id = field_id
  }

  _write_string(val: string | Buffer) {
    if (typeof val == 'string') {
      const buf = Buffer.from(val, 'utf8')
      this._write_varint(buf.length)
      this.writeBuffer(buf)
    } else {
      this._write_varint(val.length)
      this.writeBuffer(val)
    }
  }

  write_map(
    field_id: number,
    key_type: ThriftType,
    value_type: ThriftType,
    val: { [x: string]: t.TThriftTypeAny }
  ) {
    this._write_field_begin(field_id, ThriftType.MAP)
    const length = Object.keys(val).length
    if (length == 0) {
      this._write_byte(0)
    } else {
      this._write_varint(length)
      this._write_byte(((key_type & 0xf) << 4) | (value_type & 0xf))
      Object.entries(val).forEach(([k, v]) => {
        this.write_val(undefined, key_type, k)
        this.write_val(undefined, value_type, v)
      })
    }
  }
  //     def write_stop(self) -> None:
  //         self._write_byte(TType.STOP.value)
  //         self._pop_stack()
  write_stop() {
    this._write_byte(ThriftType.STOP)
    this._pop_stack()
  }

  write_list(field_id: number, item_type: ThriftType, val: any[]) {
    this._write_field_begin(field_id, ThriftType.LIST)
    if (val.length < 0x0f) {
      this._write_byte((val.length << 4) | item_type)
    } else {
      this._write_byte(0xf0 | item_type)
      this._write_varint(val.length)
    }
    val.forEach((v) => this.write_val(undefined, item_type, v))
  }

  write_struct_begin(field_id: number) {
    this._write_field_begin(field_id, ThriftType.STRUCT)
    this._push_stack()
  }

  write_val(field_id: number | undefined, type: ThriftType, val: any) {
    if (type == ThriftType.BOOLEAN) {
      if (field_id == undefined) {
        throw new ThriftError('booleans can only be used in structs')
      } else {
        return this._write_field_begin(
          field_id,
          val ? ThriftType.TRUE : ThriftType.FALSE
        )
      }
    } else if (field_id != undefined) {
      this._write_field_begin(field_id, type)
    }
    switch (type) {
      case ThriftType.BYTE:
        this._write_byte(val)
        break
      case ThriftType.INT_16:
        this._write_word(val)
        break
      case ThriftType.INT_32:
        this._write_int(val)
        break
      case ThriftType.INT_64:
        this._write_long(val)
        break
      case ThriftType.BINARY:
        this._write_string(val)
        break
      default:
        throw new ThriftError(
          `Value ${val} of type ${thriftTypeName(
            type
          )} is not supported by write_val()`
        )
    }
  }

  write_struct<T extends t.TRawShape>(
    obj: t.TypeOf<t.TStruct<T>>,
    struct: t.TStruct<T>
  ) {
    const entries = Object.entries(obj)
    if (entries.length != 0) {
      Object.entries(struct.meta.shape).forEach(([keyName, field]) => {
        const entry = entries.find(([k]) => keyName == k)
        if (entry) {
          if (
            field.meta.type.thrift_type == ThriftType.LIST ||
            field.meta.type.thrift_type == ThriftType.SET
          ) {
            this.write_list(
              field.meta.number,
              field.meta.type.meta.item.thrift_type,
              entry[1] as any
            )
          } else if (field.meta.type.thrift_type == ThriftType.MAP) {
            this.write_map(
              field.meta.number,
              field.meta.type.meta.key.thrift_type,
              field.meta.type.meta.value.thrift_type,
              entry[1] as any
            )
          } else if (field.meta.type.thrift_type == ThriftType.STRUCT) {
            this.write_struct_begin(field.meta.number)
            this.write_struct(entry[1] as any, field.meta.type)
          } else {
            this.write_val(
              field.meta.number,
              field.meta.type.thrift_type,
              entry[1]
            )
          }
        }
      })
    }
    this.write_stop()
  }
}

export const thriftWriteFromObject: <T extends t.TRawShape>(
  obj: t.TypeOf<t.TStruct<T>>,
  struct: t.TStruct<T>
) => Buffer = (obj, struct) => {
  const writer = new BufferWriter()
  writer.write_struct(obj, struct)
  return writer.buffer
}
