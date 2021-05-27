import { ThriftType, isThriftBoolean, thriftTypeName, t } from './types'
import { ThriftError } from './errors'

export class BufferReader {
  // BufferReader implements decoding the Thrift Compact protocol into JavaScript values.
  // https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

  private _buffer: Buffer
  private _cursor = 0
  private _prev_field_id: number
  private _stack: number[]
  private _prev_struct_id: number

  private get _struct_id() {
    this._prev_struct_id += 1
    return this._prev_struct_id
  }

  constructor(buffer: Buffer) {
    this._buffer = buffer
    this._prev_field_id = 0
    this._prev_struct_id = -1
    this._stack = []
  }

  private _push_stack() {
    this._stack.push(this._prev_field_id)
    this._prev_field_id = 0
  }

  private _pop_stack() {
    if (this._stack && this._stack.length > 0) {
      this._prev_field_id = this._stack.pop()!
    }
  }

  private move(bytes: number) {
    this._cursor = Math.min(
      Math.max(this._cursor + bytes, 0),
      this._buffer.length
    )
    return this._cursor - bytes
  }

  private readByte = () => this._buffer.readUInt8(this.move(1))
  private readSByte = () => this._buffer.readInt8(this.move(1))

  private reset() {
    this._cursor = 0
    this._prev_field_id = 0
    this._prev_struct_id = -1
    this._stack = []
  }

  // http://neurocline.github.io/dev/2015/09/17/zig-zag-encoding.html
  static fromZigZag = (n: number) => (n >> 1) ^ -(n & 1)

  static fromZigZagToBigInt(n: bigint): bigint {
    return (n >> BigInt(1)) ^ -(n & BigInt(1))
  }

  private readVarInt(): number {
    let shift = 0
    let result = 0
    while (this._cursor < this._buffer.length) {
      const byte = this.readByte()
      result |= (byte & 0x7f) << shift
      if ((byte & 0x80) === 0) {
        break
      }
      shift += 7
    }
    return result
  }

  private readVarBigint(): bigint {
    let shift = BigInt(0)
    let result = BigInt(0)
    while (this._cursor < this._buffer.length) {
      const byte = this.readByte()
      result = result | ((BigInt(byte) & BigInt(0x7f)) << shift)
      if ((byte & 0x80) !== 0x80) break

      shift += BigInt(7)
    }
    return result
  }

  private readBinary = (len: number): Buffer =>
    this._buffer.slice(this.move(len), this._cursor)

  private read_field(): [ThriftType, number] {
    const byte = this.readByte()
    if (byte === 0) {
      return [ThriftType.STOP, -1]
    }
    const delta = (byte & 0xf0) >> 4
    if (delta === 0) {
      this._prev_field_id = BufferReader.fromZigZag(this.readVarInt())
    } else {
      this._prev_field_id += delta
    }
    return [byte & 0x0f, this._prev_field_id]
  }

  // type == TType.DOUBLE:
  //     # Doubles are encoded as little endian
  //     # https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md#double-encoding
  //     return struct.unpack("<d", self.read(8))
  // type == TType.FLOAT:
  //     # This seems to be a facebook-specific 32-bit float
  //     return struct.unpack("<d", self.read(4))
  private read_val(type: ThriftType) {
    switch (type) {
      case ThriftType.TRUE:
        return true
      case ThriftType.FALSE:
        return false
      case ThriftType.BYTE:
        return this.readSByte()
      case ThriftType.BINARY:
        return this.readBinary(this.readVarInt())
      case ThriftType.INT_16:
      case ThriftType.INT_32:
        return BufferReader.fromZigZag(this.readVarInt())
      case ThriftType.INT_64:
        return BufferReader.fromZigZagToBigInt(this.readVarBigint())
      case ThriftType.DOUBLE:
        return this._buffer.readDoubleLE(this.move(8)) // not verified
      case ThriftType.FLOAT:
        return this._buffer.readFloatLE(this.move(4)) // not verified
      default:
        throw new ThriftError(
          `Type ${type} ${thriftTypeName(type)} not implemented`
        )
    }
  }

  private read_list_header(): [ThriftType, number] {
    const header_byte = this.readByte()
    const type = header_byte & 0x0f
    let length = header_byte >> 4

    if (length == 0x0f) {
      length = this.readVarInt()
    }
    return [type, length]
  }

  private read_map_header(): [ThriftType, ThriftType, number] {
    let current_cursor = this._cursor
    if (this.readByte() == 0) {
      return [ThriftType.STOP, ThriftType.STOP, 0]
    }
    this._cursor = current_cursor
    const length = this.readVarInt()
    const types = this.readByte()
    const key_type = types >> 4
    const value_type = types & 0x0f
    return [key_type, value_type, length]
  }

  private skip(type: ThriftType): void {
    switch (type) {
      case ThriftType.STRUCT: {
        this._push_stack()
        while (this._cursor < this._buffer.length) {
          const [field_type, _] = this.read_field()
          if (field_type == ThriftType.STOP) {
            break
          }
          this.skip(field_type)
        }
        this._pop_stack()
        break
      }
      case ThriftType.LIST:
      case ThriftType.SET: {
        const [item_type, length] = this.read_list_header()
        for (let i = 0; i < length; i++) {
          this.skip(item_type)
        }
        break
      }
      case ThriftType.MAP: {
        const [key_type, value_type, length] = this.read_map_header()
        for (let i = 0; i < length; i++) {
          this.skip(key_type)
          this.skip(value_type)
        }
        break
      }
      default:
        this.read_val(type)
    }
  }

  _read_kv(
    key_type: t.TThriftTypeAny,
    value_type: t.TThriftTypeAny,
    field_path: string,
    index: number
  ): [t.TThriftSerializable, t.TThriftSerializable] {
    const key = this.read_val_recursive(
      key_type,
      `${field_path}[{${index}}::key]`
    )
    const value_path = ['string', 'boolean', 'number'].includes(typeof key)
      ? `${key}`
      : `${index}::value`
    const value = this.read_val_recursive(
      value_type,
      `${field_path}[${{ value_path }}]`
    )
    return [key, value]
  }

  private read_val_recursive(
    field: t.TThriftTypeAny,
    field_path: string = 'root'
  ): t.TypeOf<t.TThriftTypeAny> {
    if (field.thrift_type == ThriftType.STRUCT) {
      const sub_fields = Object.entries(field.meta.shape)
      if (sub_fields.length == 0) {
        throw new ThriftError(`No fields defined for struct}`)
      } else {
        this._push_stack()
        const val = this.read_struct(field, field_path)
        this._pop_stack()
        return val
      }
    } else if (field.thrift_type == ThriftType.MAP) {
      const { key, value } = field.meta
      const [key_type, value_type, length] = this.read_map_header()
      if (length == 0) {
        return {}
      } else if (key_type != key.thrift_type) {
        throw new ThriftError(
          `Unexpected key type at ${field_path}: expected ${thriftTypeName(
            key.thrift_type
          )}, got ${thriftTypeName(key_type)}`
        )
      } else if (value_type != value.thrift_type) {
        throw new ThriftError(
          `Unexpected value type at ${field_path}: expected ${thriftTypeName(
            value.thrift_type
          )}, got ${thriftTypeName(value_type)}`
        )
      } else {
        return Object.fromEntries(
          Array(length)
            .fill(null)
            .map((_, index) => this._read_kv(key, value, field_path, index))
        )
      }
    } else if (
      field.thrift_type == ThriftType.LIST ||
      field.thrift_type == ThriftType.SET
    ) {
      const { item } = field.meta
      const [item_type, length] = this.read_list_header()
      if (length == 0) {
        return []
      } else if (item_type != item.thrift_type) {
        throw new ThriftError(
          `Unexpected item type at ${field_path}: expected ${thriftTypeName(
            item.thrift_type
          )}, got ${thriftTypeName(item_type)}`
        )
      } else {
        return Array(length)
          .fill(null)
          .map((_, index) =>
            this.read_val_recursive(item, `${field_path}[${index}]`)
          )
      }
    } else if (
      field.thrift_type == ThriftType.BINARY &&
      field.meta.type == t.TBinaryType.STRING
    ) {
      return this.readBinary(this.readVarInt()).toString('utf8')
    } else {
      return this.read_val(field.thrift_type)
    }
  }

  public read_struct<T extends t.TRawShape>(
    struct: t.TStruct<T>,
    field_path: string = 'root'
  ) {
    const response: t.TypeOf<t.TStruct<T>> = Object.create({})

    const fields = Object.entries(struct.meta.shape)
    while (this._cursor < this._buffer.length) {
      const [field_type, field_index] = this.read_field()
      if (field_type == ThriftType.STOP) {
        break
      }
      const descriptor = fields.find(
        ([_, field]) => field.meta.number == field_index
      )
      if (descriptor) {
        const [keyName, field] = descriptor
        const expected_type = isThriftBoolean(field_type)
          ? ThriftType.BOOLEAN
          : field_type
        if (field.meta.type.thrift_type != expected_type) {
          throw new ThriftError(
            `Mismatching type for for field ${keyName} #${field_index}: expected ${thriftTypeName(
              field.meta.type.thrift_type
            )}, got ${thriftTypeName(field_type)}`
          )
        } else if (expected_type == ThriftType.BOOLEAN) {
          Object.assign(response, { [keyName]: field_type == ThriftType.TRUE })
        } else {
          const fp = `${field_path}.${keyName}`
          Object.assign(response, {
            [keyName]: this.read_val_recursive(field.meta.type, fp),
          })
        }
      } else {
        console.log(
          `Skipping ${thriftTypeName(
            field_type
          )} at ${field_path}#${field_index}`
        )
        this.skip(field_type)
      }
    }
    return response
  }

  public pretty_print(
    field_type: ThriftType = ThriftType.STRUCT,
    _indent: string = '',
    _prefix: string = ''
  ) {
    let prefix = _prefix ? `${_indent}${_prefix} ` : ''
    switch (field_type) {
      case ThriftType.LIST:
      case ThriftType.SET: {
        const [item_type, length] = this.read_list_header()
        console.log(`${prefix}${thriftTypeName(item_type)} ${length} items`)
        Array(length)
          .fill(null)
          .forEach((_, index) =>
            this.pretty_print(item_type, _indent + '  ', `[${index}]`)
          )
        break
      }
      case ThriftType.MAP: {
        const [key_type, value_type, length] = this.read_map_header()
        console.log(
          `${prefix}<${thriftTypeName(key_type)}: ${thriftTypeName(
            value_type
          )}> - ${length} items`
        )
        Array(length)
          .fill(null)
          .forEach(() => {
            const key = this.read_val(key_type)
            this.pretty_print(value_type, _indent + '  ', `[${key}]:`)
          })
        break
      }
      case ThriftType.STRUCT: {
        const struct_id = this._struct_id
        console.log(`${prefix}start-${struct_id} {`)
        this._push_stack()
        while (this._cursor < this._buffer.length) {
          const [subfield_type, subfield_index] = this.read_field()
          if (subfield_type == ThriftType.STOP) {
            break
          }
          this.pretty_print(
            subfield_type,
            _indent + '  ',
            `#${subfield_index} (${thriftTypeName(subfield_type)}):`
          )
        }
        console.log(`${_indent}} end-${struct_id}`)
        this._pop_stack()
        break
      }
      case ThriftType.BINARY: {
        console.log(
          `${prefix}${this.readBinary(this.readVarInt()).toString('utf8')}`
        )
        break
      }
      default:
        console.log(`${prefix}${this.read_val(field_type)}`)
    }
  }
}

export const thriftReadToObject = <T extends t.TRawShape>(
  buffer: Buffer,
  struct: t.TStruct<T>
) => {
  const reader = new BufferReader(buffer)
  return reader.read_struct(struct)
}
