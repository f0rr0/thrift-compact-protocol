class BaseError extends Error {
  constructor(message?: string) {
    super(message)
    // @ts-ignore -- set the name to the class's actual name
    this.name = this.__proto__.constructor.name
  }
}

export class ThriftError extends BaseError {}
