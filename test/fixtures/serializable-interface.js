const { symbols } = require('bare-structured-clone')
const BroadcastChannel = require('../..')

class Foo {
  constructor(foo) {
    this.foo = foo
  }

  [symbols.serialize]() {
    return this.foo
  }

  static [symbols.deserialize](serialized) {
    return new Foo(serialized)
  }
}

main()

async function main() {
  const channel = BroadcastChannel.from(Bare.Thread.self.data, { interfaces: [Foo] })
  const port = channel.connect()

  const v = await port.read()
  if (!(v instanceof Foo) || v.foo !== 'foo') throw new Error('bad')

  await port.close()
}
