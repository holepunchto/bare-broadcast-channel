const BroadcastChannel = require('../..')

main()

async function main() {
  const channel = BroadcastChannel.from(Bare.Thread.self.data)
  const port = channel.connect()

  const a = await port.read()
  const b = await port.read()

  if (a !== 'ping' || b !== 'pong') throw new Error('bad ' + a + ' ' + b)

  await port.close()
}
