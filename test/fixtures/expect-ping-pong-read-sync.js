const BroadcastChannel = require('../..')

main()

async function main() {
  const channel = BroadcastChannel.from(Bare.Thread.self.data)
  const port = channel.connect()

  const a = port.readSync()
  const b = port.readSync()

  if (a !== 'ping' || b !== 'pong') throw new Error('bad ' + a + ' ' + b)

  await port.close()
}
