const BroadcastChannel = require('../..')

main()

async function main() {
  const channel = BroadcastChannel.from(Bare.Thread.self.data)
  const port = channel.connect()

  while (port.peers < 1) await new Promise((r) => setTimeout(r, 10))

  await port.write('hello')
  await port.write('world')
  await port.close()
}
