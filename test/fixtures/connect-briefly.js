const BroadcastChannel = require('../..')

main()

async function main() {
  const channel = BroadcastChannel.from(Bare.Thread.self.data)
  const port = channel.connect()

  await new Promise((r) => setTimeout(r, 20))

  await port.close()
}
