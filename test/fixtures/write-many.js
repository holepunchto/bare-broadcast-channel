const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, n } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)
  const port = channel.connect()

  while (port.peers < 1) await new Promise((r) => setTimeout(r, 10))

  for (let i = 0; i < n; i++) {
    await port.write(i)
  }

  await port.close()
}
