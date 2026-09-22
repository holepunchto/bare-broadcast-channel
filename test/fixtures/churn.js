const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, rounds } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)

  for (let i = 0; i < rounds; i++) {
    const port = channel.connect()

    // Exercise the cross-thread producer path against peers that may be
    // concurrently closing and having their slots reused.
    await port.write(i)
    await port.close()
  }
}
