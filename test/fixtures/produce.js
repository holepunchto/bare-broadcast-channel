const { Barrier } = require('bare-atomics')
const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, barrierHandle, id, n } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)
  const barrier = Barrier.from(barrierHandle)
  const port = channel.connect()

  barrier.wait()

  for (let i = 0; i < n; i++) await port.write({ id, i })

  await port.close()
  for await (const _ of port);
}
