const { Barrier } = require('bare-atomics')
const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, barrierHandle, expected } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)
  const barrier = Barrier.from(barrierHandle)
  const port = channel.connect()

  barrier.wait()

  let count = 0
  for await (const _ of port) {
    count++
    if (count === expected) break
  }

  if (count !== expected) throw new Error('Bad count ' + count)

  await port.close()
}
