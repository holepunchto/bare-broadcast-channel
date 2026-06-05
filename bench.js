const BroadcastChannel = require('.')
const { Thread } = Bare

const NUM_CONSUMERS = 3
const ops = 5e5

const channel = new BroadcastChannel()

// Spawn consumers first so producer always sees them.
const port = channel.connect()

const consumers = []

for (let i = 0; i < NUM_CONSUMERS; i++) {
  consumers.push(
    new Thread(__filename, { data: { handle: channel.handle, ops } }, async ({ handle, ops }) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      let n = 0
      for await (const _ of port) {
        n++
        if (n === ops) break
      }

      await port.close()
    })
  )
}

;(async () => {
  // Wait for consumers to show up
  while (port.peers < NUM_CONSUMERS) await new Promise((r) => setTimeout(r, 5))

  const start = Date.now()

  for (let i = 0; i < ops; i++) {
    await port.write(i)
  }

  const elapsed = Date.now() - start

  console.log(
    'broadcast:',
    Math.round((ops / elapsed) * 1000),
    'writes/s |',
    Math.round(((ops * NUM_CONSUMERS) / elapsed) * 1000),
    'deliveries/s'
  )

  await port.close()

  for (const c of consumers) c.join()
})()
