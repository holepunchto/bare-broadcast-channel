const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, label, per, expected, others } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)
  const port = channel.connect()

  // Wait until we've at any point observed all other nodes connected
  let maxSeen = 0
  while (maxSeen < others) {
    maxSeen = Math.max(maxSeen, port.peers)
    if (maxSeen >= others) break
    await new Promise((r) => setTimeout(r, 5))
  }

  const reader = (async () => {
    let count = 0
    for await (const _ of port) {
      count++
      if (count === expected) break
    }
    if (count !== expected) throw new Error(label + ' got ' + count)
  })()

  for (let i = 0; i < per; i++) {
    await port.write({ from: label, i })
  }

  await reader
  await port.close()
}
