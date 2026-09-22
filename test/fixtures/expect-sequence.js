const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, n } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)
  const port = channel.connect()

  const read = []
  for await (const v of port) {
    read.push(v)
    if (read.length === n) break
  }

  let ok = read.length === n
  for (let i = 0; i < n; i++) if (read[i] !== i) ok = false
  if (!ok) throw new Error('mismatch')

  await port.close()
}
