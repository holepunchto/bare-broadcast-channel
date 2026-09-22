const BroadcastChannel = require('../..')

main()

async function main() {
  const { handle, count } = Bare.Thread.self.data

  const channel = BroadcastChannel.from(handle)
  const port = channel.connect()

  const received = []

  for await (const data of port) {
    received.push(data)
    if (received.length === count) break
  }

  if (received[0] !== 'ping' || received[1] !== 'pong') {
    throw new Error('Unexpected payload: ' + JSON.stringify(received))
  }

  await port.close()
}
