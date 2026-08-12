# bare-broadcast-channel

Multi-producer, multi-consumer inter-thread broadcast messaging for JavaScript. Unlike <https://github.com/holepunchto/bare-channel>, which is single-producer / single-consumer over a fixed port pair, `bare-broadcast-channel` allows any number of ports (up to `BroadcastChannel.MAX_PORTS`) to connect to the same channel. Each write is broadcast to every other connected port, and a port never receives messages it wrote itself.

```
npm i bare-broadcast-channel
```

## Usage

```js
const BroadcastChannel = require('bare-broadcast-channel')
const { Thread } = Bare

const channel = new BroadcastChannel()

const consumer = (label) =>
  new Thread(__filename, { data: { handle: channel.handle, label } }, async ({ handle, label }) => {
    const BroadcastChannel = require('bare-broadcast-channel')
    const port = BroadcastChannel.from(handle).connect()
    console.log(label, 'got', await port.read())
    await port.close()
  })

const a = consumer('a')
const b = consumer('b')

const port = channel.connect()

while (port.peers < 2) await new Promise((r) => setTimeout(r, 10))

await port.write('hello')
await port.close()

a.join()
b.join()
```

## API

See the [full API reference](https://docs.pears.com/reference/bare/modules/bare-broadcast-channel).

## License

Apache-2.0
