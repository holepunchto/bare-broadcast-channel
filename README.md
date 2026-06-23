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

#### `const channel = new BroadcastChannel([options])`

Create a new broadcast channel. The channel is backed by a `SharedArrayBuffer` exposed as `channel.handle` that can be passed to other threads to share the channel across them.

Options include:

```js
options = {
  handle,
  interfaces: []
}
```

If `handle` is provided, the channel is restored from that existing `SharedArrayBuffer` rather than allocating a fresh one. `interfaces` is a list of serializable constructors used by <https://github.com/holepunchto/bare-structured-clone> when encoding values.

#### `BroadcastChannel.MAX_PORTS`

Maximum number of ports that may connect to a single channel over its lifetime.

#### `const channel = BroadcastChannel.from(handle[, options])`

Restore a channel from its `SharedArrayBuffer` `handle`. `options` accepts the same fields as the constructor, except for `handle`.

#### `channel.handle`

The `SharedArrayBuffer` backing the channel. Pass this to other threads to share the channel.

#### `channel.interfaces`

The serializable interfaces registered on the channel.

#### `const port = channel.connect()`

Connect a new port to the channel. Throws if `MAX_PORTS` has been reached. Port slots are not reused, so the cap applies to the total number of connections over the channel's lifetime, not the number of concurrently connected ports.

### Port

A `Port` is an `EventEmitter` and is both iterable (`for ... of`) and async iterable (`for await ... of`).

#### `port.peers`

The current number of connected peer ports, excluding `port` itself.

#### `const flushed = await port.write(value)`

Broadcast `value` to every currently connected peer. Resolves to `true` once `value` has been pushed to every peer's queue, or `false` if `port` is closed. If any peer's queue is full, the write waits for it to drain before resolving.

`value` is cloned for each peer using <https://github.com/holepunchto/bare-structured-clone>. Transferring ownership of a value is not supported; a broadcast delivers to many peers, so there is no single recipient to take ownership, and the same serialization is decoded independently by each peer.

#### `const flushed = port.writeSync(value)`

Synchronous version of `port.write()`.

#### `const value = await port.read()`

Read the next message broadcast to `port`. Resolves to `null` once `port` is closed via `port.close()`, or once every peer has left after at least one has been observed - so a read loop terminates naturally when the other side disconnects rather than waiting forever. Any messages still queued are delivered before `null`.

#### `const value = port.readSync()`

Synchronous version of `port.read()`, with the same end-of-stream semantics: returns `null` once `port` is closed, or once every peer has left after at least one has been observed.

#### `await port.close()`

Close `port`, releasing it from the channel. After close, subsequent reads resolve to `null` and subsequent writes resolve to `false`.

#### `port.ref()`

Reference `port`, keeping the event loop alive while it is open.

#### `port.unref()`

Unreference `port`, letting the event loop exit if `port` is the only thing keeping it alive.

#### `const stream = port.createReadStream([options])`

A `Readable` stream wrapping `port.read()`. `options` is passed through to <https://github.com/holepunchto/bare-stream>.

#### `const stream = port.createWriteStream([options])`

A `Writable` stream wrapping `port.write()` and `port.close()`. `options` is passed through to <https://github.com/holepunchto/bare-stream>.

#### `const stream = port.createStream([options])`

A `Duplex` stream wrapping read, write, and close. `options` is passed through to <https://github.com/holepunchto/bare-stream>.

#### `event: 'peers'`

Emitted when the number of connected peer ports changes. The listener receives the new peer count.

#### `event: 'close'`

Emitted once `port` has fully closed.

## License

Apache-2.0
