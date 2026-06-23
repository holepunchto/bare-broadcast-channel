const test = require('brittle')
const { symbols } = require('bare-structured-clone')
const BroadcastChannel = require('.')
const { Thread } = Bare

test('basic broadcast to two consumers', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const makeConsumer = () =>
    new Thread(
      __filename,
      { data: { handle: channel.handle, count: 2 } },
      async ({ handle, count }) => {
        const BroadcastChannel = require('.')

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
    )

  const a = makeConsumer()
  const b = makeConsumer()

  const port = channel.connect()

  while (port.peers < 2) await new Promise((r) => setTimeout(r, 10))

  for (const data of ['ping', 'pong']) {
    await port.write(data)
  }

  await port.close()

  a.join()
  b.join()

  t.pass('consumer a finished')
  t.pass('consumer b finished')
})

test('no peers - write succeeds as no-op', async (t) => {
  const channel = new BroadcastChannel()
  const port = channel.connect()

  t.is(await port.write('hello'), true)

  await port.close()
})

test('multi producer single consumer', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  // Consumer first so producers always see at least one peer
  const port = channel.connect()

  const NUM_PRODUCERS = 3
  const PER_PRODUCER = 100
  const TOTAL = NUM_PRODUCERS * PER_PRODUCER

  const producers = []
  for (let i = 0; i < NUM_PRODUCERS; i++) {
    producers.push(
      new Thread(
        __filename,
        { data: { handle: channel.handle, id: i, n: PER_PRODUCER } },
        async ({ handle, id, n }) => {
          const BroadcastChannel = require('.')
          const channel = BroadcastChannel.from(handle)
          const port = channel.connect()

          // Drain anything other producers send at us so their writes never stall
          const drainSelf = (async () => {
            for await (const _ of port);
          })()

          for (let i = 0; i < n; i++) await port.write({ id, i })

          await port.close()
          await drainSelf
        }
      )
    )
  }

  const received = new Map()
  for (let i = 0; i < NUM_PRODUCERS; i++) received.set(i, 0)

  let count = 0
  for await (const msg of port) {
    received.set(msg.id, received.get(msg.id) + 1)
    count++
    if (count === TOTAL) break
  }

  await port.close()

  let ok = count === TOTAL
  for (const [, c] of received) if (c !== PER_PRODUCER) ok = false

  t.ok(ok, 'received PER_PRODUCER messages from every producer')

  for (const p of producers) p.join()
})

test('multi producer multi consumer', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const NUM = 50
  const EXPECTED = NUM * 2

  const consumer = () =>
    new Thread(
      __filename,
      { data: { handle: channel.handle, expected: EXPECTED } },
      async ({ handle, expected }) => {
        const BroadcastChannel = require('.')

        const channel = BroadcastChannel.from(handle)
        const port = channel.connect()

        let count = 0
        for await (const _ of port) {
          count++
          if (count === expected) break
        }

        if (count !== expected) throw new Error('Bad count ' + count)

        await port.close()
      }
    )

  const c1 = consumer()
  const c2 = consumer()

  const producer = () =>
    new Thread(__filename, { data: { handle: channel.handle, n: NUM } }, async ({ handle, n }) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      // Drain anything the other producer sends at us so its writes never stall
      const drainSelf = (async () => {
        for await (const _ of port);
      })()

      for (let i = 0; i < n; i++) await port.write(i)

      await port.close()
      await drainSelf
    })

  const p1 = producer()
  const p2 = producer()

  p1.join()
  p2.join()
  c1.join()
  c2.join()

  t.pass('consumer 1 received all')
  t.pass('consumer 2 received all')
})

test('read async two consumers', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const make = () =>
    new Thread(__filename, { data: channel.handle }, async (handle) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      const a = await port.read()
      const b = await port.read()

      if (a !== 'ping' || b !== 'pong') throw new Error('bad ' + a + ' ' + b)

      await port.close()
    })

  const a = make()
  const b = make()

  const port = channel.connect()

  while (port.peers < 2) await new Promise((r) => setTimeout(r, 10))

  for (const data of ['ping', 'pong']) {
    await port.write(data)
  }

  await port.close()

  a.join()
  b.join()

  t.pass('a done')
  t.pass('b done')
})

test('read blocking', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const make = () =>
    new Thread(__filename, { data: channel.handle }, async (handle) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      const a = port.readSync()
      const b = port.readSync()

      if (a !== 'ping' || b !== 'pong') throw new Error('bad ' + a + ' ' + b)

      await port.close()
    })

  const a = make()
  const b = make()

  const port = channel.connect()

  while (port.peers < 2) await new Promise((r) => setTimeout(r, 10))

  for (const data of ['ping', 'pong']) {
    await port.write(data)
  }

  await port.close()

  a.join()
  b.join()

  t.pass('a done')
  t.pass('b done')
})

test('write blocking', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const make = () =>
    new Thread(__filename, { data: channel.handle }, async (handle) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      const a = await port.read()
      const b = await port.read()

      if (a !== 'ping' || b !== 'pong') throw new Error('bad ' + a + ' ' + b)

      await port.close()
    })

  const a = make()
  const b = make()

  const port = channel.connect()

  while (port.peers < 2) await new Promise((r) => setTimeout(r, 10))

  for (const data of ['ping', 'pong']) {
    port.writeSync(data)
  }

  await port.close()

  a.join()
  b.join()

  t.pass('a done')
  t.pass('b done')
})

test('big echo broadcast', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const N = 1e4

  const consumer = () =>
    new Thread(__filename, { data: { handle: channel.handle, n: N } }, async ({ handle, n }) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      const read = []
      for await (const v of port) {
        read.push(v)
        if (read.length === n) break
      }

      let ok = true
      for (let i = 0; i < n; i++) if (read[i] !== i) ok = false
      if (!ok) throw new Error('mismatch')

      await port.close()
    })

  const a = consumer()
  const b = consumer()

  const port = channel.connect()

  while (port.peers < 2) await new Promise((r) => setTimeout(r, 10))

  for (let i = 0; i < N; i++) await port.write(i)

  await port.close()

  a.join()
  b.join()

  t.pass('a ok')
  t.pass('b ok')
})

test('serializable interface', async (t) => {
  class Foo {
    constructor(foo) {
      this.foo = foo
    }

    [symbols.serialize]() {
      return this.foo
    }

    static [symbols.deserialize](serialized) {
      return new Foo(serialized)
    }
  }

  const channel = new BroadcastChannel({ interfaces: [Foo] })

  const thread = new Thread(__filename, { data: channel.handle }, async (handle) => {
    const BroadcastChannel = require('.')
    const { symbols } = require('bare-structured-clone')

    class Foo {
      constructor(foo) {
        this.foo = foo
      }

      [symbols.serialize]() {
        return this.foo
      }

      static [symbols.deserialize](serialized) {
        return new Foo(serialized)
      }
    }

    const channel = BroadcastChannel.from(handle, { interfaces: [Foo] })
    const port = channel.connect()

    const v = await port.read()
    if (!(v instanceof Foo) || v.foo !== 'foo') throw new Error('bad')

    await port.close()
  })

  const port = channel.connect()

  while (port.peers < 1) await new Promise((r) => setTimeout(r, 10))

  await port.write(new Foo('foo'))

  await port.close()

  thread.join()

  t.pass('serializable transported correctly')
})

test('peers event fires when peer connects and disconnects', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()
  const port = channel.connect()

  const thread = new Thread(__filename, { data: channel.handle }, async (handle) => {
    const BroadcastChannel = require('.')

    const channel = BroadcastChannel.from(handle)
    const port = channel.connect()

    await new Promise((r) => setTimeout(r, 20))

    await port.close()
  })

  let sawPeer = false

  await new Promise((resolve) => {
    port.on('peers', (n) => {
      if (n === 1) sawPeer = true
      if (n === 0 && sawPeer) {
        t.pass('peers transitioned 1 -> 0')
        t.is(port.peers, 0)
        resolve()
      }
    })
  })

  await port.close()
  thread.join()
})

test('read stream', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  const N = 1e3

  const thread = new Thread(
    __filename,
    { data: { handle: channel.handle, n: N } },
    async ({ handle, n }) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      while (port.peers < 1) await new Promise((r) => setTimeout(r, 10))

      for (let i = 0; i < n; i++) {
        await port.write(i)
      }

      await port.close()
    }
  )

  const port = channel.connect()
  const stream = port.createReadStream()
  const received = []

  stream.on('data', (value) => {
    received.push(value)
    if (received.length === N) stream.destroy()
  })

  stream.on('close', async () => {
    t.alike(
      received,
      new Array(N).fill(0).map((_, i) => i)
    )

    await port.close()

    thread.join()
  })
})

test('write stream', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  const N = 1e3

  const thread = new Thread(
    __filename,
    { data: { handle: channel.handle, n: N } },
    async ({ handle, n }) => {
      const BroadcastChannel = require('.')

      const channel = BroadcastChannel.from(handle)
      const port = channel.connect()

      while (port.peers < 1) await new Promise((r) => setTimeout(r, 10))

      const stream = port.createWriteStream()

      for (let i = 0; i < n; i++) {
        stream.write(Buffer.from(`${i}`))
      }

      stream.end()
    }
  )

  const port = channel.connect()
  const received = []

  for await (const v of port) {
    received.push(v.toString())
    if (received.length === N) break
  }

  await port.close()

  t.alike(
    received,
    new Array(N).fill(0).map((_, i) => `${i}`)
  )

  thread.join()
})

test('both sides close', async (t) => {
  const channel = new BroadcastChannel()

  const thread = new Thread(__filename, { data: channel.handle }, async (handle) => {
    const BroadcastChannel = require('.')

    const channel = BroadcastChannel.from(handle)
    const port = channel.connect()

    await port.close()
  })

  const port = channel.connect()
  await port.close()

  thread.join()
})

test('three way broadcast', async (t) => {
  t.plan(3)

  const channel = new BroadcastChannel()

  const PER_NODE = 10
  const NODES = 3
  const EXPECTED = PER_NODE * (NODES - 1)

  const make = (label) =>
    new Thread(
      __filename,
      {
        data: {
          handle: channel.handle,
          label,
          per: PER_NODE,
          expected: EXPECTED,
          others: NODES - 1
        }
      },
      async ({ handle, label, per, expected, others }) => {
        const BroadcastChannel = require('.')

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
    )

  const a = make('a')
  const b = make('b')
  const c = make('c')

  a.join()
  b.join()
  c.join()

  t.pass('a finished')
  t.pass('b finished')
  t.pass('c finished')
})

test('readSync terminates when all peers leave', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  const thread = new Thread(__filename, { data: channel.handle }, async (handle) => {
    const BroadcastChannel = require('.')

    const channel = BroadcastChannel.from(handle)
    const p = channel.connect()

    while (p.peers < 1) await new Promise((r) => setTimeout(r, 10))

    await p.write('hello')
    await p.write('world')
    await p.close()
  })

  const port = channel.connect()

  const received = []
  for (const msg of port) {
    received.push(msg)
  }

  t.alike(received, ['hello', 'world'])

  await port.close()
  thread.join()
})

test('port slots are reused after close', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  // Open and close many more ports in sequence than are ever live at once. Slot
  // reuse means this churns through a single slot rather than growing the
  // directory unbounded.
  for (let i = 0; i < 200; i++) {
    const port = channel.connect()

    await port.close()
  }

  t.pass('reused slots without growing the directory')
})

test('directory grows to hold many simultaneous ports', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  // Open more ports at once than a single segment can hold, forcing the
  // directory to grow across several segments.
  const COUNT = 130

  const ports = []

  for (let i = 0; i < COUNT; i++) {
    ports.push(channel.connect())
  }

  t.is(ports.length, COUNT, 'opened ports across multiple segments')
  t.is(ports[ports.length - 1].peers, COUNT - 1, 'last port sees every other peer')

  await Promise.all(ports.map((port) => port.close()))
})

test('reused slots deliver messages on a clean queue', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  // Churn through enough connections to force the next pair onto reused slots.
  for (let i = 0; i < 100; i++) {
    const port = channel.connect()

    await port.write('discarded')
    await port.close()
  }

  const reader = channel.connect()
  const writer = channel.connect()

  while (writer.peers < 1) await new Promise((r) => setTimeout(r, 10))

  await writer.write('after-reuse')

  t.is(await reader.read(), 'after-reuse')
  t.is(reader.peers, 1)

  await writer.close()
  await reader.close()
})

test('custom port capacity delivers all messages', async (t) => {
  t.plan(1)

  // A capacity of 3 rounds up to 4, leaving a tiny ring that wraps many times
  // and exercises backpressure over the course of the transfer.
  const channel = new BroadcastChannel({ portCapacity: 3 })

  const N = 1e3

  const thread = new Thread(
    __filename,
    { data: { handle: channel.handle, n: N } },
    async ({ handle, n }) => {
      const BroadcastChannel = require('.')

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
  )

  const port = channel.connect()

  while (port.peers < 1) await new Promise((r) => setTimeout(r, 10))

  for (let i = 0; i < N; i++) await port.write(i)

  await port.close()

  thread.join()

  t.pass('all messages delivered over a small ring')
})

test('concurrent connect and close churn across threads', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  const ROUNDS = 50

  const churn = () =>
    new Thread(
      __filename,
      { data: { handle: channel.handle, rounds: ROUNDS } },
      async ({ handle, rounds }) => {
        const BroadcastChannel = require('.')

        const channel = BroadcastChannel.from(handle)

        for (let i = 0; i < rounds; i++) {
          const port = channel.connect()

          // Exercise the cross-thread producer path against peers that may be
          // concurrently closing and having their slots reused.
          await port.write(i)
          await port.close()
        }
      }
    )

  const a = churn()
  const b = churn()
  const c = churn()

  for (let i = 0; i < ROUNDS; i++) {
    const port = channel.connect()

    await port.write(i)
    await port.close()
  }

  a.join()
  b.join()
  c.join()

  t.pass('churned slots across threads without exhaustion or crash')
})
