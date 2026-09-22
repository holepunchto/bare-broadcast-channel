const test = require('brittle')
const Thread = require('bare-thread')
const { symbols } = require('bare-structured-clone')
const { Barrier } = require('bare-atomics')
const BroadcastChannel = require('.')

test('basic broadcast to two consumers', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const makeConsumer = () =>
    new Thread(require.resolve('./test/fixtures/expect-ping-pong'), {
      data: { handle: channel.handle, count: 2 }
    })

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

  const barrier = new Barrier(NUM_PRODUCERS)

  const producers = []
  for (let i = 0; i < NUM_PRODUCERS; i++) {
    producers.push(
      new Thread(require.resolve('./test/fixtures/produce'), {
        data: {
          handle: channel.handle,
          barrierHandle: barrier.handle,
          id: i,
          n: PER_PRODUCER
        }
      })
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

  barrier.destroy()
})

test('multi producer multi consumer', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const NUM = 50
  const EXPECTED = NUM * 2

  const barrier = new Barrier(4)

  const consumer = () =>
    new Thread(require.resolve('./test/fixtures/consume'), {
      data: {
        handle: channel.handle,
        barrierHandle: barrier.handle,
        expected: EXPECTED
      }
    })

  const c1 = consumer()
  const c2 = consumer()

  const producer = (id) =>
    new Thread(require.resolve('./test/fixtures/produce'), {
      data: {
        handle: channel.handle,
        barrierHandle: barrier.handle,
        id,
        n: NUM
      }
    })

  const p1 = producer(0)
  const p2 = producer(1)

  p1.join()
  p2.join()
  c1.join()
  c2.join()

  barrier.destroy()

  t.pass('consumer 1 received all')
  t.pass('consumer 2 received all')
})

test('read async two consumers', async (t) => {
  t.plan(2)

  const channel = new BroadcastChannel()

  const make = () =>
    new Thread(require.resolve('./test/fixtures/expect-ping-pong-read'), {
      data: channel.handle
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
    new Thread(require.resolve('./test/fixtures/expect-ping-pong-read-sync'), {
      data: channel.handle
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
    new Thread(require.resolve('./test/fixtures/expect-ping-pong-read'), {
      data: channel.handle
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
    new Thread(require.resolve('./test/fixtures/expect-sequence'), {
      data: { handle: channel.handle, n: N }
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

  const thread = new Thread(require.resolve('./test/fixtures/serializable-interface'), {
    data: channel.handle
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

  const thread = new Thread(require.resolve('./test/fixtures/connect-briefly'), {
    data: channel.handle
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

  const thread = new Thread(require.resolve('./test/fixtures/write-many'), {
    data: { handle: channel.handle, n: N }
  })

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

  const thread = new Thread(require.resolve('./test/fixtures/write-stream'), {
    data: { handle: channel.handle, n: N }
  })

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

  const thread = new Thread(require.resolve('./test/fixtures/close'), {
    data: channel.handle
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
    new Thread(require.resolve('./test/fixtures/three-way'), {
      data: {
        handle: channel.handle,
        label,
        per: PER_NODE,
        expected: EXPECTED,
        others: NODES - 1
      }
    })

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

  const thread = new Thread(require.resolve('./test/fixtures/write-hello-world'), {
    data: channel.handle
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

test('read terminates when all peers leave', async (t) => {
  t.plan(1)

  const channel = new BroadcastChannel()

  const thread = new Thread(require.resolve('./test/fixtures/write-hello-world'), {
    data: channel.handle
  })

  const port = channel.connect()

  const received = []
  for await (const msg of port) {
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

  const thread = new Thread(require.resolve('./test/fixtures/expect-sequence'), {
    data: { handle: channel.handle, n: N }
  })

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
    new Thread(require.resolve('./test/fixtures/churn'), {
      data: { handle: channel.handle, rounds: ROUNDS }
    })

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
