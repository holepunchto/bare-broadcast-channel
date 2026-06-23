import EventEmitter, { EventMap } from 'bare-events'
import { SerializableConstructor } from 'bare-structured-clone'
import {
  Duplex,
  DuplexOptions,
  Readable,
  ReadableOptions,
  Writable,
  WritableOptions
} from 'bare-stream'

interface BroadcastChannelOptions {
  handle?: SharedArrayBuffer
  interfaces?: SerializableConstructor[]
  portCapacity?: number
}

interface BroadcastChannel<T = unknown> {
  readonly handle: SharedArrayBuffer
  readonly interfaces: SerializableConstructor[]

  connect(): Port<T>
}

declare class BroadcastChannel {
  constructor(opts?: BroadcastChannelOptions)

  static MAX_PORTS: number

  static from(handle: SharedArrayBuffer, opts?: BroadcastChannelOptions): BroadcastChannel
}

interface PortEvents extends EventMap {
  peers: [count: number]
  close: []
}

interface Port<T = unknown> extends EventEmitter<PortEvents>, Iterable<T>, AsyncIterable<T> {
  readonly peers: number

  ref(): void
  unref(): void

  read(): Promise<T | null>
  readSync(): T | null

  write(value: T): Promise<boolean>
  writeSync(value: T): boolean

  createReadStream(opts?: ReadableOptions<Port<T>>): Readable
  createWriteStream(opts?: WritableOptions<Port<T>>): Writable
  createStream(opts?: DuplexOptions<Port<T>>): Duplex

  close(): Promise<void>
}

declare class Port<T = unknown> extends EventEmitter<PortEvents> {
  constructor(channel: BroadcastChannel<T>)
}

export = BroadcastChannel
