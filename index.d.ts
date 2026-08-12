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
  /**
   * The `SharedArrayBuffer` backing the channel. Pass this to other threads to share the channel.
   */
  handle?: SharedArrayBuffer
  /** The serializable and transferable interfaces registered on the channel. */
  interfaces?: SerializableConstructor[]
  portCapacity?: number
}

interface BroadcastChannel<T = unknown> {
  readonly handle: SharedArrayBuffer
  readonly interfaces: SerializableConstructor[]

  /**
   * Connect a new port to the channel. Throws if `MAX_PORTS` has been reached. Port slots are not
   * reused, so the cap applies to the total number of connections over the channel's lifetime, not
   * the number of concurrently connected ports.
   * @returns A new port connected to the channel.
   */
  connect(): Port<T>
}

declare class BroadcastChannel {
  /**
   * Create a new broadcast channel. The channel is backed by a `SharedArrayBuffer` exposed as
   * `channel.handle` that can be passed to other threads to share the channel across them.
   * @param opts - Channel options; `handle` backs the channel with an existing `SharedArrayBuffer`,
   * `interfaces` registers serializable and transferable interfaces (default `[]`), and
   * `portCapacity` defaults to `1024`.
   */
  constructor(opts?: BroadcastChannelOptions)

  /** Maximum number of ports that may connect to a single channel over its lifetime. */
  static MAX_PORTS: number

  /**
   * Restore a channel from its `SharedArrayBuffer` `handle`. `options` accepts the same fields as
   * the constructor, except for `handle`.
   * @param handle - The `SharedArrayBuffer` backing the channel, as exposed by `channel.handle`.
   * @param opts - The same options as the constructor, except `handle`.
   * @returns A channel backed by the given `handle`.
   */
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
