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
  /** The serializable interfaces registered on the channel. */
  interfaces?: SerializableConstructor[]
  portCapacity?: number
}

interface BroadcastChannel<T = unknown> {
  readonly handle: SharedArrayBuffer
  readonly interfaces: SerializableConstructor[]

  /**
   * Connect a new port to the channel. Throws if `MAX_PORTS` ports are already connected. Port
   * slots are reused, so the cap applies to the number of concurrently connected ports, not the
   * total number of connections over the channel's lifetime; a slot is released for a later
   * connection once the port holding it has fully torn down.
   * @returns A new port connected to the channel.
   */
  connect(): Port<T>
}

declare class BroadcastChannel {
  /**
   * Create a new broadcast channel. The channel is backed by a `SharedArrayBuffer` exposed as
   * `channel.handle` that can be passed to other threads to share the channel across them.
   * @param opts - Channel options; `handle` backs the channel with an existing `SharedArrayBuffer`,
   * `interfaces` registers serializable interfaces (default `[]`), and
   * `portCapacity` defaults to `1024`.
   */
  constructor(opts?: BroadcastChannelOptions)

  /**
   * Maximum number of ports that may be concurrently connected to a single channel. A port's slot
   * is released for reuse by a later connection once the port has fully torn down.
   */
  static MAX_PORTS: number

  /**
   * Restore a channel from its `SharedArrayBuffer` `handle`.
   * @param handle - The `SharedArrayBuffer` backing the channel, as exposed by `channel.handle`.
   * @param opts - The same options as the constructor; any `handle` it carries is overridden by
   * the positional `handle`.
   * @returns A channel backed by the given `handle`.
   */
  static from(handle: SharedArrayBuffer, opts?: BroadcastChannelOptions): BroadcastChannel
}

interface PortEvents extends EventMap {
  /** Emitted when the number of connected peers changes, carrying the current count. */
  peers: [count: number]
  /** Emitted once the port has closed. */
  close: []
}

interface Port<T = unknown> extends EventEmitter<PortEvents>, Iterable<T>, AsyncIterable<T> {
  /** The number of other ports currently connected to the channel. Read-only. */
  readonly peers: number

  /**
   * Increase the reference count for the port to keep the event loop alive. Does nothing once the
   * port has started closing.
   */
  ref(): void
  /**
   * Decrease the reference count for the port to allow the event loop to exit. Does nothing once
   * the port has started closing.
   */
  unref(): void

  /**
   * Read the next message broadcast to the port, waiting for one to arrive if none is queued.
   * @returns The next message, or `null` if the port has ended or every peer has left and the
   * remaining messages have been read.
   */
  read(): Promise<T | null>
  /**
   * Read the next message broadcast to the port, blocking the thread if none is queued.
   * @returns The next message, or `null` if the port has ended or every peer has left and the
   * remaining messages have been read.
   */
  readSync(): T | null

  /**
   * Write `value` to every other port connected to the channel, waiting for room if any of them is
   * full. A port never receives the messages it wrote itself.
   * @param value - The value to broadcast, serialized using the interfaces registered on the
   * channel.
   * @returns `true` once the value has been broadcast, or `false` if `value` is `null` or the port
   * is closing or has ended.
   */
  write(value: T): Promise<boolean>
  /**
   * Write `value` to every other port connected to the channel, blocking the thread if any of them
   * is full. A port never receives the messages it wrote itself.
   * @param value - The value to broadcast, serialized using the interfaces registered on the
   * channel.
   * @returns `true` once the value has been broadcast, or `false` if `value` is `null` or the port
   * has ended.
   */
  writeSync(value: T): boolean

  /**
   * @param opts - The same options as `bare-stream`'s `Readable`.
   * @returns A readable stream of the messages read from the port.
   */
  createReadStream(opts?: ReadableOptions<Port<T>>): Readable
  /**
   * @param opts - The same options as `bare-stream`'s `Writable`.
   * @returns A writable stream whose chunks are written to the port. The port is closed when the
   * stream ends.
   */
  createWriteStream(opts?: WritableOptions<Port<T>>): Writable
  /**
   * @param opts - The same options as `bare-stream`'s `Duplex`.
   * @returns A duplex stream that reads the messages broadcast to the port and writes its chunks
   * to the port. The port is closed when the stream ends.
   */
  createStream(opts?: DuplexOptions<Port<T>>): Duplex

  /**
   * Close the port, waiting for any pending write to drain first. The port leaves the channel and
   * its slot is released for reuse once it has fully torn down.
   * @returns A promise that resolves once the port has closed.
   */
  close(): Promise<void>
}

declare class Port<T = unknown> extends EventEmitter<PortEvents> {
  /**
   * Create a new port connected to `channel`, equivalent to `channel.connect()`. Throws if
   * `MAX_PORTS` ports are already connected.
   * @param channel - The channel to connect the port to.
   */
  constructor(channel: BroadcastChannel<T>)
}

export = BroadcastChannel
