#include <assert.h>
#include <bare.h>
#include <js.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

// Ports are organised into a two-level directory. The channel holds a fixed
// table of segment pointers; each segment owns one 64-bit claim/active word
// pair and up to 64 lazily allocated ports. Growth only ever appends a new
// segment, so existing segments, ports, and their synchronisation primitives
// never move for the lifetime of the channel. This keeps the peer list
// resizable without the reclamation hazards of copying a live directory. The
// segments and ports are reclaimed in one pass when the channel handle's
// finalizer runs, once the last holder of the handle has been collected.
#define BARE_BROADCAST_CHANNEL_SEGMENT_SIZE 64
#define BARE_BROADCAST_CHANNEL_SEGMENT_MASK (BARE_BROADCAST_CHANNEL_SEGMENT_SIZE - 1)
#define BARE_BROADCAST_CHANNEL_MAX_SEGMENTS 1024

#define BARE_BROADCAST_CHANNEL_MAX_PORTS \
  (BARE_BROADCAST_CHANNEL_SEGMENT_SIZE * BARE_BROADCAST_CHANNEL_MAX_SEGMENTS)

// The per-port message ring depth is chosen when the channel is created and
// rounded up to a power of two so the cursor arithmetic can mask rather than
// divide.
#define BARE_BROADCAST_CHANNEL_DEFAULT_CAPACITY 1024
#define BARE_BROADCAST_CHANNEL_MIN_CAPACITY     2

typedef struct bare_broadcast_channel_s bare_broadcast_channel_t;
typedef struct bare_broadcast_channel_segment_s bare_broadcast_channel_segment_t;
typedef struct bare_broadcast_channel_port_s bare_broadcast_channel_port_t;
typedef struct bare_broadcast_channel_message_s bare_broadcast_channel_message_t;
typedef struct bare_broadcast_channel_payload_s bare_broadcast_channel_payload_t;
typedef struct bare_broadcast_channel_peers_s bare_broadcast_channel_peers_t;

struct bare_broadcast_channel_payload_s {
  atomic_int refcount;
  js_arraybuffer_backing_store_t *backing_store;
};

struct bare_broadcast_channel_message_s {
  bare_broadcast_channel_payload_t *payload;
};

struct bare_broadcast_channel_port_s {
  bare_broadcast_channel_t *channel;

  // The segment this port lives in and the single bit identifying its slot
  // within that segment. Both are fixed once the port is allocated and survive
  // slot reuse, so they are cached here to avoid recomputing them.
  bare_broadcast_channel_segment_t *segment;
  uint64_t bit;

  uint32_t id;

  // The ring depth minus one, used to mask the read and write cursors.
  uint32_t capacity_mask;

  struct {
    atomic_bool active;
    bool ending;
    bool ended;
    uint8_t closing;
    bool exiting;
  } state;

  struct {
    atomic_int read;
    atomic_int write;
  } cursors;

  struct {
    uv_mutex_t drain;
    uv_mutex_t flush;
    uv_mutex_t producer;
  } locks;

  struct {
    uv_cond_t drain;
    uv_cond_t flush;
  } conditions;

  struct {
    uv_async_t drain;
    uv_async_t flush;
    uv_async_t end;
  } signals;

  js_env_t *env;
  js_ref_t *ctx;
  js_ref_t *on_drain;
  js_ref_t *on_flush;
  js_ref_t *on_end;
  js_ref_t *on_close;

  js_deferred_teardown_t *teardown;

  // The message ring is a flexible array member sized to the channel's port
  // capacity so each port is a single allocation.
  bare_broadcast_channel_message_t messages[];
};

struct bare_broadcast_channel_segment_s {
  atomic_uint_least64_t claimed;
  atomic_uint_least64_t active;

  bare_broadcast_channel_port_t *slots[BARE_BROADCAST_CHANNEL_SEGMENT_SIZE];
};

struct bare_broadcast_channel_s {
  uint32_t port_capacity;

  // The number of segments currently published in the table. It only ever
  // grows, always under the growth mutex, and is read with acquire elsewhere.
  atomic_uint segment_count;

  // Serialises segment allocation so two connecting ports never publish a
  // segment into the same table slot.
  uv_mutex_t grow;

  _Atomic(bare_broadcast_channel_segment_t *) segments[BARE_BROADCAST_CHANNEL_MAX_SEGMENTS];
};

struct bare_broadcast_channel_peers_s {
  bare_broadcast_channel_t *channel;
  bare_broadcast_channel_segment_t *segment;
  uint32_t segments;
  uint32_t self_segment;
  uint64_t self_bit;
  uint32_t index;
  uint64_t bits;
};

static inline uint32_t
bare_broadcast_channel__round_capacity(uint32_t capacity) {
  if (capacity < BARE_BROADCAST_CHANNEL_MIN_CAPACITY) {
    capacity = BARE_BROADCAST_CHANNEL_MIN_CAPACITY;
  }

  capacity--;
  capacity |= capacity >> 1;
  capacity |= capacity >> 2;
  capacity |= capacity >> 4;
  capacity |= capacity >> 8;
  capacity |= capacity >> 16;
  capacity++;

  return capacity;
}

static inline bare_broadcast_channel_segment_t *
bare_broadcast_channel__segment(bare_broadcast_channel_t *channel, uint32_t index) {
  return atomic_load_explicit(&channel->segments[index], memory_order_acquire);
}

static inline bare_broadcast_channel_port_t *
bare_broadcast_channel__port(bare_broadcast_channel_t *channel, uint32_t id) {
  bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, id / BARE_BROADCAST_CHANNEL_SEGMENT_SIZE);

  return segment->slots[id & BARE_BROADCAST_CHANNEL_SEGMENT_MASK];
}

static inline bare_broadcast_channel_peers_t
bare_broadcast_channel__peers(bare_broadcast_channel_t *channel, uint32_t id) {
  return (bare_broadcast_channel_peers_t){
    .channel = channel,
    .segment = NULL,
    .segments = atomic_load_explicit(&channel->segment_count, memory_order_acquire),
    .self_segment = id / BARE_BROADCAST_CHANNEL_SEGMENT_SIZE,
    .self_bit = 1ull << (id & BARE_BROADCAST_CHANNEL_SEGMENT_MASK),
    .index = 0,
    .bits = 0,
  };
}

static inline bare_broadcast_channel_port_t *
bare_broadcast_channel__peers_next(bare_broadcast_channel_peers_t *peers) {
  while (peers->bits == 0) {
    if (peers->index >= peers->segments) return NULL;

    peers->segment = bare_broadcast_channel__segment(peers->channel, peers->index);

    uint64_t active = atomic_load_explicit(&peers->segment->active, memory_order_acquire);

    if (peers->index == peers->self_segment) active &= ~peers->self_bit;

    peers->bits = active;
    peers->index++;
  }

  int bit = __builtin_ctzll(peers->bits);

  peers->bits &= peers->bits - 1;

  return peers->segment->slots[bit];
}

static inline void
bare_broadcast_channel__signal(uv_mutex_t *lock, uv_cond_t *cond, uv_async_t *async) {
  int err;

  uv_mutex_lock(lock);
  uv_cond_signal(cond);
  uv_mutex_unlock(lock);

  err = uv_async_send(async);
  assert(err == 0);
}

static inline void
bare_broadcast_channel__wake_peer(bare_broadcast_channel_port_t *peer, bool flush, bool drain) {
  uv_mutex_lock(&peer->locks.producer);

  if (atomic_load_explicit(&peer->state.active, memory_order_acquire)) {
    if (flush) bare_broadcast_channel__signal(&peer->locks.flush, &peer->conditions.flush, &peer->signals.flush);
    if (drain) bare_broadcast_channel__signal(&peer->locks.drain, &peer->conditions.drain, &peer->signals.drain);
  }

  uv_mutex_unlock(&peer->locks.producer);
}

// Counts the active peers of a port, excluding the port itself, by summing the
// population of every published segment's active word.
static inline uint32_t
bare_broadcast_channel__count_peers(bare_broadcast_channel_t *channel, uint32_t id) {
  uint32_t self_segment = id / BARE_BROADCAST_CHANNEL_SEGMENT_SIZE;
  uint64_t self_bit = 1ull << (id & BARE_BROADCAST_CHANNEL_SEGMENT_MASK);

  uint32_t count = 0;

  uint32_t segments = atomic_load_explicit(&channel->segment_count, memory_order_acquire);

  for (uint32_t s = 0; s < segments; s++) {
    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    uint64_t active = atomic_load_explicit(&segment->active, memory_order_acquire);

    if (s == self_segment) active &= ~self_bit;

    count += __builtin_popcountll(active);
  }

  return count;
}

static inline bare_broadcast_channel_message_t *
bare_broadcast_channel__peek_read(bare_broadcast_channel_port_t *port) {
  int read = atomic_load_explicit(&port->cursors.read, memory_order_relaxed);

  int write = atomic_load_explicit(&port->cursors.write, memory_order_acquire);

  if (read == write) return NULL;

  return &port->messages[read];
}

static inline void
bare_broadcast_channel__push_read(bare_broadcast_channel_port_t *port) {
  int read = atomic_load_explicit(&port->cursors.read, memory_order_relaxed);

  int next = (read + 1) & port->capacity_mask;

  atomic_store_explicit(&port->cursors.read, next, memory_order_release);

  // Reading freed a slot in this port's ring, so wake any peer that may be
  // blocked writing to us. We do not know which peers are producers, so we
  // signal every active peer's drain condition.
  bare_broadcast_channel_peers_t peers = bare_broadcast_channel__peers(port->channel, port->id);

  for (bare_broadcast_channel_port_t *peer; (peer = bare_broadcast_channel__peers_next(&peers));) {
    bare_broadcast_channel__wake_peer(peer, false, true);
  }
}

static inline bare_broadcast_channel_message_t *
bare_broadcast_channel__peek_write(bare_broadcast_channel_port_t *port) {
  int write = atomic_load_explicit(&port->cursors.write, memory_order_relaxed);

  int read = atomic_load_explicit(&port->cursors.read, memory_order_acquire);

  int next = (write + 1) & port->capacity_mask;

  if (next == read) return NULL;

  return &port->messages[write];
}

static inline void
bare_broadcast_channel__push_write(bare_broadcast_channel_port_t *port) {
  int write = atomic_load_explicit(&port->cursors.write, memory_order_relaxed);

  int next = (write + 1) & port->capacity_mask;

  atomic_store_explicit(&port->cursors.write, next, memory_order_release);

  if (atomic_load_explicit(&port->state.active, memory_order_acquire)) {
    bare_broadcast_channel__signal(&port->locks.flush, &port->conditions.flush, &port->signals.flush);
  }
}

static inline void
bare_broadcast_channel__release_payload(js_env_t *env, bare_broadcast_channel_payload_t *payload) {
  int err;

  if (atomic_fetch_sub_explicit(&payload->refcount, 1, memory_order_acq_rel) == 1) {
    err = js_release_arraybuffer_backing_store(env, payload->backing_store);
    assert(err == 0);

    free(payload);
  }
}

// Wakes every active peer of a port, flushing and draining each, so they
// re-evaluate the peer set. Used when a port joins or leaves.
static inline void
bare_broadcast_channel__wake_peers(bare_broadcast_channel_port_t *port) {
  bare_broadcast_channel_peers_t peers = bare_broadcast_channel__peers(port->channel, port->id);

  for (bare_broadcast_channel_port_t *peer; (peer = bare_broadcast_channel__peers_next(&peers));) {
    bare_broadcast_channel__wake_peer(peer, true, true);
  }
}

static inline void
bare_broadcast_channel__port_close(bare_broadcast_channel_port_t *port);

static void
bare_broadcast_channel__on_drain(uv_async_t *handle) {
  int err;

  bare_broadcast_channel_port_t *port = handle->data;

  if (port->state.ending && !port->state.exiting) return;

  if (port->state.exiting) {
    if (!port->state.ending) {
      atomic_fetch_and_explicit(&port->segment->active, ~port->bit, memory_order_acq_rel);

      bare_broadcast_channel__wake_peers(port);

      port->state.ending = true;

      err = uv_async_send(&port->signals.end);
      assert(err == 0);
    }
  } else {
    js_env_t *env = port->env;

    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *ctx;
    err = js_get_reference_value(env, port->ctx, &ctx);
    assert(err == 0);

    js_value_t *on_drain;
    err = js_get_reference_value(env, port->on_drain, &on_drain);
    assert(err == 0);

    js_call_function(env, ctx, on_drain, 0, NULL, NULL);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static void
bare_broadcast_channel__on_flush(uv_async_t *handle) {
  int err;

  bare_broadcast_channel_port_t *port = handle->data;

  if (port->state.exiting) {
    while (true) {
      bare_broadcast_channel_message_t *message = bare_broadcast_channel__peek_read(port);

      if (message == NULL) break;

      bare_broadcast_channel__release_payload(port->env, message->payload);

      bare_broadcast_channel__push_read(port);
    }
  } else {
    js_env_t *env = port->env;

    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *ctx;
    err = js_get_reference_value(env, port->ctx, &ctx);
    assert(err == 0);

    js_value_t *on_flush;
    err = js_get_reference_value(env, port->on_flush, &on_flush);
    assert(err == 0);

    js_call_function(env, ctx, on_flush, 0, NULL, NULL);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }
}

static void
bare_broadcast_channel__on_end(uv_async_t *handle) {
  int err;

  bare_broadcast_channel_port_t *port = handle->data;

  port->state.ended = true;

  if (!port->state.exiting) {
    js_env_t *env = port->env;

    js_handle_scope_t *scope;
    err = js_open_handle_scope(env, &scope);
    assert(err == 0);

    js_value_t *ctx;
    err = js_get_reference_value(env, port->ctx, &ctx);
    assert(err == 0);

    js_value_t *on_end;
    err = js_get_reference_value(env, port->on_end, &on_end);
    assert(err == 0);

    js_call_function(env, ctx, on_end, 0, NULL, NULL);

    err = js_close_handle_scope(env, scope);
    assert(err == 0);
  }

  bare_broadcast_channel__port_close(port);
}

static void
bare_broadcast_channel__on_close(uv_handle_t *handle) {
  int err;

  bare_broadcast_channel_port_t *port = handle->data;

  if (--port->state.closing != 0) return;

  js_deferred_teardown_t *teardown = port->teardown;

  js_env_t *env = port->env;

  js_handle_scope_t *scope;
  err = js_open_handle_scope(env, &scope);
  assert(err == 0);

  js_value_t *ctx;
  err = js_get_reference_value(env, port->ctx, &ctx);
  assert(err == 0);

  js_value_t *on_destroy;
  err = js_get_reference_value(env, port->on_close, &on_destroy);
  assert(err == 0);

  err = js_delete_reference(env, port->on_drain);
  assert(err == 0);

  err = js_delete_reference(env, port->on_flush);
  assert(err == 0);

  err = js_delete_reference(env, port->on_end);
  assert(err == 0);

  err = js_delete_reference(env, port->on_close);
  assert(err == 0);

  err = js_delete_reference(env, port->ctx);
  assert(err == 0);

  if (!port->state.exiting) js_call_function(env, ctx, on_destroy, 0, NULL, NULL);

  err = js_close_handle_scope(env, scope);
  assert(err == 0);

  err = js_finish_deferred_teardown_callback(teardown);
  assert(err == 0);

  // Release the slot so a later connection can reuse it. This must happen only
  // once the port has fully torn down: its active bit is long cleared, its
  // queue drained, and the libuv handles backing it closed, so no other thread
  // can still observe or signal this incarnation. The port allocation itself is
  // kept so the reused slot starts from the same backing memory.
  atomic_fetch_and_explicit(&port->segment->claimed, ~port->bit, memory_order_release);
}

static void
bare_broadcast_channel__on_teardown(js_deferred_teardown_t *handle, void *data) {
  int err;

  bare_broadcast_channel_port_t *port = data;

  port->state.exiting = true;

  if (port->state.closing) return;

  err = uv_async_send(&port->signals.drain);
  assert(err == 0);

  err = uv_async_send(&port->signals.flush);
  assert(err == 0);
}

static void
bare_broadcast_channel__finalize(js_env_t *env, void *data, void *finalize_hint) {
  bare_broadcast_channel_t *channel = data;

  uint32_t segments = atomic_load_explicit(&channel->segment_count, memory_order_acquire);

  for (uint32_t s = 0; s < segments; s++) {
    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    for (uint32_t i = 0; i < BARE_BROADCAST_CHANNEL_SEGMENT_SIZE; i++) {
      bare_broadcast_channel_port_t *port = segment->slots[i];

      if (port == NULL) continue;

      uv_mutex_destroy(&port->locks.drain);
      uv_mutex_destroy(&port->locks.flush);
      uv_mutex_destroy(&port->locks.producer);

      uv_cond_destroy(&port->conditions.drain);
      uv_cond_destroy(&port->conditions.flush);

      free(port);
    }

    free(segment);
  }

  uv_mutex_destroy(&channel->grow);

  free(channel);
}

static js_value_t *
bare_broadcast_channel_init(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 1;
  js_value_t *argv[1];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  uint32_t capacity = BARE_BROADCAST_CHANNEL_DEFAULT_CAPACITY;

  if (argc >= 1) {
    err = js_get_value_uint32(env, argv[0], &capacity);
    assert(err == 0);
  }

  // The channel backs an externally managed shared array buffer rather than an
  // engine-owned one so that its finalizer can reclaim the lazily allocated
  // segments and ports. They live in process heap referenced from the channel,
  // so they remain valid across every environment sharing the handle.
  bare_broadcast_channel_t *channel = malloc(sizeof(bare_broadcast_channel_t));

  channel->port_capacity = bare_broadcast_channel__round_capacity(capacity);

  atomic_init(&channel->segment_count, 0);

  err = uv_mutex_init(&channel->grow);
  assert(err == 0);

  // The channel is not yet shared, so a plain clear of the table is sufficient
  // to initialise every segment pointer to NULL.
  memset(channel->segments, 0, sizeof(channel->segments));

  js_value_t *handle;
  err = js_create_external_sharedarraybuffer(env, (void *) channel, sizeof(bare_broadcast_channel_t), bare_broadcast_channel__finalize, NULL, &handle);
  assert(err == 0);

  return handle;
}

// Attempts to claim a free slot in a segment, returning the claimed bit index
// or -1 if the segment is full.
static inline int
bare_broadcast_channel__try_claim(bare_broadcast_channel_segment_t *segment) {
  uint64_t claimed = atomic_load_explicit(&segment->claimed, memory_order_acquire);

  while (true) {
    uint64_t available = ~claimed;

    if (available == 0) return -1;

    int bit = __builtin_ctzll(available);

    if (atomic_compare_exchange_weak_explicit(&segment->claimed, &claimed, claimed | (1ull << bit), memory_order_acq_rel, memory_order_acquire)) {
      return bit;
    }
  }
}

static js_value_t *
bare_broadcast_channel_port_init(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 6;
  js_value_t *argv[6];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 6);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  // Claim the lowest free slot in an existing segment, growing the directory by
  // a segment if every published segment is full. Slots are released once a
  // port has fully torn down (see bare_broadcast_channel__on_close), so they
  // are reused by later connections rather than exhausted.
  bare_broadcast_channel_segment_t *segment = NULL;
  int bit = -1;
  uint32_t id = 0;

  uint32_t segments = atomic_load_explicit(&channel->segment_count, memory_order_acquire);

  for (uint32_t s = 0; s < segments; s++) {
    segment = bare_broadcast_channel__segment(channel, s);

    bit = bare_broadcast_channel__try_claim(segment);

    if (bit >= 0) {
      id = s * BARE_BROADCAST_CHANNEL_SEGMENT_SIZE + bit;
      break;
    }
  }

  if (bit < 0) {
    uv_mutex_lock(&channel->grow);

    // Re-scan under the growth mutex in case a peer freed a slot or published a
    // new segment while we waited.
    segments = atomic_load_explicit(&channel->segment_count, memory_order_acquire);

    for (uint32_t s = 0; s < segments; s++) {
      segment = bare_broadcast_channel__segment(channel, s);

      bit = bare_broadcast_channel__try_claim(segment);

      if (bit >= 0) {
        id = s * BARE_BROADCAST_CHANNEL_SEGMENT_SIZE + bit;
        break;
      }
    }

    if (bit < 0) {
      if (segments >= BARE_BROADCAST_CHANNEL_MAX_SEGMENTS) {
        uv_mutex_unlock(&channel->grow);

        err = js_throw_error(env, NULL, "Channel has reached the maximum number of connected ports");
        assert(err == 0);

        return NULL;
      }

      segment = malloc(sizeof(bare_broadcast_channel_segment_t));

      atomic_init(&segment->claimed, 1ull);
      atomic_init(&segment->active, 0);

      memset(segment->slots, 0, sizeof(segment->slots));

      // Publish the segment pointer before bumping the count so a reader that
      // observes the higher count is guaranteed to see the pointer.
      atomic_store_explicit(&channel->segments[segments], segment, memory_order_release);
      atomic_store_explicit(&channel->segment_count, segments + 1, memory_order_release);

      bit = 0;
      id = segments * BARE_BROADCAST_CHANNEL_SEGMENT_SIZE;
    }

    uv_mutex_unlock(&channel->grow);
  }

  uv_loop_t *loop;
  err = js_get_env_loop(env, &loop);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = segment->slots[bit];

  if (port == NULL) {
    // First use of this slot: allocate the port together with its message ring
    // and initialise the synchronisation primitives that live for the lifetime
    // of the slot. Peers lock a port's producer mutex whenever they write to or
    // signal it, including while it is closing or being reused by a later
    // connection, so this backing memory must remain valid and must never be
    // reinitialised underneath them.
    uint32_t capacity = channel->port_capacity;

    port = malloc(sizeof(bare_broadcast_channel_port_t) + capacity * sizeof(bare_broadcast_channel_message_t));

    port->channel = channel;
    port->segment = segment;
    port->bit = 1ull << bit;
    port->id = id;
    port->capacity_mask = capacity - 1;

#define V(lock) \
  err = uv_mutex_init(&port->locks.lock); \
  assert(err == 0);
    V(drain)
    V(flush)
    V(producer)
#undef V

#define V(condition) \
  err = uv_cond_init(&port->conditions.condition); \
  assert(err == 0);
    V(drain)
    V(flush)
#undef V

    // We hold this slot exclusively via its claimed bit, and no peer reads the
    // slot pointer until the active bit below is published, so a plain store is
    // safe here.
    segment->slots[bit] = port;
  }

  port->env = env;

  // Reset the per-incarnation state in case this slot previously backed a now
  // closed port.
  port->state.ending = false;
  port->state.ended = false;
  port->state.closing = 0;
  port->state.exiting = false;

  atomic_store_explicit(&port->state.active, false, memory_order_relaxed);

  atomic_store_explicit(&port->cursors.read, 0, memory_order_relaxed);
  atomic_store_explicit(&port->cursors.write, 0, memory_order_relaxed);

  err = js_create_reference(env, argv[1], 1, &port->ctx);
  assert(err == 0);

  err = js_create_reference(env, argv[2], 1, &port->on_drain);
  assert(err == 0);

  err = js_create_reference(env, argv[3], 1, &port->on_flush);
  assert(err == 0);

  err = js_create_reference(env, argv[4], 1, &port->on_end);
  assert(err == 0);

  err = js_create_reference(env, argv[5], 1, &port->on_close);
  assert(err == 0);

  err = js_add_deferred_teardown_callback(env, bare_broadcast_channel__on_teardown, (void *) port, &port->teardown);
  assert(err == 0);

#define V(signal) \
  err = uv_async_init(loop, &port->signals.signal, bare_broadcast_channel__on_##signal); \
  assert(err == 0); \
  port->signals.signal.data = (void *) port;
  V(drain)
  V(flush)
  V(end)
#undef V

  atomic_store_explicit(&port->state.active, true, memory_order_release);

  // Publishing the active bit makes this port discoverable to peers. The
  // release pairs with the acquire loads peers use to read the active words,
  // ensuring the slot pointer and initialisation above are visible.
  atomic_fetch_or_explicit(&segment->active, port->bit, memory_order_acq_rel);

  // Nudge every existing peer so it observes the new peer and emits its count.
  bare_broadcast_channel__wake_peers(port);

  err = uv_async_send(&port->signals.flush);
  assert(err == 0);

  js_value_t *result;
  err = js_create_uint32(env, id, &result);
  assert(err == 0);

  return result;
}

static js_value_t *
bare_broadcast_channel_port_wait_drain(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = bare_broadcast_channel__port(channel, id);

  uv_mutex_lock(&port->locks.drain);

  while (true) {
    bool any_peers = false;
    bool any_full = false;

    bare_broadcast_channel_peers_t peers = bare_broadcast_channel__peers(channel, id);

    for (bare_broadcast_channel_port_t *peer; (peer = bare_broadcast_channel__peers_next(&peers));) {
      any_peers = true;

      if (bare_broadcast_channel__peek_write(peer) == NULL) {
        any_full = true;
        break;
      }
    }

    if (!any_peers) break;
    if (!any_full) break;

    uv_cond_wait(&port->conditions.drain, &port->locks.drain);
  }

  uv_mutex_unlock(&port->locks.drain);

  return NULL;
}

static js_value_t *
bare_broadcast_channel_port_wait_flush(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 3);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  bool had_peers;
  err = js_get_value_bool(env, argv[2], &had_peers);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = bare_broadcast_channel__port(channel, id);

  uv_mutex_lock(&port->locks.flush);

  while (bare_broadcast_channel__peek_read(port) == NULL) {
    if (port->state.ending) break;

    uint32_t peers = bare_broadcast_channel__count_peers(channel, id);

    if (peers > 0) had_peers = true;
    else if (had_peers) break;

    uv_cond_wait(&port->conditions.flush, &port->locks.flush);
  }

  uv_mutex_unlock(&port->locks.flush);

  return NULL;
}

static js_value_t *
bare_broadcast_channel_port_read(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = bare_broadcast_channel__port(channel, id);

  bare_broadcast_channel_message_t *message = bare_broadcast_channel__peek_read(port);

  js_value_t *result;

  if (message) {
    bare_broadcast_channel_payload_t *payload = message->payload;

    err = js_create_sharedarraybuffer_with_backing_store(env, payload->backing_store, NULL, NULL, &result);
    assert(err == 0);

    bare_broadcast_channel__release_payload(env, payload);

    bare_broadcast_channel__push_read(port);
  } else {
    err = js_get_null(env, &result);
    assert(err == 0);
  }

  return result;
}

static js_value_t *
bare_broadcast_channel_port_write(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 3;
  js_value_t *argv[3];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 3);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  uint32_t self_segment = id / BARE_BROADCAST_CHANNEL_SEGMENT_SIZE;
  uint64_t self_bit = 1ull << (id & BARE_BROADCAST_CHANNEL_SEGMENT_MASK);

  js_value_t *result;

  // Snapshot the active word of every published segment. The snapshot freezes
  // the set of peers we will operate on so the lock, recheck, and unlock phases
  // below all agree on membership even as ports join or leave concurrently.
  uint32_t segments = atomic_load_explicit(&channel->segment_count, memory_order_acquire);

  uint64_t snapshot[BARE_BROADCAST_CHANNEL_MAX_SEGMENTS];
  uint32_t peers = 0;

  for (uint32_t s = 0; s < segments; s++) {
    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    uint64_t active = atomic_load_explicit(&segment->active, memory_order_acquire);

    if (s == self_segment) active &= ~self_bit;

    snapshot[s] = active;
    peers += __builtin_popcountll(active);
  }

  if (peers == 0) {
    err = js_get_boolean(env, true, &result);
    assert(err == 0);

    return result;
  }

  // Phase 1: lock every candidate peer's producer mutex in ascending id order.
  // The consistent order across all writers prevents deadlock.
  for (uint32_t s = 0; s < segments; s++) {
    uint64_t bits = snapshot[s];

    if (bits == 0) continue;

    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    while (bits) {
      int b = __builtin_ctzll(bits);
      bits &= bits - 1;

      uv_mutex_lock(&segment->slots[b]->locks.producer);
    }
  }

  // Phase 2: recheck liveness now that we hold the locks and drop any peer that
  // ended after the snapshot.
  for (uint32_t s = 0; s < segments; s++) {
    uint64_t bits = snapshot[s];

    if (bits == 0) continue;

    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    uint64_t live = bits;

    while (bits) {
      int b = __builtin_ctzll(bits);
      bits &= bits - 1;

      bare_broadcast_channel_port_t *peer = segment->slots[b];

      if (!atomic_load_explicit(&peer->state.active, memory_order_acquire)) {
        uv_mutex_unlock(&peer->locks.producer);
        live &= ~(1ull << b);
        peers--;
      }
    }

    snapshot[s] = live;
  }

  if (peers == 0) {
    err = js_get_boolean(env, true, &result);
    assert(err == 0);

    return result;
  }

  // Phase 3: only commit if every live peer has room, so a message reaches all
  // peers or none and the caller can apply uniform backpressure.
  bool all_have_room = true;

  for (uint32_t s = 0; s < segments && all_have_room; s++) {
    uint64_t bits = snapshot[s];

    if (bits == 0) continue;

    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    while (bits) {
      int b = __builtin_ctzll(bits);
      bits &= bits - 1;

      if (bare_broadcast_channel__peek_write(segment->slots[b]) == NULL) {
        all_have_room = false;
        break;
      }
    }
  }

  if (!all_have_room) {
    for (uint32_t s = 0; s < segments; s++) {
      uint64_t bits = snapshot[s];

      if (bits == 0) continue;

      bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

      while (bits) {
        int b = __builtin_ctzll(bits);
        bits &= bits - 1;

        uv_mutex_unlock(&segment->slots[b]->locks.producer);
      }
    }

    err = js_get_boolean(env, false, &result);
    assert(err == 0);

    return result;
  }

  // Phase 4: publish the payload to every live peer and release its lock.
  bare_broadcast_channel_payload_t *payload = malloc(sizeof(bare_broadcast_channel_payload_t));
  atomic_init(&payload->refcount, peers);

  err = js_get_sharedarraybuffer_backing_store(env, argv[2], &payload->backing_store);
  assert(err == 0);

  for (uint32_t s = 0; s < segments; s++) {
    uint64_t bits = snapshot[s];

    if (bits == 0) continue;

    bare_broadcast_channel_segment_t *segment = bare_broadcast_channel__segment(channel, s);

    while (bits) {
      int b = __builtin_ctzll(bits);
      bits &= bits - 1;

      bare_broadcast_channel_port_t *peer = segment->slots[b];

      bare_broadcast_channel_message_t *message = bare_broadcast_channel__peek_write(peer);

      message->payload = payload;

      bare_broadcast_channel__push_write(peer);

      uv_mutex_unlock(&peer->locks.producer);
    }
  }

  err = js_get_boolean(env, true, &result);
  assert(err == 0);

  return result;
}

static js_value_t *
bare_broadcast_channel_port_end(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = bare_broadcast_channel__port(channel, id);

  atomic_fetch_and_explicit(&port->segment->active, ~port->bit, memory_order_acq_rel);

  bare_broadcast_channel__wake_peers(port);

  port->state.ending = true;

  err = uv_async_send(&port->signals.end);
  assert(err == 0);

  return NULL;
}

static js_value_t *
bare_broadcast_channel_port_ref(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = bare_broadcast_channel__port(channel, id);

#define V(signal) \
  uv_ref((uv_handle_t *) &port->signals.signal);
  V(drain)
  V(flush)
  V(end)
#undef V

  return NULL;
}

static js_value_t *
bare_broadcast_channel_port_unref(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = bare_broadcast_channel__port(channel, id);

#define V(signal) \
  uv_unref((uv_handle_t *) &port->signals.signal);
  V(drain)
  V(flush)
  V(end)
#undef V

  return NULL;
}

static inline void
bare_broadcast_channel__port_close(bare_broadcast_channel_port_t *port) {
  if (port->state.closing) return;

  atomic_fetch_and_explicit(&port->segment->active, ~port->bit, memory_order_acq_rel);

  atomic_store_explicit(&port->state.active, false, memory_order_release);

  uv_mutex_lock(&port->locks.producer);

  while (true) {
    int read = atomic_load_explicit(&port->cursors.read, memory_order_relaxed);
    int write = atomic_load_explicit(&port->cursors.write, memory_order_acquire);

    if (read == write) break;

    bare_broadcast_channel__release_payload(port->env, port->messages[read].payload);

    int next = (read + 1) & port->capacity_mask;
    atomic_store_explicit(&port->cursors.read, next, memory_order_release);
  }

  uv_mutex_unlock(&port->locks.producer);

  port->state.closing = 3;

#define V(signal) \
  uv_close((uv_handle_t *) &port->signals.signal, bare_broadcast_channel__on_close);
  V(drain)
  V(flush)
  V(end)
#undef V
}

static js_value_t *
bare_broadcast_channel_port_peers(js_env_t *env, js_callback_info_t *info) {
  int err;

  size_t argc = 2;
  js_value_t *argv[2];

  err = js_get_callback_info(env, info, &argc, argv, NULL, NULL);
  assert(err == 0);

  assert(argc == 2);

  bare_broadcast_channel_t *channel;
  err = js_get_sharedarraybuffer_info(env, argv[0], (void **) &channel, NULL);
  assert(err == 0);

  unsigned int id;
  err = js_get_value_uint32(env, argv[1], &id);
  assert(err == 0);

  uint32_t peers = bare_broadcast_channel__count_peers(channel, id);

  js_value_t *result;
  err = js_create_uint32(env, peers, &result);
  assert(err == 0);

  return result;
}

static js_value_t *
bare_broadcast_channel_exports(js_env_t *env, js_value_t *exports) {
  int err;

#define V(name, fn) \
  { \
    js_value_t *val; \
    err = js_create_function(env, name, -1, fn, NULL, &val); \
    assert(err == 0); \
    err = js_set_named_property(env, exports, name, val); \
    assert(err == 0); \
  }

  V("channelInit", bare_broadcast_channel_init)

  V("portInit", bare_broadcast_channel_port_init)
  V("portWaitDrain", bare_broadcast_channel_port_wait_drain)
  V("portWaitFlush", bare_broadcast_channel_port_wait_flush)
  V("portRead", bare_broadcast_channel_port_read)
  V("portWrite", bare_broadcast_channel_port_write)
  V("portEnd", bare_broadcast_channel_port_end)
  V("portRef", bare_broadcast_channel_port_ref)
  V("portUnref", bare_broadcast_channel_port_unref)
  V("portPeers", bare_broadcast_channel_port_peers)
#undef V

  js_value_t *max_ports;
  err = js_create_uint32(env, BARE_BROADCAST_CHANNEL_MAX_PORTS, &max_ports);
  assert(err == 0);
  err = js_set_named_property(env, exports, "maxPorts", max_ports);
  assert(err == 0);

  return exports;
}

BARE_MODULE(bare_broadcast_channel, bare_broadcast_channel_exports)
