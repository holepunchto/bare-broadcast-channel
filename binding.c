#include <assert.h>
#include <bare.h>
#include <js.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#define BARE_BROADCAST_CHANNEL_MAX_PORTS     32
#define BARE_BROADCAST_CHANNEL_PORT_CAPACITY 1024

typedef struct bare_broadcast_channel_s bare_broadcast_channel_t;
typedef struct bare_broadcast_channel_port_s bare_broadcast_channel_port_t;
typedef struct bare_broadcast_channel_message_s bare_broadcast_channel_message_t;
typedef struct bare_broadcast_channel_payload_s bare_broadcast_channel_payload_t;

struct bare_broadcast_channel_payload_s {
  atomic_int refcount;
  js_arraybuffer_backing_store_t *backing_store;
};

struct bare_broadcast_channel_message_s {
  bare_broadcast_channel_payload_t *payload;
};

struct bare_broadcast_channel_port_s {
  bare_broadcast_channel_t *channel;

  uint8_t id;

  struct {
    atomic_bool active;
    bool ending;
    bool ended;
    uint8_t closing;
    bool exiting;
  } state;

  bare_broadcast_channel_message_t messages[BARE_BROADCAST_CHANNEL_PORT_CAPACITY];

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
};

struct bare_broadcast_channel_s {
  atomic_uint next;
  atomic_uint active_ports;

  bare_broadcast_channel_port_t ports[BARE_BROADCAST_CHANNEL_MAX_PORTS];
};

static inline bare_broadcast_channel_message_t *
bare_broadcast_channel__peek_read(bare_broadcast_channel_port_t *port) {
  int read = atomic_load_explicit(&port->cursors.read, memory_order_relaxed);

  int write = atomic_load_explicit(&port->cursors.write, memory_order_acquire);

  if (read == write) return NULL;

  return &port->messages[read];
}

static inline void
bare_broadcast_channel__push_read(bare_broadcast_channel_port_t *port) {
  int err;

  int read = atomic_load_explicit(&port->cursors.read, memory_order_relaxed);

  int next = (read + 1) & (BARE_BROADCAST_CHANNEL_PORT_CAPACITY - 1);

  atomic_store_explicit(&port->cursors.read, next, memory_order_release);

  uint32_t active = atomic_load_explicit(&port->channel->active_ports, memory_order_acquire);

  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (i == port->id) continue;
    if (!(active & (1u << i))) continue;

    bare_broadcast_channel_port_t *peer = &port->channel->ports[i];

    uv_mutex_lock(&peer->locks.producer);

    if (!atomic_load_explicit(&peer->state.active, memory_order_acquire)) {
      uv_mutex_unlock(&peer->locks.producer);
      continue;
    }

    uv_mutex_lock(&peer->locks.drain);

    uv_cond_signal(&peer->conditions.drain);

    uv_mutex_unlock(&peer->locks.drain);

    err = uv_async_send(&peer->signals.drain);
    assert(err == 0);

    uv_mutex_unlock(&peer->locks.producer);
  }
}

static inline bare_broadcast_channel_message_t *
bare_broadcast_channel__peek_write(bare_broadcast_channel_port_t *port) {
  int write = atomic_load_explicit(&port->cursors.write, memory_order_relaxed);

  int read = atomic_load_explicit(&port->cursors.read, memory_order_acquire);

  int next = (write + 1) & (BARE_BROADCAST_CHANNEL_PORT_CAPACITY - 1);

  if (next == read) return NULL;

  return &port->messages[write];
}

static inline void
bare_broadcast_channel__push_write(bare_broadcast_channel_port_t *port) {
  int err;

  int write = atomic_load_explicit(&port->cursors.write, memory_order_relaxed);

  int next = (write + 1) & (BARE_BROADCAST_CHANNEL_PORT_CAPACITY - 1);

  atomic_store_explicit(&port->cursors.write, next, memory_order_release);

  if (atomic_load_explicit(&port->state.active, memory_order_acquire)) {
    uv_mutex_lock(&port->locks.flush);

    uv_cond_signal(&port->conditions.flush);

    uv_mutex_unlock(&port->locks.flush);

    err = uv_async_send(&port->signals.flush);
    assert(err == 0);
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

static inline void
bare_broadcast_channel__wake_peers(bare_broadcast_channel_port_t *port, uint32_t peers) {
  int err;

  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (!(peers & (1u << i))) continue;

    bare_broadcast_channel_port_t *peer = &port->channel->ports[i];

    uv_mutex_lock(&peer->locks.producer);

    if (!atomic_load_explicit(&peer->state.active, memory_order_acquire)) {
      uv_mutex_unlock(&peer->locks.producer);
      continue;
    }

    uv_mutex_lock(&peer->locks.flush);
    uv_cond_signal(&peer->conditions.flush);
    uv_mutex_unlock(&peer->locks.flush);

    err = uv_async_send(&peer->signals.flush);
    assert(err == 0);

    uv_mutex_lock(&peer->locks.drain);
    uv_cond_signal(&peer->conditions.drain);
    uv_mutex_unlock(&peer->locks.drain);

    err = uv_async_send(&peer->signals.drain);
    assert(err == 0);

    uv_mutex_unlock(&peer->locks.producer);
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
      uint32_t self_bit = 1u << port->id;
      uint32_t before = atomic_fetch_and_explicit(&port->channel->active_ports, ~self_bit, memory_order_acq_rel);
      uint32_t others = before & ~self_bit;

      bare_broadcast_channel__wake_peers(port, others);

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

static js_value_t *
bare_broadcast_channel_init(js_env_t *env, js_callback_info_t *info) {
  int err;

  js_value_t *handle;

  bare_broadcast_channel_t *channel;
  err = js_create_sharedarraybuffer(env, sizeof(bare_broadcast_channel_t), (void **) &channel, &handle);
  assert(err == 0);

  atomic_init(&channel->next, 0);
  atomic_init(&channel->active_ports, 0);

  for (uint8_t id = 0; id < BARE_BROADCAST_CHANNEL_MAX_PORTS; id++) {
    bare_broadcast_channel_port_t *port = &channel->ports[id];

    port->id = id;
    port->channel = channel;

    memset(&port->state, 0, sizeof(port->state));

    atomic_init(&port->state.active, false);

    atomic_init(&port->cursors.read, 0);
    atomic_init(&port->cursors.write, 0);
  }

  return handle;
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

  unsigned int id = atomic_fetch_add_explicit(&channel->next, 1, memory_order_acq_rel);

  if (id >= BARE_BROADCAST_CHANNEL_MAX_PORTS) {
    err = js_throw_error(env, NULL, "Channel has reached the maximum number of connected ports");
    assert(err == 0);

    return NULL;
  }

  uv_loop_t *loop;
  err = js_get_env_loop(env, &loop);
  assert(err == 0);

  bare_broadcast_channel_port_t *port = &channel->ports[id];

  port->env = env;

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

#define V(signal) \
  err = uv_async_init(loop, &port->signals.signal, bare_broadcast_channel__on_##signal); \
  assert(err == 0); \
  port->signals.signal.data = (void *) port;
  V(drain)
  V(flush)
  V(end)
#undef V

  atomic_store_explicit(&port->state.active, true, memory_order_release);

  uint32_t prev = atomic_fetch_or_explicit(&channel->active_ports, 1u << id, memory_order_acq_rel);

  uint32_t others = prev & ~(1u << id);

  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (!(others & (1u << i))) continue;

    bare_broadcast_channel_port_t *peer = &channel->ports[i];

    uv_mutex_lock(&peer->locks.producer);

    if (atomic_load_explicit(&peer->state.active, memory_order_acquire)) {
      uv_mutex_lock(&peer->locks.flush);
      uv_cond_signal(&peer->conditions.flush);
      uv_mutex_unlock(&peer->locks.flush);

      err = uv_async_send(&peer->signals.flush);
      assert(err == 0);
    }

    uv_mutex_unlock(&peer->locks.producer);
  }

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

  bare_broadcast_channel_port_t *port = &channel->ports[id];

  uv_mutex_lock(&port->locks.drain);

  while (true) {
    uint32_t active = atomic_load_explicit(&channel->active_ports, memory_order_acquire);
    uint32_t peers = active & ~(1u << id);

    if (peers == 0) break;

    bool any_full = false;
    for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
      if (!(peers & (1u << i))) continue;
      if (bare_broadcast_channel__peek_write(&channel->ports[i]) == NULL) {
        any_full = true;
        break;
      }
    }

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

  bare_broadcast_channel_port_t *port = &channel->ports[id];

  uv_mutex_lock(&port->locks.flush);

  while (bare_broadcast_channel__peek_read(port) == NULL) {
    if (port->state.ending) break;

    uint32_t active = atomic_load_explicit(&port->channel->active_ports, memory_order_acquire);
    uint32_t peers = active & ~(1u << id);

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

  bare_broadcast_channel_port_t *port = &channel->ports[id];

  bare_broadcast_channel_message_t *message = bare_broadcast_channel__peek_read(port);

  js_value_t *result;

  if (message) {
    bare_broadcast_channel_payload_t *payload = message->payload;

    err = js_create_sharedarraybuffer_with_backing_store(env, payload->backing_store, NULL, NULL, &result);
    assert(err == 0);

    if (atomic_fetch_sub_explicit(&payload->refcount, 1, memory_order_acq_rel) == 1) {
      err = js_release_arraybuffer_backing_store(env, payload->backing_store);
      assert(err == 0);

      free(payload);
    }

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

  uint32_t snapshot = atomic_load_explicit(&channel->active_ports, memory_order_acquire);
  uint32_t initial_peers = snapshot & ~(1u << id);

  js_value_t *result;

  if (initial_peers == 0) {
    err = js_get_boolean(env, true, &result);
    assert(err == 0);

    return result;
  }

  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (!(initial_peers & (1u << i))) continue;
    uv_mutex_lock(&channel->ports[i].locks.producer);
  }

  uint32_t current = atomic_load_explicit(&channel->active_ports, memory_order_acquire);
  uint32_t peers = initial_peers & current;
  uint32_t removed = initial_peers & ~peers;

  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (!(removed & (1u << i))) continue;
    uv_mutex_unlock(&channel->ports[i].locks.producer);
  }

  if (peers == 0) {
    err = js_get_boolean(env, true, &result);
    assert(err == 0);

    return result;
  }

  bool all_have_room = true;
  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (!(peers & (1u << i))) continue;
    if (bare_broadcast_channel__peek_write(&channel->ports[i]) == NULL) {
      all_have_room = false;
      break;
    }
  }

  if (!all_have_room) {
    for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
      if (!(peers & (1u << i))) continue;
      uv_mutex_unlock(&channel->ports[i].locks.producer);
    }

    err = js_get_boolean(env, false, &result);
    assert(err == 0);

    return result;
  }

  int num_peers = __builtin_popcount(peers);

  bare_broadcast_channel_payload_t *payload = malloc(sizeof(bare_broadcast_channel_payload_t));
  atomic_init(&payload->refcount, num_peers);

  err = js_get_sharedarraybuffer_backing_store(env, argv[2], &payload->backing_store);
  assert(err == 0);

  for (int i = 0; i < BARE_BROADCAST_CHANNEL_MAX_PORTS; i++) {
    if (!(peers & (1u << i))) continue;

    bare_broadcast_channel_port_t *peer = &channel->ports[i];

    bare_broadcast_channel_message_t *message = bare_broadcast_channel__peek_write(peer);

    message->payload = payload;

    bare_broadcast_channel__push_write(peer);

    uv_mutex_unlock(&peer->locks.producer);
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

  bare_broadcast_channel_port_t *port = &channel->ports[id];

  uint32_t self_bit = 1u << id;

  uint32_t before = atomic_fetch_and_explicit(&channel->active_ports, ~self_bit, memory_order_acq_rel);

  uint32_t others = before & ~self_bit;

  bare_broadcast_channel__wake_peers(port, others);

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

  bare_broadcast_channel_port_t *port = &channel->ports[id];

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

  bare_broadcast_channel_port_t *port = &channel->ports[id];

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

  atomic_fetch_and_explicit(&port->channel->active_ports, ~(1u << port->id), memory_order_acq_rel);

  atomic_store_explicit(&port->state.active, false, memory_order_release);

  uv_mutex_lock(&port->locks.producer);

  while (true) {
    int read = atomic_load_explicit(&port->cursors.read, memory_order_relaxed);
    int write = atomic_load_explicit(&port->cursors.write, memory_order_acquire);

    if (read == write) break;

    bare_broadcast_channel__release_payload(port->env, port->messages[read].payload);

    int next = (read + 1) & (BARE_BROADCAST_CHANNEL_PORT_CAPACITY - 1);
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

  uint32_t active = atomic_load_explicit(&channel->active_ports, memory_order_acquire);
  uint32_t peers = active & ~(1u << id);

  js_value_t *result;
  err = js_create_uint32(env, __builtin_popcount(peers), &result);
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
