/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <cstdlib>
#include <memory>
#include <string_view>
#include <utility>

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event2/event.h"
#include "event2/listener.h"
#include "type_util.h"

#ifdef KVROCKS_USE_LIBUV
#include <uv.h>
#endif

using StaticFree = StaticFunction<decltype(std::free), std::free>;

template <typename T>
struct UniqueFreePtr : std::unique_ptr<T, StaticFree> {
  using BaseType = std::unique_ptr<T, StaticFree>;

  using BaseType::BaseType;
};

struct UniqueEvbufReadln : UniqueFreePtr<char[]> {
  UniqueEvbufReadln(evbuffer *buffer, evbuffer_eol_style eol_style)
      : UniqueFreePtr(evbuffer_readln(buffer, &length, eol_style)) {}

  size_t length;

  std::string_view View() { return {get(), length}; }
};

using StaticEvbufFree = StaticFunction<decltype(evbuffer_free), evbuffer_free>;

struct UniqueEvbuf : std::unique_ptr<evbuffer, StaticEvbufFree> {
  using BaseType = std::unique_ptr<evbuffer, StaticEvbufFree>;

  UniqueEvbuf() : BaseType(evbuffer_new()) {}
  explicit UniqueEvbuf(evbuffer *buffer) : BaseType(buffer) {}
};

#ifdef KVROCKS_USE_LIBUV

// ---- libuv variants (Phase 1: only timers and listeners are backed by uv;
//      bufferevent / evbuffer are still libevent and will be replaced in Phase 2) ----

// RAII wrapper around a libuv handle. `Close` is invoked in the destructor
// so that loop cleanup doesn't race with handle destruction. Handles are
// allocated on the heap so `unique_ptr<..., Deleter>` semantics still work.
template <typename UVHandle>
struct UvHandleDeleter {
  void operator()(UVHandle *h) const {
    if (h == nullptr) return;
    if (!uv_is_closing(reinterpret_cast<uv_handle_t *>(h))) {
      uv_close(reinterpret_cast<uv_handle_t *>(h), nullptr);
    }
    // Note: we intentionally do NOT free `h` here — `uv_close` only marks
    // the handle for closing; the actual memory release happens in the
    // close callback when one is provided. Since we pass nullptr, callers
    // must ensure the handle's lifetime outlives the loop iteration that
    // drains the close callbacks. In practice the Worker destructor runs
    // after the loop stops, so this is safe for Phase 1.
  }
};

// UniqueEvent: wraps a single-shot uv_timer_t used as an I/O-less event
// (the libevent equivalent of event_new(base, -1, EV_PERSIST, ...)).
// The same name keeps Worker & friends source-compatible between backends.
using UniqueEvent = std::unique_ptr<uv_timer_t, UvHandleDeleter<uv_timer_t>>;

// Static-function deleter used by EvconnlistenerBase in the libuv branch.
// Accepts an owned uv_tcp_t created via `new uv_tcp_t` and closes/frees it.
struct UvTcpListenerDeleter {
  void operator()(uv_tcp_t *h) const {
    if (h == nullptr) return;
    if (!uv_is_closing(reinterpret_cast<uv_handle_t *>(h))) {
      uv_close(
          reinterpret_cast<uv_handle_t *>(h), +[](uv_handle_t *handle) { delete static_cast<uv_tcp_t *>(handle); });
      // Unlike UvHandleDeleter, here we hand ownership back in the close
      // callback so the heap allocation is released when the loop drains
      // close handles. That avoids leaking on shutdown.
    } else {
      delete h;
    }
  }
};

// EvbufCallbackBase is untouched in Phase 1 — bufferevent remains libevent.
template <typename Derived, bool ReadCB = true, bool WriteCB = true, bool EventCB = true>
struct EvbufCallbackBase {
 private:
  static void readCB(bufferevent *bev, void *ctx) { static_cast<Derived *>(ctx)->OnRead(bev); }

  static void writeCB(bufferevent *bev, void *ctx) { static_cast<Derived *>(ctx)->OnWrite(bev); }

  static void eventCB(bufferevent *bev, short what, void *ctx) { static_cast<Derived *>(ctx)->OnEvent(bev, what); }

  static auto getReadCB() {
    if constexpr (ReadCB) {
      return readCB;
    } else {
      return nullptr;
    }
  }
  static auto getWriteCB() {
    if constexpr (WriteCB) {
      return writeCB;
    } else {
      return nullptr;
    }
  }

  static auto getEventCB() {
    if constexpr (EventCB) {
      return eventCB;
    } else {
      return nullptr;
    }
  }

 public:
  void SetCB(bufferevent *bev) {
    bufferevent_setcb(bev, getReadCB(), getWriteCB(), getEventCB(), reinterpret_cast<void *>(this));
  }
};

// libuv-backed EventCallbackBase.
// NewEvent / NewTimer both return a heap-allocated uv_timer_t so callers can
// store the result in UniqueEvent. The same RAII object drives Start/Stop.
template <typename Derived>
struct EventCallbackBase {
 private:
  static void timerCB(uv_timer_t *handle) { static_cast<Derived *>(handle->data)->TimerCB(0, 0); }

 public:
  uv_timer_t *NewEvent(uv_loop_t *loop, [[maybe_unused]] evutil_socket_t fd, [[maybe_unused]] short events) {
    auto *t = new uv_timer_t();
    t->data = static_cast<Derived *>(this);
    uv_timer_init(loop, t);
    return t;
  }

  uv_timer_t *NewTimer(uv_loop_t *loop) {
    auto *t = new uv_timer_t();
    t->data = static_cast<Derived *>(this);
    uv_timer_init(loop, t);
    return t;
  }
};

// libuv-backed EvconnlistenerBase.
// NewEvconnlistener returns a heap-allocated uv_tcp_t already bound and
// listening. Accept callbacks drive newTCPConnection / newUnixSocketConnection
// with a plain fd, matching the libevent branch's signature so Worker.cc
// only needs to switch on the backend when actually running the loop.
template <typename Derived>
struct EvconnlistenerBase {
 private:
  // acceptCB is invoked by libuv when a new connection arrives on the
  // listening socket. Instead of using uv_accept (which would hand us a
  // new uv_tcp_t whose fd gets closed by uv_close — incompatible with the
  // still-libevent bufferevent path), we perform a direct POSIX accept on
  // the listening fd that uv_tcp_t wraps. The freshly accepted fd is then
  // passed up to Derived::HandleNewConnection() where Worker routes it
  // to newTCPConnection() / newUnixSocketConnection() unchanged.
  static void acceptCB(uv_stream_t *server, int status) {
    if (status != 0) return;
    int server_fd = -1;
    uv_fileno(reinterpret_cast<uv_handle_t *>(server), &server_fd);
    if (server_fd < 0) return;

    sockaddr_storage addr{};
    socklen_t addrlen = sizeof(addr);
    int fd = ::accept(server_fd, reinterpret_cast<sockaddr *>(&addr), &addrlen);
    if (fd < 0) return;

    // `server->data` carries an intptr_t tag (1=TCP, 2=UNIX) set by Worker
    // right after NewEvconnlistener returns. `loop->data` still points to
    // the Worker itself so we can reach it without extra lookups.
    auto *self = static_cast<Derived *>(server->loop->data);
    int tag = static_cast<int>(reinterpret_cast<intptr_t>(server->data));
    self->HandleNewConnection(fd, tag);
  }

 public:
  template <void (Derived::*cb)(evconnlistener *, evutil_socket_t, sockaddr *, int)>
  uv_tcp_t *NewEvconnlistener(uv_loop_t *loop, unsigned flags, int backlog, evutil_socket_t fd) {
    auto *tcp = new uv_tcp_t();
    uv_tcp_init(loop, tcp);
    // Stash the Worker on loop->data so acceptCB above can reach it. One
    // loop per Worker means no multi-callback races here.
    loop->data = static_cast<Derived *>(this);
    // Per-handle tag is filled in by the caller after NewEvconnlistener
    // returns (see Worker::listenFD / listenTCP / ListenUnixSocket).
    tcp->data = nullptr;

    if (uv_tcp_open(tcp, fd) != 0) {
      uv_close(reinterpret_cast<uv_handle_t *>(tcp), +[](uv_handle_t *h) { delete h; });
      return nullptr;
    }
    // fd ownership transfers to tcp. When we later uv_close(tcp) the
    // underlying listener fd will be closed automatically.

    if (uv_listen(reinterpret_cast<uv_stream_t *>(tcp), backlog, &acceptCB) != 0) {
      uv_close(reinterpret_cast<uv_handle_t *>(tcp), +[](uv_handle_t *h) { delete h; });
      return nullptr;
    }
    (void)flags;  // LEV_OPT_CLOSE_ON_FREE is what we get automatically;
                  // LEV_OPT_THREADSAFE is unnecessary (single loop per thread).
    return tcp;
  }
};

#else  // KVROCKS_USE_LIBUV

using StaticEventFree = StaticFunction<decltype(event_free), event_free>;

struct UniqueEvent : std::unique_ptr<event, StaticEventFree> {
  using BaseType = std::unique_ptr<event, StaticEventFree>;

  UniqueEvent() : BaseType(nullptr) {}
  explicit UniqueEvent(event *buffer) : BaseType(buffer) {}
};

template <typename Derived, bool ReadCB = true, bool WriteCB = true, bool EventCB = true>
struct EvbufCallbackBase {
 private:
  static void readCB(bufferevent *bev, void *ctx) { static_cast<Derived *>(ctx)->OnRead(bev); }

  static void writeCB(bufferevent *bev, void *ctx) { static_cast<Derived *>(ctx)->OnWrite(bev); }

  static void eventCB(bufferevent *bev, short what, void *ctx) { static_cast<Derived *>(ctx)->OnEvent(bev, what); }

  static auto getReadCB() {
    if constexpr (ReadCB) {
      return readCB;
    } else {
      return nullptr;
    }
  }
  static auto getWriteCB() {
    if constexpr (WriteCB) {
      return writeCB;
    } else {
      return nullptr;
    }
  }

  static auto getEventCB() {
    if constexpr (EventCB) {
      return eventCB;
    } else {
      return nullptr;
    }
  }

 public:
  void SetCB(bufferevent *bev) {
    bufferevent_setcb(bev, getReadCB(), getWriteCB(), getEventCB(), reinterpret_cast<void *>(this));
  }
};

template <typename Derived>
struct EventCallbackBase {
 private:
  static void timerCB(evutil_socket_t fd, short events, void *ctx) { static_cast<Derived *>(ctx)->TimerCB(fd, events); }

 public:
  event *NewEvent(event_base *base, evutil_socket_t fd, short events) {
    return event_new(base, fd, events, timerCB, reinterpret_cast<void *>(this));
  }

  event *NewTimer(event_base *base) { return evtimer_new(base, timerCB, reinterpret_cast<void *>(this)); }
};

template <typename Derived>
struct EvconnlistenerBase {
 private:
  template <void (Derived::*cb)(evconnlistener *, evutil_socket_t, sockaddr *, int)>
  static void callback(evconnlistener *listener, evutil_socket_t fd, sockaddr *address, int socklen, void *ctx) {
    return (reinterpret_cast<Derived *>(ctx)->*cb)(listener, fd, address, socklen);
  }

 public:
  template <void (Derived::*cb)(evconnlistener *, evutil_socket_t, sockaddr *, int)>
  evconnlistener *NewEvconnlistener(event_base *base, unsigned flags, int backlog, evutil_socket_t fd) {
    return evconnlistener_new(base, callback<cb>, this, flags, backlog, fd);
  }
};

#endif  // KVROCKS_USE_LIBUV

// libuv branch needs a dispatcher because the single acceptCB above shares
// the loop->data with Worker, but we still need to route to the right
// newTCPConnection / newUnixSocketConnection. Provide a default in the
// libevent branch so Worker.h doesn't have to be #ifdef-heavy.
#ifdef KVROCKS_USE_LIBUV
// Declared here; Worker implements it as non-virtual helper resolved via
// CRTP (EvconnlistenerBase<Worker>::acceptCB calls this).
#else
#endif

namespace details {

template <auto F, typename>
struct EventCallbackImpl;

template <auto F, typename T, typename R, typename... Args>
struct EventCallbackImpl<F, R (T::*)(Args...)> {
  static R Func(Args... args, void *ctx) { return (reinterpret_cast<T *>(ctx)->*F)(args...); }
};

}  // namespace details

// convert member functions to eventbuffer callbacks
// e.g. for member function `void A::f(int x)` from class A
// EventCallback<&A::f> generate a function
// void EventCallback<&A::f>::Func(int x, void *ctx)
// and put `this` pointer of A to `void *ctx`
template <auto F>
struct EventCallback : details::EventCallbackImpl<F, decltype(F)> {};

template <auto F>
constexpr auto EventCallbackFunc = EventCallback<F>::Func;
