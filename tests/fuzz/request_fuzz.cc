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
 */

// Build and run this target with Clang and libFuzzer:
//   cmake -S . -B build-fuzz -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++
//     -DENABLE_FUZZ=ON -DENABLE_ASAN=ON -DDISABLE_JEMALLOC=ON
//   cmake --build build-fuzz --target fuzz_request --parallel 2
// Put valid RESP examples in seed-corpus, then run:
//   ./build-fuzz/fuzz_request seed-corpus -max_total_time=60

#include <event2/buffer.h>
#include <spdlog/spdlog.h>

#include <cstddef>
#include <cstdint>
#include <memory>

#include "config/config.h"
#include "server/redis_request.h"
#include "server/server.h"

namespace {

struct EvbufferDeleter {
  void operator()(evbuffer *buffer) const { evbuffer_free(buffer); }
};

struct Env {
  Config config;
  std::unique_ptr<Server> server;

  Env() {
    spdlog::set_level(spdlog::level::off);
    config.workers = 0;
    // Tokenize only uses Server statistics and configuration, so storage is not needed.
    server = std::make_unique<Server>(nullptr, &config);
  }
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  static Env env;
  if (size == 0) return 0;

  // Use the first byte to select a fragmentation point without removing it from the request.
  size_t cut = data[0] % size;

  redis::Request request(env.server.get());
  std::unique_ptr<evbuffer, EvbufferDeleter> buffer(evbuffer_new());
  if (!buffer) return 0;

  if (evbuffer_add(buffer.get(), data, cut) != 0) return 0;
  if (auto status = request.Tokenize(buffer.get()); !status.IsOK()) return 0;

  if (evbuffer_add(buffer.get(), data + cut, size - cut) != 0) return 0;
  (void)request.Tokenize(buffer.get());
  return 0;
}
