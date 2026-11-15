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

#pragma once

#include <rocksdb/advanced_cache.h>

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace engine {

class BlockCacheStats : public std::enable_shared_from_this<BlockCacheStats> {
 public:
  using Clock = std::chrono::steady_clock;
  struct Snapshot {
    size_t pinned_usage;
    Clock::time_point sampled_at;
  };

  // Take the first sample after DB Open, never on an INFO request.
  explicit BlockCacheStats(std::shared_ptr<rocksdb::Cache> cache)
      : cache_(std::move(cache)), snapshot_{cache_->GetPinnedUsage(), Clock::now()} {}

  Snapshot GetSnapshot() const {
    std::lock_guard lock(mu_);
    return snapshot_;
  }

  std::function<void()> MakeSamplingTask(Clock::time_point now = Clock::now()) {
    auto reservation = std::make_shared<Reservation>(shared_from_this());
    std::lock_guard lock(mu_);
    if (pending_ || now < snapshot_.sampled_at + std::chrono::seconds(1)) return {};
    pending_ = true;
    reservation->armed = true;
    return [reservation] { reservation->stats->sample(); };
  }

 private:
  // Queue cancellation destroys tasks without executing them. Keep the reservation
  // until the last task copy is destroyed, covering both queued and running work.
  struct Reservation {
    explicit Reservation(std::shared_ptr<BlockCacheStats> stats) : stats(std::move(stats)) {}
    Reservation(const Reservation &) = delete;
    Reservation &operator=(const Reservation &) = delete;
    ~Reservation() {
      if (armed) {
        std::lock_guard lock(stats->mu_);
        stats->pending_ = false;
      }
    }
    std::shared_ptr<BlockCacheStats> stats;
    bool armed = false;
  };

  void sample() {
    // HCC scans the cache. Neither INFO's snapshot mutex nor RocksDB's DB mutex
    // may be held here; the owned cache remains valid even after DB Close/Open.
    auto pinned_usage = cache_->GetPinnedUsage();
    std::lock_guard lock(mu_);
    snapshot_ = {pinned_usage, Clock::now()};
  }

  std::shared_ptr<rocksdb::Cache> cache_;
  mutable std::mutex mu_;
  Snapshot snapshot_;
  bool pending_ = false;
};

}  // namespace engine
