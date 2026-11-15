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

#include "storage/block_cache_stats.h"

#include <gtest/gtest.h>
#include <rocksdb/cache.h>

#include <atomic>
#include <chrono>
#include <fstream>
#include <future>
#include <memory>
#include <thread>
#include <vector>

#include "common/scope_exit.h"
#include "common/task_runner.h"
#include "test_base.h"
#include "thread_util.h"

using namespace std::chrono_literals;

namespace {
class CountingCache : public rocksdb::CacheWrapper {
 public:
  CountingCache() : CacheWrapper(rocksdb::NewLRUCache(1 << 20)), resume_(resume.get_future().share()) {}
  const char *Name() const override { return "CountingCache"; }
  size_t GetPinnedUsage() const override {
    ++calls;
    if (block.exchange(false)) {
      entered.set_value();
      resume_.wait();
    }
    return value.load();
  }
  mutable std::atomic<size_t> calls{0};
  mutable std::atomic<bool> block{false};
  std::atomic<size_t> value{64};
  mutable std::promise<void> entered;
  std::promise<void> resume;

 private:
  std::shared_future<void> resume_;
};

constexpr auto kDue = engine::BlockCacheStats::Clock::time_point::max();
volatile sig_atomic_t received_int = 0;
volatile sig_atomic_t received_term = 0;

void ShutdownSignalHandler(int signal) {
  if (signal == SIGINT) received_int = 1;
  if (signal == SIGTERM) received_term = 1;
}
}  // namespace

TEST(BlockCacheStats, ShutdownSignalsAreDeferredUntilTheGateIsReleased) {
  struct sigaction action {
  }, old_int{}, old_term{};
  action.sa_handler = ShutdownSignalHandler;
  sigemptyset(&action.sa_mask);
  ASSERT_EQ(sigaction(SIGINT, &action, &old_int), 0);
  ASSERT_EQ(sigaction(SIGTERM, &action, &old_term), 0);
  auto restore = MakeScopeExit([&] {
    sigaction(SIGINT, &old_int, nullptr);
    sigaction(SIGTERM, &old_term, nullptr);
  });
  received_int = 0;
  received_term = 0;
  sigset_t before{}, after{};
  ASSERT_EQ(pthread_sigmask(SIG_BLOCK, nullptr, &before), 0);
  ASSERT_EQ(sigismember(&before, SIGINT), 0);
  ASSERT_EQ(sigismember(&before, SIGTERM), 0);
  {
    util::ScopedShutdownSignalBlocker signal_blocker;
    ASSERT_EQ(signal_blocker.GetError(), 0);
    std::mutex gate;
    std::lock_guard lock(gate);
    ASSERT_EQ(raise(SIGINT), 0);
    ASSERT_EQ(raise(SIGTERM), 0);
    EXPECT_EQ(received_int, 0);
    EXPECT_EQ(received_term, 0);
  }
  EXPECT_EQ(received_int, 1);
  EXPECT_EQ(received_term, 1);
  ASSERT_EQ(pthread_sigmask(SIG_BLOCK, nullptr, &after), 0);
  EXPECT_EQ(sigismember(&before, SIGINT), sigismember(&after, SIGINT));
  EXPECT_EQ(sigismember(&before, SIGTERM), sigismember(&after, SIGTERM));
}

TEST(BlockCacheStats, ConcurrentSamplingAndReads) {
  auto cache = std::make_shared<CountingCache>();
  auto stats = std::make_shared<engine::BlockCacheStats>(cache);
  EXPECT_EQ(cache->calls, 1);
  auto old = stats->GetSnapshot();

  std::vector<std::function<void()>> tasks(16);
  std::vector<std::thread> schedulers;
  schedulers.reserve(tasks.size());
  for (size_t i = 0; i < tasks.size(); ++i) {
    schedulers.emplace_back([&, i] { tasks[i] = stats->MakeSamplingTask(kDue); });
  }
  for (auto &thread : schedulers) thread.join();
  size_t scheduled = 0;
  std::function<void()> task;
  for (auto &candidate : tasks) {
    if (candidate) {
      ++scheduled;
      task = std::move(candidate);
    }
  }
  ASSERT_EQ(scheduled, 1);
  tasks.clear();
  cache->block = true;
  cache->value = 128;
  auto entered = cache->entered.get_future();
  auto sampling = std::async(std::launch::async, [task = std::move(task)] { task(); });
  task = {};
  auto cleanup = MakeScopeExit([&] {
    cache->resume.set_value();
    sampling.wait();
  });
  ASSERT_EQ(entered.wait_for(5s), std::future_status::ready);

  auto readers = std::async(std::launch::async, [&] {
    std::vector<std::thread> threads;
    threads.reserve(8);
    std::atomic<bool> consistent{true};
    for (int i = 0; i < 8; ++i) {
      threads.emplace_back([&] {
        for (int j = 0; j < 1000; ++j) {
          auto snapshot = stats->GetSnapshot();
          if (snapshot.pinned_usage != old.pinned_usage || snapshot.sampled_at != old.sampled_at ||
              stats->MakeSamplingTask(kDue)) {
            consistent = false;
          }
        }
      });
    }
    for (auto &thread : threads) thread.join();
    return consistent.load();
  });
  auto ready = readers.wait_for(5s);
  EXPECT_EQ(ready, std::future_status::ready);
  cache->resume.set_value();
  sampling.get();
  cleanup.Disable();
  EXPECT_TRUE(readers.get());
  EXPECT_EQ(cache->calls, 2);
  auto updated = stats->GetSnapshot();
  EXPECT_EQ(updated.pinned_usage, 128);
  EXPECT_GE(updated.sampled_at, old.sampled_at);
  EXPECT_FALSE(stats->MakeSamplingTask(updated.sampled_at + 999ms));
  EXPECT_TRUE(stats->MakeSamplingTask(updated.sampled_at + 1s));
}

TEST(BlockCacheStats, NestedSignalBlockersPreserveTheIncomingMask) {
  sigset_t original{}, signals{}, current{};
  sigemptyset(&signals);
  sigaddset(&signals, SIGINT);
  ASSERT_EQ(pthread_sigmask(SIG_BLOCK, &signals, &original), 0);
  auto restore = MakeScopeExit([&] { pthread_sigmask(SIG_SETMASK, &original, nullptr); });
  {
    util::ScopedShutdownSignalBlocker outer;
    ASSERT_EQ(outer.GetError(), 0);
    {
      util::ScopedShutdownSignalBlocker inner;
      ASSERT_EQ(inner.GetError(), 0);
    }
    ASSERT_EQ(pthread_sigmask(SIG_BLOCK, nullptr, &current), 0);
    EXPECT_EQ(sigismember(&current, SIGINT), 1);
    EXPECT_EQ(sigismember(&current, SIGTERM), 1);
  }
  ASSERT_EQ(pthread_sigmask(SIG_BLOCK, nullptr, &current), 0);
  EXPECT_EQ(sigismember(&current, SIGINT), 1);
  EXPECT_EQ(sigismember(&current, SIGTERM), sigismember(&original, SIGTERM));
}

TEST(BlockCacheStats, TaskOwnsTheCacheGeneration) {
  auto cache = std::make_shared<CountingCache>();
  auto stats = std::make_shared<engine::BlockCacheStats>(cache);
  std::weak_ptr<CountingCache> weak_cache = cache;
  std::weak_ptr<engine::BlockCacheStats> weak_stats = stats;
  auto task = stats->MakeSamplingTask(kDue);
  ASSERT_TRUE(task);
  stats.reset();
  cache.reset();
  EXPECT_FALSE(weak_cache.expired());
  EXPECT_FALSE(weak_stats.expired());
  task();
  task = {};
  EXPECT_TRUE(weak_cache.expired());
  EXPECT_TRUE(weak_stats.expired());
}

TEST(BlockCacheStats, DroppedAndRejectedTasksReleaseReservation) {
  auto cache = std::make_shared<CountingCache>();
  auto stats = std::make_shared<engine::BlockCacheStats>(cache);
  auto old = stats->GetSnapshot();
  {
    auto task = stats->MakeSamplingTask(kDue);
    auto copy = task;
    task = {};
    EXPECT_FALSE(stats->MakeSamplingTask(kDue));
  }
  EXPECT_TRUE(stats->MakeSamplingTask(kDue));

  TaskRunner runner(1, 1);
  ASSERT_TRUE(runner.TryPublish([] {}).IsOK());
  {
    auto task = stats->MakeSamplingTask(kDue);
    ASSERT_TRUE(task);
    EXPECT_FALSE(runner.TryPublish(std::move(task)).IsOK());
  }
  EXPECT_TRUE(stats->MakeSamplingTask(kDue));
  EXPECT_EQ(stats->GetSnapshot().sampled_at, old.sampled_at);
  EXPECT_EQ(cache->calls, 1);
}

TEST(BlockCacheStats, QueueCancellationAndRestart) {
  auto cache = std::make_shared<CountingCache>();
  auto stats = std::make_shared<engine::BlockCacheStats>(cache);
  TaskRunner runner;
  std::promise<void> entered, resume;
  bool released = false;
  auto release = resume.get_future().share();
  ASSERT_TRUE(runner
                  .TryPublish([&] {
                    entered.set_value();
                    release.wait();
                  })
                  .IsOK());
  ASSERT_TRUE(runner.Start().IsOK());
  auto cleanup = MakeScopeExit([&] {
    if (!released) resume.set_value();
    runner.Cancel();
    (void)runner.Join();
  });
  ASSERT_EQ(entered.get_future().wait_for(5s), std::future_status::ready);
  ASSERT_TRUE(runner.TryPublish(stats->MakeSamplingTask(kDue)).IsOK());
  runner.Cancel();
  resume.set_value();
  released = true;
  ASSERT_TRUE(runner.Join().IsOK());
  cleanup.Disable();
  EXPECT_EQ(cache->calls, 1);

  std::promise<void> done;
  auto task = stats->MakeSamplingTask(kDue);
  ASSERT_TRUE(task);
  ASSERT_TRUE(runner
                  .TryPublish([task = std::move(task), &done] {
                    task();
                    done.set_value();
                  })
                  .IsOK());
  ASSERT_TRUE(runner.Start().IsOK());
  auto stop = MakeScopeExit([&] {
    runner.Cancel();
    (void)runner.Join();
  });
  ASSERT_EQ(done.get_future().wait_for(5s), std::future_status::ready);
  EXPECT_EQ(cache->calls, 2);
}

TEST(BlockCacheStats, RunningTaskCancellationRetainsCacheAndReleasesReservation) {
  auto cache = std::make_shared<CountingCache>();
  auto stats = std::make_shared<engine::BlockCacheStats>(cache);
  cache->block = true;
  TaskRunner runner;
  ASSERT_TRUE(runner.TryPublish(stats->MakeSamplingTask(kDue)).IsOK());
  ASSERT_TRUE(runner.Start().IsOK());
  auto cleanup = MakeScopeExit([&] {
    cache->resume.set_value();
    runner.Cancel();
    (void)runner.Join();
  });
  ASSERT_EQ(cache->entered.get_future().wait_for(5s), std::future_status::ready);
  runner.Cancel();
  EXPECT_FALSE(stats->MakeSamplingTask(kDue));
  auto old = stats->GetSnapshot();
  auto next_cache = std::make_shared<CountingCache>();
  next_cache->value = 256;
  auto next = std::make_shared<engine::BlockCacheStats>(next_cache);
  auto next_snapshot = next->GetSnapshot();
  cache->value = 128;
  cache->resume.set_value();
  cleanup.Disable();
  ASSERT_TRUE(runner.Join().IsOK());
  EXPECT_EQ(stats->GetSnapshot().pinned_usage, 128);
  EXPECT_GE(stats->GetSnapshot().sampled_at, old.sampled_at);
  EXPECT_EQ(next->GetSnapshot().pinned_usage, next_snapshot.pinned_usage);
  EXPECT_EQ(next->GetSnapshot().sampled_at, next_snapshot.sampled_at);
  EXPECT_TRUE(stats->MakeSamplingTask(kDue));
}

TEST(BlockCacheStats, PinnedValueIsUpdatedAfterRelease) {
  auto cache = rocksdb::NewLRUCache(1 << 20);
  rocksdb::Cache::Handle *handle = nullptr;
  ASSERT_TRUE(cache->Insert("key", nullptr, &rocksdb::kNoopCacheItemHelper, 64, &handle).ok());
  auto stats = std::make_shared<engine::BlockCacheStats>(cache);
  EXPECT_GT(stats->GetSnapshot().pinned_usage, 0);
  EXPECT_EQ(stats->GetSnapshot().pinned_usage, cache->GetPinnedUsage());
  cache->Release(handle);
  auto task = stats->MakeSamplingTask(kDue);
  ASSERT_TRUE(task);
  task();
  EXPECT_EQ(stats->GetSnapshot().pinned_usage, 0);
}

TEST_F(TestBase, BlockCacheStatsDoNotCrossDBReopen) {
  auto old = storage_->GetBlockCacheStats();
  ASSERT_NE(old, nullptr);
  auto late_task = old->MakeSamplingTask(kDue);
  ASSERT_TRUE(late_task);
  storage_->CloseDB();
  EXPECT_EQ(storage_->GetBlockCacheStats(), nullptr);

  const auto original_dir = config_.db_dir;
  const auto invalid_path = original_dir + ".file";
  auto remove_file = MakeScopeExit([&] { std::filesystem::remove(invalid_path); });
  { std::ofstream output(invalid_path); }
  config_.db_dir = invalid_path;
  EXPECT_FALSE(storage_->Open().IsOK());
  EXPECT_EQ(storage_->GetBlockCacheStats(), nullptr);
  config_.db_dir = original_dir;
  ASSERT_TRUE(storage_->Open().IsOK());
  auto current = storage_->GetBlockCacheStats();
  ASSERT_NE(current, nullptr);
  ASSERT_NE(current, old);
  auto before = current->GetSnapshot();
  late_task();
  EXPECT_EQ(current->GetSnapshot().sampled_at, before.sampled_at);
  EXPECT_EQ(current->GetSnapshot().pinned_usage, before.pinned_usage);

  storage_->CloseDB();
  EXPECT_EQ(storage_->GetBlockCacheStats(), nullptr);
  EXPECT_FALSE(old->MakeSamplingTask(kDue));
  late_task = {};
  EXPECT_TRUE(old->MakeSamplingTask(kDue));
}
