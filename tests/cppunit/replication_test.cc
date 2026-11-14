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

#include "cluster/replication.h"

#include <event2/thread.h>
#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <thread>

#include "common/scope_exit.h"
#include "server/server.h"
#include "test_base.h"

class ReplicationThreadTest : public TestBase {
 protected:
  static void SetUpTestSuite() { ASSERT_EQ(evthread_use_pthreads(), 0); }

  void SetUp() override {
    config_.workers = 0;
    server_ = std::make_unique<Server>(storage_.get(), &config_);
  }

  // Exercise the production Stop/TimerCB/publication code without a network connection.
  void checkStop(ReplicationThread::StopMode mode, bool publish_after_stop, bool timer_race = false) {
    ReplicationThread repl("127.0.0.1", 1, server_.get());
    std::promise<bool> ready;
    auto prepared = ready.get_future();
    bool active_after_publication = false;
    repl.t_ = std::thread([&] {
      bool ready_sent = false;
      auto notify_on_exit = MakeScopeExit([&] {
        if (!ready_sent) ready.set_value(false);
      });
      std::function<void()> notify_ready = [&] {
        ready_sent = true;
        ready.set_value(true);
      };
      repl.base_ = event_base_new();
      auto free_base = MakeScopeExit([&] {
        if (repl.base_) event_base_free(repl.base_);
        repl.base_ = nullptr;
      });
      ASSERT_NE(repl.base_, nullptr);
      auto timer = UniqueEvent(repl.NewEvent(repl.base_, -1, EV_PERSIST));
      ASSERT_NE(timer, nullptr);
      // Withdraw before timer/base destruction on every exit, including failed assertions.
      auto withdraw_event = MakeScopeExit([&] { repl.setStopEvent(nullptr); });
      // A long fallback makes a missed wakeup fail independently of the normal 100 ms phase.
      timeval timeout{5, 0};
      if (mode == ReplicationThread::StopMode::kWaitForTimer) timeout = {0, 20000};
      if (timer_race) timeout = {0, 1};
      ASSERT_EQ(evtimer_add(timer.get(), &timeout), 0);
      if (publish_after_stop) {
        notify_ready();
        while (!repl.IsStopped()) std::this_thread::yield();
      }
      repl.setStopEvent(timer.get());
      // No dispatcher is running yet: only publication can activate this event here.
      if (publish_after_stop)
        active_after_publication = event_base_get_num_events(repl.base_, EVENT_BASE_COUNT_ACTIVE) > 0;
      if (!publish_after_stop) {
        timeval immediate{0, 0};
        ASSERT_EQ(event_base_once(
                      repl.base_, -1, EV_TIMEOUT,
                      [](evutil_socket_t, short, void *arg) { (*static_cast<std::function<void()> *>(arg))(); },
                      &notify_ready, &immediate),
                  0);
      }
      EXPECT_EQ(event_base_dispatch(repl.base_), 0);
    });
    const auto ready_status = prepared.wait_for(std::chrono::seconds(10));
    EXPECT_EQ(ready_status, std::future_status::ready);
    if (ready_status == std::future_status::ready) EXPECT_TRUE(prepared.get());
    // Give the dispatcher time to enter its five-second idle wait after the ready callback.
    if (!publish_after_stop && !timer_race) std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto start = std::chrono::steady_clock::now();
    if (mode == ReplicationThread::StopMode::kWaitForTimer) {
      repl.Stop();
    } else {
      repl.Stop(mode);
    }
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    EXPECT_EQ(repl.stop_event_, nullptr);
    EXPECT_EQ(repl.base_, nullptr);
    EXPECT_FALSE(repl.t_.joinable());
    if (publish_after_stop) EXPECT_EQ(active_after_publication, mode == ReplicationThread::StopMode::kWakeEventLoop);
    repl.Stop(mode);
    EXPECT_FALSE(repl.t_.joinable());
  }

  std::unique_ptr<Server> server_;
};

TEST_F(ReplicationThreadTest, WakeIdleEventLoop) { checkStop(ReplicationThread::StopMode::kWakeEventLoop, false); }

TEST_F(ReplicationThreadTest, StopBeforeEventPublication) {
  checkStop(ReplicationThread::StopMode::kWakeEventLoop, true);
}

TEST_F(ReplicationThreadTest, DefaultStopKeepsTimerPath) {
  checkStop(ReplicationThread::StopMode::kWaitForTimer, true);
}

TEST_F(ReplicationThreadTest, WakeupAndTimerExpiryRace) {
  for (int i = 0; i < 100; ++i) checkStop(ReplicationThread::StopMode::kWakeEventLoop, false, true);
}
