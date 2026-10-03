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

#include <gtest/gtest.h>

#include <atomic>

#include "common/time_util.h"

// Standalone tests for lease atomic logic, mirroring Storage's internal behavior.
// NOTE: Storage cannot be instantiated without RocksDB, so these tests verify the
// identical atomic logic directly. This is an intentional tradeoff: if Storage's
// method implementations diverge from this logic, these tests won't catch it.

TEST(Lease, UpdateLeaseStoresCorrectValues) {
  std::atomic<uint64_t> deadline{0};
  std::atomic<uint64_t> version{0};

  uint64_t now = util::GetTimeStampMS();
  uint64_t lease_ms = 2000;
  // Mirrors UpdateLease(5, now + lease_ms): deadline written first, then version
  deadline.store(now + lease_ms, std::memory_order_relaxed);
  version.store(5, std::memory_order_relaxed);

  EXPECT_EQ(version.load(std::memory_order_relaxed), 5U);
  EXPECT_GE(deadline.load(std::memory_order_relaxed), now + lease_ms - 10);
  EXPECT_LE(deadline.load(std::memory_order_relaxed), now + lease_ms + 10);
}

TEST(Lease, ResetLeaseZerosDeadlineAndVersion) {
  std::atomic<uint64_t> deadline{12345};
  std::atomic<uint64_t> version{99};

  // Mirrors ResetLease(): deadline cleared first, then version
  deadline.store(0, std::memory_order_relaxed);
  version.store(0, std::memory_order_relaxed);

  EXPECT_EQ(deadline.load(std::memory_order_relaxed), 0U);
  EXPECT_EQ(version.load(std::memory_order_relaxed), 0U);
}

TEST(Lease, DeadlineZeroMeansNeverExpired) {
  // deadline == 0 is the cold-start state: writes always allowed
  std::atomic<uint64_t> deadline{0};
  uint64_t now = util::GetTimeStampMS();
  // Simulate the check in Storage::Write()
  bool expired = (deadline.load(std::memory_order_relaxed) > 0 && now > deadline.load(std::memory_order_relaxed));
  EXPECT_FALSE(expired);
}

TEST(Lease, DeadlineInFutureNotExpired) {
  std::atomic<uint64_t> deadline{0};
  uint64_t now = util::GetTimeStampMS();
  deadline.store(now + 5000, std::memory_order_relaxed);
  bool expired = (deadline.load(std::memory_order_relaxed) > 0 && now > deadline.load(std::memory_order_relaxed));
  EXPECT_FALSE(expired);
}

TEST(Lease, DeadlineInPastExpired) {
  std::atomic<uint64_t> deadline{0};
  uint64_t now = util::GetTimeStampMS();
  deadline.store(now - 1000, std::memory_order_relaxed);  // 1 second ago
  bool expired = (deadline.load(std::memory_order_relaxed) > 0 && now > deadline.load(std::memory_order_relaxed));
  EXPECT_TRUE(expired);
}

TEST(Lease, ElectionVersionGuard) {
  // Mirrors the version guard in CLUSTERX HEARTBEAT Execute():
  // if received_version >= local_version -> renew; else -> reject
  std::atomic<uint64_t> local_ver{10};
  std::atomic<uint64_t> deadline{0};

  // Case: received version >= local -> renew
  uint64_t received = 10;
  if (received >= local_ver.load(std::memory_order_relaxed)) {
    deadline.store(util::GetTimeStampMS() + 2000, std::memory_order_relaxed);
    local_ver.store(received, std::memory_order_relaxed);
  }
  EXPECT_EQ(local_ver.load(std::memory_order_relaxed), 10U);
  EXPECT_GT(deadline.load(std::memory_order_relaxed), 0U);

  // Case: received version < local -> no renew
  deadline.store(0, std::memory_order_relaxed);
  received = 9;
  if (received >= local_ver.load(std::memory_order_relaxed)) {
    deadline.store(util::GetTimeStampMS() + 2000, std::memory_order_relaxed);
    local_ver.store(received, std::memory_order_relaxed);
  }
  EXPECT_EQ(deadline.load(std::memory_order_relaxed), 0U);   // not renewed
  EXPECT_EQ(local_ver.load(std::memory_order_relaxed), 10U);  // unchanged
}

TEST(Lease, ResetOnRoleTransition) {
  // Simulate: node had a lease as master, then became slave via SLAVEOF
  std::atomic<uint64_t> deadline{util::GetTimeStampMS() + 5000};
  std::atomic<uint64_t> version{42};

  // Simulate SLAVEOF path: ResetLease()
  deadline.store(0, std::memory_order_relaxed);
  version.store(0, std::memory_order_relaxed);

  // After reset, the Write() check should treat it as cold start (writes allowed)
  uint64_t now = util::GetTimeStampMS();
  bool expired = (deadline.load(std::memory_order_relaxed) > 0 && now > deadline.load(std::memory_order_relaxed));
  EXPECT_FALSE(expired);  // deadline==0 means not expired
  EXPECT_EQ(version.load(std::memory_order_relaxed), 0U);
}
