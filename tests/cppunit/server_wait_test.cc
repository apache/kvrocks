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

#include <memory>

#include "server/server.h"
#include "test_base.h"

class ServerWaitTest : public TestBase {
 protected:
  void SetUp() override {
    // Create server
    server_ = std::make_unique<Server>(storage_.get(), storage_->GetConfig());
    // we don't need the server resource, so just stop it once it's started
    server_->Stop();
    server_->Join();
  }

  ~ServerWaitTest() override = default;

  // Helper method to add wait contexts for testing using the public API
  void addWaitContext(rocksdb::SequenceNumber target_seq, uint64_t num_replicas) {
    // Use the public BlockOnWait method to add wait contexts
    server_->BlockOnWait(nullptr, target_seq, num_replicas);
  }

  std::unique_ptr<Server> server_;
};

TEST_F(ServerWaitTest, EmptyMap) {
  // Test with empty wait_contexts_ map
  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 0) << "Should return 0 for empty map";
}

TEST_F(ServerWaitTest, AllTargetSeqsGreaterThanSeq) {
  // Add wait contexts with target_seq > 100
  addWaitContext(150, 1);
  addWaitContext(200, 1);
  addWaitContext(250, 1);

  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 0) << "Should return 0 when all target_seqs > seq";
}

TEST_F(ServerWaitTest, SingleTargetSeqLessThanSeq) {
  // Add a single wait context with target_seq < 100
  addWaitContext(50, 1);

  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 50) << "Should return the target_seq when it's <= seq";
}

TEST_F(ServerWaitTest, MultipleTargetSeqsLessThanSeq) {
  // Add multiple wait contexts with target_seq < 100
  addWaitContext(30, 1);
  addWaitContext(50, 1);
  addWaitContext(80, 1);

  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 80) << "Should return the largest target_seq <= seq";
}

TEST_F(ServerWaitTest, MixedTargetSeqs) {
  // Add wait contexts with mixed target_seqs
  addWaitContext(30, 1);   // < 100
  addWaitContext(50, 1);   // < 100
  addWaitContext(80, 1);   // < 100
  addWaitContext(120, 1);  // > 100
  addWaitContext(150, 1);  // > 100

  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 80) << "Should return the largest target_seq <= seq";
}

TEST_F(ServerWaitTest, ExactMatch) {
  // Add wait context with target_seq exactly equal to seq
  addWaitContext(100, 1);

  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 100) << "Should return target_seq when it equals seq";
}

TEST_F(ServerWaitTest, MultipleExactMatches) {
  // Add multiple wait contexts with the same target_seq
  addWaitContext(100, 1);
  addWaitContext(100, 2);  // Same target_seq, different num_replicas
  addWaitContext(150, 1);  // > 100

  auto result = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result, 100) << "Should return target_seq for exact match";
}

TEST_F(ServerWaitTest, BoundaryConditions) {
  // Test boundary conditions
  addWaitContext(1, 1);
  addWaitContext(99, 1);
  addWaitContext(100, 1);
  addWaitContext(101, 1);

  // Test with seq = 0
  auto result1 = server_->LargestTargetSeqToWakeup(0);
  EXPECT_EQ(result1, 0) << "Should return 0 when seq = 0";

  // Test with seq = 1
  auto result2 = server_->LargestTargetSeqToWakeup(1);
  EXPECT_EQ(result2, 1) << "Should return 1 when seq = 1";

  // Test with seq = 100
  auto result3 = server_->LargestTargetSeqToWakeup(100);
  EXPECT_EQ(result3, 100) << "Should return 100 for exact match";

  // Test with seq = 99
  auto result4 = server_->LargestTargetSeqToWakeup(99);
  EXPECT_EQ(result4, 99) << "Should return 99 for exact match";
}

TEST_F(ServerWaitTest, LargeSequenceNumbers) {
  // Test with large sequence numbers
  addWaitContext(1000000, 1);
  addWaitContext(2000000, 1);
  addWaitContext(3000000, 1);

  auto result = server_->LargestTargetSeqToWakeup(2500000);
  EXPECT_EQ(result, 2000000) << "Should work with large sequence numbers";
}
