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

#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include "cluster/replication.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "test_base.h"

class ReplicationAckTest : public TestFixture, public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    config_.replication_group_sync = GetParam();
    config_.rocks_db.write_options.sync = GetParam();
    storage_->SetWriteOptions(config_.rocks_db.write_options);
    ASSERT_TRUE(storage_->GetDB()->Put(storage_->DefaultWriteOptions(), "ack-sequence", "value").ok());
    ASSERT_GT(storage_->LatestSeqNumber(), 0);

    server_ = std::make_unique<Server>(storage_.get(), &config_);
    server_->Stop();
    server_->Join();
    replication_ = std::make_unique<ReplicationThread>("127.0.0.1", 0, server_.get());
    event_base_.reset(event_base_new());
    ASSERT_NE(event_base_, nullptr);
    bev_.reset(bufferevent_socket_new(event_base_.get(), -1, 0));
    ASSERT_NE(bev_, nullptr);
    suppressPeriodicAck();
  }

  void suppressPeriodicAck() { replication_->last_ack_time_secs_ = std::numeric_limits<int64_t>::max(); }
  void allowPeriodicAck() { replication_->last_ack_time_secs_ = 0; }

  void process(std::string_view data) {
    auto *input = bufferevent_get_input(bev_.get());
    ASSERT_EQ(evbuffer_unfreeze(input, 0), 0);
    ASSERT_EQ(evbuffer_add(input, data.data(), data.size()), 0);
    ASSERT_EQ(evbuffer_freeze(input, 0), 0);
    ASSERT_EQ(replication_->incrementBatchLoopCB(bev_.get()), ReplicationThread::CBState::AGAIN);
  }

  void expectAck() {
    auto *output = bufferevent_get_output(bev_.get());
    auto expected = redis::ArrayOfBulkStrings({"replconf", "ack", std::to_string(storage_->LatestSeqNumber())});
    ASSERT_EQ(evbuffer_get_length(output), expected.size());
    std::string actual(expected.size(), '\0');
    ASSERT_EQ(evbuffer_unfreeze(output, 1), 0);
    ASSERT_EQ(evbuffer_remove(output, actual.data(), actual.size()), static_cast<int>(actual.size()));
    ASSERT_EQ(evbuffer_freeze(output, 1), 0);
    EXPECT_EQ(actual, expected);
  }

  void expectNoAck() { EXPECT_EQ(evbuffer_get_length(bufferevent_get_output(bev_.get())), 0); }
  void expectInputDrained() { EXPECT_EQ(evbuffer_get_length(bufferevent_get_input(bev_.get())), 0); }

  std::unique_ptr<Server> server_;
  std::unique_ptr<ReplicationThread> replication_;
  std::unique_ptr<event_base, decltype(&event_base_free)> event_base_{nullptr, event_base_free};
  std::unique_ptr<bufferevent, decltype(&bufferevent_free)> bev_{nullptr, bufferevent_free};
};

TEST_P(ReplicationAckTest, StandaloneGetAckBypassesThrottle) {
  process("$7\r\n_getack\r\n");
  expectAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, GetAckBeforePartialHeader) {
  process("$7\r\n_getack\r\n$");
  expectAck();

  suppressPeriodicAck();
  process("4\r\nping\r\n");
  expectNoAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, GetAckBeforePartialPayload) {
  process("$7\r\n_getack\r\n$4\r\npi");
  expectAck();

  suppressPeriodicAck();
  process("ng\r\n");
  expectNoAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, FragmentedGetAckWaitsForCompleteFrame) {
  process("$7\r\n_get");
  expectNoAck();
  process("ack\r");
  expectNoAck();
  process("\n");
  expectAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, GetAckAcrossCallbacksAlwaysBypassesThrottle) {
  for (int i = 0; i < 3; ++i) {
    suppressPeriodicAck();
    process("$7\r\n_getack\r\n");
    expectAck();
    expectInputDrained();
  }
}

TEST_P(ReplicationAckTest, PingBeforeGetAckInSameCallback) {
  process("$4\r\nping\r\n$7\r\n_getack\r\n");
  expectAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, GetAckBeforePingInSameCallback) {
  process("$7\r\n_getack\r\n$4\r\nping\r\n");
  expectAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, MultipleGetAckFramesProduceOneAck) {
  process("$7\r\n_getack\r\n$7\r\n_getack\r\n$7\r\n_getack\r\n");
  expectAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, PingRemainsThrottled) {
  process("$4\r\nping\r\n");
  expectNoAck();
  expectInputDrained();
}

TEST_P(ReplicationAckTest, PingAcknowledgesWhenThrottleExpires) {
  allowPeriodicAck();
  process("$4\r\nping\r\n$4\r\nping\r\n");
  expectAck();
  expectInputDrained();
}

INSTANTIATE_TEST_SUITE_P(GroupSync, ReplicationAckTest, ::testing::Bool());
