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

#include "cluster/batch_sender.h"

#include <sys/socket.h>

#include "io_util.h"
#include "server/redis_reply.h"
#include "storage/batch_extractor.h"
#include "test_base.h"
#include "unique_fd.h"

class BatchSenderTest : public TestBase {
 protected:
  void SetUp() override {
    int fds[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
    sender_fd_.Reset(fds[0]);
    receiver_fd_.Reset(fds[1]);
    timeval timeout{5, 0};
    for (int fd : fds) {
      ASSERT_EQ(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)), 0);
      ASSERT_EQ(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)), 0);
    }
  }

  void sendAndCheck(BatchSender &sender, const std::vector<std::vector<std::string>> &commands) {
    // These batches fit in the socket buffer, so the receiver can acknowledge before draining them.
    ASSERT_TRUE(util::SockSend(*receiver_fd_, "+OK\r\n").IsOK());
    auto sent_bytes = sender.GetSentBytes();
    ASSERT_TRUE(sender.Send().IsOK());
    auto length = sender.GetSentBytes() - sent_bytes;
    auto header = "*2\r\n$10\r\nAPPLYBATCH\r\n$" + std::to_string(length) + "\r\n";
    std::string payload(header.size() + length + 2, '\0');
    ASSERT_EQ(recv(*receiver_fd_, payload.data(), payload.size(), MSG_WAITALL), static_cast<ssize_t>(payload.size()));
    ASSERT_EQ(payload.substr(0, header.size()), header);
    ASSERT_EQ(payload.substr(header.size() + length), "\r\n");

    rocksdb::WriteBatch batch(payload.substr(header.size(), length));
    WriteBatchExtractor extractor(false, -1, true);
    ASSERT_TRUE(batch.Iterate(&extractor).ok());
    std::vector<std::string> expected;
    expected.reserve(commands.size());
    for (const auto &command : commands) expected.emplace_back(redis::ArrayOfBulkStrings(command));
    if (expected.empty()) {
      EXPECT_TRUE(extractor.GetRESPCommands()->empty());
    } else {
      ASSERT_EQ(extractor.GetRESPCommands()->size(), 1);
      EXPECT_EQ(extractor.GetRESPCommands()->at("ns"), expected);
    }
  }

  UniqueFD sender_fd_, receiver_fd_;
};

TEST_F(BatchSenderTest, SnapshotContextChangesAndSplits) {
  BatchSender sender(*sender_fd_, 1024, 1024 * 1024);
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey);
  HashMetadata hfe(false, HashSubkeyEncodingMode::kFieldExpiration);
  const auto encoded = hfe.EncodeSubkeyValue("value", 4000000000123ULL);
  for (const auto &name : {"hfe-first", "legacy", "hfe-last"}) {
    auto mode = std::string_view(name) == "legacy" ? HashSubkeyEncodingMode::kLegacy : hfe.mode;
    sender.SetPrefixLogData(redis::WriteBatchLogData(mode).Encode());
    auto key = InternalKey(ComposeNamespaceKey("ns", name, false), "field", 1, false).Encode();
    ASSERT_TRUE(sender.Put(cf, key, encoded).IsOK());
  }
  ASSERT_FALSE(sender.IsFull());
  sendAndCheck(sender, {{"HSET", "hfe-first", "field", "value"},
                        {"HPEXPIREAT", "hfe-first", "4000000000123", "FIELDS", "1", "field"},
                        {"HSET", "legacy", "field", encoded},
                        {"HSET", "hfe-last", "field", "value"},
                        {"HPEXPIREAT", "hfe-last", "4000000000123", "FIELDS", "1", "field"}});

  auto key = InternalKey(ComposeNamespaceKey("ns", "hfe-last", false), "next-field", 1, false).Encode();
  ASSERT_TRUE(sender.Put(cf, key, hfe.EncodeSubkeyValue("")).IsOK());
  sendAndCheck(sender, {{"HSET", "hfe-last", "next-field", ""}});
}

TEST_F(BatchSenderTest, IncrementalContextSurvivesSplitsAndServerLogs) {
  BatchSender sender(*sender_fd_, 1, 1024 * 1024);
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey);
  auto key = InternalKey(ComposeNamespaceKey("ns", "hash", false), "field", 1, false).Encode();
  HashMetadata hfe(false, HashSubkeyEncodingMode::kFieldExpiration);
  ASSERT_TRUE(sender.PutLogData(redis::WriteBatchLogData(hfe.mode).Encode()).IsOK());
  // A split immediately after LogData must not leave the following data without context.
  sendAndCheck(sender, {});
  ASSERT_TRUE(sender.PutLogData("r replication-id").IsOK());
  ASSERT_TRUE(sender.Put(cf, key, hfe.EncodeSubkeyValue("value", 4000000000123ULL)).IsOK());
  sendAndCheck(sender,
               {{"HSET", "hash", "field", "value"}, {"HPEXPIREAT", "hash", "4000000000123", "FIELDS", "1", "field"}});
  ASSERT_TRUE(sender.Delete(cf, key).IsOK());
  sendAndCheck(sender, {{"HDEL", "hash", "field"}});

  ASSERT_TRUE(sender.PutLogData(redis::WriteBatchLogData(HashSubkeyEncodingMode::kLegacy).Encode()).IsOK());
  ASSERT_TRUE(sender.Put(cf, key, "legacy").IsOK());
  sendAndCheck(sender, {{"HSET", "hash", "field", "legacy"}});
  ASSERT_TRUE(sender.Delete(cf, key).IsOK());
  sendAndCheck(sender, {{"HDEL", "hash", "field"}});

  sender.SetPrefixLogData(redis::WriteBatchLogData(kRedisNone).Encode());
  ASSERT_TRUE(sender.Put(cf, key, "unmarked").IsOK());
  sendAndCheck(sender, {});
}

TEST_F(BatchSenderTest, SnapshotPrefixesAndIncrementalCommandsHaveDifferentLifetimes) {
  BatchSender sender(*sender_fd_, 1, 1024 * 1024);
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey);
  auto key = InternalKey(ComposeNamespaceKey("ns", "list", false), "subkey", 1, false).Encode();
  sender.SetPrefixLogData(redis::WriteBatchLogData(kRedisList, {std::to_string(kRedisCmdRPush)}).Encode());
  for (const auto &value : {"first", "second"}) {
    ASSERT_TRUE(sender.Put(cf, key, value).IsOK());
    sendAndCheck(sender, {{"RPUSH", "list", value}});
  }

  ASSERT_TRUE(
      sender.PutLogData(redis::WriteBatchLogData(kRedisList, {std::to_string(kRedisCmdLTrim), "1", "2"}).Encode())
          .IsOK());
  ASSERT_TRUE(sender.Delete(cf, key).IsOK());
  sendAndCheck(sender, {{"LTRIM", "list", "1", "2"}});
  ASSERT_TRUE(sender.Delete(cf, key).IsOK());
  sendAndCheck(sender, {});
}
