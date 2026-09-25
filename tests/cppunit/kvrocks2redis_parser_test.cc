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

#include <algorithm>
#include <tuple>

#include "server/redis_reply.h"
#include "test_base.h"
#include "utils/kvrocks2redis/parser.h"

class RecordingWriter : public Writer {
 public:
  RecordingWriter() : Writer(nullptr) {}
  Status Write(const std::string &ns, const std::vector<std::string> &aofs) override {
    if (fail) return {Status::NotOK, "test writer failure"};
    auto &output = commands[ns];
    output.insert(output.end(), aofs.begin(), aofs.end());
    return Status::OK();
  }
  std::map<std::string, std::vector<std::string>> commands;
  bool fail = false;
};

class Kvrocks2RedisParserTest : public TestFixture, public ::testing::TestWithParam<std::tuple<bool, bool>> {
 protected:
  void SetUp() override { config_.slot_id_encoded = std::get<0>(GetParam()); }

  using Field = std::tuple<std::string, std::string, uint64_t>;
  void putHash(const std::string &ns, const std::string &key, HashSubkeyEncodingMode mode,
               const std::vector<Field> &fields, uint64_t key_expire = 0) {
    HashMetadata metadata(false, mode);
    metadata.flags = kRedisHash | (std::get<1>(GetParam()) ? METADATA_64BIT_ENCODING_MASK : 0);
    metadata.version = 42;
    metadata.expire = key_expire;
    metadata.size = fields.size();
    auto ns_key = ComposeNamespaceKey(ns, key, storage_->IsSlotIdEncoded());
    rocksdb::WriteBatch batch;
    for (const auto &[field, value, expire] : fields) {
      if (expire == 0) {
        ++metadata.persist;
      } else {
        metadata.lower = metadata.lower == 0 ? expire : std::min(metadata.lower, expire);
        metadata.upper = std::max(metadata.upper, expire);
      }
      auto subkey = InternalKey(ns_key, field, metadata.version, storage_->IsSlotIdEncoded()).Encode();
      ASSERT_TRUE(batch.Put(subkey, metadata.EncodeSubkeyValue(value, expire)).ok());
    }
    std::string bytes;
    metadata.Encode(&bytes);
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), ns_key, bytes).ok());
    ASSERT_TRUE(storage_->GetDB()->Write(storage_->DefaultWriteOptions(), &batch).ok());
  }
};

TEST_P(Kvrocks2RedisParserTest, FullExportUsesPerKeyEncodingAndAbsoluteExpiration) {
  const auto hfe = HashSubkeyEncodingMode::kFieldExpiration;
  const auto legacy = HashSubkeyEncodingMode::kLegacy;
  const uint64_t field_expire = 4000000000123ULL;
  const uint64_t key_expire = 4000000060456ULL;
  const std::string binary("value\0\xff", 7);
  auto legacy_value = HashMetadata(false, hfe).EncodeSubkeyValue(binary, field_expire);
  putHash("ns", "a{hash}", legacy, {{"field", legacy_value, 0}});
  putHash("ns", "b{hash}", hfe,
          {{"a-binary", binary, 0}, {"b-empty", "", 0}, {"c-expired", "gone", 1}, {"d-live", "live", field_expire}},
          key_expire);
  putHash("ns", "c{hash}", hfe, {{"field", "expired-key", 0}}, 1);
  putHash("ns", "d{hash}", hfe, {{"field", "expired-field", 1}});
  putHash("other", "b{hash}", legacy, {{"field", legacy_value, 0}});

  // Ignore subkeys from an obsolete version, even when their payload is not valid HFE encoding.
  auto stale = InternalKey(ComposeNamespaceKey("ns", "b{hash}", storage_->IsSlotIdEncoded()), "old", 41,
                           storage_->IsSlotIdEncoded())
                   .Encode();
  ASSERT_TRUE(storage_->GetDB()->Put(storage_->DefaultWriteOptions(), stale, "stale").ok());

  for (auto default_mode : {legacy, hfe}) {
    config_.hash_encoding_mode = default_mode;
    RecordingWriter writer;
    Parser parser(storage_.get(), &writer);
    ASSERT_TRUE(parser.ParseFullDB().IsOK());
    auto stored_key_expire = std::get<1>(GetParam()) ? key_expire : Metadata::ExpireMsToS(key_expire) * 1000;
    std::vector<std::string> expected = {
        redis::ArrayOfBulkStrings({"HSET", "a{hash}", "field", legacy_value}),
        redis::ArrayOfBulkStrings({"HSET", "b{hash}", "a-binary", binary}),
        redis::ArrayOfBulkStrings({"HSET", "b{hash}", "b-empty", ""}),
        redis::ArrayOfBulkStrings({"HSET", "b{hash}", "d-live", "live"}),
        redis::ArrayOfBulkStrings({"HPEXPIREAT", "b{hash}", std::to_string(field_expire), "FIELDS", "1", "d-live"}),
        redis::ArrayOfBulkStrings({"EXPIREAT", "b{hash}", std::to_string(stored_key_expire / 1000)})};
    ASSERT_EQ(writer.commands.size(), 2);
    EXPECT_EQ(writer.commands.at("ns"), expected);
    EXPECT_EQ(writer.commands.at("other"),
              std::vector<std::string>{redis::ArrayOfBulkStrings({"HSET", "b{hash}", "field", legacy_value})});
  }
}

TEST_P(Kvrocks2RedisParserTest, InvalidHashDataAndWriterErrorsAreNotIgnored) {
  putHash("ns", "hash", HashSubkeyEncodingMode::kFieldExpiration, {{"field", "value", 0}});
  RecordingWriter writer;
  Parser parser(storage_.get(), &writer);
  writer.fail = true;
  EXPECT_FALSE(parser.ParseFullDB().IsOK());
  writer.fail = false;

  auto ns_key = ComposeNamespaceKey("ns", "hash", storage_->IsSlotIdEncoded());
  auto key = InternalKey(ns_key, "field", 42, storage_->IsSlotIdEncoded()).Encode();
  ASSERT_TRUE(storage_->GetDB()->Put(storage_->DefaultWriteOptions(), key, std::string(7, '\0')).ok());
  EXPECT_FALSE(parser.ParseFullDB().IsOK());
  EXPECT_TRUE(writer.commands.empty());

  std::string metadata;
  auto *cf = storage_->GetCFHandle(ColumnFamilyID::Metadata);
  ASSERT_TRUE(storage_->GetDB()->Get(storage_->DefaultScanOptions(), cf, ns_key, &metadata).ok());
  metadata.resize(metadata.size() - 1);
  ASSERT_TRUE(storage_->GetDB()->Put(storage_->DefaultWriteOptions(), cf, ns_key, metadata).ok());
  EXPECT_FALSE(parser.ParseFullDB().IsOK());
  EXPECT_TRUE(writer.commands.empty());
}

INSTANTIATE_TEST_SUITE_P(Encodings, Kvrocks2RedisParserTest, ::testing::Combine(::testing::Bool(), ::testing::Bool()));
