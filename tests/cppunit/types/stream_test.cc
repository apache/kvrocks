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

#include "test_base.h"
#include "time_util.h"
#include "types/redis_stream.h"

class RedisStreamTest : public TestBase {  // NOLINT
 public:
  static void checkStreamEntryValues(const std::vector<std::string> &got, const std::vector<std::string> &expected) {
    EXPECT_EQ(got.size(), expected.size());
    for (size_t i = 0; i < got.size(); ++i) {
      EXPECT_EQ(got[i], expected[i]);
    }
  }

 protected:
  RedisStreamTest() : name("test_stream") { stream = new Redis::Stream(storage_, "stream_ns"); }

  ~RedisStreamTest() override { delete stream; }

  void SetUp() override { stream->Del(name); }

  void TearDown() override { stream->Del(name); }

  std::string name;
  Redis::Stream *stream;
};

TEST_F(RedisStreamTest, EncodeDecodeEntryValue) {
  std::vector<std::string> values = {"day", "first", "month", "eleventh", "epoch", "fairly-very-old-one"};
  auto encoded = Redis::EncodeStreamEntryValue(values);
  std::vector<std::string> decoded;
  auto s = Redis::DecodeRawStreamEntryValue(encoded, &decoded);
  EXPECT_TRUE(s.IsOK());
  checkStreamEntryValues(decoded, values);
}

TEST_F(RedisStreamTest, AddEntryToNonExistingStreamWithNomkstreamOption) {
  Redis::StreamAddOptions options;
  options.nomkstream = true;
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisStreamTest, AddEntryPredefinedIDAsZeroZero) {
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{0, 0};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(!s.ok());
}

TEST_F(RedisStreamTest, AddEntryWithPredefinedIDAsZeroMsAndAnySeq) {
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{0};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id.ToString(), "0-1");
}

TEST_F(RedisStreamTest, AddFirstEntryWithoutPredefinedID) {
  Redis::StreamAddOptions options;
  options.with_entry_id = false;
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id.seq, 0);
  EXPECT_TRUE(id.ms <= Util::GetTimeStampMS());
}

TEST_F(RedisStreamTest, AddEntryFirstEntryWithPredefinedID) {
  Redis::StreamEntryID expected_id{12345, 6789};
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{expected_id.ms, expected_id.seq};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id.ms, expected_id.ms);
  EXPECT_EQ(id.seq, expected_id.seq);
}

TEST_F(RedisStreamTest, AddFirstEntryWithPredefinedNonZeroMsAndAnySeqNo) {
  uint64_t ms = Util::GetTimeStampMS();
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{ms};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id.ms, ms);
  EXPECT_EQ(id.seq, 0);
}

TEST_F(RedisStreamTest, AddEntryToNonEmptyStreamWithPredefinedMsAndAnySeqNo) {
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{12345, 678};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, options, values1, &id1);
  EXPECT_TRUE(s.ok());
  options.entry_id = Redis::NewStreamEntryID{12346};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, options, values2, &id2);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id2.ToString(), "12346-0");
}

TEST_F(RedisStreamTest, AddEntryWithPredefinedButExistingMsAndAnySeqNo) {
  uint64_t ms = 12345;
  uint64_t seq = 6789;
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{ms, seq};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{ms};
  s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(id.ms, ms);
  EXPECT_EQ(id.seq, seq + 1);
}

TEST_F(RedisStreamTest, AddEntryWithExistingMsAnySeqNoAndExistingSeqNoIsAlreadyMax) {
  uint64_t ms = 12345;
  uint64_t seq = UINT64_MAX;
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{ms, seq};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{ms};
  s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(!s.ok());
}

TEST_F(RedisStreamTest, AddEntryAndExistingMsAndSeqNoAreAlreadyMax) {
  uint64_t ms = UINT64_MAX;
  uint64_t seq = UINT64_MAX;
  Redis::StreamAddOptions options;
  options.with_entry_id = true;
  options.entry_id = Redis::NewStreamEntryID{ms, seq};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(s.ok());
  options.with_entry_id = false;
  s = stream->Add(name, options, values, &id);
  EXPECT_TRUE(!s.ok());
}

TEST_F(RedisStreamTest, AddEntryWithTrimMaxLenStrategy) {
  Redis::StreamAddOptions add_options;
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions trim_options;
  trim_options.strategy = Redis::StreamTrimStrategy::MaxLen;
  trim_options.max_len = 2;
  add_options.trim_options = trim_options;
  Redis::StreamEntryID id3;
  std::vector<std::string> values3 = {"key3", "val3"};
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id3.ToString());
  checkStreamEntryValues(entries[1].values, values3);
}

TEST_F(RedisStreamTest, AddEntryWithTrimMaxLenStrategyThatDeletesAddedEntry) {
  Redis::StreamAddOptions add_options;
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions trim_options;
  trim_options.strategy = Redis::StreamTrimStrategy::MaxLen;
  trim_options.max_len = 0;
  add_options.trim_options = trim_options;
  Redis::StreamEntryID id3;
  std::vector<std::string> values3 = {"key3", "val3"};
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, AddEntryWithTrimMinIdStrategy) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{12346, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions trim_options;
  trim_options.strategy = Redis::StreamTrimStrategy::MinID;
  trim_options.min_id = Redis::StreamEntryID{12346, 0};
  add_options.trim_options = trim_options;
  add_options.entry_id = Redis::NewStreamEntryID{12347, 0};
  Redis::StreamEntryID id3;
  std::vector<std::string> values3 = {"key3", "val3"};
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id3.ToString());
  checkStreamEntryValues(entries[1].values, values3);
}

TEST_F(RedisStreamTest, AddEntryWithTrimMinIdStrategyThatDeletesAddedEntry) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{12346, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions trim_options;
  trim_options.strategy = Redis::StreamTrimStrategy::MinID;
  trim_options.min_id = Redis::StreamEntryID{1234567, 0};
  add_options.trim_options = trim_options;
  add_options.entry_id = Redis::NewStreamEntryID{12347, 0};
  Redis::StreamEntryID id3;
  std::vector<std::string> values3 = {"key3", "val3"};
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeOnNonExistingStream) {
  Redis::StreamRangeOptions options;
  options.start = Redis::StreamEntryID{0, 0};
  options.end = Redis::StreamEntryID{1234567, 0};
  std::vector<Redis::StreamEntry> entries;
  auto s = stream->Range(name, options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeOnEmptyStream) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = false;
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());
  uint64_t removed = 0;
  s = stream->DeleteEntries(name, {id}, &removed);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeWithStartAndEndSameMs) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345678, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{12345678, 1};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{12345679, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{12345678, 0};
  range_options.end = Redis::StreamEntryID{12345678, UINT64_MAX};
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id1.ToString());
  checkStreamEntryValues(entries[0].values, values1);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
}

TEST_F(RedisStreamTest, RangeInterval) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123456, 0};
  range_options.end = Redis::StreamEntryID{123459, 0};
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 3);
  EXPECT_EQ(entries[0].key, id1.ToString());
  checkStreamEntryValues(entries[0].values, values1);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
  EXPECT_EQ(entries[2].key, id3.ToString());
  checkStreamEntryValues(entries[2].values, values3);
}

TEST_F(RedisStreamTest, RangeFromMinimumToMaximum) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 4);
  EXPECT_EQ(entries[0].key, id1.ToString());
  checkStreamEntryValues(entries[0].values, values1);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
  EXPECT_EQ(entries[2].key, id3.ToString());
  checkStreamEntryValues(entries[2].values, values3);
  EXPECT_EQ(entries[3].key, id4.ToString());
  checkStreamEntryValues(entries[3].values, values4);
}

TEST_F(RedisStreamTest, RangeFromMinimumToMinimum) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Minimum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeWithStartGreaterThanEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Maximum();
  range_options.end = Redis::StreamEntryID::Minimum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeWithStartAndEndAreEqual) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = id2;
  range_options.end = id2;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
}

TEST_F(RedisStreamTest, RangeWithStartAndEndAreEqualAndExludedStart) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = id2;
  range_options.exclude_start = true;
  range_options.end = id2;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeWithStartAndEndAreEqualAndExludedEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = id2;
  range_options.end = id2;
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeWithExcludedStart) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123456, 1};
  range_options.exclude_start = true;
  range_options.end = Redis::StreamEntryID{123458, 3};
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id3.ToString());
  checkStreamEntryValues(entries[1].values, values3);
}

TEST_F(RedisStreamTest, RangeWithExcludedEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123457, 2};
  range_options.end = Redis::StreamEntryID{123459, 4};
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id3.ToString());
  checkStreamEntryValues(entries[1].values, values3);
}

TEST_F(RedisStreamTest, RangeWithExcludedStartAndExcludedEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123456, 1};
  range_options.exclude_start = true;
  range_options.end = Redis::StreamEntryID{123459, 4};
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id3.ToString());
  checkStreamEntryValues(entries[1].values, values3);
}

TEST_F(RedisStreamTest, RangeWithStartAsMaximumAndExlusion) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Maximum();
  range_options.exclude_start = true;
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(!s.ok());
}

TEST_F(RedisStreamTest, RangeWithEndAsMinimumAndExlusion) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Minimum();
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(!s.ok());
}

TEST_F(RedisStreamTest, RangeWithCountEqualToZero) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123456, 0};
  range_options.end = Redis::StreamEntryID{123459, 0};
  range_options.with_count = true;
  range_options.count = 0;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RangeWithCountGreaterThanRequiredElements) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123456, 0};
  range_options.end = Redis::StreamEntryID{123459, 0};
  range_options.with_count = true;
  range_options.count = 3;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 3);
  EXPECT_EQ(entries[0].key, id1.ToString());
  checkStreamEntryValues(entries[0].values, values1);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
  EXPECT_EQ(entries[2].key, id3.ToString());
  checkStreamEntryValues(entries[2].values, values3);
}

TEST_F(RedisStreamTest, RangeWithCountLessThanRequiredElements) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID{123456, 0};
  range_options.end = Redis::StreamEntryID{123459, 0};
  range_options.with_count = true;
  range_options.count = 2;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id1.ToString());
  checkStreamEntryValues(entries[0].values, values1);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
}

TEST_F(RedisStreamTest, RevRangeWithStartAndEndSameMs) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345678, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{12345678, 1};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{12345679, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID{12345678, UINT64_MAX};
  range_options.end = Redis::StreamEntryID{12345678, 0};
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id1.ToString());
  checkStreamEntryValues(entries[1].values, values1);
}

TEST_F(RedisStreamTest, RevRangeInterval) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID{123459, 0};
  range_options.end = Redis::StreamEntryID{123456, 0};
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 3);
  EXPECT_EQ(entries[0].key, id3.ToString());
  checkStreamEntryValues(entries[0].values, values3);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
  EXPECT_EQ(entries[2].key, id1.ToString());
  checkStreamEntryValues(entries[2].values, values1);
}

TEST_F(RedisStreamTest, RevRangeFromMaximumToMinimum) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID::Maximum();
  range_options.end = Redis::StreamEntryID::Minimum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 4);
  EXPECT_EQ(entries[0].key, id4.ToString());
  checkStreamEntryValues(entries[0].values, values4);
  EXPECT_EQ(entries[1].key, id3.ToString());
  checkStreamEntryValues(entries[1].values, values3);
  EXPECT_EQ(entries[2].key, id2.ToString());
  checkStreamEntryValues(entries[2].values, values2);
  EXPECT_EQ(entries[3].key, id1.ToString());
  checkStreamEntryValues(entries[3].values, values1);
}

TEST_F(RedisStreamTest, RevRangeFromMinimumToMinimum) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Minimum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RevRangeWithStartLessThanEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RevRangeStartAndEndAreEqual) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = id2;
  range_options.end = id2;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
}

TEST_F(RedisStreamTest, RevRangeStartAndEndAreEqualAndExcludedStart) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = id2;
  range_options.exclude_start = true;
  range_options.end = id2;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RevRangeStartAndEndAreEqualAndExcludedEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = id2;
  range_options.end = id2;
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 0);
}

TEST_F(RedisStreamTest, RevRangeWithExcludedStart) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID{123458, 3};
  range_options.exclude_start = true;
  range_options.end = Redis::StreamEntryID{123456, 1};
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id1.ToString());
  checkStreamEntryValues(entries[1].values, values1);
}

TEST_F(RedisStreamTest, RevRangeWithExcludedEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID{123458, 3};
  range_options.end = Redis::StreamEntryID{123456, 1};
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id3.ToString());
  checkStreamEntryValues(entries[0].values, values3);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
}

TEST_F(RedisStreamTest, RevRangeWithExcludedStartAndExcludedEnd) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 1};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 2};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 3};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 4};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamRangeOptions range_options;
  range_options.reverse = true;
  range_options.start = Redis::StreamEntryID{123459, 4};
  range_options.exclude_start = true;
  range_options.end = Redis::StreamEntryID{123456, 1};
  range_options.exclude_end = true;
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id3.ToString());
  checkStreamEntryValues(entries[0].values, values3);
  EXPECT_EQ(entries[1].key, id2.ToString());
  checkStreamEntryValues(entries[1].values, values2);
}

TEST_F(RedisStreamTest, DeleteFromNonExistingStream) {
  std::vector<Redis::StreamEntryID> ids = {Redis::StreamEntryID{12345, 6789}};
  uint64_t deleted = 0;
  auto s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 0);
}

TEST_F(RedisStreamTest, DeleteExistingEntry) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {id};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 1);
}

TEST_F(RedisStreamTest, DeleteNonExistingEntry) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {Redis::StreamEntryID{123, 456}};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 0);
}

TEST_F(RedisStreamTest, DeleteMultipleEntries) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {Redis::StreamEntryID{123456, 0}, Redis::StreamEntryID{1234567, 89},
                                           Redis::StreamEntryID{123458, 0}};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(deleted, 2);

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id2.ToString());
  checkStreamEntryValues(entries[0].values, values2);
  EXPECT_EQ(entries[1].key, id4.ToString());
  checkStreamEntryValues(entries[1].values, values4);
}

TEST_F(RedisStreamTest, LenOnNonExistingStream) {
  uint64_t length = 0;
  auto s = stream->Len(name, Redis::StreamLenOptions{}, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);
}

TEST_F(RedisStreamTest, LenOnEmptyStream) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {id};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  s = stream->Len(name, Redis::StreamLenOptions{}, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);
}

TEST_F(RedisStreamTest, Len) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  s = stream->Len(name, Redis::StreamLenOptions{}, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 2);
}

TEST_F(RedisStreamTest, LenWithStartOptionGreaterThanLastEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;

  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, {"key1", "val1"}, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, {"key2", "val2"}, &id2);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  Redis::StreamLenOptions len_options;
  len_options.with_entry_id = true;
  len_options.entry_id = Redis::StreamEntryID{id2.ms + 10, 0};
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);

  len_options.to_first = true;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 2);
}

TEST_F(RedisStreamTest, LenWithStartOptionEqualToLastEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;

  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, {"key1", "val1"}, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, {"key2", "val2"}, &id2);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  Redis::StreamLenOptions len_options;
  len_options.with_entry_id = true;
  len_options.entry_id = Redis::StreamEntryID{id2.ms, id2.seq};
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);

  len_options.to_first = true;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 1);
}

TEST_F(RedisStreamTest, LenWithStartOptionLessThanFirstEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;

  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, {"key1", "val1"}, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, {"key2", "val2"}, &id2);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  Redis::StreamLenOptions len_options;
  len_options.with_entry_id = true;
  len_options.entry_id = Redis::StreamEntryID{123, 0};
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 2);

  len_options.to_first = true;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);
}

TEST_F(RedisStreamTest, LenWithStartOptionEqualToFirstEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;

  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, {"key1", "val1"}, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, {"key2", "val2"}, &id2);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  Redis::StreamLenOptions len_options;
  len_options.with_entry_id = true;
  len_options.entry_id = id1;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 1);

  len_options.to_first = true;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);
}

TEST_F(RedisStreamTest, LenWithStartOptionEqualToExistingEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;

  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, {"key1", "val1"}, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, {"key2", "val2"}, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, {"key3", "val3"}, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, {"key4", "val4"}, &id4);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  Redis::StreamLenOptions len_options;
  len_options.with_entry_id = true;
  len_options.entry_id = id2;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 2);

  len_options.to_first = true;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 1);
}

TEST_F(RedisStreamTest, LenWithStartOptionNotEqualToExistingEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;

  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, {"key1", "val1"}, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, {"key2", "val2"}, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, {"key3", "val3"}, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, {"key4", "val4"}, &id4);
  EXPECT_TRUE(s.ok());

  uint64_t length = 0;
  Redis::StreamLenOptions len_options;
  len_options.with_entry_id = true;
  len_options.entry_id = Redis::StreamEntryID{id1.ms, id1.seq + 10};
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 3);

  len_options.to_first = true;
  s = stream->Len(name, len_options, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 1);
}

TEST_F(RedisStreamTest, TrimNonExistingStream) {
  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 10;
  uint64_t trimmed = 0;
  auto s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimEmptyStream) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());
  std::vector<Redis::StreamEntryID> ids = {id};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 10;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimWithNoStrategySpecified) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.min_id = Redis::StreamEntryID{123456, 0};
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimWithMaxLenGreaterThanStreamSize) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 10;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimWithMaxLenEqualToStreamSize) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 4;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimWithMaxLenLessThanStreamSize) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 2;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 2);

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id3.ToString());
  checkStreamEntryValues(entries[0].values, values3);
  EXPECT_EQ(entries[1].key, id4.ToString());
  checkStreamEntryValues(entries[1].values, values4);
}

TEST_F(RedisStreamTest, TrimWithMaxLenEqualTo1) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 1;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 3);

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 1);
  EXPECT_EQ(entries[0].key, id4.ToString());
  checkStreamEntryValues(entries[0].values, values4);
}

TEST_F(RedisStreamTest, TrimWithMaxLenZero) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 0;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 4);
  uint64_t length = 0;
  s = stream->Len(name, Redis::StreamLenOptions{}, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);
}

TEST_F(RedisStreamTest, TrimWithMinIdLessThanFirstEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MinID;
  options.min_id = Redis::StreamEntryID{12345, 0};
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimWithMinIdEqualToFirstEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MinID;
  options.min_id = Redis::StreamEntryID{123456, 0};
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 0);
}

TEST_F(RedisStreamTest, TrimWithMinId) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MinID;
  options.min_id = Redis::StreamEntryID{123457, 10};
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 2);

  Redis::StreamRangeOptions range_options;
  range_options.start = Redis::StreamEntryID::Minimum();
  range_options.end = Redis::StreamEntryID::Maximum();
  std::vector<Redis::StreamEntry> entries;
  s = stream->Range(name, range_options, &entries);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(entries.size(), 2);
  EXPECT_EQ(entries[0].key, id3.ToString());
  checkStreamEntryValues(entries[0].values, values3);
  EXPECT_EQ(entries[1].key, id4.ToString());
  checkStreamEntryValues(entries[1].values, values4);
}

TEST_F(RedisStreamTest, TrimWithMinIdGreaterThanLastEntryID) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MinID;
  options.min_id = Redis::StreamEntryID{12345678, 0};
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(trimmed, 4);

  uint64_t length = 0;
  s = stream->Len(name, Redis::StreamLenOptions{}, &length);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(length, 0);
}

TEST_F(RedisStreamTest, StreamInfoOnNonExistingStream) {
  Redis::StreamInfo info;
  auto s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisStreamTest, StreamInfoOnEmptyStream) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {id};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 0);
  EXPECT_EQ(info.last_generated_id.ToString(), id.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id.ToString());
  EXPECT_EQ(info.entries_added, 1);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), "0-0");
  EXPECT_FALSE(info.first_entry);
  EXPECT_FALSE(info.last_entry);
}

TEST_F(RedisStreamTest, StreamInfoOneEntry) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{12345, 6789};
  std::vector<std::string> values = {"key1", "val1"};
  Redis::StreamEntryID id;
  auto s = stream->Add(name, add_options, values, &id);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 1);
  EXPECT_EQ(info.last_generated_id.ToString(), id.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), "0-0");
  EXPECT_EQ(info.entries_added, 1);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id.ToString());
  EXPECT_TRUE(info.first_entry);
  EXPECT_EQ(info.first_entry->key, id.ToString());
  checkStreamEntryValues(info.first_entry->values, values);
  EXPECT_TRUE(info.last_entry);
  EXPECT_EQ(info.last_entry->key, id.ToString());
  checkStreamEntryValues(info.last_entry->values, values);
}

TEST_F(RedisStreamTest, StreamInfoOnStreamWithElements) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 3);
  EXPECT_EQ(info.last_generated_id.ToString(), id3.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), "0-0");
  EXPECT_EQ(info.entries_added, 3);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id1.ToString());
  EXPECT_TRUE(info.first_entry);
  EXPECT_EQ(info.first_entry->key, id1.ToString());
  checkStreamEntryValues(info.first_entry->values, values1);
  EXPECT_TRUE(info.last_entry);
  EXPECT_EQ(info.last_entry->key, id3.ToString());
  checkStreamEntryValues(info.last_entry->values, values3);
  EXPECT_EQ(info.entries.size(), 0);
}

TEST_F(RedisStreamTest, StreamInfoOnStreamWithElementsFullOption) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, true, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 3);
  EXPECT_EQ(info.last_generated_id.ToString(), id3.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), "0-0");
  EXPECT_EQ(info.entries_added, 3);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id1.ToString());
  EXPECT_FALSE(info.first_entry);
  EXPECT_FALSE(info.last_entry);
  EXPECT_EQ(info.entries.size(), 3);
  EXPECT_EQ(info.entries[0].key, id1.ToString());
  checkStreamEntryValues(info.entries[0].values, values1);
  EXPECT_EQ(info.entries[1].key, id2.ToString());
  checkStreamEntryValues(info.entries[1].values, values2);
  EXPECT_EQ(info.entries[2].key, id3.ToString());
  checkStreamEntryValues(info.entries[2].values, values3);
}

TEST_F(RedisStreamTest, StreamInfoCheckAfterLastEntryDeletion) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {id3};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 2);
  EXPECT_EQ(info.last_generated_id.ToString(), id3.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id3.ToString());
  EXPECT_EQ(info.entries_added, 3);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id1.ToString());
  EXPECT_TRUE(info.first_entry);
  EXPECT_EQ(info.first_entry->key, id1.ToString());
  checkStreamEntryValues(info.first_entry->values, values1);
  EXPECT_TRUE(info.last_entry);
  EXPECT_EQ(info.last_entry->key, id2.ToString());
  checkStreamEntryValues(info.last_entry->values, values2);
  EXPECT_EQ(info.entries.size(), 0);
}

TEST_F(RedisStreamTest, StreamInfoCheckAfterFirstEntryDeletion) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());

  std::vector<Redis::StreamEntryID> ids = {id1};
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, ids, &deleted);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 2);
  EXPECT_EQ(info.last_generated_id.ToString(), id3.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id1.ToString());
  EXPECT_EQ(info.entries_added, 3);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id2.ToString());
  EXPECT_TRUE(info.first_entry);
  EXPECT_EQ(info.first_entry->key, id2.ToString());
  checkStreamEntryValues(info.first_entry->values, values2);
  EXPECT_TRUE(info.last_entry);
  EXPECT_EQ(info.last_entry->key, id3.ToString());
  checkStreamEntryValues(info.last_entry->values, values3);
  EXPECT_EQ(info.entries.size(), 0);
}

TEST_F(RedisStreamTest, StreamInfoCheckAfterTrimMinId) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MinID;
  options.min_id = Redis::StreamEntryID{123458, 0};
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 2);
  EXPECT_EQ(info.last_generated_id.ToString(), id4.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id2.ToString());
  EXPECT_EQ(info.entries_added, 4);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id3.ToString());
  EXPECT_TRUE(info.first_entry);
  EXPECT_EQ(info.first_entry->key, id3.ToString());
  checkStreamEntryValues(info.first_entry->values, values3);
  EXPECT_TRUE(info.last_entry);
  EXPECT_EQ(info.last_entry->key, id4.ToString());
  checkStreamEntryValues(info.last_entry->values, values4);
  EXPECT_EQ(info.entries.size(), 0);
}

TEST_F(RedisStreamTest, StreamInfoCheckAfterTrimMaxLen) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 2;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 2);
  EXPECT_EQ(info.last_generated_id.ToString(), id4.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id2.ToString());
  EXPECT_EQ(info.entries_added, 4);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), id3.ToString());
  EXPECT_TRUE(info.first_entry);
  EXPECT_EQ(info.first_entry->key, id3.ToString());
  checkStreamEntryValues(info.first_entry->values, values3);
  EXPECT_TRUE(info.last_entry);
  EXPECT_EQ(info.last_entry->key, id4.ToString());
  checkStreamEntryValues(info.last_entry->values, values4);
  EXPECT_EQ(info.entries.size(), 0);
}

TEST_F(RedisStreamTest, StreamInfoCheckAfterTrimAllEntries) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123457, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  s = stream->Add(name, add_options, values2, &id2);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123458, 0};
  std::vector<std::string> values3 = {"key3", "val3"};
  Redis::StreamEntryID id3;
  s = stream->Add(name, add_options, values3, &id3);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123459, 0};
  std::vector<std::string> values4 = {"key4", "val4"};
  Redis::StreamEntryID id4;
  s = stream->Add(name, add_options, values4, &id4);
  EXPECT_TRUE(s.ok());

  Redis::StreamTrimOptions options;
  options.strategy = Redis::StreamTrimStrategy::MaxLen;
  options.max_len = 0;
  uint64_t trimmed = 0;
  s = stream->Trim(name, options, &trimmed);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 0);
  EXPECT_EQ(info.last_generated_id.ToString(), id4.ToString());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id4.ToString());
  EXPECT_EQ(info.entries_added, 4);
  EXPECT_EQ(info.recorded_first_entry_id.ToString(), "0-0");
  EXPECT_FALSE(info.first_entry);
  EXPECT_FALSE(info.last_entry);
  EXPECT_EQ(info.entries.size(), 0);
}

TEST_F(RedisStreamTest, StreamSetIdNonExistingStreamCreatesEmptyStream) {
  Redis::StreamEntryID last_id(5, 0);
  std::optional<Redis::StreamEntryID> max_del_id = Redis::StreamEntryID{2, 0};
  uint64_t entries_added = 3;
  auto s = stream->SetId("some-non-existing-stream1", last_id, entries_added, max_del_id);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo("some-non-existing-stream1", false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.last_generated_id.ToString(), last_id.ToString());
  EXPECT_EQ(info.entries_added, entries_added);
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), max_del_id->ToString());

  s = stream->SetId("some-non-existing-stream2", last_id, std::nullopt, max_del_id);
  EXPECT_FALSE(s.ok());

  s = stream->SetId("some-non-existing-stream3", last_id, entries_added, std::nullopt);
  EXPECT_FALSE(s.ok());
}

TEST_F(RedisStreamTest, StreamSetIdLastIdLessThanExisting) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());

  s = stream->SetId(name, {1, 0}, std::nullopt, std::nullopt);
  EXPECT_FALSE(s.ok());
}

TEST_F(RedisStreamTest, StreamSetIdEntriesAddedLessThanStreamSize) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values2 = {"key2", "val2"};
  Redis::StreamEntryID id2;
  stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());

  s = stream->SetId(name, {id2.ms + 1, 0}, 1, std::nullopt);
  EXPECT_FALSE(s.ok());
}

TEST_F(RedisStreamTest, StreamSetIdLastIdEqualToExisting) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());

  s = stream->SetId(name, {id1.ms, id1.seq}, std::nullopt, std::nullopt);
  EXPECT_TRUE(s.ok());
}

TEST_F(RedisStreamTest, StreamSetIdMaxDeletedIdLessThanCurrent) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, {id1}, &deleted);
  EXPECT_TRUE(s.ok());

  std::optional<Redis::StreamEntryID> max_del_id = Redis::StreamEntryID{1, 0};
  s = stream->SetId(name, {id1.ms, id1.seq}, std::nullopt, max_del_id);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), max_del_id->ToString());
}

TEST_F(RedisStreamTest, StreamSetIdMaxDeletedIdIsZero) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, {id1}, &deleted);
  EXPECT_TRUE(s.ok());

  std::optional<Redis::StreamEntryID> max_del_id = Redis::StreamEntryID{0, 0};
  s = stream->SetId(name, {id1.ms, id1.seq}, std::nullopt, max_del_id);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), id1.ToString());
}

TEST_F(RedisStreamTest, StreamSetIdMaxDeletedIdGreaterThanLastGeneratedId) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());
  uint64_t deleted = 0;
  s = stream->DeleteEntries(name, {id1}, &deleted);
  EXPECT_TRUE(s.ok());

  std::optional<Redis::StreamEntryID> max_del_id = Redis::StreamEntryID{id1.ms + 1, 0};
  s = stream->SetId(name, {id1.ms, id1.seq}, std::nullopt, max_del_id);
  EXPECT_FALSE(s.ok());
}

TEST_F(RedisStreamTest, StreamSetIdLastIdGreaterThanExisting) {
  Redis::StreamAddOptions add_options;
  add_options.with_entry_id = true;
  add_options.entry_id = Redis::NewStreamEntryID{123456, 0};
  std::vector<std::string> values1 = {"key1", "val1"};
  Redis::StreamEntryID id1;
  auto s = stream->Add(name, add_options, values1, &id1);
  EXPECT_TRUE(s.ok());

  s = stream->SetId(name, {id1.ms + 1, id1.seq}, std::nullopt, std::nullopt);
  EXPECT_TRUE(s.ok());

  uint64_t added = 10;
  s = stream->SetId(name, {id1.ms + 1, id1.seq}, added, std::nullopt);
  EXPECT_TRUE(s.ok());

  Redis::StreamInfo info;
  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.entries_added, added);

  added = 5;
  std::optional<Redis::StreamEntryID> max_del_id = Redis::StreamEntryID{5, 0};
  s = stream->SetId(name, {id1.ms + 1, id1.seq}, added, max_del_id);
  EXPECT_TRUE(s.ok());

  s = stream->GetStreamInfo(name, false, 0, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.entries_added, added);
  EXPECT_EQ(info.max_deleted_entry_id.ToString(), max_del_id->ToString());
}
