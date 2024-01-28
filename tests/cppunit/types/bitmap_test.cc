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

#include "test_base.h"
#include "types/redis_bitmap.h"
#include "types/redis_string.h"

class RedisBitmapTest : public TestBase {
 protected:
  explicit RedisBitmapTest() {
    bitmap_ = std::make_unique<redis::Bitmap>(storage_.get(), "bitmap_ns");
    string_ = std::make_unique<redis::String>(storage_.get(), "bitmap_ns");
  }
  ~RedisBitmapTest() override = default;

  void SetUp() override { key_ = "test_bitmap_key"; }
  void TearDown() override {}

  std::unique_ptr<redis::Bitmap> bitmap_;
  std::unique_ptr<redis::String> string_;
};

TEST_F(RedisBitmapTest, GetAndSetBit) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap_->GetBit(key_, offset, &bit);
    EXPECT_FALSE(bit);
    bitmap_->SetBit(key_, offset, true, &bit);
    bitmap_->GetBit(key_, offset, &bit);
    EXPECT_TRUE(bit);
  }
  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, BitCount) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap_->SetBit(key_, offset, true, &bit);
  }
  uint32_t cnt = 0;
  bitmap_->BitCount(key_, 0, 4 * 1024, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap_->BitCount(key_, 0, -1, &cnt);
  EXPECT_EQ(cnt, 6);
  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, BitCount2) {
  {
    bool bit = false;
    bitmap_->SetBit(key_, 0, true, &bit);
    EXPECT_FALSE(bit);
  }
  uint32_t cnt = 0;
  bitmap_->BitCount(key_, 0, 4 * 1024, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 0, 0, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 0, -1, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, -1, -1, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 1, 1, &cnt);
  EXPECT_EQ(cnt, 0);
  bitmap_->BitCount(key_, -10000, -10000, &cnt);
  EXPECT_EQ(cnt, 1);

  {
    bool bit = false;
    bitmap_->SetBit(key_, 5, true, &bit);
    EXPECT_FALSE(bit);
  }
  bitmap_->BitCount(key_, -10000, -10000, &cnt);
  EXPECT_EQ(cnt, 2);

  {
    bool bit = false;
    bitmap_->SetBit(key_, 8 * 1024 - 1, true, &bit);
    EXPECT_FALSE(bit);
  }

  bitmap_->BitCount(key_, 0, 1024, &cnt);
  EXPECT_EQ(cnt, 3);

  bitmap_->BitCount(key_, 0, 1023, &cnt);
  EXPECT_EQ(cnt, 3);

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, BitPosClearBit) {
  int64_t pos = 0;
  bool old_bit = false;
  for (int i = 0; i < 1024 + 16; i++) {
    bitmap_->BitPos(key_, false, 0, -1, true, &pos);
    EXPECT_EQ(pos, i);
    bitmap_->SetBit(key_, i, true, &old_bit);
    EXPECT_FALSE(old_bit);
  }
  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, BitPosSetBit) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 16, 3 * 1024 * 8, 3 * 1024 * 8 + 16};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap_->SetBit(key_, offset, true, &bit);
  }
  int64_t pos = 0;
  int start_indexes[] = {0, 1, 124, 1025, 1027, 3 * 1024 + 1};
  for (size_t i = 0; i < sizeof(start_indexes) / sizeof(start_indexes[0]); i++) {
    bitmap_->BitPos(key_, true, start_indexes[i], -1, true, &pos);
    EXPECT_EQ(pos, offsets[i]);
  }
  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, BitfieldGetSetTest) {
  constexpr uint32_t magic = 0xdeadbeef;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kUnsigned, 32).GetValue();
  op.offset = 114514;
  op.value = magic;

  EXPECT_TRUE(bitmap_->Bitfield(key_, {op}, &rets).ok());
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kGet;
  auto _ = op.encoding.SetBitsCount(1);

  // bitfield is stored in big-endian.
  for (int i = 31; i != -1; --i) {
    EXPECT_TRUE(bitmap_->Bitfield(key_, {op}, &rets).ok());
    EXPECT_EQ((magic >> i) & 1, rets[0].value());
    rets.clear();
    op.offset++;
  }

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, UnsignedBitfieldTest) {
  constexpr uint8_t bits = 5;
  static_assert(bits < 64);
  constexpr uint64_t max = (uint64_t(1) << bits) - 1;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kUnsigned, bits).GetValue();
  op.offset = 8189;  // the two bitmap segments divide the bitfield
  op.value = 0;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  for (uint64_t i = 1; i <= max; ++i) {
    op.value = int64_t(i);
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(i - 1, rets[0].value());
    rets.clear();
  }

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, SignedBitfieldTest) {
  constexpr uint8_t bits = 10;
  constexpr int64_t max = (uint64_t(1) << (bits - 1)) - 1;
  constexpr int64_t min = -max - 1;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kSigned, bits).GetValue();
  op.offset = 8189;  // the two bitmap segments divide the bitfield
  op.value = min;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  for (int64_t i = min + 1; i <= max; ++i) {
    op.value = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(i - 1, rets[0].value());
    rets.clear();
  }
  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, SignedBitfieldWrapSetTest) {
  constexpr uint8_t bits = 6;
  constexpr int64_t max = (int64_t(1) << (bits - 1)) - 1;
  constexpr int64_t min = -max - 1;
  constexpr int64_t loopback = int64_t(1) << bits;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kSigned, bits).GetValue();
  op.offset = 0;
  op.value = max;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kIncrBy;
  op.value = 1;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(min, rets[0].value());
  rets.clear();

  op.value = loopback;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(min, rets[0].value());
  rets.clear();

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, UnsignedBitfieldWrapSetTest) {
  constexpr uint8_t bits = 6;
  static_assert(bits < 64);
  constexpr uint64_t max = (uint64_t(1) << bits) - 1;
  constexpr int64_t loopback = int64_t(1) << bits;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kUnsigned, bits).GetValue();
  op.offset = 0;
  op.value = max;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kIncrBy;
  op.value = 1;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.value = loopback;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, SignedBitfieldSatSetTest) {
  constexpr uint8_t bits = 6;
  constexpr int64_t max = (int64_t(1) << (bits - 1)) - 1;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kSigned, bits).GetValue();
  op.overflow = BitfieldOverflowBehavior::kSat;
  op.offset = 0;
  op.value = max * 2;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kGet;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(max, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kIncrBy;
  for (int64_t i = 0; i <= max + 10; ++i) {
    op.value = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(max, rets[0].value());
    rets.clear();
  }

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, UnsignedBitfieldSatSetTest) {
  constexpr uint8_t bits = 6;
  static_assert(bits < 64);
  constexpr uint64_t max = (uint64_t(1) << bits) - 1;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kUnsigned, bits).GetValue();
  op.overflow = BitfieldOverflowBehavior::kSat;
  op.offset = 0;
  op.value = max * 2;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kGet;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(max, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kIncrBy;
  for (int64_t i = 0; i <= int64_t(max) + 10; ++i) {
    op.value = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(max, rets[0].value());
    rets.clear();
  }

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, SignedBitfieldFailSetTest) {
  constexpr uint8_t bits = 5;
  constexpr int64_t max = (int64_t(1) << (bits - 1)) - 1;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kSigned, bits).GetValue();
  op.overflow = BitfieldOverflowBehavior::kFail;
  op.offset = 0;
  op.value = max * 2;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_FALSE(rets[0].has_value());
  rets.clear();

  op.value = max;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kIncrBy;
  for (int64_t i = 1; i <= max; ++i) {
    op.value = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_FALSE(rets[0].has_value());
    rets.clear();
  }

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, UnsignedBitfieldFailSetTest) {
  constexpr uint8_t bits = 5;
  constexpr int64_t max = (int64_t(1) << bits) - 1;

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kSet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kUnsigned, bits).GetValue();
  op.overflow = BitfieldOverflowBehavior::kFail;
  op.offset = 0;
  op.value = max * 2;

  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_FALSE(rets[0].has_value());
  rets.clear();

  op.value = max;
  bitmap_->Bitfield(key_, {op}, &rets);
  EXPECT_EQ(1, rets.size());
  EXPECT_EQ(0, rets[0].value());
  rets.clear();

  op.type = BitfieldOperation::Type::kIncrBy;
  for (int64_t i = 1; i <= max; ++i) {
    op.value = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_FALSE(rets[0].has_value());
    rets.clear();
  }

  auto s = bitmap_->Del(key_);
}

TEST_F(RedisBitmapTest, BitfieldStringGetSetTest) {
  std::string str = "dan yuan ren chang jiu, qian li gong chan juan.";
  string_->Set(key_, str);

  std::vector<std::optional<BitfieldValue>> rets;

  BitfieldOperation op;
  op.type = BitfieldOperation::Type::kGet;
  op.encoding = BitfieldEncoding::Create(BitfieldEncoding::Type::kSigned, 8).GetValue();

  int i = 0;
  for (auto ch : str) {
    op.offset = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(ch, rets[0].value());
    rets.clear();
    i += 8;
  }

  for (; static_cast<size_t>(i) <= str.size() + 10; i += 8) {
    op.offset = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(0, rets[0].value());
    rets.clear();
  }

  // reverse all i8 in bitmap.
  op.type = BitfieldOperation::Type::kSet;
  for (int l = 0, r = static_cast<int>(str.size() - 1); l < r; ++l, --r) {
    op.offset = l * 8;
    op.value = str[r];
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(str[l], rets[0].value());
    rets.clear();

    op.offset = r * 8;
    op.value = str[l];
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(str[r], rets[0].value());
    rets.clear();
  }
  std::reverse(str.begin(), str.end());

  // check reversed string.
  i = 0;
  op.type = BitfieldOperation::Type::kGet;
  for (auto ch : str) {
    op.offset = i;
    bitmap_->Bitfield(key_, {op}, &rets);
    EXPECT_EQ(1, rets.size());
    EXPECT_EQ(ch, rets[0].value());
    rets.clear();
    i += 8;
  }
}
