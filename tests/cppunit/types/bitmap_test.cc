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

class RedisBitmapTest : public TestFixture, public ::testing::TestWithParam<bool> {
 protected:
  explicit RedisBitmapTest() {
    bitmap_ = std::make_unique<redis::Bitmap>(storage_.get(), "bitmap_ns");
    string_ = std::make_unique<redis::String>(storage_.get(), "bitmap_ns");
  }
  ~RedisBitmapTest() override = default;

  void SetUp() override {
    key_ = "test_bitmap_key";
    if (bool use_bitmap = GetParam(); !use_bitmap) {
      // Set an empty string.
      string_->Set(key_, "");
    }
  }
  void TearDown() override {
    [[maybe_unused]] auto s = bitmap_->Del(key_);
    s = string_->Del(key_);
  }

  std::unique_ptr<redis::Bitmap> bitmap_;
  std::unique_ptr<redis::String> string_;
};

INSTANTIATE_TEST_SUITE_P(UseBitmap, RedisBitmapTest, testing::Values(true, false));

TEST_P(RedisBitmapTest, GetAndSetBit) {
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

TEST_P(RedisBitmapTest, BitCount) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap_->SetBit(key_, offset, true, &bit);
  }
  uint32_t cnt = 0;
  bitmap_->BitCount(key_, 0, 4 * 1024, false, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap_->BitCount(key_, 0, -1, false, &cnt);
  EXPECT_EQ(cnt, 6);
  auto s = bitmap_->Del(key_);
}

TEST_P(RedisBitmapTest, BitCountNegative) {
  {
    bool bit = false;
    bitmap_->SetBit(key_, 0, true, &bit);
    EXPECT_FALSE(bit);
  }
  uint32_t cnt = 0;
  bitmap_->BitCount(key_, 0, 4 * 1024, false, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 0, 0, false, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 0, -1, false, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, -1, -1, false, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 1, 1, false, &cnt);
  EXPECT_EQ(cnt, 0);
  bitmap_->BitCount(key_, -10000, -10000, false, &cnt);
  EXPECT_EQ(cnt, 1);

  {
    bool bit = false;
    bitmap_->SetBit(key_, 5, true, &bit);
    EXPECT_FALSE(bit);
  }
  bitmap_->BitCount(key_, -10000, -10000, false, &cnt);
  EXPECT_EQ(cnt, 2);

  {
    bool bit = false;
    bitmap_->SetBit(key_, 8 * 1024 - 1, true, &bit);
    EXPECT_FALSE(bit);
    bitmap_->SetBit(key_, 8 * 1024, true, &bit);
    EXPECT_FALSE(bit);
  }

  bitmap_->BitCount(key_, 0, 1024, false, &cnt);
  EXPECT_EQ(cnt, 4);

  bitmap_->BitCount(key_, 0, 1023, false, &cnt);
  EXPECT_EQ(cnt, 3);

  auto s = bitmap_->Del(key_);
}

TEST_P(RedisBitmapTest, BitCountBITOption) {
  std::set<uint32_t> offsets = {0, 100, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap_->SetBit(key_, offset, true, &bit);
  }

  for (uint32_t bit_offset = 0; bit_offset <= 3 * 1024 * 8 + 10; ++bit_offset) {
    uint32_t cnt = 0;
    EXPECT_TRUE(bitmap_->BitCount(key_, bit_offset, bit_offset, true, &cnt).ok());
    if (offsets.count(bit_offset) > 0) {
      ASSERT_EQ(1, cnt) << "bit_offset: " << bit_offset;
    } else {
      ASSERT_EQ(0, cnt) << "bit_offset: " << bit_offset;
    }
  }

  uint32_t cnt = 0;
  bitmap_->BitCount(key_, 0, 4 * 1024 * 8, true, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap_->BitCount(key_, 0, -1, true, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap_->BitCount(key_, 0, 3 * 1024 * 8 + 1, true, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap_->BitCount(key_, 1, 3 * 1024 * 8 + 1, true, &cnt);
  EXPECT_EQ(cnt, 5);
  bitmap_->BitCount(key_, 0, 0, true, &cnt);
  EXPECT_EQ(cnt, 1);
  bitmap_->BitCount(key_, 0, 100, true, &cnt);
  EXPECT_EQ(cnt, 2);
  bitmap_->BitCount(key_, 100, 1024 * 8, true, &cnt);
  EXPECT_EQ(cnt, 2);
  bitmap_->BitCount(key_, 100, 3 * 1024 * 8, true, &cnt);
  EXPECT_EQ(cnt, 4);
  bitmap_->BitCount(key_, -1, -1, true, &cnt);
  EXPECT_EQ(cnt, 0);  // NOTICE: the min storage unit is byte, the result is the same as Redis.
  auto s = bitmap_->Del(key_);
}

TEST_P(RedisBitmapTest, BitPosClearBit) {
  int64_t pos = 0;
  bool old_bit = false;
  bool use_bitmap = GetParam();
  for (int i = 0; i < 1024 + 16; i++) {
    /// ```
    /// redis> set k1 ""
    /// "OK"
    /// redis> bitpos k1 0
    /// (integer) -1
    /// redis> bitpos k2 0
    /// (integer) 0
    /// ```
    ///
    /// String will set a empty string value when initializing, so, when first
    /// querying, it should return -1.
    bitmap_->BitPos(key_, false, 0, -1, /*stop_given=*/false, &pos);
    if (i == 0 && !use_bitmap) {
      EXPECT_EQ(pos, -1);
    } else {
      EXPECT_EQ(pos, i);
    }

    bitmap_->SetBit(key_, i, true, &old_bit);
    EXPECT_FALSE(old_bit);
  }
  auto s = bitmap_->Del(key_);
}

TEST_P(RedisBitmapTest, BitPosSetBit) {
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

TEST_P(RedisBitmapTest, BitPosNegative) {
  {
    bool bit = false;
    bitmap_->SetBit(key_, 8 * 1024 - 1, true, &bit);
    EXPECT_FALSE(bit);
  }
  int64_t pos = 0;
  // First bit is negative
  bitmap_->BitPos(key_, false, 0, -1, true, &pos);
  EXPECT_EQ(0, pos);
  // 8 * 1024 - 1 bit is positive
  bitmap_->BitPos(key_, true, 0, -1, true, &pos);
  EXPECT_EQ(8 * 1024 - 1, pos);
  // First bit in 1023 byte is negative
  bitmap_->BitPos(key_, false, -1, -1, true, &pos);
  EXPECT_EQ(8 * 1023, pos);
  // Last Bit in 1023 byte is positive
  bitmap_->BitPos(key_, true, -1, -1, true, &pos);
  EXPECT_EQ(8 * 1024 - 1, pos);
  // Large negative number will be normalized.
  bitmap_->BitPos(key_, false, -10000, -10000, true, &pos);
  EXPECT_EQ(0, pos);

  auto s = bitmap_->Del(key_);
}

// When `stop_given` is true, even searching for 0,
// we cannot exceeds the stop position.
TEST_P(RedisBitmapTest, BitPosStopGiven) {
  for (int i = 0; i < 8; ++i) {
    bool bit = true;
    bitmap_->SetBit(key_, i, true, &bit);
    EXPECT_FALSE(bit);
  }
  int64_t pos = 0;
  bitmap_->BitPos(key_, false, 0, 0, /*stop_given=*/true, &pos);
  EXPECT_EQ(-1, pos);
  bitmap_->BitPos(key_, false, 0, 0, /*stop_given=*/false, &pos);
  EXPECT_EQ(8, pos);

  // Set a bit at 8 not affect that
  {
    bool bit = true;
    bitmap_->SetBit(key_, 8, true, &bit);
    EXPECT_FALSE(bit);
  }
  bitmap_->BitPos(key_, false, 0, 0, /*stop_given=*/true, &pos);
  EXPECT_EQ(-1, pos);
  bitmap_->BitPos(key_, false, 0, 1, /*stop_given=*/false, &pos);
  EXPECT_EQ(9, pos);

  auto s = bitmap_->Del(key_);
}

TEST_P(RedisBitmapTest, BitfieldGetSetTest) {
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

TEST_P(RedisBitmapTest, UnsignedBitfieldTest) {
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

TEST_P(RedisBitmapTest, SignedBitfieldTest) {
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

TEST_P(RedisBitmapTest, SignedBitfieldWrapSetTest) {
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

TEST_P(RedisBitmapTest, UnsignedBitfieldWrapSetTest) {
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

TEST_P(RedisBitmapTest, SignedBitfieldSatSetTest) {
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

TEST_P(RedisBitmapTest, UnsignedBitfieldSatSetTest) {
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

TEST_P(RedisBitmapTest, SignedBitfieldFailSetTest) {
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

TEST_P(RedisBitmapTest, UnsignedBitfieldFailSetTest) {
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

TEST_P(RedisBitmapTest, BitfieldStringGetSetTest) {
  if (bool use_bitmap = GetParam(); use_bitmap) {
    GTEST_SKIP() << "skip bitmap test for BitfieldStringGetSetTest";
  }
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
