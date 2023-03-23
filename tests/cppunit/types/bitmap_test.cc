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

class RedisBitmapTest : public TestBase {
 protected:
  explicit RedisBitmapTest() { bitmap = std::make_unique<Redis::Bitmap>(storage_, "bitmap_ns"); }
  ~RedisBitmapTest() override = default;

  void SetUp() override { key_ = "test_bitmap_key"; }
  void TearDown() override {}

  std::unique_ptr<Redis::Bitmap> bitmap;
};

TEST_F(RedisBitmapTest, GetAndSetBit) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap->GetBit(key_, offset, &bit);
    EXPECT_FALSE(bit);
    bitmap->SetBit(key_, offset, true, &bit);
    bitmap->GetBit(key_, offset, &bit);
    EXPECT_TRUE(bit);
  }
  bitmap->Del(key_);
}

TEST_F(RedisBitmapTest, BitCount) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 1, 3 * 1024 * 8, 3 * 1024 * 8 + 1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap->SetBit(key_, offset, true, &bit);
  }
  uint32_t cnt = 0;
  bitmap->BitCount(key_, 0, 4 * 1024, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap->BitCount(key_, 0, -1, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap->Del(key_);
}

TEST_F(RedisBitmapTest, BitPosClearBit) {
  int64_t pos = 0;
  bool old_bit = false;
  for (int i = 0; i < 1024 + 16; i++) {
    bitmap->BitPos(key_, false, 0, -1, true, &pos);
    EXPECT_EQ(pos, i);
    bitmap->SetBit(key_, i, true, &old_bit);
    EXPECT_FALSE(old_bit);
  }
  bitmap->Del(key_);
}

TEST_F(RedisBitmapTest, BitPosSetBit) {
  uint32_t offsets[] = {0, 123, 1024 * 8, 1024 * 8 + 16, 3 * 1024 * 8, 3 * 1024 * 8 + 16};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap->SetBit(key_, offset, true, &bit);
  }
  int64_t pos = 0;
  int start_indexes[] = {0, 1, 124, 1025, 1027, 3 * 1024 + 1};
  for (size_t i = 0; i < sizeof(start_indexes) / sizeof(start_indexes[0]); i++) {
    bitmap->BitPos(key_, true, start_indexes[i], -1, true, &pos);
    EXPECT_EQ(pos, offsets[i]);
  }
  bitmap->Del(key_);
}
