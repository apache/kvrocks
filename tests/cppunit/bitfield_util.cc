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

#include "common/bitfield_util.h"

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "common/encoding.h"

TEST(BitfieldUtil, Get) {
  std::vector<uint8_t> big_endian_bitmap{0x00, 0xff, 0x00, 0xff, 0x00, 0xff, 0x00, 0xff};
  std::vector<uint8_t> little_endian_bitmap(big_endian_bitmap);
  std::reverse(little_endian_bitmap.begin(), little_endian_bitmap.end());

  ArrayBitfieldBitmap bitfield(0);
  auto s = bitfield.Set(0, big_endian_bitmap.size(), big_endian_bitmap.data());

  for (int bits = 16; bits < 64; bits *= 2) {
    for (uint64_t offset = 0; bits + offset <= big_endian_bitmap.size() * 8; offset += bits) {
      uint64_t value = bitfield.GetUnsignedBitfield(offset, bits).GetValue();
      if (IsBigEndian()) {
        EXPECT_EQ(0, memcmp(&value, big_endian_bitmap.data(), bits / 8));
      } else {
        EXPECT_EQ(0, memcmp(&value, little_endian_bitmap.data(), bits / 8));
      }
    }
  }
}
