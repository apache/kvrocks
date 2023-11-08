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

#include "common/string_stream.h"

#include <gtest/gtest.h>

#include <filesystem>

TEST(InputStringStream, Basic) {
  std::string data = "1234567890";
  InputStringStream stream(data);

  ASSERT_TRUE(stream.ReadableSize() == data.size());
  ASSERT_TRUE(stream.Data() == data.data());

  stream.Consume(5);
  ASSERT_TRUE(stream.ReadableSize() == data.size() - 5);
  ASSERT_TRUE(stream.Data() == data.data() + 5);

  stream.Consume(5);
  ASSERT_TRUE(stream.ReadableSize() == 0);

  ASSERT_THROW(stream.Consume(1), std::out_of_range);
}

TEST(InputStringStream, Read) {
  std::string data = "1234_ABCD";
  InputStringStream stream(data);

  ASSERT_TRUE(stream.Read(4) == "1234");
  ASSERT_TRUE(stream.Read(1) == "_");
  ASSERT_TRUE(stream.Read(4) == "ABCD");
  ASSERT_THROW(stream.Read(1), std::out_of_range);
}

template <typename T>
static void Append(std::string *str, T n) {
  str->append(reinterpret_cast<const char *>(&n), sizeof(T));
}

TEST(InputStringStream, ReadInt) {
  std::string data;

  int32_t i32 = 0x55555555;
  Append(&data, i32);

  uint64_t u64 = std::numeric_limits<uint64_t>::max();
  Append(&data, u64);

  int16_t i16 = std::numeric_limits<int16_t>::min();
  Append(&data, i16);

  uint8_t u8 = std::numeric_limits<uint8_t>::min();
  Append(&data, u8);

  InputStringStream stream(data);

  ASSERT_TRUE(stream.Read<decltype(i32)>() == i32);
  ASSERT_TRUE(stream.Read<decltype(u64)>() == u64);
  ASSERT_TRUE(stream.Read<decltype(i16)>() == i16);
  ASSERT_TRUE(stream.Read<decltype(u8)>() == u8);
  ASSERT_THROW(stream.Read(1), std::out_of_range);
}
