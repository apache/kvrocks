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
#include "common/rdb_stream.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "rdb_util.h"

TEST(RdbFileStreamOpenTest, FileNotExist) {
  RdbFileStream reader("not_exist.rdb");
  ASSERT_FALSE(reader.Open().IsOK());
}

TEST(RdbFileStreamOpenTest, FileExist) {
  std::string test_file = "hash-zipmap.rdb";
  ScopedTestRDBFile temp(test_file, hash_zipmap_payload, sizeof(hash_zipmap_payload) - 1);
  RdbFileStream reader(test_file);
  ASSERT_TRUE(reader.Open().IsOK());
}

TEST(RdbFileStreamReadTest, ReadRdb) {
  const std::string test_file = "encodings.rdb";
  ScopedTestRDBFile temp(test_file, encodings_rdb_payload, sizeof(encodings_rdb_payload) - 1);

  std::ifstream file(test_file, std::ios::binary | std::ios::ate);
  std::streamsize size = file.tellg();
  file.close();

  RdbFileStream reader(test_file);
  ASSERT_TRUE(reader.Open().IsOK());

  char buf[16] = {0};
  ASSERT_TRUE(reader.Read(buf, 5).IsOK());
  ASSERT_EQ(strncmp(buf, "REDIS", 5), 0);
  size -= 5;

  auto len = static_cast<std::streamsize>(sizeof(buf) / sizeof(buf[0]));
  while (size >= len) {
    ASSERT_TRUE(reader.Read(buf, len).IsOK());
    size -= len;
  }

  if (size > 0) {
    ASSERT_TRUE(reader.Read(buf, size).IsOK());
  }
}

TEST(RdbFileStreamReadTest, ReadRdbLittleChunk) {
  const std::string test_file = "encodings.rdb";
  ScopedTestRDBFile temp(test_file, encodings_rdb_payload, sizeof(encodings_rdb_payload) - 1);

  std::ifstream file(test_file, std::ios::binary | std::ios::ate);
  std::streamsize size = file.tellg();
  file.close();

  RdbFileStream reader(test_file, 16);
  ASSERT_TRUE(reader.Open().IsOK());
  char buf[32] = {0};
  auto len = static_cast<std::streamsize>(sizeof(buf) / sizeof(buf[0]));

  while (size >= len) {
    ASSERT_TRUE(reader.Read(buf, len).IsOK());
    size -= len;
  }

  if (size > 0) {
    ASSERT_TRUE(reader.Read(buf, size).IsOK());
  }
}
