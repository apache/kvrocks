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
#include <iostream>

TEST(RdbFileStreamOpenTest, FileNotExist) {
  RdbFileStream reader("../../tests/testdata/not_exist.rdb");
  ASSERT_FALSE(reader.Open().IsOK());
  std::filesystem::path current_path = std::filesystem::current_path();
  std::cout << "cur path " << current_path.string() << std::endl;
  EXPECT_EQ(current_path.string(), "/home/runner/work/kvrocks/kvrocks");
}

TEST(RdbFileStreamReadTest, FileExist) {
  RdbFileStream reader("../../tests/testdata/empty.rdb");
  ASSERT_TRUE(reader.Open().IsOK());
}

TEST(RdbFileStreamReadTest, EmptyFile) {
  RdbFileStream reader("tests/testdata/empty.rdb");
  ASSERT_TRUE(reader.Open().IsOK());
  char buf[16] = {0};
  ASSERT_EQ(reader.Read(buf, 16).GetValue(), 0);
}

TEST(RdbFileStreamReadTest, LenLessThanMaxLen) {
  RdbFileStream reader("tests/testdata/version4.rdb");
  ASSERT_TRUE(reader.Open().IsOK());
  char buf[16] = {0};
  ASSERT_EQ(reader.Read(buf, 16).GetValue(), 0);
}

TEST(RdbFileStreamReadTest, LenLargeThanMaxLen) {
  RdbFileStream reader("tests/testdata/version4.rdb");
  ASSERT_TRUE(reader.Open().IsOK());
  char buf[16] = {0};
  ASSERT_EQ(reader.Read(buf, 16).GetValue(), 0);
}

TEST(RdbFileStreamReadTest, ExceedFileLen) {
  RdbFileStream reader("tests/testdata/version4.rdb");
  ASSERT_TRUE(reader.Open().IsOK());
  char buf[16] = {0};
  ASSERT_EQ(reader.Read(buf, 16).GetValue(), 0);
}