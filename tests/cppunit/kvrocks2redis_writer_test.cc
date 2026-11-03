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

#include <gtest/gtest.h>

#include <string>
#include <string_view>

#include "kvrocks2redis/redis_writer.h"
#include "server/redis_reply.h"

TEST(Kvrocks2RedisWriter, ParseCommands) {
  auto set = redis::ArrayOfBulkStrings({"SET", "key_49", "value_49"});
  auto del = redis::ArrayOfBulkStrings({"DEL", "key_49"});
  auto aof = set + del;

  auto complete_length = [](std::string_view data) {
    auto commands = ParseCommands(data);
    EXPECT_TRUE(commands) << commands.Msg();
    return commands ? commands->size() : 0;
  };

  EXPECT_EQ(aof.size(), complete_length(aof));
  EXPECT_EQ(0, complete_length(""));

  // A command that is still being appended must be left out, whether or not
  // complete commands precede it in the same read.
  for (size_t i = 1; i < set.size(); i++) {
    EXPECT_EQ(0, complete_length(std::string_view(aof).substr(0, i))) << "truncated at offset " << i;
  }
  for (size_t i = 1; i < del.size(); i++) {
    EXPECT_EQ(set.size(), complete_length(std::string_view(aof).substr(0, set.size() + i)))
        << "truncated at offset " << set.size() + i;
  }

  EXPECT_FALSE(ParseCommands("+OK" CRLF));
  EXPECT_FALSE(ParseCommands("*2" CRLF "+OK" CRLF));
  EXPECT_FALSE(ParseCommands("*x" CRLF "$3" CRLF "SET" CRLF));
}
