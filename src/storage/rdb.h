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
#pragma once

#include <string_view>

#include "status.h"
// Redis object type
constexpr const int RDB_TYPE_STRING = 0;
constexpr const int RDB_TYPE_LIST = 1;
constexpr const int RDB_TYPE_SET = 2;
constexpr const int RDB_TYPE_ZSET = 3;
constexpr const int RDB_TYPE_HASH = 4;
constexpr const int RDB_TYPE_ZSET2 = 5;

class RDB {
 public:
  explicit RDB(std::string_view input) : buffer_(input){};
  ~RDB() = default;

  Status VerifyPayloadChecksum();
  StatusOr<int> LoadObjectType();
  StatusOr<std::string> LoadObject(int type);

 private:
  std::string_view buffer_;
  size_t pos_ = 0;

  StatusOr<std::string> loadLzfString();
  StatusOr<std::string> loadEncodedString();
  StatusOr<uint64_t> loadObjectLen(bool *is_encoded);
  Status peekOk(size_t n);
};