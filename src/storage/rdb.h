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
#include <utility>

#include "status.h"
#include "types/redis_zset.h"

// Redis object type
constexpr const int RDBTypeString = 0;
constexpr const int RDBTypeList = 1;
constexpr const int RDBTypeSet = 2;
constexpr const int RDBTypeZSet = 3;
constexpr const int RDBTypeHash = 4;
constexpr const int RDBTypeZSet2 = 5;

// Redis object encoding
constexpr const int RDBTypeHashZipMap = 9;
constexpr const int RDBTypeListZipList = 10;
constexpr const int RDBTypeSetIntSet = 11;
constexpr const int RDBTypeZSetZipList = 12;
constexpr const int RDBTypeHashZipList = 13;
constexpr const int RDBTypeListQuickList = 14;
constexpr const int RDBTypeStreamListPack = 15;
constexpr const int RDBTypeHashListPack = 16;
constexpr const int RDBTypeZSetListPack = 17;
constexpr const int RDBTypeListQuickList2 = 18;
constexpr const int RDBTypeStreamListPack2 = 19;
constexpr const int RDBTypeSetListPack = 20;

// Quick list node encoding
constexpr const int QuickListNodeContainerPlain = 1;
constexpr const int QuickListNodeContainerPacked = 2;

class RDB {
 public:
  explicit RDB(engine::Storage *storage, std::string ns, std::string_view input)
      : storage_(storage), ns_(std::move(ns)), input_(input){};
  ~RDB() = default;

  Status VerifyPayloadChecksum();
  StatusOr<int> LoadObjectType();
  Status Restore(const std::string &key, uint64_t ttl_ms);

  // String
  StatusOr<std::string> LoadStringObject();

  // List
  StatusOr<std::map<std::string, std::string>> LoadHashObject();
  StatusOr<std::map<std::string, std::string>> LoadHashWithZipMap();
  StatusOr<std::map<std::string, std::string>> LoadHashWithListPack();
  StatusOr<std::map<std::string, std::string>> LoadHashWithZipList();

  // Sorted Set
  StatusOr<std::vector<MemberScore>> LoadZSetObject(int type);
  StatusOr<std::vector<MemberScore>> LoadZSetWithListPack();
  StatusOr<std::vector<MemberScore>> LoadZSetWithZipList();

  // Set
  StatusOr<std::vector<std::string>> LoadSetObject();
  StatusOr<std::vector<std::string>> LoadSetWithIntSet();
  StatusOr<std::vector<std::string>> LoadSetWithListPack();

  // List
  StatusOr<std::vector<std::string>> LoadListObject();
  StatusOr<std::vector<std::string>> LoadListWithZipList();
  StatusOr<std::vector<std::string>> LoadListWithQuickList(int type);

 private:
  engine::Storage *storage_;
  std::string ns_;
  std::string_view input_;
  size_t pos_ = 0;

  StatusOr<std::string> loadLzfString();
  StatusOr<std::string> loadEncodedString();
  StatusOr<uint64_t> loadObjectLen(bool *is_encoded);
  Status peekOk(size_t n);
  StatusOr<double> loadBinaryDouble();
  StatusOr<double> loadDouble();
};
