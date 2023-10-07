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

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "config/config.h"
#include "status.h"
#include "types/redis_zset.h"

// Redis object type
constexpr const int RDBTypeString = 0;
constexpr const int RDBTypeList = 1;
constexpr const int RDBTypeSet = 2;
constexpr const int RDBTypeZSet = 3;
constexpr const int RDBTypeHash = 4;
constexpr const int RDBTypeZSet2 = 5;
// NOTE: when adding new Redis object type, update LoadObjectType.

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
// NOTE: when adding new Redis object encoding type, update isObjectType.

// Quick list node encoding
constexpr const int QuickListNodeContainerPlain = 1;
constexpr const int QuickListNodeContainerPacked = 2;

class RdbStream;

using RedisObjValue =
    std::variant<std::string, std::vector<std::string>, std::vector<MemberScore>, std::map<std::string, std::string>>;

class RDB {
 public:
  explicit RDB(engine::Storage *storage, Config *config, std::string ns, std::shared_ptr<RdbStream> stream)
      : storage_(storage), config_(config), ns_(std::move(ns)), stream_(std::move(stream)){};
  ~RDB() = default;

  Status VerifyPayloadChecksum(const std::string_view &payload);
  StatusOr<int> LoadObjectType();
  Status Restore(const std::string &key, std::string_view payload, uint64_t ttl_ms);

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

  // Load rdb
  Status LoadRdb();

 private:
  engine::Storage *storage_;
  Config *config_;
  std::string ns_;
  std::shared_ptr<RdbStream> stream_;

  StatusOr<std::string> loadLzfString();
  StatusOr<std::string> loadEncodedString();
  StatusOr<uint64_t> loadObjectLen(bool *is_encoded);
  StatusOr<double> loadBinaryDouble();
  StatusOr<double> loadDouble();

  StatusOr<int> loadRdbType();
  StatusOr<RedisObjValue> loadRdbObject(int rdbtype, const std::string &key);
  Status saveRdbObject(int type, const std::string &key, const RedisObjValue &obj, uint64_t ttl_ms);
  StatusOr<uint32_t> loadTime();
  StatusOr<uint64_t> loadMillisecondTime(int rdb_version);
  Status addNamespace(const std::string &ns, const std::string &token);

  /*0-5 is the basic type of Redis objects and 9-21 is the encoding type of Redis objects.
   Redis allow basic is 0-7 and 6/7 is for the module type which we don't support here.*/
  static bool isObjectType(int type) { return (type >= 0 && type <= 5) || (type >= 9 && type <= 21); };
  static bool isEmptyRedisObject(const RedisObjValue &value);
};
