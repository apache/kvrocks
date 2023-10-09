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

#include "storage/rdb.h"

#include <gtest/gtest.h>

#include <cmath>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/rdb_stream.h"
#include "config/config.h"
#include "storage/storage.h"
#include "types/redis_hash.h"
#include "types/redis_list.h"
#include "types/redis_set.h"
#include "types/redis_string.h"
#include "types/redis_zset.h"
#include "vendor/crc64.h"

const std::string test_data_path = "../tests/testdata/";
const std::string test_config = "test.conf";

class RDBTest : public testing::Test {
 public:
  RDBTest(const RDBTest &) = delete;
  RDBTest &operator=(const RDBTest &) = delete;

 protected:
  explicit RDBTest() : storage_(nullptr), ns_(kDefaultNamespace) {}
  ~RDBTest() override {
    if (nullptr != storage_) {
      delete storage_;
    }
  }
  void SetUp() override {
    crc64_init();

    std::string test_config_path = test_data_path + test_config;
    auto s = config_.Load(CLIOptions(test_config_path));
    ASSERT_TRUE(s.IsOK());

    ASSERT_TRUE(clearDBDir(config_.db_dir));

    storage_ = new engine::Storage(&config_);
    s = storage_->Open();
    ASSERT_TRUE(s.IsOK());
  }

  void TearDown() override { ASSERT_TRUE(clearDBDir(config_.db_dir)); }

  static std::string getRdbFullPath(const std::string &name) { return test_data_path + name; }

  void loadRdb(const std::string &path) {
    auto stream_ptr = std::make_shared<RdbFileStream>(path);
    auto s = stream_ptr->Open();
    ASSERT_TRUE(s.IsOK());

    RDB rdb(storage_, &config_, ns_, stream_ptr);
    s = rdb.LoadRdb();
    ASSERT_TRUE(s.IsOK());
  }

  void stringCheck(const std::string &key, const std::string &expect) {
    redis::String string_db(storage_, ns_);
    std::string value;
    auto s = string_db.Get(key, &value);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(expect == value);
  }

  void setCheck(const std::string &key, const std::vector<std::string> &expect) {
    redis::Set set_db(storage_, ns_);
    std::vector<std::string> members;
    auto s = set_db.Members(key, &members);

    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(expect == members);
  }

  void hashCheck(const std::string &key, const std::map<std::string, std::string> &expect) {
    redis::Hash hash_db(storage_, ns_);
    std::vector<FieldValue> field_values;
    auto s = hash_db.GetAll(key, &field_values);
    ASSERT_TRUE(s.ok());

    // size check
    ASSERT_TRUE(field_values.size() == expect.size());
    for (const auto &p : field_values) {
      auto iter = expect.find(p.field);
      if (iter == expect.end()) {
        ASSERT_TRUE(false);
      }
      ASSERT_TRUE(iter->second == p.value);
    }
  }

  void listCheck(const std::string &key, const std::vector<std::string> &expect) {
    redis::List list_db(storage_, ns_);
    std::vector<std::string> values;
    auto s = list_db.Range(key, 0, -1, &values);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(expect == values);
  }

  void zsetCheck(const std::string &key, const std::vector<MemberScore> &expect) {
    redis::ZSet zset_db(storage_, ns_);
    std::vector<MemberScore> member_scores;

    RangeRankSpec spec;
    auto s = zset_db.RangeByRank(key, spec, &member_scores, nullptr);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(expect.size() == member_scores.size());
    for (size_t i = 0; i < expect.size(); ++i) {
      ASSERT_TRUE(expect[i].member == member_scores[i].member);
      ASSERT_TRUE(std::fabs(expect[i].score - member_scores[i].score) < 0.000001);
    }
  }

  rocksdb::Status exists(const std::string &key) {
    int cnt = 0;
    std::vector<rocksdb::Slice> keys;
    keys.emplace_back(key);
    redis::Database redis(storage_, ns_);
    auto s = redis.Exists(keys, &cnt);
    if (!s.ok()) {
      return s;
    }
    if (cnt == 0) {
      return rocksdb::Status::NotFound();
    }
    return rocksdb::Status::OK();
  }

  void flushDB() {
    redis::Database redis(storage_, ns_);
    auto s = redis.FlushDB();
    ASSERT_TRUE(s.ok());
  }

  void encodingDataCheck();

  engine::Storage *storage_;
  std::string ns_;
  Config config_;

 private:
  static bool clearDBDir(const std::string &path) {
    try {
      std::filesystem::remove_all(path);
    } catch (std::filesystem::filesystem_error &e) {
      return false;
    }
    return true;
  }
};

void RDBTest::encodingDataCheck() {
  // string
  stringCheck("compressible",
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
  stringCheck("string", "Hello World");
  stringCheck("number", "10");

  // list
  std::vector<std::string> list_expect = {"1", "2", "3", "a", "b", "c", "100000", "6000000000",
                                          "1", "2", "3", "a", "b", "c", "100000", "6000000000",
                                          "1", "2", "3", "a", "b", "c", "100000", "6000000000"};
  listCheck("list", list_expect);

  std::vector<std::string> list_zipped_expect = {"1", "2", "3", "a", "b", "c", "100000", "6000000000"};
  listCheck("list_zipped", list_zipped_expect);

  // set
  std::vector<std::string> set_expect = {"1", "100000", "2", "3", "6000000000", "a", "b", "c"};
  setCheck("set", set_expect);

  std::vector<std::string> set_zipped_1_expect = {"1", "2", "3", "4"};
  setCheck("set_zipped_1", set_zipped_1_expect);

  std::vector<std::string> set_zipped_2_expect = {"100000", "200000", "300000", "400000"};
  setCheck("set_zipped_2", set_zipped_2_expect);

  std::vector<std::string> set_zipped_3_expect = {"1000000000", "2000000000", "3000000000",
                                                  "4000000000", "5000000000", "6000000000"};
  setCheck("set_zipped_3", set_zipped_3_expect);

  // hash
  std::map<std::string, std::string> hash_expect = {{"a", "1"},     {"aa", "10"},   {"aaa", "100"},       {"b", "2"},
                                                    {"bb", "20"},   {"bbb", "200"}, {"c", "3"},           {"cc", "30"},
                                                    {"ccc", "300"}, {"ddd", "400"}, {"eee", "5000000000"}};
  hashCheck("hash", hash_expect);

  std::map<std::string, std::string> hash_zipped_expect = {
      {"a", "1"},
      {"b", "2"},
      {"c", "3"},
  };
  hashCheck("hash_zipped", hash_zipped_expect);

  // zset
  std::vector<MemberScore> zset_expect = {
      {"a", 1},     {"b", 2},     {"c", 3},     {"aa", 10},     {"bb", 20},          {"cc", 30},
      {"aaa", 100}, {"bbb", 200}, {"ccc", 300}, {"aaaa", 1000}, {"cccc", 123456789}, {"bbbb", 5000000000}};
  zsetCheck("zset", zset_expect);

  std::vector<MemberScore> zset_zipped_expect = {
      {"a", 1},
      {"b", 2},
      {"c", 3},
  };
  zsetCheck("zset_zipped", zset_zipped_expect);
}

TEST_F(RDBTest, LoadEncodings) {
  for (const auto &entry : std::filesystem::directory_iterator(test_data_path)) {
    std::string filename = entry.path().filename().string();
    if (filename.rfind("encodings", 0) != 0) {
      continue;
    }
    auto full_path = getRdbFullPath(filename);
    loadRdb(full_path);
    encodingDataCheck();
    flushDB();
  }
}

TEST_F(RDBTest, LoadHashZipMap) {
  auto full_path = getRdbFullPath("hash-zipmap.rdb");
  loadRdb(full_path);

  // hash
  std::map<std::string, std::string> hash_expect = {
      {"f1", "v1"},
      {"f2", "v2"},
  };
  hashCheck("hash", hash_expect);
}

TEST_F(RDBTest, LoadHashZipList) {
  auto full_path = getRdbFullPath("hash-ziplist.rdb");
  loadRdb(full_path);

  // hash
  std::map<std::string, std::string> hash_expect = {
      {"f1", "v1"},
      {"f2", "v2"},
  };
  hashCheck("hash", hash_expect);
}

TEST_F(RDBTest, LoadListQuickList) {
  auto full_path = getRdbFullPath("list-quicklist.rdb");
  loadRdb(full_path);

  // list
  std::vector<std::string> list_expect = {"7"};
  listCheck("list", list_expect);
}

TEST_F(RDBTest, LoadZSetZipList) {
  auto full_path = getRdbFullPath("zset-ziplist.rdb");
  loadRdb(full_path);

  // zset
  std::vector<MemberScore> zset_expect = {
      {"one", 1},
      {"two", 2},
  };
  zsetCheck("zset", zset_expect);
}

TEST_F(RDBTest, LoadEmptyKeys) {
  auto full_path = getRdbFullPath("corrupt_empty_keys.rdb");
  loadRdb(full_path);

  /* corrupt_empty_keys.rdb contains 9 keys with empty value:
   "set"  "hash" "list_ziplist" "zset" "zset_listpack" "hash_ziplist" "list_quicklist" "zset_ziplist"
   "list_quicklist_empty_ziplist"
  */

  // string
  rocksdb::Status s = exists("empty_string");  // empty_string not exist in rdb file
  ASSERT_TRUE(s.IsNotFound());

  // list
  s = exists("list_ziplist");
  ASSERT_TRUE(s.IsNotFound());

  s = exists("list_quicklist");
  ASSERT_TRUE(s.IsNotFound());

  s = exists("list_quicklist_empty_ziplist");

  // set
  s = exists("set");
  ASSERT_TRUE(s.IsNotFound());

  // hash
  s = exists("hash");
  ASSERT_TRUE(s.IsNotFound());

  s = exists("hash_ziplist");
  ASSERT_TRUE(s.IsNotFound());

  // zset
  s = exists("zset");
  ASSERT_TRUE(s.IsNotFound());

  s = exists("zset_ziplist");
  ASSERT_TRUE(s.IsNotFound());

  s = exists("zset_listpack");
  ASSERT_TRUE(s.IsNotFound());
}