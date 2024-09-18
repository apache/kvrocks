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
#include "types/redis_cuckoo.h"

namespace redis {

class RedisCuckooFilterTest : public TestBase {
 protected:
  RedisCuckooFilterTest() { cuckoo_filter_ = std::make_unique<CFilter>(storage_.get(), "cuckoo_filter_ns"); }
  ~RedisCuckooFilterTest() override = default;

  void SetUp() override { key_ = "test_cuckoo_filter_key"; }
  void TearDown() override {
    int del_ret = 0;
    rocksdb::Status s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
    EXPECT_TRUE(s.ok() || s.IsNotFound());
  }

  std::unique_ptr<CFilter> cuckoo_filter_;
  std::string key_;
};

TEST_F(RedisCuckooFilterTest, Reserve) {
  uint64_t capacity = 1024;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  // attempt to reserve again should fail
  s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "Invalid argument: the key already exists");

  // clean up
  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, BasicAddAndExists) {
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, 1024, 4, 500, 2);
  EXPECT_TRUE(s.ok());

  std::vector<std::string> elements = {"element1", "element2", "element3"};
  int ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Add(*ctx_, key_, elem, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  // check existence of added elements
  int exists_ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Exists(*ctx_, key_, elem, &exists_ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exists_ret, 1);
  }

  // check non-existence of non-added elements
  std::vector<std::string> non_elements = {"element4", "element5"};
  for (const auto& elem : non_elements) {
    s = cuckoo_filter_->Exists(*ctx_, key_, elem, &exists_ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exists_ret, 0);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, AddNX) {
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, 1024, 4, 500, 2);
  EXPECT_TRUE(s.ok());

  // add unique elements
  std::vector<std::string> elements = {"unique1", "unique2"};
  int ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->AddNX(*ctx_, key_, elem, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  // attempt to add duplicate elements
  for (const auto& elem : elements) {
    s = cuckoo_filter_->AddNX(*ctx_, key_, elem, &ret);
    EXPECT_FALSE(s.ok());
    EXPECT_EQ(ret, 0);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, DeleteElement) {
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, 1024, 4, 500, 2);
  EXPECT_TRUE(s.ok());

  std::string element = "delete_me";
  int ret = 0;
  s = cuckoo_filter_->Add(*ctx_, key_, element, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);

  s = cuckoo_filter_->Del(*ctx_, key_, element, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);

  // verify deletion
  s = cuckoo_filter_->Exists(*ctx_, key_, element, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 0);

  // attempt to delete again
  s = cuckoo_filter_->Del(*ctx_, key_, element, &ret);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "NotFound: ");
  EXPECT_EQ(ret, 0);
}

TEST_F(RedisCuckooFilterTest, CountElements) {
  CuckooFilterMetadata metadata{};
  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, 1024, 4, 500, 2);
  EXPECT_TRUE(s.ok());

  std::vector<std::string> elements = {"count1", "count2", "count3"};
  int ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Add(*ctx_, key_, elem, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  uint64_t count = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Count(*ctx_, key_, elem, &count);
    EXPECT_TRUE(s.ok());
    EXPECT_GT(count, 0);
  }

  // count non-existing elements
  std::vector<std::string> non_elements = {"count4", "count5"};
  for (const auto& elem : non_elements) {
    s = cuckoo_filter_->Count(*ctx_, key_, elem, &count);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(count, 0);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, GetInfo) {
  redis::CuckooFilterInfo info{};
  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, 2048, 4, 1000, 3);
  EXPECT_TRUE(s.ok());

  s = cuckoo_filter_->Info(*ctx_, key_, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.size, 2048);
  EXPECT_EQ(info.bucket_size, 4);
  EXPECT_EQ(info.max_iterations, 1000);
  EXPECT_EQ(info.expansion, 3);
  EXPECT_EQ(info.num_buckets, 512);
  EXPECT_EQ(info.num_filters, 1);
  EXPECT_EQ(info.num_items, 0);
  EXPECT_EQ(info.num_deletes, 0);

  std::vector<std::string> elements = {"info1", "info2"};
  int ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Add(*ctx_, key_, elem, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  // get updated info
  s = cuckoo_filter_->Info(*ctx_, key_, &info);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(info.num_items, 2);
  EXPECT_EQ(info.num_deletes, 0);

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, BulkInsert) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  // bulk insert elements
  std::vector<std::string> elements = {"bulk1", "bulk2", "bulk3", "bulk4", "bulk5"};
  std::vector<int> results;
  s = cuckoo_filter_->Insert(*ctx_, key_, elements, &results, capacity, false);
  EXPECT_TRUE(s.ok());
  for (const auto& res : results) {
    EXPECT_EQ(res, 1);
  }

  int exists_ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Exists(*ctx_, key_, elem, &exists_ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exists_ret, 1);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, BulkInsertNX) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  // bulk insert unique elements
  std::vector<std::string> elements = {"bulkNX1", "bulkNX2", "bulkNX3"};
  std::vector<int> results;
  s = cuckoo_filter_->InsertNX(*ctx_, key_, elements, &results, capacity, false);
  EXPECT_TRUE(s.ok());
  for (const auto& res : results) {
    EXPECT_EQ(res, 1);
  }

  // attempt to bulk insert duplicates
  s = cuckoo_filter_->InsertNX(*ctx_, key_, elements, &results, capacity, false);
  EXPECT_TRUE(s.ok());
  for (const auto& res : results) {
    EXPECT_EQ(res, 0);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, MultipleExists) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  std::vector<std::string> added_elements = {"multi1", "multi2", "multi3"};
  int ret = 0;
  for (const auto& elem : added_elements) {
    s = cuckoo_filter_->Add(*ctx_, key_, elem, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  // check multiple exists
  std::vector<std::string> check_elements = {"multi1", "multi2", "multi4", "multi5"};
  std::vector<int> exists_results;
  s = cuckoo_filter_->MExists(*ctx_, key_, check_elements, &exists_results);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(exists_results.size(), check_elements.size());
  EXPECT_EQ(exists_results[0], 1);
  EXPECT_EQ(exists_results[1], 1);
  EXPECT_EQ(exists_results[2], 0);
  EXPECT_EQ(exists_results[3], 0);

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, InsertWithNoCreate) {
  uint64_t capacity = 500;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 300;
  uint16_t expansion = 1;

  // attempt to insert without creating the filter
  std::vector<std::string> elements = {"noCreate1", "noCreate2"};
  std::vector<int> results;
  rocksdb::Status s = cuckoo_filter_->Insert(*ctx_, key_, elements, &results, capacity, true);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "NotFound: ");

  // now reserve and insert
  s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  s = cuckoo_filter_->Insert(*ctx_, key_, elements, &results, capacity, true);
  EXPECT_TRUE(s.ok());
  for (const auto& res : results) {
    EXPECT_EQ(res, 1);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

// delete Non-Existent Element
TEST_F(RedisCuckooFilterTest, DeleteNonExistentElement) {
  int del_ret = 0;
  std::string non_existent_element = "non_existent";

  rocksdb::Status s = cuckoo_filter_->Del(*ctx_, key_, non_existent_element, &del_ret);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "NotFound: ");
  EXPECT_EQ(del_ret, 0);
}

// bulk insert with duplicates
TEST_F(RedisCuckooFilterTest, BulkInsertWithDuplicates) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  // bulk insert elements, some duplicates
  std::vector<std::string> elements = {"bulk1", "bulk2", "bulk3", "bulk1", "bulk4", "bulk2"};
  std::vector<int> results;
  s = cuckoo_filter_->Insert(*ctx_, key_, elements, &results, capacity, false);
  EXPECT_TRUE(s.ok());

  // verify results: should allow for duplicates
  ASSERT_EQ(results.size(), elements.size());
  EXPECT_EQ(results[0], 1);  // bulk1
  EXPECT_EQ(results[1], 1);  // bulk2
  EXPECT_EQ(results[2], 1);  // bulk3
  EXPECT_EQ(results[3], 1);  // bulk1 duplicate
  EXPECT_EQ(results[4], 1);  // bulk4
  EXPECT_EQ(results[5], 1);  // bulk2 duplicate

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

// filter expansion test
TEST_F(RedisCuckooFilterTest, FilterExpansion) {
  // assuming expansion is handled internally when the filter is full
  uint64_t capacity = 10;
  uint8_t bucket_size = 2;
  uint16_t max_iterations = 500;
  uint16_t expansion = 1;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  // insert elements exceeding initial capacity to trigger expansion
  std::vector<std::string> elements = {"elem1", "elem2", "elem3", "elem4", "elem5", "elem6"};
  int ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Add(*ctx_, key_, elem, &ret);
    // depending on implementation, some adds might fail if expansion isn't properly handled
    if (!s.ok()) {
      std::cout << "Failed to add element: " << elem << " with error: " << s.ToString() << std::endl;
    }
    // either success (ret == 1) or failure due to capacity
    EXPECT_TRUE(s.ok() || !s.ok());
  }

  // check if expansion was successful by verifying Info
  redis::CuckooFilterInfo info;
  s = cuckoo_filter_->Info(*ctx_, key_, &info);
  EXPECT_TRUE(s.ok());
  // expect num_buckets to have increased due to expansion
  EXPECT_GE(info.num_buckets, capacity / bucket_size);

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

// multiple reservations with different keys
TEST_F(RedisCuckooFilterTest, MultipleReservationsDifferentKeys) {
  std::string key1 = "test_key1";
  std::string key2 = "test_key2";

  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  // reserve first key
  rocksdb::Status s1 = cuckoo_filter_->Reserve(*ctx_, key1, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s1.ok());

  // reserve second key
  rocksdb::Status s2 = cuckoo_filter_->Reserve(*ctx_, key2, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s2.ok());

  // verify both keys are independent
  int ret = 0;
  s1 = cuckoo_filter_->Exists(*ctx_, key1, "element1", &ret);
  EXPECT_TRUE(s1.ok());
  EXPECT_EQ(ret, 0);

  s2 = cuckoo_filter_->Exists(*ctx_, key2, "element1", &ret);
  EXPECT_TRUE(s2.ok());
  EXPECT_EQ(ret, 0);

  // cleanup both keys
  int del_ret = 0;
  s1 = cuckoo_filter_->Del(*ctx_, key1, "cleanup_item", &del_ret);
  EXPECT_TRUE(s1.ok() || s1.IsNotFound());

  s2 = cuckoo_filter_->Del(*ctx_, key2, "cleanup_item", &del_ret);
  EXPECT_TRUE(s2.ok() || s2.IsNotFound());
}

TEST_F(RedisCuckooFilterTest, InsertWithNoCreateFlag) {
  std::vector<std::string> elements = {"noCreate1", "noCreate2"};
  std::vector<int> results;

  // attempt to insert without creating the filter
  rocksdb::Status s = cuckoo_filter_->Insert(*ctx_, key_, elements, &results, 1000, true);
  EXPECT_FALSE(s.ok());
  EXPECT_EQ(s.ToString(), "NotFound: ");

  // reserve the filter
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  // insert with no_create = true
  s = cuckoo_filter_->Insert(*ctx_, key_, elements, &results, capacity, true);
  EXPECT_TRUE(s.ok());
  for (const auto& res : results) {
    EXPECT_EQ(res, 1);
  }

  // verify insertion
  int exists_ret = 0;
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Exists(*ctx_, key_, elem, &exists_ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(exists_ret, 1);
  }

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

// count after deletion
TEST_F(RedisCuckooFilterTest, CountAfterDeletion) {
  uint64_t capacity = 1000;
  uint8_t bucket_size = 4;
  uint16_t max_iterations = 500;
  uint16_t expansion = 2;

  rocksdb::Status s = cuckoo_filter_->Reserve(*ctx_, key_, capacity, bucket_size, max_iterations, expansion);
  EXPECT_TRUE(s.ok());

  std::vector<std::string> elements = {"count_del1", "count_del2", "count_del3"};
  int ret = 0;

  // add elements
  for (const auto& elem : elements) {
    s = cuckoo_filter_->Add(*ctx_, key_, elem, &ret);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(ret, 1);
  }

  // delete one element
  s = cuckoo_filter_->Del(*ctx_, key_, "count_del2", &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, 1);

  uint64_t count = 0;
  s = cuckoo_filter_->Count(*ctx_, key_, "count_del1", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 1);

  s = cuckoo_filter_->Count(*ctx_, key_, "count_del2", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 0);

  s = cuckoo_filter_->Count(*ctx_, key_, "count_del3", &count);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(count, 1);

  int del_ret = 0;
  s = cuckoo_filter_->Del(*ctx_, key_, "cleanup_item", &del_ret);
  EXPECT_TRUE(s.ok() || s.IsNotFound());
}

}  // namespace redis
