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
#include "types/redis_list.h"

class RedisListTest : public TestBase {
 protected:
  explicit RedisListTest() { list = std::make_unique<Redis::List>(storage_, "list_ns"); }
  ~RedisListTest() override = default;

  void SetUp() override {
    key_ = "test-list-key";
    fields_ = {"list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
               "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
               "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5",
               "list-test-key-1", "list-test-key-2", "list-test-key-3", "list-test-key-4", "list-test-key-5"};
  }

  std::unique_ptr<Redis::List> list;
};

class RedisListSpecificTest : public RedisListTest {
 protected:
  void SetUp() override {
    key_ = "test-list-specific-key";
    fields_ = {"0", "1", "2", "3", "4", "3", "6", "7", "3", "8", "9", "3", "9", "3", "9"};
  }
};

class RedisListLMoveTest : public RedisListTest {
 protected:
  void SetUp() override {
    list->Del(key_);
    list->Del(dst_key_);
    fields_ = {"src1", "src2", "src3", "src4"};
    dst_fields_ = {"dst", "dst2", "dst3", "dst4"};
  }

  void TearDown() override {
    list->Del(key_);
    list->Del(dst_key_);
  }

  void listElementsAreEqualTo(const Slice &key, int start, int stop, const std::vector<Slice> &expected_elems) {
    std::vector<std::string> actual_elems;
    auto s = list->Range(key, start, stop, &actual_elems);
    EXPECT_TRUE(s.ok());

    EXPECT_EQ(actual_elems.size(), expected_elems.size());

    for (size_t i = 0; i < actual_elems.size(); ++i) {
      EXPECT_EQ(actual_elems[i], expected_elems[i].ToString());
    }
  }

  std::string dst_key_ = "test-dst-key";
  std::vector<Slice> dst_fields_;
};

TEST_F(RedisListTest, PushAndPop) {
  int ret = 0;
  list->Push(key_, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (auto &field : fields_) {
    std::string elem;
    list->Pop(key_, false, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (auto &field : fields_) {
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Pushx) {
  int ret = 0;
  Slice pushx_key("test-pushx-key");
  rocksdb::Status s = list->PushX(pushx_key, fields_, true, &ret);
  EXPECT_TRUE(s.ok());
  list->Push(pushx_key, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  s = list->PushX(pushx_key, fields_, true, &ret);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(ret, fields_.size() * 2);
  list->Del(pushx_key);
}

TEST_F(RedisListTest, Index) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::string elem;
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Index(key_, static_cast<int>(i), &elem);
    EXPECT_EQ(fields_[i].ToString(), elem);
  }
  for (auto &field : fields_) {
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  rocksdb::Status s = list->Index(key_, -1, &elem);
  EXPECT_TRUE(s.IsNotFound());
  list->Del(key_);
}

TEST_F(RedisListTest, Set) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice new_elem("new_elem");
  list->Set(key_, -1, new_elem);
  std::string elem;
  list->Index(key_, -1, &elem);
  EXPECT_EQ(new_elem.ToString(), elem);
  for (size_t i = 0; i < fields_.size(); i++) {
    list->Pop(key_, true, &elem);
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Range) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  list->Range(key_, 0, int(elems.size() - 1), &elems);
  EXPECT_EQ(elems.size(), fields_.size());
  for (size_t i = 0; i < elems.size(); i++) {
    EXPECT_EQ(fields_[i].ToString(), elems[i]);
  }
  for (auto &field : fields_) {
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Rem) {
  int ret = 0;
  uint32_t len = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice del_elem("list-test-key-1");
  // lrem key_ 1 list-test-key-1
  list->Rem(key_, 1, del_elem, &ret);
  EXPECT_EQ(1, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 1, len);
  for (size_t i = 1; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  // lrem key_ 0 list-test-key-1
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Rem(key_, 0, del_elem, &ret);
  EXPECT_EQ(4, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 4, len);
  for (auto &field : fields_) {
    std::string elem;
    if (field == del_elem) continue;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  // lrem key_ 1 nosuchelement
  Slice no_elem("no_such_element");
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Rem(key_, 1, no_elem, &ret);
  EXPECT_EQ(0, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size(), len);
  for (auto &field : fields_) {
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  // lrem key_ -1 list-test-key-1
  list->Push(key_, fields_, false, &ret);
  list->Rem(key_, -1, del_elem, &ret);
  EXPECT_EQ(1, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 1, len);
  int cnt = 0;
  for (auto &field : fields_) {
    std::string elem;
    if (field == del_elem) {
      if (++cnt > 3) continue;
    }
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  // lrem key_ -5 list-test-key-1
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Rem(key_, -5, del_elem, &ret);
  EXPECT_EQ(4, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 4, len);
  for (auto &field : fields_) {
    std::string elem;
    if (field == del_elem) continue;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListSpecificTest, Rem) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice del_elem("9");
  // lrem key_ 1 9
  list->Rem(key_, 1, del_elem, &ret);
  EXPECT_EQ(1, ret);
  uint32_t len = 0;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 1, len);
  int cnt = 0;
  for (auto &field : fields_) {
    if (field == del_elem) {
      if (++cnt <= 1) continue;
    }
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  // lrem key_ -2 9
  list->Push(key_, fields_, false, &ret);
  list->Rem(key_, -2, del_elem, &ret);
  EXPECT_EQ(2, ret);
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 2, len);
  cnt = 0;
  for (size_t i = fields_.size(); i > 0; i--) {
    if (fields_[i - 1] == del_elem) {
      if (++cnt <= 2) continue;
    }
    std::string elem;
    list->Pop(key_, false, &elem);
    EXPECT_EQ(elem, fields_[i - 1].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, Trim) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Trim(key_, 1, 2000);
  uint32_t len = 0;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 1, len);
  for (size_t i = 1; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListSpecificTest, Trim) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  // ltrim key_ 3 -3 then linsert 2 3 and lrem key_ 5 3
  Slice del_elem("3");
  list->Trim(key_, 3, -3);
  uint32_t len = 0;
  list->Size(key_, &len);
  EXPECT_EQ(fields_.size() - 5, len);
  Slice insert_elem("3");
  list->Insert(key_, Slice("2"), insert_elem, true, &ret);
  EXPECT_EQ(-1, ret);
  list->Rem(key_, 5, del_elem, &ret);
  EXPECT_EQ(4, ret);
  for (size_t i = 3; i < fields_.size() - 2; i++) {
    if (fields_[i] == del_elem) continue;
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, RPopLPush) {
  int ret = 0;
  list->Push(key_, fields_, true, &ret);
  EXPECT_EQ(fields_.size(), ret);
  Slice dst("test-list-rpoplpush-key");
  for (auto &field : fields_) {
    std::string elem;
    list->RPopLPush(key_, dst, &elem);
    EXPECT_EQ(field.ToString(), elem);
  }
  for (auto &field : fields_) {
    std::string elem;
    list->Pop(dst, false, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  list->Del(key_);
  list->Del(dst);
}

TEST_F(RedisListLMoveTest, LMoveSrcNotExist) {
  std::string elem;
  auto s = list->LMove(key_, dst_key_, true, true, &elem);
  EXPECT_EQ(elem, "");
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisListLMoveTest, LMoveSrcAndDstAreTheSameSingleElem) {
  int ret = 0;
  Slice element = fields_[0];
  list->Push(key_, {element}, false, &ret);
  EXPECT_EQ(1, ret);
  std::string expected_elem;
  auto s = list->LMove(key_, key_, true, true, &expected_elem);
  EXPECT_EQ(expected_elem, element);
  EXPECT_TRUE(s.ok());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[0]});
}

TEST_F(RedisListLMoveTest, LMoveSrcAndDstAreTheSameManyElemsLeftRight) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, key_, true, false, &elem);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(elem, fields_[0].ToString());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size() + 1),
                         {fields_[1], fields_[2], fields_[3], fields_[0]});
}

TEST_F(RedisListLMoveTest, LMoveSrcAndDstAreTheSameManyElemsRightLeft) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, key_, false, true, &elem);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(elem, fields_[fields_.size() - 1].ToString());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size() + 1),
                         {fields_[3], fields_[0], fields_[1], fields_[2]});
}

TEST_F(RedisListLMoveTest, LMoveDstNotExist) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, dst_key_, true, false, &elem);
  EXPECT_EQ(elem, fields_[0].ToString());
  EXPECT_TRUE(s.ok());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[1], fields_[2], fields_[3]});
  listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size()), {fields_[0]});
}

TEST_F(RedisListLMoveTest, LMoveSrcLeftDstLeft) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Push(dst_key_, dst_fields_, false, &ret);
  EXPECT_EQ(dst_fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, dst_key_, true, true, &elem);
  EXPECT_EQ(elem, fields_[0].ToString());
  EXPECT_TRUE(s.ok());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[1], fields_[2], fields_[3]});
  listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                         {fields_[0], dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3]});
}

TEST_F(RedisListLMoveTest, LMoveSrcLeftDstRight) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Push(dst_key_, dst_fields_, false, &ret);
  EXPECT_EQ(dst_fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, dst_key_, true, false, &elem);
  EXPECT_EQ(elem, fields_[0].ToString());
  EXPECT_TRUE(s.ok());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[1], fields_[2], fields_[3]});
  listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                         {dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3], fields_[0]});
}

TEST_F(RedisListLMoveTest, LMoveSrcRightDstLeft) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Push(dst_key_, dst_fields_, false, &ret);
  EXPECT_EQ(dst_fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, dst_key_, false, true, &elem);
  EXPECT_EQ(elem, fields_[3].ToString());
  EXPECT_TRUE(s.ok());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[0], fields_[1], fields_[2]});
  listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                         {fields_[3], dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3]});
}

TEST_F(RedisListLMoveTest, LMoveSrcRightDstRight) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  list->Push(dst_key_, dst_fields_, false, &ret);
  EXPECT_EQ(dst_fields_.size(), ret);
  std::string elem;
  auto s = list->LMove(key_, dst_key_, false, false, &elem);
  EXPECT_EQ(elem, fields_[3].ToString());
  EXPECT_TRUE(s.ok());
  listElementsAreEqualTo(key_, 0, static_cast<int>(fields_.size()), {fields_[0], fields_[1], fields_[2]});
  listElementsAreEqualTo(dst_key_, 0, static_cast<int>(dst_fields_.size() + 1),
                         {dst_fields_[0], dst_fields_[1], dst_fields_[2], dst_fields_[3], fields_[3]});
}

TEST_F(RedisListTest, LPopEmptyList) {
  std::string non_existing_key{"non-existing-key"};
  list->Del(non_existing_key);
  std::string elem;
  auto s = list->Pop(non_existing_key, true, &elem);
  EXPECT_TRUE(s.IsNotFound());
  std::vector<std::string> elems;
  s = list->PopMulti(non_existing_key, true, 10, &elems);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisListTest, LPopOneElement) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (auto &field : fields_) {
    std::string elem;
    list->Pop(key_, true, &elem);
    EXPECT_EQ(elem, field.ToString());
  }
  std::string elem;
  auto s = list->Pop(key_, true, &elem);
  EXPECT_TRUE(s.IsNotFound());
  list->Del(key_);
}

TEST_F(RedisListTest, LPopMulti) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  size_t requested_size = fields_.size() / 3;
  auto s = list->PopMulti(key_, true, requested_size, &elems);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(elems.size(), requested_size);
  for (size_t i = 0; i < elems.size(); ++i) {
    EXPECT_EQ(elems[i], fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, LPopMultiCountGreaterThanListSize) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  auto s = list->PopMulti(key_, true, 2 * ret, &elems);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(elems.size(), ret);
  for (size_t i = 0; i < elems.size(); ++i) {
    EXPECT_EQ(elems[i], fields_[i].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, RPopEmptyList) {
  std::string non_existing_key{"non-existing-key"};
  list->Del(non_existing_key);
  std::string elem;
  auto s = list->Pop(non_existing_key, false, &elem);
  EXPECT_TRUE(s.IsNotFound());
  std::vector<std::string> elems;
  s = list->PopMulti(non_existing_key, false, 10, &elems);
  EXPECT_TRUE(s.IsNotFound());
}

TEST_F(RedisListTest, RPopOneElement) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  for (size_t i = 0; i < fields_.size(); i++) {
    std::string elem;
    list->Pop(key_, false, &elem);
    EXPECT_EQ(elem, fields_[fields_.size() - i - 1].ToString());
  }
  std::string elem;
  auto s = list->Pop(key_, false, &elem);
  EXPECT_TRUE(s.IsNotFound());
  list->Del(key_);
}

TEST_F(RedisListTest, RPopMulti) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  size_t requested_size = fields_.size() / 3;
  auto s = list->PopMulti(key_, false, requested_size, &elems);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(elems.size(), requested_size);
  for (size_t i = 0; i < elems.size(); ++i) {
    EXPECT_EQ(elems[i], fields_[fields_.size() - i - 1].ToString());
  }
  list->Del(key_);
}

TEST_F(RedisListTest, RPopMultiCountGreaterThanListSize) {
  int ret = 0;
  list->Push(key_, fields_, false, &ret);
  EXPECT_EQ(fields_.size(), ret);
  std::vector<std::string> elems;
  auto s = list->PopMulti(key_, false, 2 * ret, &elems);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(elems.size(), ret);
  for (size_t i = 0; i < elems.size(); ++i) {
    EXPECT_EQ(elems[i], fields_[fields_.size() - i - 1].ToString());
  }
  list->Del(key_);
}
