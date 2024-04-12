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

#include "search/indexer.h"

#include <gtest/gtest.h>
#include <test_base.h>

#include <memory>

#include "search/index_info.h"
#include "search/search_encoding.h"
#include "storage/redis_metadata.h"
#include "types/redis_hash.h"

struct IndexerTest : TestBase {
  redis::GlobalIndexer indexer;
  kqir::IndexMap map;
  std::string ns = "index_test";

  IndexerTest() : indexer(storage_.get()) {
    SearchMetadata hash_field_meta(false);
    hash_field_meta.on_data_type = SearchOnDataType::HASH;

    kqir::IndexInfo hash_info("hashtest", hash_field_meta);
    hash_info.Add(kqir::FieldInfo("x", std::make_unique<redis::SearchTagFieldMetadata>()));
    hash_info.Add(kqir::FieldInfo("y", std::make_unique<redis::SearchNumericFieldMetadata>()));
    hash_info.prefixes.prefixes.emplace_back("idxtesthash");

    map.emplace("hashtest", std::move(hash_info));

    redis::IndexUpdater hash_updater{&map.at("hashtest")};

    SearchMetadata json_field_meta(false);
    json_field_meta.on_data_type = SearchOnDataType::JSON;

    kqir::IndexInfo json_info("jsontest", json_field_meta);
    json_info.Add(kqir::FieldInfo("$.x", std::make_unique<redis::SearchTagFieldMetadata>()));
    json_info.Add(kqir::FieldInfo("$.y", std::make_unique<redis::SearchNumericFieldMetadata>()));
    json_info.prefixes.prefixes.emplace_back("idxtestjson");

    map.emplace("jsontest", std::move(json_info));

    redis::IndexUpdater json_updater{&map.at("jsontest")};

    indexer.Add(std::move(hash_updater));
    indexer.Add(std::move(json_updater));
  }
};

TEST_F(IndexerTest, HashTag) {
  redis::Hash db(storage_.get(), ns);
  auto cfhandler = storage_->GetCFHandle("search");

  {
    auto s = indexer.Record("no_exist", ns);
    ASSERT_TRUE(s.Is<Status::NoPrefixMatched>());
  }

  auto key1 = "idxtesthash:k1";
  auto idxname = "hashtest";

  {
    auto s = indexer.Record(key1, ns);
    ASSERT_TRUE(s);
    ASSERT_EQ(s->first.info->name, idxname);
    ASSERT_TRUE(s->second.empty());

    uint64_t cnt = 0;
    db.Set(key1, "x", "food,kitChen,Beauty", &cnt);
    ASSERT_EQ(cnt, 1);

    auto s2 = indexer.Update(*s, key1, ns);
    ASSERT_TRUE(s2);

    auto subkey = redis::ConstructTagFieldSubkey("x", "food", key1);
    auto nskey = ComposeNamespaceKey(ns, idxname, false);
    auto key = InternalKey(nskey, subkey, 0, false);

    std::string val;
    auto s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("x", "kitchen", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("x", "beauty", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");
  }

  {
    auto s = indexer.Record(key1, ns);
    ASSERT_TRUE(s);
    ASSERT_EQ(s->first.info->name, idxname);
    ASSERT_EQ(s->second.size(), 1);
    ASSERT_EQ(s->second["x"], "food,kitChen,Beauty");

    uint64_t cnt = 0;
    auto s_set = db.Set(key1, "x", "Clothing,FOOD,sport", &cnt);
    ASSERT_EQ(cnt, 0);
    ASSERT_TRUE(s_set.ok());

    auto s2 = indexer.Update(*s, key1, ns);
    ASSERT_TRUE(s2);

    auto subkey = redis::ConstructTagFieldSubkey("x", "food", key1);
    auto nskey = ComposeNamespaceKey(ns, idxname, false);
    auto key = InternalKey(nskey, subkey, 0, false);

    std::string val;
    auto s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("x", "clothing", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("x", "sport", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("x", "kitchen", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.IsNotFound());

    subkey = redis::ConstructTagFieldSubkey("x", "beauty", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.IsNotFound());
  }
}

TEST_F(IndexerTest, JsonTag) {
  redis::Json db(storage_.get(), ns);
  auto cfhandler = storage_->GetCFHandle("search");

  {
    auto s = indexer.Record("no_exist", ns);
    ASSERT_TRUE(s.Is<Status::NoPrefixMatched>());
  }

  auto key1 = "idxtestjson:k1";
  auto idxname = "jsontest";

  {
    auto s = indexer.Record(key1, ns);
    ASSERT_TRUE(s);
    ASSERT_EQ(s->first.info->name, idxname);
    ASSERT_TRUE(s->second.empty());

    auto s_set = db.Set(key1, "$", R"({"x": "food,kitChen,Beauty"})");
    ASSERT_TRUE(s_set.ok());

    auto s2 = indexer.Update(*s, key1, ns);
    ASSERT_TRUE(s2);

    auto subkey = redis::ConstructTagFieldSubkey("$.x", "food", key1);
    auto nskey = ComposeNamespaceKey(ns, idxname, false);
    auto key = InternalKey(nskey, subkey, 0, false);

    std::string val;
    auto s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("$.x", "kitchen", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("$.x", "beauty", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");
  }

  {
    auto s = indexer.Record(key1, ns);
    ASSERT_TRUE(s);
    ASSERT_EQ(s->first.info->name, idxname);
    ASSERT_EQ(s->second.size(), 1);
    ASSERT_EQ(s->second["$.x"], "food,kitChen,Beauty");

    auto s_set = db.Set(key1, "$.x", "\"Clothing,FOOD,sport\"");
    ASSERT_TRUE(s_set.ok());

    auto s2 = indexer.Update(*s, key1, ns);
    ASSERT_TRUE(s2);

    auto subkey = redis::ConstructTagFieldSubkey("$.x", "food", key1);
    auto nskey = ComposeNamespaceKey(ns, idxname, false);
    auto key = InternalKey(nskey, subkey, 0, false);

    std::string val;
    auto s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("$.x", "clothing", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("$.x", "sport", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.ok());
    ASSERT_EQ(val, "");

    subkey = redis::ConstructTagFieldSubkey("$.x", "kitchen", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.IsNotFound());

    subkey = redis::ConstructTagFieldSubkey("$.x", "beauty", key1);
    nskey = ComposeNamespaceKey(ns, idxname, false);
    key = InternalKey(nskey, subkey, 0, false);

    s3 = storage_->Get(storage_->DefaultMultiGetOptions(), cfhandler, key.Encode(), &val);
    ASSERT_TRUE(s3.IsNotFound());
  }
}
