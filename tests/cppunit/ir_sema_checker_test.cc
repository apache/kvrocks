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

#include "search/ir_sema_checker.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

#include "gtest/gtest.h"
#include "search/search_encoding.h"
#include "search/sql_transformer.h"
#include "storage/redis_metadata.h"

using namespace kqir;

static auto Parse(const std::string& in) { return sql::ParseToIR(peg::string_input(in, "test")); }

static IndexMap MakeIndexMap() {
  auto f1 = FieldInfo("f1", std::make_unique<redis::TagFieldMetadata>());
  auto f2 = FieldInfo("f2", std::make_unique<redis::NumericFieldMetadata>());
  auto f3 = FieldInfo("f3", std::make_unique<redis::NumericFieldMetadata>());

  auto hnsw_field_meta = std::make_unique<redis::HnswVectorFieldMetadata>();
  hnsw_field_meta->vector_type = redis::VectorType::FLOAT64;
  hnsw_field_meta->dim = 3;
  hnsw_field_meta->distance_metric = redis::DistanceMetric::L2;
  auto f4 = FieldInfo("f4", std::move(hnsw_field_meta));

  hnsw_field_meta = std::make_unique<redis::HnswVectorFieldMetadata>();
  hnsw_field_meta->vector_type = redis::VectorType::FLOAT64;
  hnsw_field_meta->dim = 3;
  hnsw_field_meta->distance_metric = redis::DistanceMetric::COSINE;
  auto f5 = FieldInfo("f5", std::move(hnsw_field_meta));
  f5.metadata->noindex = true;

  auto ia = std::make_unique<IndexInfo>("ia", redis::IndexMetadata(), "");
  ia->Add(std::move(f1));
  ia->Add(std::move(f2));
  ia->Add(std::move(f3));
  ia->Add(std::move(f4));
  ia->Add(std::move(f5));

  IndexMap res;
  res.Insert(std::move(ia));
  return res;
}

using testing::MatchesRegex;

TEST(SemaCheckerTest, Simple) {
  auto index_map = MakeIndexMap();

  {
    SemaChecker checker(index_map);
    ASSERT_EQ(checker.Check(Parse("select a from b")->get()).Msg(), "index `b` not found");
    ASSERT_EQ(checker.Check(Parse("select a from ia")->get()).Msg(), "field `a` not found in index `ia`");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia")->get()).Msg(), "ok");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where b = 1")->get()).Msg(), "field `b` not found in index `ia`");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 = 1")->get()).Msg(), "field `f1` is not a numeric field");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f2 hastag \"a\"")->get()).Msg(),
              "field `f2` is not a tag field");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 hastag \"a\" and f2 = 1")->get()).Msg(), "ok");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 hastag \"\"")->get()).Msg(),
              "tag cannot be an empty string");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia where f1 hastag \",\"")->get()).Msg(),
              "tag cannot contain the separator `,`");
    ASSERT_EQ(checker.Check(Parse("select f1 from ia order by a")->get()).Msg(), "field `a` not found in index `ia`");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia order by f4 <-> [3.6,4.7] limit 5")->get()).Msg(),
              "vector should be of size `3` for field `f4`");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia where f4 <-> [3.6,4.7] < 5")->get()).Msg(),
              "vector should be of size `3` for field `f4`");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia where f4 <-> [3.6,4.7,5.6] < -5")->get()).Msg(),
              "range cannot be a negative number for l2 distance metric");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia order by f4 limit 5")->get()).Msg(),
              "field `f4` is a vector field according to metadata and does expect a vector parameter");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia order by f1 <-> [3.6,4.7,5.6] limit 5")->get()).Msg(),
              "field `f1` is not sortable");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia order by f2 <-> [3.6,4.7,5.6] limit 5")->get()).Msg(),
              "field `f2` is not a vector field according to metadata and does not expect a vector parameter");
    ASSERT_EQ(checker.Check(Parse("select f4 from ia order by f4 <-> [3.6,4.7,5.6]")->get()).Msg(),
              "expect a LIMIT clause for vector field to construct a KNN search");
    ASSERT_EQ(checker.Check(Parse("select f5 from ia order by f5 <-> [3.6,4.7,5.6] limit 5")->get()).Msg(),
              "field `f5` is marked as NOINDEX and cannot be used for KNN search");
    ASSERT_EQ(checker.Check(Parse("select f5 from ia where f5 <-> [3.6,4.7,5.6] < 5")->get()).Msg(),
              "range has to be between 0 and 2 for cosine distance metric");
    ASSERT_EQ(checker.Check(Parse("select f5 from ia where f5 <-> [3.6,4.7,5.6] < 0.5")->get()).Msg(), "ok");
  }

  {
    SemaChecker checker(index_map);
    auto root = *Parse("select f1 from ia where f1 hastag \"a\" and f2 = 1 order by f3");

    ASSERT_EQ(checker.Check(root.get()).Msg(), "ok");
  }
}
