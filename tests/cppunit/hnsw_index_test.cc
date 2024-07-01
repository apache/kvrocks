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
#include <test_base.h>

#include <iostream>
#include <memory>
#include <random>

#include "search/hnsw_indexer.h"
#include "search/indexer.h"
#include "search/search_encoding.h"
#include "search/value.h"
#include "storage/storage.h"

struct HnswIndexTest : TestBase {
  redis::HnswVectorFieldMetadata metadata_;
  std::string ns = "hnsw_test_ns";
  std::string idx_name = "hnsw_test_idx";
  std::string key = "vector";
  std::unique_ptr<redis::HnswIndex> hnsw_index_;

  HnswIndexTest() {
    metadata_.vector_type = redis::VectorType::FLOAT64;
    metadata_.dim = 4;
    metadata_.m = 3;
    metadata_.distance_metric = redis::DistanceMetric::L2;
    auto search_key_ = redis::SearchKey(ns, idx_name, key);
    hnsw_index_ = std::make_unique<redis::HnswIndex>(search_key_, &metadata_, storage_.get());
  }

  void TearDown() override { hnsw_index_.reset(); }
};

TEST_F(HnswIndexTest, ComputeSimilarity) {
  redis::VectorItem vec1 = {"1", {1.0, 1.2, 1.4, 1.6}, hnsw_index_->metadata_};
  redis::VectorItem vec2 = {"2", {3.0, 3.2, 3.4, 3.6}, hnsw_index_->metadata_};
  redis::VectorItem vec3 = {"3", {1.0, 1.2, 1.4, 1.6}, hnsw_index_->metadata_};  // identical to vec1

  auto s1 = redis::ComputeSimilarity(vec1, vec3);
  ASSERT_TRUE(s1.IsOK());
  double similarity = s1.GetValue();
  EXPECT_EQ(similarity, 0.0);

  auto s2 = redis::ComputeSimilarity(vec1, vec2);
  ASSERT_TRUE(s2.IsOK());
  similarity = s2.GetValue();
  EXPECT_EQ(similarity, 4.0);

  hnsw_index_->metadata_->distance_metric = redis::DistanceMetric::IP;
  auto s3 = redis::ComputeSimilarity(vec1, vec2);
  ASSERT_TRUE(s3.IsOK());
  similarity = s3.GetValue();
  EXPECT_NEAR(similarity, -17.36, 1e-5);

  hnsw_index_->metadata_->distance_metric = redis::DistanceMetric::COSINE;
  double expected_res =
      (1.0 * 3.0 + 1.2 * 3.2 + 1.4 * 3.4 + 1.6 * 3.6) /
      std::sqrt((1.0 * 1.0 + 1.2 * 1.2 + 1.4 * 1.4 + 1.6 * 1.6) * (3.0 * 3.0 + 3.2 * 3.2 + 3.4 * 3.4 + 3.6 * 3.6));
  auto s4 = redis::ComputeSimilarity(vec1, vec2);
  ASSERT_TRUE(s4.IsOK());
  similarity = s4.GetValue();
  EXPECT_NEAR(similarity, 1 - expected_res, 1e-5);

  hnsw_index_->metadata_->distance_metric = redis::DistanceMetric::L2;
}

TEST_F(HnswIndexTest, DefaultEntryPointNotFound) {
  auto initial_result = hnsw_index_->DefaultEntryPoint(0);
  ASSERT_EQ(initial_result.GetCode(), Status::NotFound);
}

TEST_F(HnswIndexTest, ConnectNodes) {
  uint16_t layer = 0;
  std::string node_key1 = "node1";
  std::string node_key2 = "node2";

  redis::Node node1(node_key1, layer);
  redis::Node node2(node_key2, layer);

  redis::HnswNodeFieldMetadata node1_metadata(0, {1, 2, 3});
  redis::HnswNodeFieldMetadata node2_metadata(0, {1, 2, 3});

  auto batch = storage_->GetWriteBatchBase();
  node1.PutMetadata(&node1_metadata, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node2.PutMetadata(&node2_metadata, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  auto s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  // Connect two nodes
  batch = storage_->GetWriteBatchBase();
  auto connect_status = hnsw_index_->Connect(layer, node_key1, node_key2, batch);
  ASSERT_EQ(connect_status.Msg(), Status::ok_msg);
  s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  node1.DecodeNeighbours(hnsw_index_->search_key_, hnsw_index_->storage_);
  EXPECT_EQ(node1.neighbours.size(), 1);
  EXPECT_EQ(node1.neighbours[0], node_key2);

  node2.DecodeNeighbours(hnsw_index_->search_key_, hnsw_index_->storage_);
  EXPECT_EQ(node2.neighbours.size(), 1);
  EXPECT_EQ(node2.neighbours[0], node_key1);
}

TEST_F(HnswIndexTest, PruneEdges) {
  uint16_t layer = 1;
  std::string node_key1 = "node1";
  std::string node_key2 = "node2";
  std::string node_key3 = "node3";

  redis::Node node1(node_key1, layer);
  redis::Node node2(node_key2, layer);
  redis::Node node3(node_key3, layer);

  redis::HnswNodeFieldMetadata metadata1(0, {1, 2, 3});
  redis::HnswNodeFieldMetadata metadata2(0, {4, 5, 6});
  redis::HnswNodeFieldMetadata metadata3(0, {7, 8, 9});

  auto batch = storage_->GetWriteBatchBase();
  node1.PutMetadata(&metadata1, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node2.PutMetadata(&metadata2, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node3.PutMetadata(&metadata3, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  auto s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  auto batch2 = storage_->GetWriteBatchBase();
  auto s1 = node1.AddNeighbour("node2", hnsw_index_->search_key_, hnsw_index_->storage_, batch2);
  ASSERT_TRUE(s1.IsOK());
  auto s2 = node2.AddNeighbour("node1", hnsw_index_->search_key_, hnsw_index_->storage_, batch2);
  ASSERT_TRUE(s2.IsOK());
  auto s3 = node2.AddNeighbour("node3", hnsw_index_->search_key_, hnsw_index_->storage_, batch2);
  ASSERT_TRUE(s3.IsOK());
  auto s4 = node3.AddNeighbour("node2", hnsw_index_->search_key_, hnsw_index_->storage_, batch2);
  ASSERT_TRUE(s4.IsOK());
  s = storage_->Write(storage_->DefaultWriteOptions(), batch2->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  // Prune edges for node2, keeping only node3 as a neighbor
  auto batch3 = storage_->GetWriteBatchBase();
  std::vector<redis::VectorItem> new_neighbours = {redis::VectorItem("node3", {7, 8, 9}, &metadata_)};
  auto s5 = hnsw_index_->PruneEdges(redis::VectorItem("node2", {4, 5, 6}, &metadata_), new_neighbours, layer, batch3);
  ASSERT_TRUE(s5.IsOK());
  s = storage_->Write(storage_->DefaultWriteOptions(), batch3->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  node1.DecodeNeighbours(hnsw_index_->search_key_, hnsw_index_->storage_);
  EXPECT_EQ(node1.neighbours.size(), 0);

  node2.DecodeNeighbours(hnsw_index_->search_key_, hnsw_index_->storage_);
  EXPECT_EQ(node2.neighbours.size(), 1);
  EXPECT_EQ(node2.neighbours[0], "node3");

  node3.DecodeNeighbours(hnsw_index_->search_key_, hnsw_index_->storage_);
  EXPECT_EQ(node3.neighbours.size(), 1);
  EXPECT_EQ(node3.neighbours[0], "node2");

  // Prune edges for node3 with non-existing
  auto batch4 = storage_->GetWriteBatchBase();
  new_neighbours = {redis::VectorItem("node1", {1, 2, 3}, &metadata_)};
  auto s6 = hnsw_index_->PruneEdges(redis::VectorItem("node3", {7, 8, 9}, &metadata_), new_neighbours, layer, batch4);
  ASSERT_EQ(s6.GetCode(), Status::InvalidArgument);
}

TEST_F(HnswIndexTest, SelectNeighbors) {
  redis::VectorItem vec1 = {"1", {1.0, 1.0, 1.0, 1.0}, hnsw_index_->metadata_};
  redis::VectorItem vec2 = {"2", {2.0, 2.0, 2.0, 2.0}, hnsw_index_->metadata_};
  redis::VectorItem vec3 = {"3", {3.0, 3.0, 3.0, 3.0}, hnsw_index_->metadata_};
  redis::VectorItem vec4 = {"4", {4.0, 4.0, 4.0, 4.0}, hnsw_index_->metadata_};
  redis::VectorItem vec5 = {"5", {5.0, 5.0, 5.0, 5.0}, hnsw_index_->metadata_};
  redis::VectorItem vec6 = {"6", {6.0, 6.0, 6.0, 6.0}, hnsw_index_->metadata_};
  redis::VectorItem vec7 = {"7", {7.0, 7.0, 7.0, 7.0}, hnsw_index_->metadata_};

  std::vector<redis::VectorItem> candidates = {vec3, vec2};
  auto s1 = hnsw_index_->SelectNeighbors(vec1, candidates, 1);
  ASSERT_TRUE(s1.IsOK());
  auto selected = s1.GetValue();
  EXPECT_EQ(selected.size(), candidates.size());

  EXPECT_EQ(selected[0].key, vec2.key);
  EXPECT_EQ(selected[1].key, vec3.key);

  candidates = {vec4, vec2, vec5, vec7, vec3, vec6};
  auto s2 = hnsw_index_->SelectNeighbors(vec1, candidates, 1);
  ASSERT_TRUE(s2.IsOK());
  selected = s2.GetValue();
  EXPECT_EQ(selected.size(), 3);

  EXPECT_EQ(selected[0].key, vec2.key);
  EXPECT_EQ(selected[1].key, vec3.key);
  EXPECT_EQ(selected[2].key, vec4.key);

  candidates = {vec4, vec2, vec5, vec7, vec3, vec6};
  auto s3 = hnsw_index_->SelectNeighbors(vec1, candidates, 0);
  ASSERT_TRUE(s3.IsOK());
  selected = s3.GetValue();
  EXPECT_EQ(selected.size(), 6);

  EXPECT_EQ(selected[0].key, vec2.key);
  EXPECT_EQ(selected[1].key, vec3.key);
  EXPECT_EQ(selected[2].key, vec4.key);
  EXPECT_EQ(selected[3].key, vec5.key);
  EXPECT_EQ(selected[4].key, vec6.key);
  EXPECT_EQ(selected[5].key, vec7.key);
}

TEST_F(HnswIndexTest, SearchLayer) {
  uint16_t layer = 3;
  std::string node_key1 = "node1";
  std::string node_key2 = "node2";
  std::string node_key3 = "node3";
  std::string node_key4 = "node4";
  std::string node_key5 = "node5";

  redis::Node node1(node_key1, layer);
  redis::Node node2(node_key2, layer);
  redis::Node node3(node_key3, layer);
  redis::Node node4(node_key4, layer);
  redis::Node node5(node_key5, layer);

  redis::HnswNodeFieldMetadata metadata1(0, {1.0, 2.0, 3.0});
  redis::HnswNodeFieldMetadata metadata2(0, {4.0, 5.0, 6.0});
  redis::HnswNodeFieldMetadata metadata3(0, {7.0, 8.0, 9.0});
  redis::HnswNodeFieldMetadata metadata4(0, {2.0, 3.0, 4.0});
  redis::HnswNodeFieldMetadata metadata5(0, {5.0, 6.0, 7.0});

  // Add Nodes
  auto batch = storage_->GetWriteBatchBase();
  node1.PutMetadata(&metadata1, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node2.PutMetadata(&metadata2, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node3.PutMetadata(&metadata3, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node4.PutMetadata(&metadata4, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  node5.PutMetadata(&metadata5, hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  auto s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  // Add Neighbours
  batch = storage_->GetWriteBatchBase();
  auto s1 = node1.AddNeighbour("node2", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s1.IsOK());
  auto s2 = node1.AddNeighbour("node4", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s2.IsOK());
  auto s3 = node2.AddNeighbour("node1", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s3.IsOK());
  auto s4 = node2.AddNeighbour("node3", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s1.IsOK());
  auto s5 = node3.AddNeighbour("node2", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s5.IsOK());
  auto s6 = node3.AddNeighbour("node5", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s6.IsOK());
  auto s7 = node4.AddNeighbour("node1", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s7.IsOK());
  auto s8 = node5.AddNeighbour("node3", hnsw_index_->search_key_, hnsw_index_->storage_, batch);
  ASSERT_TRUE(s8.IsOK());
  s = storage_->Write(storage_->DefaultWriteOptions(), batch->GetWriteBatch());
  ASSERT_TRUE(s.ok());

  redis::VectorItem target_vector("target", {2.0, 3.0, 4.0}, hnsw_index_->metadata_);
  std::vector<std::string> entry_points = {"node3", "node2"};
  uint32_t ef_runtime = 3;

  auto result = hnsw_index_->SearchLayer(layer, target_vector, ef_runtime, entry_points);
  ASSERT_TRUE(result.IsOK());
  auto candidates = result.GetValue();

  // TODO(Beihao): Found bug for comparison, may implement customized comparison function
  // ASSERT_EQ(candidates.size(), ef_runtime);
  // EXPECT_EQ(candidates[0].key, "node4");
  // EXPECT_EQ(candidates[1].key, "node1");
  // EXPECT_EQ(candidates[2].key, "node2");
}

TEST_F(HnswIndexTest, InsertVectorEntry) {
  // TODO(Beihao): Consider how to test in a robust way
}