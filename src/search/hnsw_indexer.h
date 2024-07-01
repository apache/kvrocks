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

#include <random>
#include <string>
#include <vector>

#include "search/indexer.h"
#include "search/search_encoding.h"
#include "search/value.h"
#include "storage/storage.h"

namespace redis {

class HnswIndex;

struct Node {
  using NodeKey = std::string;
  NodeKey key;
  uint16_t level;
  std::vector<NodeKey> neighbours;

  Node(const NodeKey& key, uint16_t level);

  StatusOr<HnswNodeFieldMetadata> DecodeMetadata(const SearchKey& search_key, engine::Storage* storage);
  void PutMetadata(HnswNodeFieldMetadata* node_meta, const SearchKey& search_key, engine::Storage* storage,
                   ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
  void DecodeNeighbours(const SearchKey& search_key, engine::Storage* storage);
  Status AddNeighbour(const NodeKey& neighbour_key, const SearchKey& search_key, engine::Storage* storage,
                      ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
  Status RemoveNeighbour(const NodeKey& neighbour_key, const SearchKey& search_key, engine::Storage* storage,
                         ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
  Status UpdateNeighbours(std::vector<NodeKey>& neighbours, const SearchKey& search_key, engine::Storage* storage,
                          ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                          std::unordered_set<NodeKey>& deleted_neighbours);

  friend class HnswIndex;
};

struct VectorItem {
  using NodeKey = Node::NodeKey;

  NodeKey key;
  kqir::NumericArray vector;
  const HnswVectorFieldMetadata* metadata;

  VectorItem(const NodeKey& key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata);
  VectorItem(const NodeKey& key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata);

  bool operator<(const VectorItem& other) const;
};

StatusOr<double> ComputeSimilarity(const VectorItem& left, const VectorItem& right);

class HnswIndex {
 public:
  using NodeKey = Node::NodeKey;

  SearchKey search_key_;
  HnswVectorFieldMetadata* metadata_;
  engine::Storage* storage_ = nullptr;

  std::mt19937 generator_;
  double m_level_normalization_factor_;

  HnswIndex(const SearchKey& search_key, HnswVectorFieldMetadata* vector, engine::Storage* storage);

  uint16_t RandomizeLayer();
  StatusOr<NodeKey> DefaultEntryPoint(uint16_t level);
  Status Connect(uint16_t layer, const NodeKey& node_key1, const NodeKey& node_key2,
                 ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
  Status PruneEdges(const VectorItem& vec, const std::vector<VectorItem>& new_neighbour_vectors, uint16_t layer,
                    ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
  StatusOr<std::vector<VectorItem>> SelectNeighbors(const VectorItem& vec, const std::vector<VectorItem>& vectors,
                                                    uint16_t layer);
  StatusOr<std::vector<VectorItem>> SearchLayer(uint16_t level, const VectorItem& target_vector, uint32_t ef_runtime,
                                                const std::vector<NodeKey>& entry_points);
  Status InsertVectorEntry(std::string_view key, kqir::NumericArray vector,
                           ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch);
};

}  // namespace redis
