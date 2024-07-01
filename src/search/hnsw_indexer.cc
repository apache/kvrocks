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

#include "hnsw_indexer.h"

#include <fmt/core.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <queue>
#include <random>
#include <unordered_set>
#include <vector>

#include "db_util.h"

namespace redis {

Node::Node(const NodeKey& key, uint16_t level) : key(key), level(level) {}

StatusOr<HnswNodeFieldMetadata> Node::DecodeMetadata(const SearchKey& search_key, engine::Storage* storage) {
  auto node_index_key = search_key.ConstructHnswNode(level, key);
  rocksdb::PinnableSlice value;
  auto s = storage->Get(rocksdb::ReadOptions(), storage->GetCFHandle(ColumnFamilyID::Search), node_index_key, &value);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  HnswNodeFieldMetadata metadata;
  s = metadata.Decode(&value);
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return metadata;
}

void Node::PutMetadata(HnswNodeFieldMetadata* node_meta, const SearchKey& search_key, engine::Storage* storage,
                       ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  std::string updated_metadata;
  node_meta->Encode(&updated_metadata);
  batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), search_key.ConstructHnswNode(level, key), updated_metadata);
}

void Node::DecodeNeighbours(const SearchKey& search_key, engine::Storage* storage) {
  neighbours.clear();
  auto edge_prefix = search_key.ConstructHnswEdgeWithSingleEnd(level, key);
  util::UniqueIterator iter(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
  for (iter->Seek(edge_prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(edge_prefix)) {
      break;
    }
    auto neighbour_edge = iter->key();
    neighbour_edge.remove_prefix(edge_prefix.size());
    Slice neighbour;
    GetSizedString(&neighbour_edge, &neighbour);
    neighbours.push_back(neighbour.ToString());
  }
}

Status Node::AddNeighbour(const NodeKey& neighbour_key, const SearchKey& search_key, engine::Storage* storage,
                          ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto edge_index_key = search_key.ConstructHnswEdge(level, key, neighbour_key);
  batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key, Slice());

  HnswNodeFieldMetadata node_metadata = GET_OR_RET(DecodeMetadata(search_key, storage));
  node_metadata.num_neighbours++;
  PutMetadata(&node_metadata, search_key, storage, batch);
  return Status::OK();
}

Status Node::RemoveNeighbour(const NodeKey& neighbour_key, const SearchKey& search_key, engine::Storage* storage,
                             ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto edge_index_key = search_key.ConstructHnswEdge(level, key, neighbour_key);
  auto s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete edge, {}", s.ToString())};
  }

  HnswNodeFieldMetadata node_metadata = GET_OR_RET(DecodeMetadata(search_key, storage));
  node_metadata.num_neighbours--;
  PutMetadata(&node_metadata, search_key, storage, batch);
  return Status::OK();
}

Status Node::UpdateNeighbours(std::vector<NodeKey>& neighbours, const SearchKey& search_key, engine::Storage* storage,
                              ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                              std::unordered_set<NodeKey>& deleted_neighbours) {
  deleted_neighbours.clear();
  auto cf_handle = storage->GetCFHandle(ColumnFamilyID::Search);
  auto edge_prefix = search_key.ConstructHnswEdgeWithSingleEnd(level, key);
  std::unordered_set<NodeKey> to_be_added{neighbours.begin(), neighbours.end()};

  util::UniqueIterator iter(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
  for (iter->Seek(edge_prefix); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(edge_prefix)) {
      break;
    }
    auto neighbour_edge = iter->key();
    neighbour_edge.remove_prefix(edge_prefix.size());
    Slice neighbour;
    GetSizedString(&neighbour_edge, &neighbour);
    auto neighbour_key = neighbour.ToString();

    if (to_be_added.count(neighbour_key) == 0) {
      batch->Delete(cf_handle, iter->key());
      deleted_neighbours.insert(neighbour_key);
    } else {
      to_be_added.erase(neighbour_key);
    }
  }

  for (const auto& neighbour : to_be_added) {
    auto edge_index_key = search_key.ConstructHnswEdge(level, key, neighbour);
    batch->Put(cf_handle, edge_index_key, Slice());
  }

  HnswNodeFieldMetadata node_metadata = GET_OR_RET(DecodeMetadata(search_key, storage));
  node_metadata.num_neighbours = static_cast<uint16_t>(neighbours.size());
  PutMetadata(&node_metadata, search_key, storage, batch);
  return Status::OK();
}

VectorItem::VectorItem(const NodeKey& key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata)
    : key(key), vector(std::move(vector)), metadata(metadata) {}
VectorItem::VectorItem(const NodeKey& key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata)
    : key(key), vector(std::move(vector)), metadata(metadata) {}

bool VectorItem::operator<(const VectorItem& other) const { return key < other.key; }

StatusOr<double> ComputeSimilarity(const VectorItem& left, const VectorItem& right) {
  if (left.metadata->distance_metric != right.metadata->distance_metric || left.metadata->dim != right.metadata->dim)
    return {Status::InvalidArgument, "Vectors must be of the same metric and dimension to compute distance."};

  auto metric = left.metadata->distance_metric;
  auto dim = left.metadata->dim;

  switch (metric) {
    case DistanceMetric::L2: {
      double dist = 0.0;
      for (auto i = 0; i < dim; i++) {
        double diff = left.vector[i] - right.vector[i];
        dist += diff * diff;
      }
      return std::sqrt(dist);
    }
    case DistanceMetric::IP: {
      double dist = 0.0;
      for (auto i = 0; i < dim; i++) {
        dist += left.vector[i] * right.vector[i];
      }
      return -dist;
    }
    case DistanceMetric::COSINE: {
      double dist = 0.0;
      double norm_left = 0.0;
      double norm_right = 0.0;
      for (auto i = 0; i < dim; i++) {
        dist += left.vector[i] * right.vector[i];
        norm_left += left.vector[i] * left.vector[i];
        norm_right += right.vector[i] * right.vector[i];
      }
      auto similarity = dist / std::sqrt(norm_left * norm_right);
      return 1.0 - similarity;
    }
    default:
      __builtin_unreachable();
  }
}

HnswIndex::HnswIndex(const SearchKey& search_key, HnswVectorFieldMetadata* vector, engine::Storage* storage)
    : search_key_(search_key), metadata_(vector), storage_(storage) {
  m_level_normalization_factor_ = 1.0 / std::log(metadata_->m);
  std::random_device rand_dev;
  generator_ = std::mt19937(rand_dev());
}

uint16_t HnswIndex::RandomizeLayer() {
  std::uniform_real_distribution<double> level_dist(0.0, 1.0);
  return static_cast<uint16_t>(std::floor(-std::log(level_dist(generator_)) * m_level_normalization_factor_));
}

StatusOr<HnswIndex::NodeKey> HnswIndex::DefaultEntryPoint(uint16_t level) {
  auto prefix = search_key_.ConstructHnswLevelNodePrefix(level);
  util::UniqueIterator it(storage_, storage_->DefaultScanOptions(), ColumnFamilyID::Search);
  it->Seek(prefix);

  Slice node_key;
  Slice node_key_dst;
  if (it->Valid() && it->key().starts_with(prefix)) {
    node_key = Slice(it->key().ToString().substr(prefix.size()));
    if (!GetSizedString(&node_key, &node_key_dst)) {
      return {Status::NotOK, fmt::format("fail to decode the default node key layer {}", level)};
    }
    return node_key_dst.ToString();
  }
  return {Status::NotFound, fmt::format("No node found in layer {}", level)};
}

Status HnswIndex::Connect(uint16_t layer, const NodeKey& node_key1, const NodeKey& node_key2,
                          ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto node1 = Node(node_key1, layer);
  GET_OR_RET(node1.AddNeighbour(node_key2, search_key_, storage_, batch));

  auto node2 = Node(node_key2, layer);
  GET_OR_RET(node2.AddNeighbour(node_key1, search_key_, storage_, batch));

  return Status::OK();
}

Status HnswIndex::PruneEdges(const VectorItem& vec, const std::vector<VectorItem>& new_neighbour_vectors,
                             uint16_t layer, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto node = Node(vec.key, layer);
  node.DecodeNeighbours(search_key_, storage_);
  std::unordered_set original_neighbours{node.neighbours.begin(), node.neighbours.end()};

  uint16_t neighbours_sz = static_cast<uint16_t>(new_neighbour_vectors.size());
  std::vector<NodeKey> neighbours(neighbours_sz);
  for (auto i = 0; i < neighbours_sz; i++) {
    auto neighbour_key = new_neighbour_vectors[i].key;
    if (original_neighbours.count(neighbour_key) == 0) {
      return {Status::InvalidArgument,
              fmt::format("Node \"{}\" is not a neighbour of \"{}\" and can't be pruned", neighbour_key, vec.key)};
    }
    neighbours[i] = new_neighbour_vectors[i].key;
  }

  std::unordered_set<NodeKey> deleted_neighbours;
  GET_OR_RET(node.UpdateNeighbours(neighbours, search_key_, storage_, batch, deleted_neighbours));

  for (const auto& key : deleted_neighbours) {
    auto neighbour_node = Node(key, layer);
    GET_OR_RET(neighbour_node.RemoveNeighbour(vec.key, search_key_, storage_, batch));
  }
  return Status::OK();
}

StatusOr<std::vector<VectorItem>> HnswIndex::SelectNeighbors(const VectorItem& vec,
                                                             const std::vector<VectorItem>& vertors, uint16_t layer) {
  std::vector<std::pair<double, VectorItem>> distances;
  distances.reserve(vertors.size());
  for (const auto& candidate : vertors) {
    auto dist = GET_OR_RET(ComputeSimilarity(vec, candidate));
    distances.push_back({dist, candidate});
  }

  std::sort(distances.begin(), distances.end());
  std::vector<VectorItem> selected_vs;

  selected_vs.reserve(vertors.size());
  uint16_t m_max = layer != 0 ? metadata_->m : 2 * metadata_->m;
  for (auto i = 0; i < std::min(m_max, (uint16_t)distances.size()); i++) {
    selected_vs.push_back(distances[i].second);
  }
  return selected_vs;
}

StatusOr<std::vector<VectorItem>> HnswIndex::SearchLayer(uint16_t level, const VectorItem& target_vector,
                                                         uint32_t ef_runtime,
                                                         const std::vector<NodeKey>& entry_points) {
  std::vector<VectorItem> candidates;
  std::unordered_set<NodeKey> visited;
  std::priority_queue<std::pair<double, VectorItem>, std::vector<std::pair<double, VectorItem>>, std::greater<>>
      explore_heap;
  std::priority_queue<std::pair<double, VectorItem>> result_heap;

  for (const auto& entry_point_key : entry_points) {
    Node entry_node = Node(entry_point_key, level);
    auto entry_node_metadata = GET_OR_RET(entry_node.DecodeMetadata(search_key_, storage_));

    auto entry_point_vector = VectorItem(entry_point_key, std::move(entry_node_metadata.vector), metadata_);
    auto dist = GET_OR_RET(ComputeSimilarity(target_vector, entry_point_vector));

    explore_heap.push(std::make_pair(dist, entry_point_vector));
    result_heap.push(std::make_pair(dist, std::move(entry_point_vector)));
    visited.insert(entry_point_key);
  }

  while (!explore_heap.empty()) {
    auto& [dist, current_vector] = explore_heap.top();
    explore_heap.pop();
    if (dist > result_heap.top().first) {
      break;
    }

    auto current_node = Node(current_vector.key, level);
    current_node.DecodeNeighbours(search_key_, storage_);

    for (const auto& neighbour_key : current_node.neighbours) {
      if (visited.find(neighbour_key) != visited.end()) {
        continue;
      }
      visited.insert(neighbour_key);

      auto neighbour_node = Node(neighbour_key, level);
      auto neighbour_node_metadata = GET_OR_RET(neighbour_node.DecodeMetadata(search_key_, storage_));
      auto neighbour_node_vector = VectorItem(neighbour_key, std::move(neighbour_node_metadata.vector), metadata_);

      auto dist = GET_OR_RET(ComputeSimilarity(target_vector, neighbour_node_vector));
      explore_heap.push(std::make_pair(dist, neighbour_node_vector));
      result_heap.push(std::make_pair(dist, neighbour_node_vector));
      while (result_heap.size() > ef_runtime) {
        result_heap.pop();
      }
    }
  }
  while (!result_heap.empty()) {
    candidates.push_back(result_heap.top().second);
    result_heap.pop();
  }
  std::reverse(candidates.begin(), candidates.end());
  return candidates;
}

Status HnswIndex::InsertVectorEntry(std::string_view key, kqir::NumericArray vector,
                                    ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto cf_handle = storage_->GetCFHandle(ColumnFamilyID::Search);
  auto inserted_vector_item = VectorItem(std::string(key), vector, metadata_);
  auto target_level = RandomizeLayer();
  std::vector<VectorItem> nearest_vec_items;

  if (metadata_->num_levels != 0) {
    auto level = metadata_->num_levels - 1;
    auto default_entry_node = GET_OR_RET(DefaultEntryPoint(level));
    std::vector<NodeKey> entry_points{default_entry_node};

    for (; level > target_level; level--) {
      nearest_vec_items = GET_OR_RET(SearchLayer(level, inserted_vector_item, metadata_->ef_runtime, entry_points));
      entry_points = {nearest_vec_items[0].key};
    }

    for (; level >= 0; level--) {
      nearest_vec_items =
          GET_OR_RET(SearchLayer(level, inserted_vector_item, metadata_->ef_construction, entry_points));
      auto connect_vec_items = GET_OR_RET(SelectNeighbors(inserted_vector_item, nearest_vec_items, level));

      for (const auto& connected_vec_item : connect_vec_items) {
        GET_OR_RET(Connect(level, inserted_vector_item.key, connected_vec_item.key, batch));
      }

      for (const auto& connected_vec_item : connect_vec_items) {
        auto connected_node = Node(connected_vec_item.key, level);
        auto connected_node_metadata = GET_OR_RET(connected_node.DecodeMetadata(search_key_, storage_));

        uint16_t connected_node_num_neighbours = connected_node_metadata.num_neighbours;
        auto m_max = level == 0 ? 2 * metadata_->m : metadata_->m;
        if (connected_node_num_neighbours <= m_max) continue;

        connected_node.DecodeNeighbours(search_key_, storage_);
        std::vector<VectorItem> connected_node_neighbour_vec_items;
        for (const auto& connected_node_neighbour_key : connected_node.neighbours) {
          Node connected_node_neighbour = Node(connected_node_neighbour_key, level);
          auto connected_node_neighbour_metadata =
              GET_OR_RET(connected_node_neighbour.DecodeMetadata(search_key_, storage_));
          auto neighbour_vector =
              VectorItem(connected_node_neighbour_key, std::move(connected_node_neighbour_metadata.vector), metadata_);
          connected_node_neighbour_vec_items.push_back(neighbour_vector);
        }
        auto new_neighbors = GET_OR_RET(SelectNeighbors(connected_vec_item, connected_node_neighbour_vec_items, level));
        GET_OR_RET(PruneEdges(connected_vec_item, new_neighbors, level, batch));
      }

      entry_points.clear();
      for (const auto& new_entry_point : nearest_vec_items) {
        entry_points.push_back(std::move(new_entry_point.key));
      }
    }
  } else {
    auto node = Node(std::string(key), 0);
    HnswNodeFieldMetadata node_metadata(0, vector);
    node.PutMetadata(&node_metadata, search_key_, storage_, batch);
    metadata_->num_levels = 1;
  }

  while (target_level > metadata_->num_levels - 1) {
    auto node = Node(std::string(key), metadata_->num_levels);
    HnswNodeFieldMetadata node_metadata(0, vector);
    node.PutMetadata(&node_metadata, search_key_, storage_, batch);
    metadata_->num_levels++;
  }

  std::string encoded_index_metadata;
  metadata_->Encode(&encoded_index_metadata);
  auto index_meta_key = search_key_.ConstructFieldMeta();
  batch->Put(cf_handle, index_meta_key, encoded_index_metadata);

  return Status::OK();
}

}  // namespace redis
