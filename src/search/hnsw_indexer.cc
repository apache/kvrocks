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

HnswNode::HnswNode(NodeKey key, uint16_t level) : key(std::move(key)), level(level) {}

StatusOr<HnswNodeFieldMetadata> HnswNode::DecodeMetadata(const SearchKey& search_key, engine::Storage* storage) const {
  auto node_index_key = search_key.ConstructHnswNode(level, key);
  rocksdb::PinnableSlice value;
  auto s = storage->Get(rocksdb::ReadOptions(), storage->GetCFHandle(ColumnFamilyID::Search), node_index_key, &value);
  if (!s.ok()) return {Status::NotOK, s.ToString()};

  HnswNodeFieldMetadata metadata;
  s = metadata.Decode(&value);
  if (!s.ok()) return {Status::NotOK, s.ToString()};
  return metadata;
}

void HnswNode::PutMetadata(HnswNodeFieldMetadata* node_meta, const SearchKey& search_key, engine::Storage* storage,
                           rocksdb::WriteBatchBase* batch) const {
  std::string updated_metadata;
  node_meta->Encode(&updated_metadata);
  batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), search_key.ConstructHnswNode(level, key), updated_metadata);
}

void HnswNode::DecodeNeighbours(const SearchKey& search_key, engine::Storage* storage) {
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

Status HnswNode::AddNeighbour(const NodeKey& neighbour_key, const SearchKey& search_key, engine::Storage* storage,
                              rocksdb::WriteBatchBase* batch) const {
  auto edge_index_key = search_key.ConstructHnswEdge(level, key, neighbour_key);
  batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key, Slice());

  HnswNodeFieldMetadata node_metadata = GET_OR_RET(DecodeMetadata(search_key, storage));
  node_metadata.num_neighbours++;
  PutMetadata(&node_metadata, search_key, storage, batch);
  return Status::OK();
}

Status HnswNode::RemoveNeighbour(const NodeKey& neighbour_key, const SearchKey& search_key, engine::Storage* storage,
                                 rocksdb::WriteBatchBase* batch) const {
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

VectorItem::VectorItem(NodeKey key, const kqir::NumericArray& vector, const HnswVectorFieldMetadata* metadata)
    : key(std::move(key)), vector(vector), metadata(metadata) {}
VectorItem::VectorItem(NodeKey key, kqir::NumericArray&& vector, const HnswVectorFieldMetadata* metadata)
    : key(std::move(key)), vector(std::move(vector)), metadata(metadata) {}

bool VectorItem::operator==(const VectorItem& other) const { return key == other.key; }

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
    : search_key(search_key),
      metadata(vector),
      storage(storage),
      m_level_normalization_factor(1.0 / std::log(metadata->m)) {
  std::random_device rand_dev;
  generator = std::mt19937(rand_dev());
}

uint16_t HnswIndex::RandomizeLayer() {
  std::uniform_real_distribution<double> level_dist(0.0, 1.0);
  double r = level_dist(generator);
  double log_val = -std::log(r);
  double layer_val = log_val * m_level_normalization_factor;
  return static_cast<uint16_t>(std::floor(layer_val));
}

StatusOr<HnswIndex::NodeKey> HnswIndex::DefaultEntryPoint(uint16_t level) const {
  auto prefix = search_key.ConstructHnswLevelNodePrefix(level);
  util::UniqueIterator it(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
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

StatusOr<std::vector<VectorItem>> HnswIndex::DecodeNodesToVectorItems(const std::vector<NodeKey>& node_keys,
                                                                      uint16_t level, const SearchKey& search_key,
                                                                      engine::Storage* storage,
                                                                      const HnswVectorFieldMetadata* metadata) {
  std::vector<VectorItem> vector_items;
  vector_items.reserve(node_keys.size());

  for (const auto& neighbour_key : node_keys) {
    HnswNode neighbour_node(neighbour_key, level);
    auto neighbour_metadata_status = neighbour_node.DecodeMetadata(search_key, storage);
    if (!neighbour_metadata_status.IsOK()) {
      continue;  // Skip this neighbour if metadata can't be decoded
    }
    auto neighbour_metadata = neighbour_metadata_status.GetValue();
    vector_items.emplace_back(VectorItem(neighbour_key, std::move(neighbour_metadata.vector), metadata));
  }
  return vector_items;
}

Status HnswIndex::AddEdge(const NodeKey& node_key1, const NodeKey& node_key2, uint16_t layer,
                          ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const {
  auto edge_index_key1 = search_key.ConstructHnswEdge(layer, node_key1, node_key2);
  auto s = batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key1, Slice());
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to add edge, {}", s.ToString())};
  }

  auto edge_index_key2 = search_key.ConstructHnswEdge(layer, node_key2, node_key1);
  s = batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key2, Slice());
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to add edge, {}", s.ToString())};
  }
  return Status::OK();
}

Status HnswIndex::RemoveEdge(const NodeKey& node_key1, const NodeKey& node_key2, uint16_t layer,
                             ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const {
  auto edge_index_key1 = search_key.ConstructHnswEdge(layer, node_key1, node_key2);
  auto s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key1);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete edge, {}", s.ToString())};
  }

  auto edge_index_key2 = search_key.ConstructHnswEdge(layer, node_key2, node_key1);
  s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), edge_index_key2);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("failed to delete edge, {}", s.ToString())};
  }
  return Status::OK();
}

StatusOr<std::vector<VectorItem>> HnswIndex::SelectNeighbors(const VectorItem& vec,
                                                             const std::vector<VectorItem>& vertors,
                                                             uint16_t layer) const {
  std::vector<std::pair<double, VectorItem>> distances;
  distances.reserve(vertors.size());
  for (const auto& candidate : vertors) {
    auto dist = GET_OR_RET(ComputeSimilarity(vec, candidate));
    distances.emplace_back(dist, candidate);
  }

  std::sort(distances.begin(), distances.end());
  std::vector<VectorItem> selected_vs;

  selected_vs.reserve(vertors.size());
  uint16_t m_max = layer != 0 ? metadata->m : 2 * metadata->m;
  for (auto i = 0; i < std::min(m_max, (uint16_t)distances.size()); i++) {
    selected_vs.push_back(distances[i].second);
  }
  return selected_vs;
}

StatusOr<std::vector<VectorItem>> HnswIndex::SearchLayer(uint16_t level, const VectorItem& target_vector,
                                                         uint32_t ef_runtime,
                                                         const std::vector<NodeKey>& entry_points) const {
  std::vector<VectorItem> candidates;
  std::unordered_set<NodeKey> visited;
  std::priority_queue<std::pair<double, VectorItem>, std::vector<std::pair<double, VectorItem>>, std::greater<>>
      explore_heap;
  std::priority_queue<std::pair<double, VectorItem>> result_heap;

  for (const auto& entry_point_key : entry_points) {
    HnswNode entry_node = HnswNode(entry_point_key, level);
    auto entry_node_metadata = GET_OR_RET(entry_node.DecodeMetadata(search_key, storage));

    auto entry_point_vector = VectorItem(entry_point_key, std::move(entry_node_metadata.vector), metadata);
    auto dist = GET_OR_RET(ComputeSimilarity(target_vector, entry_point_vector));

    explore_heap.push(std::make_pair(dist, entry_point_vector));
    result_heap.push(std::make_pair(dist, std::move(entry_point_vector)));
    visited.insert(entry_point_key);
  }

  while (!explore_heap.empty()) {
    auto [dist, current_vector] = explore_heap.top();
    explore_heap.pop();
    if (dist > result_heap.top().first) {
      break;
    }

    auto current_node = HnswNode(current_vector.key, level);
    current_node.DecodeNeighbours(search_key, storage);

    for (const auto& neighbour_key : current_node.neighbours) {
      if (visited.find(neighbour_key) != visited.end()) {
        continue;
      }
      visited.insert(neighbour_key);

      auto neighbour_node = HnswNode(neighbour_key, level);
      auto neighbour_node_metadata = GET_OR_RET(neighbour_node.DecodeMetadata(search_key, storage));
      auto neighbour_node_vector = VectorItem(neighbour_key, std::move(neighbour_node_metadata.vector), metadata);

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

Status HnswIndex::InsertVectorEntryInternal(std::string_view key, const kqir::NumericArray& vector,
                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                                            uint16_t target_level) const {
  auto cf_handle = storage->GetCFHandle(ColumnFamilyID::Search);
  auto inserted_vector_item = VectorItem(std::string(key), vector, metadata);
  std::vector<VectorItem> nearest_vec_items;

  if (metadata->num_levels != 0) {
    auto level = metadata->num_levels - 1;

    auto default_entry_node = GET_OR_RET(DefaultEntryPoint(level));
    std::vector<NodeKey> entry_points{default_entry_node};

    for (; level > target_level; level--) {
      nearest_vec_items = GET_OR_RET(SearchLayer(level, inserted_vector_item, metadata->ef_runtime, entry_points));
      entry_points = {nearest_vec_items[0].key};
    }

    for (; level >= 0; level--) {
      nearest_vec_items = GET_OR_RET(SearchLayer(level, inserted_vector_item, metadata->ef_construction, entry_points));
      auto candidate_vec_items = GET_OR_RET(SelectNeighbors(inserted_vector_item, nearest_vec_items, level));
      auto node = HnswNode(std::string(key), level);
      auto m_max = level == 0 ? 2 * metadata->m : metadata->m;

      std::unordered_set<NodeKey> connected_edges_set;
      std::unordered_map<NodeKey, std::unordered_set<NodeKey>> deleted_edges_map;

      // Check if candidate node has room for more outgoing edges
      auto has_room_for_more_edges = [&](uint16_t candidate_node_num_neighbours) {
        return candidate_node_num_neighbours < m_max;
      };

      // Check if candidate node has room after some other nodes' are pruned in current batch
      auto has_room_after_deletions = [&](const HnswNode& candidate_node, uint16_t candidate_node_num_neighbours) {
        auto it = deleted_edges_map.find(candidate_node.key);
        if (it != deleted_edges_map.end()) {
          auto num_deleted_edges = static_cast<uint16_t>(it->second.size());
          return (candidate_node_num_neighbours - num_deleted_edges) < m_max;
        }
        return false;
      };

      for (const auto& candidate_vec : candidate_vec_items) {
        auto candidate_node = HnswNode(candidate_vec.key, level);
        auto candidate_node_metadata = GET_OR_RET(candidate_node.DecodeMetadata(search_key, storage));
        uint16_t candidate_node_num_neighbours = candidate_node_metadata.num_neighbours;

        if (has_room_for_more_edges(candidate_node_num_neighbours) ||
            has_room_after_deletions(candidate_node, candidate_node_num_neighbours)) {
          GET_OR_RET(AddEdge(inserted_vector_item.key, candidate_node.key, level, batch));
          connected_edges_set.insert(candidate_node.key);
          continue;
        }

        // Re-evaluate the neighbours for the candidate node
        candidate_node.DecodeNeighbours(search_key, storage);
        auto candidate_node_neighbour_vec_items =
            GET_OR_RET(DecodeNodesToVectorItems(candidate_node.neighbours, level, search_key, storage, metadata));
        candidate_node_neighbour_vec_items.push_back(inserted_vector_item);
        auto sorted_neighbours_by_distance =
            GET_OR_RET(SelectNeighbors(candidate_vec, candidate_node_neighbour_vec_items, level));

        bool inserted_node_is_selected =
            std::find(sorted_neighbours_by_distance.begin(), sorted_neighbours_by_distance.end(),
                      inserted_vector_item) != sorted_neighbours_by_distance.end();

        if (inserted_node_is_selected) {
          // Add the edge between candidate and inserted node
          GET_OR_RET(AddEdge(inserted_vector_item.key, candidate_node.key, level, batch));
          connected_edges_set.insert(candidate_node.key);

          auto find_deleted_item = [&](const std::vector<VectorItem>& candidate_neighbours,
                                       const std::vector<VectorItem>& selected_neighbours) -> VectorItem {
            auto it =
                std::find_if(candidate_neighbours.begin(), candidate_neighbours.end(), [&](const VectorItem& item) {
                  return std::find(selected_neighbours.begin(), selected_neighbours.end(), item) ==
                         selected_neighbours.end();
                });
            return *it;
          };

          // Remove the edge for candidate and the pruned node
          auto deleted_node = find_deleted_item(candidate_node_neighbour_vec_items, sorted_neighbours_by_distance);
          GET_OR_RET(RemoveEdge(deleted_node.key, candidate_node.key, level, batch));
          deleted_edges_map[candidate_node.key].insert(deleted_node.key);
          deleted_edges_map[deleted_node.key].insert(candidate_node.key);
        }
      }

      // Update inserted node metadata
      HnswNodeFieldMetadata node_metadata(static_cast<uint16_t>(connected_edges_set.size()), vector);
      node.PutMetadata(&node_metadata, search_key, storage, batch.Get());

      // Update modified nodes metadata
      for (const auto& node_edges : deleted_edges_map) {
        auto& current_node_key = node_edges.first;
        auto current_node = HnswNode(current_node_key, level);
        auto current_node_metadata = GET_OR_RET(current_node.DecodeMetadata(search_key, storage));
        auto new_num_neighbours = current_node_metadata.num_neighbours - node_edges.second.size();
        if (connected_edges_set.count(current_node_key) != 0) {
          new_num_neighbours++;
          connected_edges_set.erase(current_node_key);
        }
        current_node_metadata.num_neighbours = new_num_neighbours;
        current_node.PutMetadata(&current_node_metadata, search_key, storage, batch.Get());
      }

      for (const auto& current_node_key : connected_edges_set) {
        auto current_node = HnswNode(current_node_key, level);
        HnswNodeFieldMetadata current_node_metadata = GET_OR_RET(current_node.DecodeMetadata(search_key, storage));
        current_node_metadata.num_neighbours++;
        current_node.PutMetadata(&current_node_metadata, search_key, storage, batch.Get());
      }

      entry_points.clear();
      for (const auto& new_entry_point : nearest_vec_items) {
        entry_points.push_back(new_entry_point.key);
      }
    }
  } else {
    auto node = HnswNode(std::string(key), 0);
    HnswNodeFieldMetadata node_metadata(0, vector);
    node.PutMetadata(&node_metadata, search_key, storage, batch.Get());
    metadata->num_levels = 1;
  }

  while (target_level > metadata->num_levels - 1) {
    auto node = HnswNode(std::string(key), metadata->num_levels);
    HnswNodeFieldMetadata node_metadata(0, vector);
    node.PutMetadata(&node_metadata, search_key, storage, batch.Get());
    metadata->num_levels++;
  }

  std::string encoded_index_metadata;
  metadata->Encode(&encoded_index_metadata);
  auto index_meta_key = search_key.ConstructFieldMeta();
  batch->Put(cf_handle, index_meta_key, encoded_index_metadata);

  return Status::OK();
}

Status HnswIndex::InsertVectorEntry(std::string_view key, const kqir::NumericArray& vector,
                                    ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) {
  auto target_level = RandomizeLayer();
  return InsertVectorEntryInternal(key, vector, batch, target_level);
}

Status HnswIndex::DeleteVectorEntry(std::string_view key, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch) const {
  std::string node_key(key);
  for (uint16_t level = 0; level < metadata->num_levels; level++) {
    auto node = HnswNode(node_key, level);
    auto node_metadata_status = node.DecodeMetadata(search_key, storage);
    if (!node_metadata_status.IsOK()) {
      break;
    }

    auto node_metadata = std::move(node_metadata_status).GetValue();
    auto node_index_key = search_key.ConstructHnswNode(level, key);
    auto s = batch->Delete(storage->GetCFHandle(ColumnFamilyID::Search), node_index_key);
    if (!s.ok()) {
      return {Status::NotOK, s.ToString()};
    }

    node.DecodeNeighbours(search_key, storage);
    for (const auto& neighbour_key : node.neighbours) {
      GET_OR_RET(RemoveEdge(node_key, neighbour_key, level, batch));
      auto neighbour_node = HnswNode(neighbour_key, level);
      HnswNodeFieldMetadata neighbour_node_metadata = GET_OR_RET(neighbour_node.DecodeMetadata(search_key, storage));
      neighbour_node_metadata.num_neighbours--;
      neighbour_node.PutMetadata(&neighbour_node_metadata, search_key, storage, batch.Get());
    }
  }

  auto has_other_nodes_at_level = [&](uint16_t level, std::string_view skip_key) -> bool {
    auto prefix = search_key.ConstructHnswLevelNodePrefix(level);
    util::UniqueIterator it(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
    it->Seek(prefix);

    Slice node_key;
    Slice node_key_dst;
    while (it->Valid() && it->key().starts_with(prefix)) {
      node_key = Slice(it->key().ToString().substr(prefix.size()));
      if (!GetSizedString(&node_key, &node_key_dst)) {
        continue;
      }
      if (node_key_dst.ToString() != skip_key) {
        return true;
      }
      it->Next();
    }
    return false;
  };

  while (metadata->num_levels > 0) {
    if (has_other_nodes_at_level(metadata->num_levels - 1, key)) {
      break;
    }
    metadata->num_levels--;
  }

  std::string encoded_index_metadata;
  metadata->Encode(&encoded_index_metadata);
  auto index_meta_key = search_key.ConstructFieldMeta();
  batch->Put(storage->GetCFHandle(ColumnFamilyID::Search), index_meta_key, encoded_index_metadata);

  return Status::OK();
}

}  // namespace redis
