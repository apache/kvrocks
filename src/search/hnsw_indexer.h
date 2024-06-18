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

#include <algorithm>
#include <cmath>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <queue>

#include "db_util.h"
#include "parse_util.h"
#include "search/indexer.h"
#include "search/search_encoding.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"


namespace redis {

struct VectorItem {
  std::string key;
  // TODO: use template based on VectorType
  std::vector<double> vector;
  HnswVectorFieldMetadata* metadata;

  VectorItem(std::string_view key, std::string_view vector_str, HnswVectorFieldMetadata* metadata) : key(key), metadata(metadata) { 
    Decode(vector_str); 
  }

  // TODO: move it to util
  void Decode(std::string_view vector_str) {
    std::string trimmed = std::string(vector_str);
    trimmed.erase(0, 1);                   // remove the first '['
    trimmed.erase(trimmed.size() - 1, 1);  // remove the last ']'

    std::istringstream iss(trimmed);
    std::string num;

    vector.clear();

    while (std::getline(iss, num, ',')) {
      try {
        double value = std::stod(num);
        vector.push_back(value);
      } catch (const std::invalid_argument& ia) {
        throw std::runtime_error("Invalid number in vector string: " + num);
      }
    }
  }
};

auto ComputeDistance(const VectorItem& left, const VectorItem& right) {
  if (left.metadata->distance_metric != right.metadata->distance_metric)
    // throw error
    ;

  if (left.metadata->dim != right.metadata->dim)
    // throw error
    ;
  
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
      double norma = 0.0;
      double normb = 0.0;
      for (auto i = 0; i < dim; i++) {
        dist += left.vector[i] * right.vector[i];
        norma += left.vector[i] * right.vector[i];
        normb += left.vector[i] * right.vector[i];
      }
      auto similarity = dist / std::sqrt(norma * normb);
      return 1.0 - similarity;
    }
    default:
      // throw error
      return 0.0;
  }
}

bool operator<(const VectorItem& lhs, const VectorItem& rhs) {
    if (lhs.key != rhs.key) {
        return lhs.key < rhs.key;
    }
    if (!lhs.vector.empty() && !rhs.vector.empty()) {
        return lhs.vector[0] < rhs.vector[0];
    }
    return false;
}

struct Node {
  using NodeKey = std::string;

  NodeKey key;
  uint16_t level;
  std::vector<NodeKey> neighbours;

  Node(const NodeKey& key, uint16_t level) : key(key), level(level) {}

  HnswNodeFieldMetadata DecodeNodeMetadata(const SearchKey& search_key, engine::Storage *storage) {
    auto node_index_key = search_key.ConstructHnswNode(level, key);
    std::string value;
    rocksdb::Status s = storage->Get(rocksdb::ReadOptions(), node_index_key, &value);
    HnswNodeFieldMetadata metadata;
    Slice input(value);
    s = metadata.Decode(&input);
    return metadata;
  }

  void DecodeNeighbours(const SearchKey& search_key, engine::Storage *storage) {
    auto edge_prefix = search_key.ConstructHnswEdgeWithSingleEnd(level, key);
    util::UniqueIterator iter(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
    for (iter->Seek(edge_prefix); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(edge_prefix)) {
        break;
      }
      auto neighbour_key = iter->key().ToString().substr(edge_prefix.size());
      neighbours.push_back(neighbour_key);
    }
  }
};

class HnswIndex {
 public:
  using NodeKey = Node::NodeKey;

  SearchKey search_key_;
  HnswVectorFieldMetadata* metadata_;
  std::mt19937 generator_;
  double m_level_normalization_factor_;
  engine::Storage *storage = nullptr;

  HnswIndex(const SearchKey& search_key, HnswVectorFieldMetadata* vector, engine::Storage *storage)
      : search_key_(search_key), metadata_(vector), storage(storage) {
    m_level_normalization_factor_ = 1.0 / std::log(metadata_->m);
    std::random_device rand_dev;
    generator_ = std::mt19937(rand_dev());
  }

  int RandomizeLayer() {
    std::uniform_real_distribution<double> level_dist(0.0, 1.0);
    return static_cast<int>(std::floor(-std::log(level_dist(generator_)) * m_level_normalization_factor_));
  }

  NodeKey DefaultEntryPoint(uint16_t level) {
    auto prefix = search_key_.ConstructHnswLevelNodePrefix(level);
    util::UniqueIterator it(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
    it->Seek(prefix);

    if (it->Valid() && it->key().starts_with(prefix)) {
      Slice node_key_dst;
      auto node_key = Slice(it->key().ToString().substr(prefix.size()));
      if (!GetSizedString(&node_key, &node_key_dst)) {
        // error handling
        return "";
      }
      return node_key_dst.ToString();
    }
    return "";
  }

  void Connect(uint16_t layer, NodeKey node_key1, NodeKey node_key2,
               ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch, rocksdb::ColumnFamilyHandle* cf_handle) {
    auto edge_index_key1 = search_key_.ConstructHnswEdge(layer, node_key1, node_key2);
    batch->Put(cf_handle, edge_index_key1, Slice());

    auto edge_index_key2 = search_key_.ConstructHnswEdge(layer, node_key2, node_key1);
    batch->Put(cf_handle, edge_index_key2, Slice());

    Node node1 = Node(node_key1, layer);
    HnswNodeFieldMetadata node1_metadata = node1.DecodeNodeMetadata(search_key_, storage);
    node1_metadata.num_neighbours += 1;
    std::string node1_updated_metadata;
    node1_metadata.Encode(&node1_updated_metadata);
    batch->Put(cf_handle, node_key1, node1_updated_metadata);

    Node node2 = Node(node_key2, layer);
    HnswNodeFieldMetadata node2_metadata = node2.DecodeNodeMetadata(search_key_, storage);
    node2_metadata.num_neighbours += 1;
    std::string node2_updated_metadata;
    node2_metadata.Encode(&node2_updated_metadata);
    batch->Put(cf_handle, node_key2, node2_updated_metadata);
  }

  void ResetEdges(const VectorItem& vec, const std::vector<VectorItem>& neighbour_vertors, uint16_t layer, 
                  ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch, rocksdb::ColumnFamilyHandle* cf_handle) {
    std::unordered_set<NodeKey> neighbours;
    for (const auto& neighbour_vector : neighbour_vertors) {
      neighbours.insert(neighbour_vector.key);
    }

    auto edge_prefix = search_key_.ConstructHnswEdgeWithSingleEnd(layer, vec.key);
    util::UniqueIterator iter(storage, storage->DefaultScanOptions(), ColumnFamilyID::Search);
    for (iter->Seek(edge_prefix); iter->Valid(); iter->Next()) {
      if (!iter->key().starts_with(edge_prefix)) {
        break;
      }
      auto neighbour_key = iter->key().ToString().substr(edge_prefix.size());

      if (neighbours.count(neighbour_key) == 0) {
        batch->Delete(cf_handle, iter->key());
      }
    }

    Node node = Node(vec.key, layer);
    HnswNodeFieldMetadata node_metadata = node.DecodeNodeMetadata(search_key_, storage);
    node_metadata.num_neighbours = neighbours.size();
    std::string node_updated_metadata;
    node_metadata.Encode(&node_updated_metadata);
    batch->Put(cf_handle, vec.key, node_updated_metadata);
  }

  std::vector<VectorItem> SelectNeighbors(const VectorItem& vec, const std::vector<VectorItem>& vertors, uint16_t layer) {
    std::vector<std::pair<double, VectorItem>> distances;
    distances.reserve(vertors.size());
    for (const auto& candidate : vertors) {
      distances.push_back( { ComputeDistance(vec, candidate), candidate } );
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

  std::vector<VectorItem> SearchLayer(uint16_t level, const VectorItem& base_vector, uint32_t ef_runtime,
                                 const std::vector<NodeKey>& entry_points) {
    std::vector<VectorItem> candidates;
    std::unordered_set<NodeKey> visited;
    std::priority_queue<std::pair<double, VectorItem>, std::vector<std::pair<double, VectorItem>>, std::greater<>>
        explore_heap;
    std::priority_queue<std::pair<double, VectorItem>> result_heap;

    for (const auto& entry_point_key : entry_points) {
      Node entry_node = Node(entry_point_key, level);
      HnswNodeFieldMetadata node_metadata = entry_node.DecodeNodeMetadata(search_key_, storage);
      auto entry_point_vector = VectorItem(entry_point_key, node_metadata.vector, metadata_);
      auto dist = ComputeDistance(base_vector, entry_point_vector);
      explore_heap.push({dist, entry_point_vector});
      result_heap.push({dist, entry_point_vector});
      visited.insert(entry_point_key);
    }

    while (!explore_heap.empty()) {
      auto [dist, current_vector] = explore_heap.top();
      explore_heap.pop();
      if (dist > result_heap.top().first) {
        break;
      }

      auto node = Node(current_vector.key, level);
      node.DecodeNeighbours(search_key_, storage);
      for (const auto& neighbour_key : node.neighbours) {
        if (visited.find(neighbour_key) != visited.end()) {
          continue;
        }
        visited.insert(neighbour_key);
        Node neighbour_node = Node(neighbour_key, level);
        HnswNodeFieldMetadata neighbour_metadata = neighbour_node.DecodeNodeMetadata(search_key_, storage);
        auto neighbour_node_vector = VectorItem(neighbour_key, neighbour_metadata.vector, metadata_);
        auto dist = ComputeDistance(current_vector, neighbour_node_vector);
        explore_heap.push({dist, neighbour_node_vector});
        result_heap.push({dist, neighbour_node_vector});
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

  void InsertVectorEntry(std::string_view key, std::string_view vector_str, ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch) {
    auto cf_handle = storage->GetCFHandle(ColumnFamilyID::Search);
    auto vector_item = VectorItem(key, vector_str, metadata_);
    int target_level = RandomizeLayer();
    std::vector<VectorItem> nearest_elements;

    if (metadata_->num_levels != 0) {
      auto level = metadata_->num_levels - 1;
      std::vector<NodeKey> entry_points{DefaultEntryPoint(level)};

      for (; level > target_level; level--) {
        nearest_elements = SearchLayer(level, vector_item, metadata_->ef_runtime, entry_points);
        entry_points = {nearest_elements[0].key};
      }

      for (; level >= 0; level--) {
        nearest_elements = SearchLayer(level, vector_item, metadata_->ef_construction, entry_points);
        auto connect_vec_items = SelectNeighbors(vector_item, nearest_elements, level);
        for (const auto& connected_vec_item : connect_vec_items) {
          Connect(level, vector_item.key, connected_vec_item.key, batch, cf_handle);
        }

        for (const auto& connected_vec_item : connect_vec_items) {
          auto connected_node = Node(connected_vec_item.key, level);
          auto connected_node_metadata = connected_node.DecodeNodeMetadata(search_key_, storage);
          uint16_t connected_node_num_neighbours = connected_node_metadata.num_neighbours;
          auto m_max = level == 0 ? 2 * metadata_->m : metadata_->m;

          if (connected_node_num_neighbours <= m_max) {
            continue;
          }

          connected_node.DecodeNeighbours(search_key_, storage);
          std::vector<VectorItem> connected_node_neighbour_vec_items;
          for (const auto& connected_node_neighbour_key : connected_node.neighbours) {
            Node connected_node_neighbour = Node(connected_node_neighbour_key, level);
            auto connected_node_neighbour_metadata = connected_node_neighbour.DecodeNodeMetadata(search_key_, storage);
            auto neighbour_vector = VectorItem(connected_node_neighbour_key, connected_node_neighbour_metadata.vector, metadata_);
            connected_node_neighbour_vec_items.push_back(neighbour_vector);
          }
          auto new_neighbors = SelectNeighbors(connected_vec_item, connected_node_neighbour_vec_items, level);
          ResetEdges(connected_vec_item, new_neighbors, level, batch, cf_handle);
        }

        entry_points.clear();
        for (const auto& new_entry_point : nearest_elements) {
          entry_points.push_back(new_entry_point.key);
        }
      }
    } else {
      auto node_index_key = search_key_.ConstructHnswNode(0, key);
      HnswNodeFieldMetadata node_metadata(0, vector_str);
      std::string encoded_metadata;
      node_metadata.Encode(&encoded_metadata);
      batch->Put(cf_handle, node_index_key, encoded_metadata);
      metadata_->num_levels = 1;
    }

    while (metadata_->num_levels - 1 < target_level) {
      auto node_index_key = search_key_.ConstructHnswNode(metadata_->num_levels, key);
      HnswNodeFieldMetadata node_metadata(0, vector_str);
      std::string encoded_metadata;
      node_metadata.Encode(&encoded_metadata);
      batch->Put(cf_handle, node_index_key, encoded_metadata);
      metadata_->num_levels++;
    }

    std::string encoded_index_metadata;
    metadata_->Encode(&encoded_index_metadata);
    auto index_meta_key = search_key_.ConstructFieldMeta();
    batch->Put(cf_handle, index_meta_key, encoded_index_metadata);
  }
};

}  // namespace redis
