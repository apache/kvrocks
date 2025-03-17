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

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/ir_plan.h"

namespace kqir {

// Forward declarations
class ENode;
class EClass;
class EGraph;

// Represents a unique ID for an equivalence class
using ClassId = size_t;

// Represents a node in the e-graph
class ENode {
 public:
  explicit ENode(std::string op) : op_(std::move(op)) {}

  ENode(std::string op, std::vector<ClassId> children) : op_(std::move(op)), children_(std::move(children)) {}

  // Get the operator name
  const std::string& op() const { return op_; }

  // Get the children class IDs
  const std::vector<ClassId>& children() const { return children_; }

  // Hash function for ENode
  size_t hash() const;

  // Equality operator for ENode
  bool operator==(const ENode& other) const;

 private:
  std::string op_;
  std::vector<ClassId> children_;
};

// Hash function for ENode to use in unordered containers
struct ENodeHash {
  size_t operator()(const ENode& node) const { return node.hash(); }
};

// Equivalence class in the e-graph
class EClass {
 public:
  explicit EClass(ClassId id) : id_(id) {}

  // Get the class ID
  ClassId id() const { return id_; }

  // Add a node to this equivalence class
  void add(ENode node);

  // Get all nodes in this equivalence class
  const std::unordered_set<ENode, ENodeHash>& nodes() const { return nodes_; }

 private:
  ClassId id_;
  std::unordered_set<ENode, ENodeHash> nodes_;
};

// The e-graph data structure
class EGraph {
 public:
  EGraph() = default;

  // Add a node to the e-graph, returns the class ID
  ClassId add(ENode node);

  // Find the canonical class ID for a given class ID
  ClassId find(ClassId id) const;

  // Non-const version for internal use
  ClassId find_mutable(ClassId id);

  // Merge two equivalence classes
  ClassId merge(ClassId id1, ClassId id2);

  // Get an equivalence class by ID
  const EClass& get_class(ClassId id) const;

  // Get all equivalence classes
  const std::unordered_map<ClassId, EClass>& get_classes() const { return classes_; }

  // Convert a KQIR node to an e-graph representation
  ClassId add_node(const Node* node);

  // Extract the best KQIR node from the e-graph based on a cost function
  std::unique_ptr<Node> extract_best();

 private:
  // Map from class ID to equivalence class
  std::unordered_map<ClassId, EClass> classes_;

  // Union-find data structure for class IDs
  std::unordered_map<ClassId, ClassId> parents_;

  // Next available class ID
  ClassId next_id_ = 0;
};

// Represents a rewrite rule in the e-graph
class Rewrite {
 public:
  virtual ~Rewrite() = default;

  // Apply this rewrite rule to the e-graph
  virtual void apply(EGraph& egraph) = 0;

  // Get the name of this rewrite rule
  virtual std::string name() const = 0;
};

// Represents a collection of rewrite rules
class RuleSet {
 public:
  // Add a rewrite rule to the rule set
  void add(std::unique_ptr<Rewrite> rule);

  // Apply all rewrite rules to the e-graph until saturation
  void run_until_saturation(EGraph& egraph, size_t max_iterations = 100);

 private:
  std::vector<std::unique_ptr<Rewrite>> rules_;
};

// Implementation of ENode::hash
inline size_t ENode::hash() const {
  size_t h = std::hash<std::string>{}(op_);
  for (ClassId id : children_) {
    h = h * 31 + std::hash<ClassId>{}(id);
  }
  return h;
}

// Implementation of ENode::operator==
inline bool ENode::operator==(const ENode& other) const { return op_ == other.op_ && children_ == other.children_; }

}  // namespace kqir
