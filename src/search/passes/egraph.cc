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

#include "search/passes/egraph.h"

#include <algorithm>
#include <queue>
#include <unordered_map>
#include <unordered_set>

namespace kqir {

// Implementation of EClass::add
void EClass::add(ENode node) { nodes_.insert(std::move(node)); }

// Implementation of EGraph::add
ClassId EGraph::add(ENode node) {
  // Create new class IDs for each child node if not already in the e-graph
  std::vector<ClassId> new_children;
  new_children.reserve(node.children().size());

  for (ClassId child : node.children()) {
    new_children.push_back(find_mutable(child));
  }

  // Update node with canonical class IDs
  ENode new_node(node.op(), std::move(new_children));

  // Check if this node already exists in the e-graph
  for (auto& [id, eclass] : classes_) {
    for (const auto& existing_node : eclass.nodes()) {
      if (existing_node == new_node) {
        return id;
      }
    }
  }

  // Create a new class for this node
  ClassId id = next_id_++;
  classes_.emplace(id, EClass(id));
  parents_[id] = id;
  classes_.at(id).add(std::move(new_node));

  return id;
}

// Implementation of EGraph::find_mutable (non-const version)
ClassId EGraph::find_mutable(ClassId id) {
  if (parents_.count(id) == 0) {
    // Not found, return the original ID
    return id;
  }

  // Path compression for union-find
  if (parents_[id] != id) {
    parents_[id] = find_mutable(parents_[id]);
  }

  return parents_[id];
}

// Implementation of EGraph::find (const version)
ClassId EGraph::find(ClassId id) const {
  if (parents_.count(id) == 0) {
    // Not found, return the original ID
    return id;
  }

  // For const version, we can't do path compression
  if (parents_.at(id) != id) {
    return find(parents_.at(id));
  }

  return parents_.at(id);
}

// Implementation of EGraph::merge
ClassId EGraph::merge(ClassId id1, ClassId id2) {
  ClassId root1 = find_mutable(id1);
  ClassId root2 = find_mutable(id2);

  if (root1 == root2) {
    return root1;
  }

  // Union by rank (or just picking the first one for simplicity)
  parents_[root2] = root1;

  // Merge the equivalence classes
  for (const auto& node : classes_.at(root2).nodes()) {
    classes_.at(root1).add(node);
  }

  return root1;
}

// Implementation of EGraph::get_class
const EClass& EGraph::get_class(ClassId id) const { return classes_.at(id); }

// Implementation of EGraph::add_node
ClassId EGraph::add_node(const Node* node) {
  if (node == nullptr) {
    return 0;  // Special ID for null nodes
  }

  // Create an ENode representation based on the KQIR node type
  std::string op = std::string(node->Name());
  std::vector<ClassId> children;

  // Process child nodes recursively
  for (auto it = const_cast<Node*>(node)->ChildBegin(); it != const_cast<Node*>(node)->ChildEnd(); ++it) {
    Node* child = *it;
    children.push_back(add_node(child));
  }

  // Add content to the op name to distinguish literals, field references, etc.
  if (!node->Content().empty()) {
    op += ":" + node->Content();
  }

  return add(ENode(op, std::move(children)));
}

// Implementation of EGraph::extract_best
std::unique_ptr<Node> EGraph::extract_best() {
  // This default extraction just creates a new node tree based on the structure
  // of the e-graph. A more sophisticated implementation would use a cost model.

  std::unordered_map<ClassId, std::unique_ptr<Node>> extracted;

  // Function to recursively extract nodes
  std::function<std::unique_ptr<Node>(ClassId)> extract_recursive = [&](ClassId id) -> std::unique_ptr<Node> {
    id = find_mutable(id);

    // If already extracted, return a clone
    if (extracted.count(id) > 0) {
      return extracted.at(id)->Clone();
    }

    // Get the best node from this equivalence class
    const EClass& eclass = get_class(id);
    std::unique_ptr<Node> best_node;

    // Find the first node that can be reconstructed
    for (const auto& enode : eclass.nodes()) {
      // Extract children first
      std::vector<std::unique_ptr<Node>> child_nodes;
      bool all_children_extracted = true;

      for (ClassId child_id : enode.children()) {
        auto child_node = extract_recursive(child_id);
        if (child_node) {
          child_nodes.push_back(std::move(child_node));
        } else {
          all_children_extracted = false;
          break;
        }
      }

      if (!all_children_extracted) {
        continue;
      }

      // Create a new node based on the operator type
      std::string_view op_name = enode.op();

      // Parse the operator name to handle content-enhanced op names
      std::string op_str(op_name);
      std::string content;
      size_t colon_pos = op_str.find(':');

      if (colon_pos != std::string::npos) {
        content = op_str.substr(colon_pos + 1);
        op_str = op_str.substr(0, colon_pos);
      }

      // This is a simplified reconstruction that would need to be expanded
      // based on the actual node types in your system
      if (op_str == "Filter") {
        if (child_nodes.size() == 2) {
          auto source = Node::MustAs<PlanOperator>(std::move(child_nodes[0]));
          auto filter_expr = Node::MustAs<QueryExpr>(std::move(child_nodes[1]));
          best_node = std::make_unique<Filter>(std::move(source), std::move(filter_expr));
        }
      } else if (op_str == "Merge") {
        std::vector<std::unique_ptr<PlanOperator>> ops;
        for (auto& child : child_nodes) {
          ops.push_back(Node::MustAs<PlanOperator>(std::move(child)));
        }
        best_node = std::make_unique<Merge>(std::move(ops));
      }
      // Add more node types as needed...

      if (best_node) {
        break;
      }
    }

    if (best_node) {
      extracted[id] = best_node->Clone();
    }

    return best_node;
  };

  // Start extraction from the root nodes
  for (const auto& [id, _] : classes_) {
    if (id == find(id)) {  // Only consider canonical classes
      auto node = extract_recursive(id);
      if (node) {
        return node;
      }
    }
  }

  return nullptr;
}

// Implementation of RuleSet::add
void RuleSet::add(std::unique_ptr<Rewrite> rule) { rules_.push_back(std::move(rule)); }

// Implementation of RuleSet::run_until_saturation
void RuleSet::run_until_saturation(EGraph& egraph, size_t max_iterations) {
  size_t iterations = 0;
  size_t prev_size = 0;

  // Run until we reach max iterations or the e-graph stops growing
  while (iterations < max_iterations) {
    // Calculate the current size of the e-graph
    size_t current_size = 0;
    for (const auto& [_, eclass] : egraph.get_classes()) {
      current_size += eclass.nodes().size();
    }

    // Check if we've reached saturation
    if (iterations > 0 && current_size == prev_size) {
      break;
    }

    prev_size = current_size;

    // Apply all rewrite rules
    for (auto& rule : rules_) {
      rule->apply(egraph);
    }

    iterations++;
  }
}

}  // namespace kqir
