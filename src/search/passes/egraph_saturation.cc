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

#include "search/passes/egraph_saturation.h"

#include <cmath>  // For INFINITY
#include <unordered_map>
#include <vector>

#include "search/interval.h"

namespace kqir {

// Cost model implementation
double QueryPlanCostModel::calculate_cost(const Node* node) const {
  if (!node) return std::numeric_limits<double>::max();

  std::string_view node_type = node->Name();

  // Base costs for different operation types (heuristic values)
  double base_cost = 0.0;

  if (node_type == "Filter") {
    base_cost = 10.0;
  } else if (node_type == "FullIndexScan") {
    base_cost = 100.0;  // Full scans are expensive
  } else if (node_type == "NumericFieldScan") {
    base_cost = 5.0;
  } else if (node_type == "TagFieldScan") {
    base_cost = 5.0;
  } else if (node_type == "HnswVectorFieldKnnScan") {
    base_cost = 20.0;
  } else if (node_type == "HnswVectorFieldRangeScan") {
    base_cost = 15.0;
  } else if (node_type == "Merge") {
    base_cost = 30.0;
  } else if (node_type == "Sort") {
    base_cost = 40.0;
  } else if (node_type == "TopNSort") {
    base_cost = 35.0;  // Slightly cheaper than separate Sort + Limit
  } else if (node_type == "Limit") {
    base_cost = 1.0;
  } else if (node_type == "Projection") {
    base_cost = 2.0;
  } else {
    base_cost = 1.0;  // Default cost for unknown types
  }

  // Calculate child costs
  double child_cost = 0.0;
  for (auto it = const_cast<Node*>(node)->ChildBegin(); it != const_cast<Node*>(node)->ChildEnd(); ++it) {
    child_cost += calculate_cost(*it);
  }

  // Total cost is base cost plus child costs
  return base_cost + child_cost;
}

std::unique_ptr<Node> QueryPlanCostModel::choose_best(std::unique_ptr<Node> a, std::unique_ptr<Node> b) const {
  if (!a) return b;
  if (!b) return a;

  double cost_a = calculate_cost(a.get());
  double cost_b = calculate_cost(b.get());

  return (cost_a <= cost_b) ? std::move(a) : std::move(b);
}

// EGraphSaturation pass implementation
EGraphSaturation::EGraphSaturation() { create_rule_set(); }

std::unique_ptr<Node> EGraphSaturation::Transform(std::unique_ptr<Node> node) {
  if (!node) return nullptr;

  // Build e-graph from the input node
  EGraph egraph = build_egraph(node.get());

  // Apply rewrite rules until saturation
  rule_set_.run_until_saturation(egraph);

  // Extract the best plan from the e-graph
  auto best_plan = extract_best_plan(egraph);
  return best_plan ? std::move(best_plan) : std::move(node);
}

void EGraphSaturation::Reset() {
  // Reset the rule set and cost model if needed
}

void EGraphSaturation::create_rule_set() {
  // Add rewrite rules to the rule set
  rule_set_.add(std::make_unique<FilterPushDownRewrite>());
  rule_set_.add(std::make_unique<MergeFilterRewrite>());
  rule_set_.add(std::make_unique<SortPushDownRewrite>());
  rule_set_.add(std::make_unique<FilterMergeRewrite>());
  rule_set_.add(std::make_unique<CommonSubexpressionRewrite>());
}

EGraph EGraphSaturation::build_egraph(const Node* node) {
  EGraph egraph;
  egraph.add_node(node);
  return egraph;
}

std::unique_ptr<Node> EGraphSaturation::extract_best_plan(const EGraph& egraph) {
  // This is a more sophisticated extraction that uses the cost model
  // to select the best plan from the e-graph

  // Map to store the best node for each class ID
  std::unordered_map<ClassId, std::unique_ptr<Node>> best_nodes;

  // Process classes in topological order (from leaves to root)
  std::vector<ClassId> class_ids;
  for (const auto& [id, _] : egraph.get_classes()) {
    if (id == const_cast<EGraph&>(egraph).find(id)) {  // Only consider canonical class IDs
      class_ids.push_back(id);
    }
  }

  // Sort classes by their depth (approximated by ID for simplicity)
  std::sort(class_ids.begin(), class_ids.end());

  // Extract best node for each class
  for (ClassId id : class_ids) {
    const EClass& eclass = egraph.get_class(id);
    std::unique_ptr<Node> best_node;

    for (const auto& enode : eclass.nodes()) {
      // Attempt to reconstruct this node
      std::vector<std::unique_ptr<Node>> children;
      bool can_reconstruct = true;

      for (ClassId child_id : enode.children()) {
        child_id = const_cast<EGraph&>(egraph).find(child_id);  // Get canonical ID

        // Skip if we don't have the child
        if (best_nodes.find(child_id) == best_nodes.end()) {
          can_reconstruct = false;
          break;
        }

        // Add a clone of the child
        children.push_back(best_nodes[child_id]->Clone());
      }

      if (!can_reconstruct) continue;

      // Reconstruct the node (this is a simplified version)
      std::unique_ptr<Node> reconstructed = reconstruct_node(enode.op(), children);

      if (reconstructed) {
        // Use the cost model to select the best node
        best_node = cost_model_.choose_best(std::move(best_node), std::move(reconstructed));
      }
    }

    if (best_node) {
      best_nodes[id] = std::move(best_node);
    }
  }

  // Find the root node (the one that contains the entire plan)
  for (auto it = class_ids.rbegin(); it != class_ids.rend(); ++it) {
    ClassId id = *it;
    if (best_nodes.find(id) != best_nodes.end()) {
      return std::move(best_nodes[id]);
    }
  }

  return nullptr;
}

// Implementation of reconstruct_node helper function
std::unique_ptr<Node> EGraphSaturation::reconstruct_node(const std::string& op,
                                                         const std::vector<std::unique_ptr<Node>>& children) {
  // Parse the operator name to handle content-enhanced op names
  std::string op_str = op;
  std::string content;
  size_t colon_pos = op_str.find(':');

  if (colon_pos != std::string::npos) {
    content = op_str.substr(colon_pos + 1);
    op_str = op_str.substr(0, colon_pos);
  }

  // Reconstruct different node types based on the operator name
  if (op_str == "Filter") {
    if (children.size() == 2) {
      auto source = Node::MustAs<PlanOperator>(children[0]->Clone());
      auto filter_expr = Node::MustAs<QueryExpr>(children[1]->Clone());
      return std::make_unique<Filter>(std::move(source), std::move(filter_expr));
    }
  } else if (op_str == "Merge") {
    std::vector<std::unique_ptr<PlanOperator>> ops;
    for (const auto& child : children) {
      ops.push_back(Node::MustAs<PlanOperator>(child->Clone()));
    }
    return std::make_unique<Merge>(std::move(ops));
  } else if (op_str == "Sort") {
    if (children.size() == 2) {
      auto source = Node::MustAs<PlanOperator>(children[0]->Clone());
      auto order = Node::MustAs<SortByClause>(children[1]->Clone());
      return std::make_unique<Sort>(std::move(source), std::move(order));
    }
  } else if (op_str == "Limit") {
    if (children.size() == 2) {
      auto source = Node::MustAs<PlanOperator>(children[0]->Clone());
      auto limit = Node::MustAs<LimitClause>(children[1]->Clone());
      return std::make_unique<Limit>(std::move(source), std::move(limit));
    }
  } else if (op_str == "TopNSort") {
    if (children.size() == 3) {
      auto source = Node::MustAs<PlanOperator>(children[0]->Clone());
      auto order = Node::MustAs<SortByClause>(children[1]->Clone());
      auto limit = Node::MustAs<LimitClause>(children[2]->Clone());
      return std::make_unique<TopNSort>(std::move(source), std::move(order), std::move(limit));
    }
  } else if (op_str == "Projection") {
    if (children.size() == 2) {
      auto source = Node::MustAs<PlanOperator>(children[0]->Clone());
      auto select = Node::MustAs<SelectClause>(children[1]->Clone());
      return std::make_unique<Projection>(std::move(source), std::move(select));
    }
  } else if (op_str == "FullIndexScan") {
    if (children.size() == 1) {
      auto index = Node::MustAs<IndexRef>(children[0]->Clone());
      return std::make_unique<FullIndexScan>(std::move(index));
    }
  } else if (op_str == "NumericFieldScan") {
    // This is a simplification; actual implementation would parse content for range and order
    if (children.size() == 1) {
      auto field = Node::MustAs<FieldRef>(children[0]->Clone());
      return std::make_unique<NumericFieldScan>(std::move(field), Interval(-INFINITY, INFINITY), SortByClause::ASC);
    }
  } else if (op_str == "TagFieldScan") {
    if (children.size() == 1 && !content.empty()) {
      auto field = Node::MustAs<FieldRef>(children[0]->Clone());
      return std::make_unique<TagFieldScan>(std::move(field), content);
    }
  } else if (op_str == "AndExpr") {
    std::vector<std::unique_ptr<QueryExpr>> inners;
    for (const auto& child : children) {
      inners.push_back(Node::MustAs<QueryExpr>(child->Clone()));
    }
    return std::make_unique<AndExpr>(std::move(inners));
  } else if (op_str == "OrExpr") {
    std::vector<std::unique_ptr<QueryExpr>> inners;
    for (const auto& child : children) {
      inners.push_back(Node::MustAs<QueryExpr>(child->Clone()));
    }
    return std::make_unique<OrExpr>(std::move(inners));
  } else if (op_str == "NotExpr") {
    if (children.size() == 1) {
      auto inner = Node::MustAs<QueryExpr>(children[0]->Clone());
      return std::make_unique<NotExpr>(std::move(inner));
    }
  }

  // Add more node types as needed

  // Return nullptr if reconstruction isn't possible
  return nullptr;
}

// FilterPushDownRewrite implementation
void FilterPushDownRewrite::apply(EGraph& egraph) {
  // Find Filter(Merge(...)) patterns and rewrite to Merge(Filter(...), ...)
  for (auto& [class_id, eclass] : egraph.get_classes()) {
    std::vector<ENode> new_nodes;

    for (const auto& node : eclass.nodes()) {
      if (node.op() == "Filter") {
        const auto& children = node.children();
        if (children.size() == 2) {  // Filter has source and predicate
          ClassId source_id = children[0];
          ClassId filter_expr_id = children[1];

          // Check if the source is a Merge node
          const EClass& source_class = egraph.get_class(egraph.find(source_id));
          for (const auto& source_node : source_class.nodes()) {
            if (source_node.op() == "Merge") {
              // Create a Filter for each input of the Merge
              std::vector<ClassId> new_sources;
              for (ClassId merge_input : source_node.children()) {
                // Create new Filter(input, filter_expr)
                std::vector<ClassId> filter_children = {merge_input, filter_expr_id};
                ClassId new_filter_id = egraph.add(ENode("Filter", filter_children));
                new_sources.push_back(new_filter_id);
              }

              // Create new Merge(Filter(input1, expr), Filter(input2, expr), ...)
              ClassId new_merge_id = egraph.add(ENode("Merge", new_sources));

              // Add to the e-graph and merge with the original class
              egraph.merge(class_id, new_merge_id);
            }
          }
        }
      }
    }
  }
}

// MergeFilterRewrite implementation
void MergeFilterRewrite::apply(EGraph& egraph) {
  // Find Filter(Filter(source, expr1), expr2) and rewrite to Filter(source, AndExpr(expr1, expr2))
  for (auto& [class_id, eclass] : egraph.get_classes()) {
    for (const auto& node : eclass.nodes()) {
      if (node.op() == "Filter") {
        const auto& children = node.children();
        if (children.size() == 2) {
          ClassId source_id = children[0];
          ClassId outer_filter_expr_id = children[1];

          // Check if the source is also a Filter
          const EClass& source_class = egraph.get_class(egraph.find(source_id));
          for (const auto& source_node : source_class.nodes()) {
            if (source_node.op() == "Filter") {
              // Get the inner filter's source and expression
              const auto& inner_children = source_node.children();
              if (inner_children.size() == 2) {
                ClassId inner_source_id = inner_children[0];
                ClassId inner_filter_expr_id = inner_children[1];

                // Create a new AndExpr combining both filter expressions
                std::vector<ClassId> and_children = {inner_filter_expr_id, outer_filter_expr_id};
                ClassId and_expr_id = egraph.add(ENode("AndExpr", and_children));

                // Create a new Filter with the combined expression
                std::vector<ClassId> filter_children = {inner_source_id, and_expr_id};
                ClassId new_filter_id = egraph.add(ENode("Filter", filter_children));

                // Merge with the original class
                egraph.merge(class_id, new_filter_id);
              }
            }
          }
        }
      }
    }
  }
}

// SortPushDownRewrite implementation
void SortPushDownRewrite::apply(EGraph& egraph) {
  // Find Sort(Merge(...), order) patterns and try to push sort down when possible
  for (auto& [class_id, eclass] : egraph.get_classes()) {
    for (const auto& node : eclass.nodes()) {
      if (node.op() == "Sort") {
        const auto& children = node.children();
        if (children.size() == 2) {  // Sort has source and order
          ClassId source_id = children[0];
          ClassId order_id = children[1];

          // Check if the source is a Merge
          const EClass& source_class = egraph.get_class(egraph.find(source_id));
          for (const auto& source_node : source_class.nodes()) {
            if (source_node.op() == "Merge") {
              // Create a Sort for each input of the Merge
              std::vector<ClassId> sorted_inputs;
              for (ClassId merge_input : source_node.children()) {
                // Create Sort(input, order)
                std::vector<ClassId> sort_children = {merge_input, order_id};
                ClassId sorted_input_id = egraph.add(ENode("Sort", sort_children));
                sorted_inputs.push_back(sorted_input_id);
              }

              // Create a new Merge of the sorted inputs
              ClassId new_merge_id = egraph.add(ENode("Merge", sorted_inputs));

              // Create a Sort(Merge(Sort(...)), order) to maintain the overall ordering
              std::vector<ClassId> outer_sort_children = {new_merge_id, order_id};
              ClassId outer_sort_id = egraph.add(ENode("Sort", outer_sort_children));

              // Merge with the original class
              egraph.merge(class_id, outer_sort_id);
            }
          }
        }
      }
    }
  }
}

// FilterMergeRewrite implementation
void FilterMergeRewrite::apply(EGraph& egraph) {
  // Find Merge(Filter(A, expr), Filter(B, expr)) and rewrite to Filter(Merge(A, B), expr)
  std::unordered_map<std::string, std::vector<std::pair<ClassId, ClassId>>> filter_map;

  // First pass: collect all Filter nodes by their expression
  for (auto& [class_id, eclass] : egraph.get_classes()) {
    for (const auto& node : eclass.nodes()) {
      if (node.op() == "Filter") {
        const auto& children = node.children();
        if (children.size() == 2) {
          ClassId source_id = children[0];
          ClassId expr_id = children[1];

          // Use the canonical ID of the expression as the key
          ClassId canonical_expr_id = egraph.find(expr_id);
          std::string key = std::to_string(canonical_expr_id);

          filter_map[key].push_back({class_id, source_id});
        }
      }
    }
  }

  // Second pass: find matching filters and create merged versions
  for (const auto& [expr_key, filters] : filter_map) {
    if (filters.size() < 2) continue;

    // Get the expression ID from the key
    ClassId expr_id = std::stoul(expr_key);

    // Create all possible combinations of merges
    for (size_t i = 0; i < filters.size(); ++i) {
      for (size_t j = i + 1; j < filters.size(); ++j) {
        // ClassId filter1_id = filters[i].first; // Using filters[i] directly below
        ClassId source1_id = filters[i].second;
        // ClassId filter2_id = filters[j].first; // Using filters[j] directly below
        ClassId source2_id = filters[j].second;

        // Create Merge(source1, source2)
        std::vector<ClassId> merge_children = {source1_id, source2_id};
        ClassId merge_id = egraph.add(ENode("Merge", merge_children));

        // Create Filter(Merge(source1, source2), expr)
        std::vector<ClassId> filter_children = {merge_id, expr_id};
        egraph.add(ENode("Filter", filter_children));  // No need to store the ID

        // We can't directly merge the filter IDs since they might be in different
        // parts of the e-graph. Instead, we add this as a new pattern.
      }
    }
  }
}

// CommonSubexpressionRewrite implementation
void CommonSubexpressionRewrite::apply(EGraph& egraph) {
  // Identify common subexpressions and merge their equivalence classes
  std::unordered_map<std::string, ClassId> expr_map;

  // First pass: collect all subexpressions
  for (auto& [class_id, eclass] : egraph.get_classes()) {
    for (const auto& node : eclass.nodes()) {
      // Create a string representation of the node to identify common subexpressions
      std::string node_str = node.op();
      for (ClassId child_id : node.children()) {
        node_str += ":" + std::to_string(egraph.find(child_id));
      }

      // Check if we've seen this subexpression before
      if (expr_map.count(node_str) > 0) {
        ClassId existing_id = expr_map[node_str];
        // Only merge if they're not already the same class
        if (egraph.find(class_id) != egraph.find(existing_id)) {
          // Merge this class with the previously seen class
          egraph.merge(class_id, existing_id);
        }
      } else {
        // Record this subexpression
        expr_map[node_str] = class_id;
      }
    }
  }
}

}  // namespace kqir
