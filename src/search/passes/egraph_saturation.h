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
#include <vector>

#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/ir_plan.h"
#include "search/passes/egraph.h"

namespace kqir {

// Forward declarations for rewrite rules
class FilterPushDownRewrite;
class MergeFilterRewrite;
class SortPushDownRewrite;
class FilterMergeRewrite;
class CommonSubexpressionRewrite;

// Cost model for extracting the best plan from the e-graph
class QueryPlanCostModel {
 public:
  // Calculate the cost of a node in the e-graph
  double calculate_cost(const Node* node) const;

  // Compare two nodes and return the one with lower cost
  std::unique_ptr<Node> choose_best(std::unique_ptr<Node> a, std::unique_ptr<Node> b) const;
};

// E-graph equality saturation pass for KQIR optimizer
class EGraphSaturation : public Pass {
 public:
  EGraphSaturation();

  // Transform a query plan using e-graph equality saturation
  std::unique_ptr<Node> Transform(std::unique_ptr<Node> node) override;

  // Reset the pass state
  void Reset() override;

 private:
  // Create the rule set with all rewrite rules
  void create_rule_set();

  // Convert a KQIR node to an e-graph
  EGraph build_egraph(const Node* node);

  // Extract the best plan from the e-graph
  std::unique_ptr<Node> extract_best_plan(const EGraph& egraph);

  // Helper function to reconstruct a Node from an ENode's operator and children
  std::unique_ptr<Node> reconstruct_node(const std::string& op, const std::vector<std::unique_ptr<Node>>& children);

  // The rule set containing all rewrite rules
  RuleSet rule_set_;

  // The cost model for extracting the best plan
  QueryPlanCostModel cost_model_;
};

// Rewrite rule for pushing down filter operations
class FilterPushDownRewrite : public Rewrite {
 public:
  void apply(EGraph& egraph) override;
  std::string name() const override { return "FilterPushDown"; }
};

// Rewrite rule for merging adjacent filter operations
class MergeFilterRewrite : public Rewrite {
 public:
  void apply(EGraph& egraph) override;
  std::string name() const override { return "MergeFilter"; }
};

// Rewrite rule for pushing down sort operations
class SortPushDownRewrite : public Rewrite {
 public:
  void apply(EGraph& egraph) override;
  std::string name() const override { return "SortPushDown"; }
};

// Rewrite rule for merging filter operations across merges
class FilterMergeRewrite : public Rewrite {
 public:
  void apply(EGraph& egraph) override;
  std::string name() const override { return "FilterMerge"; }
};

// Rewrite rule for identifying and eliminating common subexpressions
class CommonSubexpressionRewrite : public Rewrite {
 public:
  void apply(EGraph& egraph) override;
  std::string name() const override { return "CommonSubexpression"; }
};

}  // namespace kqir
