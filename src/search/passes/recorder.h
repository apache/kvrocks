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

#include "search/ir.h"
#include "search/ir_pass.h"

namespace kqir {

struct Recorder : Pass {
  struct Result {
    std::unique_ptr<Node> ir;
    std::string_view after_pass;
  };

  std::vector<Result> &results;
  std::string_view after_pass;

  std::string_view Name() override { return "Recorder"; }

  explicit Recorder(std::string_view after_pass, std::vector<Result> &results)
      : results(results), after_pass(after_pass) {}

  std::unique_ptr<Node> Transform(std::unique_ptr<Node> node) override {
    results.push_back(Result{node->Clone(), after_pass});
    return node;
  }
};

}  // namespace kqir
