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

#include "ir.h"
#include "string_util.h"

namespace kqir {

struct DotDumper {
  std::ostream &os;

  explicit DotDumper(std::ostream &os) : os(os) {}

  void Dump(Node *node) {
    os << "digraph {\n";
    dump(node);
    os << "}\n";
  }

 private:
  static std::string nodeId(Node *node) { return fmt::format("x{:x}", (uint64_t)node); }

  void dump(Node *node) {
    os << "  " << nodeId(node) << " [ label = \"" << node->Name();
    if (auto content = node->Content(); !content.empty()) {
      os << " (" << util::EscapeString(content) << ")\" ];\n";
    } else {
      os << "\" ];\n";
    }
    for (auto i = node->ChildBegin(); i != node->ChildEnd(); ++i) {
      os << "  " << nodeId(node) << " -> " << nodeId(*i) << ";\n";
      dump(*i);
    }
  }
};

}  // namespace kqir
