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

#include <functional>
#include <memory>
#include <variant>
#include <vector>

#include "type_util.h"

namespace kqir {

struct Node;

struct NodeIterator {
  std::variant<Node *, std::array<Node *, 2>,
               std::pair<Node *, std::vector<std::function<Node *(Node *)>>::const_iterator>,
               std::vector<std::unique_ptr<Node>>::iterator>
      val;

  NodeIterator() : val(nullptr) {}
  explicit NodeIterator(Node *node) : val(node) {}
  NodeIterator(Node *n1, Node *n2) : val(std::array<Node *, 2>{n1, n2}) {}
  explicit NodeIterator(Node *parent, std::vector<std::function<Node *(Node *)>>::const_iterator iter)
      : val(std::make_pair(parent, iter)) {}
  template <typename Iterator,
            std::enable_if_t<std::is_base_of_v<Node, typename Iterator::value_type::element_type>, int> = 0>
  explicit NodeIterator(Iterator iter) : val(*CastToNodeIter(&iter)) {}

  template <typename Iterator>
  static auto CastToNodeIter(Iterator *iter) {
    auto res __attribute__((__may_alias__)) = reinterpret_cast<std::vector<std::unique_ptr<Node>>::iterator *>(iter);
    return res;
  }

  template <auto F>
  static Node *MemFn(Node *parent) {
    return (reinterpret_cast<typename GetClassFromMember<decltype(F)>::type *>(parent)->*F).get();
  }

  friend bool operator==(NodeIterator l, NodeIterator r) { return l.val == r.val; }

  friend bool operator!=(NodeIterator l, NodeIterator r) { return l.val != r.val; }

  Node *operator*() {
    if (val.index() == 0) {
      return std::get<0>(val);
    } else if (val.index() == 1) {
      return std::get<1>(val)[0];
    } else if (val.index() == 2) {
      auto &[parent, iter] = std::get<2>(val);
      return (*iter)(parent);
    } else {
      return std::get<3>(val)->get();
    }
  }

  NodeIterator &operator++() {
    if (val.index() == 0) {
      val = nullptr;
    } else if (val.index() == 1) {
      val = std::get<1>(val)[1];
    } else if (val.index() == 2) {
      ++std::get<2>(val).second;
    } else {
      ++std::get<3>(val);
    }

    return *this;
  }
};

}  // namespace kqir