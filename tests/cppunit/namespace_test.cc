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
#include "server/namespace.h"

#include <gtest/gtest.h>

#include <fstream>

#include "config/config.h"
#include "test_base.h"

class NamespaceTest : public TestBase {
 protected:
  explicit NamespaceTest() { config_->requirepass = "123"; }

  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(NamespaceTest, AddAndDelete) {
  for (const auto &v : {true, false}) {
    auto ns = std::make_unique<Namespace>(storage_);
    std::map<std::string, std::string> tokens = {
        {"tokens2", "test_ns"}, {"tokens", "test_ns2"}, {"tokens3", "test_ns3"}};
    config_->repl_namespace_enabled = v;
    for (const auto &iter : tokens) {
      ASSERT_TRUE(ns->Add(iter.second, iter.first).IsOK());
    }

    // test add duplicate namespace
    for (const auto &iter : tokens) {
      ASSERT_FALSE(ns->Add(iter.second, "new_" + iter.first).IsOK());
    }

    for (const auto &iter : tokens) {
      ASSERT_EQ(iter.first, ns->Get(iter.second).GetValue());
    }

    for (const auto &iter : tokens) {
      ASSERT_TRUE(ns->Set(iter.second, "new_" + iter.first).IsOK());
    }

    auto list_tokens = ns->List();
    ASSERT_EQ(list_tokens.size(), tokens.size());
    for (const auto &iter : tokens) {
      ASSERT_EQ(iter.second, list_tokens["new_" + iter.first]);
    }

    for (const auto &iter : tokens) {
      ASSERT_TRUE(ns->Del(iter.second).IsOK());
    }
    ASSERT_EQ(0, ns->List().size());
  }
}
