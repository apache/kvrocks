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

#include <gtest/gtest.h>

#include "commands/commander.h"
#include "common/scope_exit.h"
namespace {

TEST(SubcommandResolution, ResolveRootCommandWithoutSubcommand) {
  auto resolved = redis::CommandTable::Resolve({"ping"});

  ASSERT_TRUE(resolved);
  EXPECT_EQ(resolved->root, "ping");
  ASSERT_NE(resolved->attributes, nullptr);
  EXPECT_EQ(resolved->attributes->name, "ping");
}

TEST(SubcommandResolution, ResolveRegisteredNamespaceSubcommand) {
  auto resolved = redis::CommandTable::Resolve({"namespace", "add", "ns-1", "token-1"});

  ASSERT_TRUE(resolved);
  EXPECT_EQ(resolved->root, "namespace");
  ASSERT_NE(resolved->attributes, nullptr);
  EXPECT_EQ(resolved->attributes->name, "namespace|add");
  EXPECT_EQ(resolved->attributes->arity, 4);
}

TEST(SubcommandResolution, UnknownNamespaceSubcommandUsesRootCommand) {
  auto resolved = redis::CommandTable::Resolve({"namespace", "missing"});

  ASSERT_TRUE(resolved);
  EXPECT_EQ(resolved->root, "namespace");
  ASSERT_NE(resolved->attributes, nullptr);
  EXPECT_EQ(resolved->attributes->name, "namespace");
}

TEST(SubcommandResolution, MissingNamespaceSubcommandRejectsArity) {
  auto resolved = redis::CommandTable::Resolve({"namespace"});

  ASSERT_TRUE(resolved);
  EXPECT_EQ(resolved->root, "namespace");
  ASSERT_NE(resolved->attributes, nullptr);
  EXPECT_EQ(resolved->attributes->name, "namespace");
  EXPECT_FALSE(resolved->attributes->CheckArity(1));
}

TEST(SubcommandResolution, ResolveRenamedRootCommandWithSubcommand) {
  auto reset_guard = MakeScopeExit([] { redis::CommandTable::Reset(); });

  auto *commands = redis::CommandTable::Get();
  auto command_iter = commands->find("namespace");
  ASSERT_NE(command_iter, commands->end());
  (*commands)["ns"] = command_iter->second;
  commands->erase(command_iter);

  auto resolved = redis::CommandTable::Resolve({"ns", "add", "ns-1", "token-1"});

  ASSERT_TRUE(resolved);
  EXPECT_EQ(resolved->root, "namespace");
  ASSERT_NE(resolved->attributes, nullptr);
  EXPECT_EQ(resolved->attributes->name, "namespace|add");
}

}  // namespace
