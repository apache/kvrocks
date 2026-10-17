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

#include "common/keyspace_events.h"

#include <gtest/gtest.h>

#include <utility>

#include "config/config.h"
#include "storage/storage.h"

TEST(KeyspaceEvents, ContextFiltersAndCapturesEvent) {
  auto ctx = engine::Context::NoTransactionContext(nullptr);
  EXPECT_FALSE(ctx.HasKeyspaceEvents());

  ctx.AddKeyspaceEventIfEnabled(kNotifyString, "set", "tenant", "disabled");
  EXPECT_FALSE(ctx.HasKeyspaceEvents());

  ctx.EnableKeyspaceEventCollection(kNotifyNoChannel, kNotifyString);
  EXPECT_FALSE(ctx.IsKeyspaceEventEnabled(kNotifyString));

  ctx.EnableKeyspaceEventCollection(kNotifyKeyspace, kNotifyString);
  EXPECT_TRUE(ctx.IsKeyspaceEventEnabled(kNotifyString));
  EXPECT_FALSE(ctx.IsKeyspaceEventEnabled(kNotifyGeneric));

  ctx.AddKeyspaceEventIfEnabled(kNotifyGeneric, "del", "tenant", "ignored");
  EXPECT_FALSE(ctx.HasKeyspaceEvents());
  ctx.AddKeyspaceEventIfEnabled(kNotifyString, "set", "tenant", "key");

  auto events = ctx.TakeKeyspaceEvents();
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].channel_flags, kNotifyKeyspace);
  EXPECT_EQ(events[0].event, "set");
  EXPECT_EQ(events[0].ns, "tenant");
  EXPECT_EQ(events[0].key, "key");
  EXPECT_FALSE(ctx.HasKeyspaceEvents());
  EXPECT_TRUE(ctx.TakeKeyspaceEvents().empty());
}

TEST(KeyspaceEvents, ContextCapturesNamespacePerEvent) {
  auto ctx = engine::Context::NoTransactionContext(nullptr);
  ctx.EnableKeyspaceEventCollection(kNotifyKeyspace, kNotifyString);
  ctx.AddKeyspaceEventIfEnabled(kNotifyString, "set", "tenant-1", "first");
  ctx.AddKeyspaceEventIfEnabled(kNotifyString, "set", "tenant-2", "second");

  auto events = ctx.TakeKeyspaceEvents();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].ns, "tenant-1");
  EXPECT_EQ(events[1].ns, "tenant-2");
}

TEST(KeyspaceEvents, ContextMovePreservesEventOrder) {
  const auto channel_flags = static_cast<KeyspaceEventChannel>(kNotifyKeyspace | kNotifyKeyevent);
  auto ctx = engine::Context::NoTransactionContext(nullptr);
  ctx.EnableKeyspaceEventCollection(channel_flags, kNotifyAll);
  ctx.AddKeyspaceEventIfEnabled(kNotifyString, "set", "tenant", "first");
  ctx.AddKeyspaceEventIfEnabled(kNotifyGeneric, "del", "tenant", "second");

  auto moved_ctx = std::move(ctx);
  auto assigned_ctx = engine::Context::NoTransactionContext(nullptr);
  assigned_ctx = std::move(moved_ctx);

  auto events = assigned_ctx.TakeKeyspaceEvents();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].channel_flags, channel_flags);
  EXPECT_EQ(events[0].event, "set");
  EXPECT_EQ(events[0].ns, "tenant");
  EXPECT_EQ(events[0].key, "first");
  EXPECT_EQ(events[1].channel_flags, channel_flags);
  EXPECT_EQ(events[1].event, "del");
  EXPECT_EQ(events[1].ns, "tenant");
  EXPECT_EQ(events[1].key, "second");
  EXPECT_FALSE(assigned_ctx.HasKeyspaceEvents());
}

TEST(KeyspaceEvents, ParseFlags) {
  // Empty disables notifications.
  auto empty_flags = ParseNotifyKeyspaceEventsFlags("");
  ASSERT_TRUE(empty_flags.IsOK());
  EXPECT_EQ(empty_flags->first, kNotifyNoChannel);
  EXPECT_EQ(empty_flags->second, kNotifyNoType);

  // Channel and event type flags are parsed into independent masks.
  auto channel_flags = ParseNotifyKeyspaceEventsFlags("KE");
  ASSERT_TRUE(channel_flags.IsOK());
  EXPECT_EQ(channel_flags->first, kNotifyKeyspace | kNotifyKeyevent);
  EXPECT_EQ(channel_flags->second, kNotifyNoType);

  auto type_flags = ParseNotifyKeyspaceEventsFlags("g$");
  ASSERT_TRUE(type_flags.IsOK());
  EXPECT_EQ(type_flags->first, kNotifyNoChannel);
  EXPECT_EQ(type_flags->second, kNotifyGeneric | kNotifyString);

  // KEA enables both channels and set or del.
  auto flags = ParseNotifyKeyspaceEventsFlags("KEA");
  ASSERT_TRUE(flags.IsOK());
  EXPECT_EQ(flags->first, kNotifyKeyspace | kNotifyKeyevent);
  EXPECT_EQ(flags->second, kNotifyAll);
}

TEST(KeyspaceEvents, ParseFlagsAExpansion) {
  auto flags = ParseNotifyKeyspaceEventsFlags("A");
  ASSERT_TRUE(flags.IsOK());
  // A expands to all supported event classes without K or E.
  EXPECT_EQ(flags->first, kNotifyNoChannel);
  EXPECT_EQ(flags->second, kNotifyAll);
}

TEST(KeyspaceEvents, ParseFlagsRejectsUnsupported) {
  // Unsupported flags are rejected.
  for (const auto *bad : {"a", "d", "x", "e", "m", "n", "o", "c", "l", "s", "h", "z", "t", "Kx", "KEl", "?"}) {
    ASSERT_FALSE(ParseNotifyKeyspaceEventsFlags(bad).IsOK()) << "should reject: " << bad;
  }
}

TEST(KeyspaceEvents, FormatKeyspaceNotificationScope) {
  // Default namespace maps to db 0.
  EXPECT_EQ(FormatKeyspaceNotificationScope(kDefaultNamespace, 0), "0");
  EXPECT_EQ(FormatKeyspaceNotificationScope(kDefaultNamespace, 16), "0");

  // Non-default namespaces use their original names.
  EXPECT_EQ(FormatKeyspaceNotificationScope("0", 0), "0");
  EXPECT_EQ(FormatKeyspaceNotificationScope("tenantA", 0), "tenantA");
  EXPECT_EQ(FormatKeyspaceNotificationScope("a.b-c_d", 0), "a.b-c_d");
  EXPECT_EQ(FormatKeyspaceNotificationScope("db1", 0), "db1");
  EXPECT_EQ(FormatKeyspaceNotificationScope("a b", 0), "a b");
  EXPECT_EQ(FormatKeyspaceNotificationScope("a:b", 0), "a:b");
  EXPECT_EQ(FormatKeyspaceNotificationScope("100%", 0), "100%");

  // Redis database namespaces map back to numeric database names when redis-databases is enabled.
  EXPECT_EQ(FormatKeyspaceNotificationScope("db1", 16), "1");
  EXPECT_EQ(FormatKeyspaceNotificationScope("db15", 16), "15");
}
