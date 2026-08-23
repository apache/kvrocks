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

TEST(KeyspaceEvents, NotificationRequiresEventClassAndChannel) {
  EXPECT_FALSE(ShouldNotifyKeyspaceEvent(kNotifyString, KeyspaceEventName::kSet));
  EXPECT_FALSE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace, KeyspaceEventName::kSet));
}

TEST(KeyspaceEvents, EventNamesAndTypes) {
  EXPECT_EQ(KeyspaceEventToString(KeyspaceEventName::kSet), "set");
  EXPECT_EQ(KeyspaceEventToString(KeyspaceEventName::kDel), "del");
  EXPECT_TRUE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace | kNotifyString, KeyspaceEventName::kSet));
  EXPECT_FALSE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace | kNotifyString, KeyspaceEventName::kDel));
  EXPECT_TRUE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace | kNotifyGeneric, KeyspaceEventName::kDel));
}

TEST(KeyspaceEvents, ContextFiltersAndCapturesEvent) {
  auto ctx = engine::Context::NoTransactionContext(nullptr);
  EXPECT_FALSE(ctx.HasKeyspaceEvents());

  ctx.AddKeyspaceEvent(KeyspaceEventName::kSet, "tenant", "disabled");
  EXPECT_FALSE(ctx.HasKeyspaceEvents());

  ctx.EnableKeyspaceEventCollection(kNotifyKeyspace | kNotifyString);
  EXPECT_TRUE(ctx.IsKeyspaceEventEnabled(KeyspaceEventName::kSet));
  EXPECT_FALSE(ctx.IsKeyspaceEventEnabled(KeyspaceEventName::kDel));

  ctx.AddKeyspaceEvent(KeyspaceEventName::kDel, "tenant", "ignored");
  EXPECT_FALSE(ctx.HasKeyspaceEvents());
  ctx.AddKeyspaceEvent(KeyspaceEventName::kSet, "tenant", "key");

  auto events = ctx.TakeKeyspaceEvents();
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].channel_flags, kNotifyKeyspace);
  EXPECT_EQ(events[0].event, KeyspaceEventName::kSet);
  EXPECT_EQ(events[0].ns, "tenant");
  EXPECT_EQ(events[0].key, "key");
  EXPECT_FALSE(ctx.HasKeyspaceEvents());
  EXPECT_TRUE(ctx.TakeKeyspaceEvents().empty());
}

TEST(KeyspaceEvents, ContextCapturesNamespacePerEvent) {
  auto ctx = engine::Context::NoTransactionContext(nullptr);
  ctx.EnableKeyspaceEventCollection(kNotifyKeyspace | kNotifyString);
  ctx.AddKeyspaceEvent(KeyspaceEventName::kSet, "tenant-1", "first");
  ctx.AddKeyspaceEvent(KeyspaceEventName::kSet, "tenant-2", "second");

  auto events = ctx.TakeKeyspaceEvents();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].ns, "tenant-1");
  EXPECT_EQ(events[1].ns, "tenant-2");
}

TEST(KeyspaceEvents, ContextMovePreservesEventOrder) {
  auto ctx = engine::Context::NoTransactionContext(nullptr);
  ctx.EnableKeyspaceEventCollection(kNotifyKeyspace | kNotifyKeyevent | kNotifyAll);
  ctx.AddKeyspaceEvent(KeyspaceEventName::kSet, "tenant", "first");
  ctx.AddKeyspaceEvent(KeyspaceEventName::kDel, "tenant", "second");

  auto moved_ctx = std::move(ctx);
  auto assigned_ctx = engine::Context::NoTransactionContext(nullptr);
  assigned_ctx = std::move(moved_ctx);

  auto events = assigned_ctx.TakeKeyspaceEvents();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].channel_flags, kNotifyKeyspace | kNotifyKeyevent);
  EXPECT_EQ(events[0].event, KeyspaceEventName::kSet);
  EXPECT_EQ(events[0].ns, "tenant");
  EXPECT_EQ(events[0].key, "first");
  EXPECT_EQ(events[1].channel_flags, kNotifyKeyspace | kNotifyKeyevent);
  EXPECT_EQ(events[1].event, KeyspaceEventName::kDel);
  EXPECT_EQ(events[1].ns, "tenant");
  EXPECT_EQ(events[1].key, "second");
  EXPECT_FALSE(assigned_ctx.HasKeyspaceEvents());
}

TEST(KeyspaceEvents, ParseFlags) {
  // Empty disables notifications.
  EXPECT_EQ(ParseNotifyKeyspaceEventsFlags("").ValueOr(-1), 0);

  // Each flag maps to one bit.
  EXPECT_EQ(ParseNotifyKeyspaceEventsFlags("K").ValueOr(-1), kNotifyKeyspace);
  EXPECT_EQ(ParseNotifyKeyspaceEventsFlags("E").ValueOr(-1), kNotifyKeyevent);
  EXPECT_EQ(ParseNotifyKeyspaceEventsFlags("g").ValueOr(-1), kNotifyGeneric);
  EXPECT_EQ(ParseNotifyKeyspaceEventsFlags("$").ValueOr(-1), kNotifyString);

  // KEA enables both channels and set or del.
  auto flags = ParseNotifyKeyspaceEventsFlags("KEA");
  ASSERT_TRUE(flags.IsOK());
  ASSERT_TRUE(*flags & kNotifyKeyspace);
  ASSERT_TRUE(*flags & kNotifyKeyevent);
  ASSERT_TRUE(*flags & kNotifyGeneric);  // del
  ASSERT_TRUE(*flags & kNotifyString);   // set
}

TEST(KeyspaceEvents, ParseFlagsAExpansion) {
  auto flags = ParseNotifyKeyspaceEventsFlags("A");
  ASSERT_TRUE(flags.IsOK());
  // A expands to all supported event classes without K or E.
  ASSERT_EQ(*flags, kNotifyAll);
  ASSERT_FALSE(*flags & kNotifyKeyspace);
  ASSERT_FALSE(*flags & kNotifyKeyevent);
  ASSERT_TRUE(*flags & kNotifyGeneric);
  ASSERT_TRUE(*flags & kNotifyString);
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
