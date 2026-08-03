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

#include "config/config.h"

TEST(KeyspaceEvents, CollectorRequiresEventClassAndChannel) {
  KeyspaceEventCollector no_channel("tenant", kNotifyString);
  EXPECT_FALSE(ShouldNotifyKeyspaceEvent(kNotifyString, kNotifyString));
  no_channel.Add(kNotifyString, "set", "key");
  EXPECT_TRUE(no_channel.Take().empty());

  KeyspaceEventCollector no_event_class("tenant", kNotifyKeyspace);
  EXPECT_FALSE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace, kNotifyString));
  no_event_class.Add(kNotifyString, "set", "key");
  EXPECT_TRUE(no_event_class.Take().empty());
}

TEST(KeyspaceEvents, CollectorFiltersAndCapturesEvent) {
  KeyspaceEventCollector collector("tenant", kNotifyKeyspace | kNotifyString);
  EXPECT_TRUE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace | kNotifyString, kNotifyString));
  EXPECT_FALSE(ShouldNotifyKeyspaceEvent(kNotifyKeyspace | kNotifyString, kNotifyGeneric));

  collector.Add(kNotifyGeneric, "del", "ignored");
  collector.Add(kNotifyString, "set", "key");

  auto events = collector.Take();
  ASSERT_EQ(events.size(), 1);
  EXPECT_EQ(events[0].channel_flags, kNotifyKeyspace);
  EXPECT_EQ(events[0].event, "set");
  EXPECT_EQ(events[0].ns, "tenant");
  EXPECT_EQ(events[0].key, "key");
}

TEST(KeyspaceEvents, CollectorPreservesEventOrder) {
  KeyspaceEventCollector collector("tenant", kNotifyKeyspace | kNotifyKeyevent | kNotifyAll);
  collector.Add(kNotifyString, "set", "first");
  collector.Add(kNotifyGeneric, "del", "second");

  auto events = collector.Take();
  ASSERT_EQ(events.size(), 2);
  EXPECT_EQ(events[0].channel_flags, kNotifyKeyspace | kNotifyKeyevent);
  EXPECT_EQ(events[0].event, "set");
  EXPECT_EQ(events[0].key, "first");
  EXPECT_EQ(events[1].channel_flags, kNotifyKeyspace | kNotifyKeyevent);
  EXPECT_EQ(events[1].event, "del");
  EXPECT_EQ(events[1].key, "second");
  EXPECT_TRUE(collector.Empty());
}

TEST(KeyspaceEvents, ParseFlags) {
  int flags = 0;

  // Empty disables notifications.
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("", &flags).IsOK());
  ASSERT_EQ(flags, 0);

  // Each flag maps to one bit.
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("K", &flags).IsOK());
  ASSERT_EQ(flags, kNotifyKeyspace);
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("E", &flags).IsOK());
  ASSERT_EQ(flags, kNotifyKeyevent);
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("g", &flags).IsOK());
  ASSERT_EQ(flags, kNotifyGeneric);
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("$", &flags).IsOK());
  ASSERT_EQ(flags, kNotifyString);

  // KEA enables both channels and set or del.
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("KEA", &flags).IsOK());
  ASSERT_TRUE(flags & kNotifyKeyspace);
  ASSERT_TRUE(flags & kNotifyKeyevent);
  ASSERT_TRUE(flags & kNotifyGeneric);  // del
  ASSERT_TRUE(flags & kNotifyString);   // set
}

TEST(KeyspaceEvents, ParseFlagsAExpansion) {
  int flags = 0;
  ASSERT_TRUE(ParseNotifyKeyspaceEventsFlags("A", &flags).IsOK());
  // A expands to all supported event classes without K or E.
  ASSERT_EQ(flags, kNotifyAll);
  ASSERT_FALSE(flags & kNotifyKeyspace);
  ASSERT_FALSE(flags & kNotifyKeyevent);
  ASSERT_TRUE(flags & kNotifyGeneric);
  ASSERT_TRUE(flags & kNotifyString);
}

TEST(KeyspaceEvents, ParseFlagsRejectsUnsupported) {
  int flags = 0;
  // Unsupported flags are rejected.
  for (const auto *bad : {"a", "d", "x", "e", "m", "n", "o", "c", "l", "s", "h", "z", "t", "Kx", "KEl", "?"}) {
    ASSERT_FALSE(ParseNotifyKeyspaceEventsFlags(bad, &flags).IsOK()) << "should reject: " << bad;
  }
}

TEST(KeyspaceEvents, MapNamespaceToKeyspaceDB) {
  // Default namespace maps to db 0.
  EXPECT_EQ(MapNamespaceToKeyspaceDB(kDefaultNamespace, 0), "0");
  EXPECT_EQ(MapNamespaceToKeyspaceDB(kDefaultNamespace, 16), "0");

  // Non-default namespaces are encoded and prefixed.
  EXPECT_EQ(MapNamespaceToKeyspaceDB("0", 0), "ns:0");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("tenantA", 0), "ns:tenantA");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("a.b-c_d", 0), "ns:a.b-c_d");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("db1", 0), "ns:db1");
  // Unsafe bytes are escaped.
  EXPECT_EQ(MapNamespaceToKeyspaceDB("a b", 0), "ns:a%20b");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("a:b", 0), "ns:a%3Ab");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("100%", 0), "ns:100%25");

  // Redis database namespaces map back to numeric database names when redis-databases is enabled.
  EXPECT_EQ(MapNamespaceToKeyspaceDB("db1", 16), "1");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("db15", 16), "15");
}
