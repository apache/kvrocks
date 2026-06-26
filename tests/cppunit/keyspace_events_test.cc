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
  EXPECT_EQ(MapNamespaceToKeyspaceDB(kDefaultNamespace), "0");

  // Non-default namespaces are encoded and prefixed.
  EXPECT_EQ(MapNamespaceToKeyspaceDB("0"), "ns:0");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("tenantA"), "ns:tenantA");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("a.b-c_d"), "ns:a.b-c_d");
  // Unsafe bytes are escaped.
  EXPECT_EQ(MapNamespaceToKeyspaceDB("a b"), "ns:a%20b");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("a:b"), "ns:a%3Ab");
  EXPECT_EQ(MapNamespaceToKeyspaceDB("100%"), "ns:100%25");
}
