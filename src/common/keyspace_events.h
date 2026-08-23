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

#include <string>
#include <string_view>

#include "status.h"

enum KeyspaceEventChannel {
  kNotifyKeyspace = 1 << 0,  // K, keyspace channels
  kNotifyKeyevent = 1 << 1,  // E, keyevent channels
};

// Event type flags for notify-keyspace-events, separate from RedisType.
enum KeyspaceEventType {
  kNotifyGeneric = 1 << 2,  // g, emits del
  kNotifyString = 1 << 3,   // $, emits set
  // A, supported data classes without K or E.
  kNotifyAll = kNotifyGeneric | kNotifyString,
};

enum class KeyspaceEventName {
  kSet,
  kDel,
};

bool ShouldNotifyKeyspaceEvent(int notify_flags, KeyspaceEventName event);
std::string_view KeyspaceEventToString(KeyspaceEventName event);

struct KeyspaceEvent {
  KeyspaceEvent(KeyspaceEventName event, int channel_flags, std::string_view ns, std::string_view key)
      : event(event), channel_flags(channel_flags), ns(ns), key(key) {}

  KeyspaceEventName event;
  int channel_flags;
  std::string ns;
  std::string key;
};

// Parses notify-keyspace-events flags.
StatusOr<int> ParseNotifyKeyspaceEventsFlags(std::string_view input);

// Formats the namespace or database scope used in keyspace notification channel names.
// Default namespace maps to 0; database namespaces map back to db indexes when redis-databases is enabled.
// Other namespaces use their original names.
std::string FormatKeyspaceNotificationScope(const std::string &ns, int redis_databases);
