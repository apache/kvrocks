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
#include <utility>

#include "status.h"

enum KeyspaceEventChannel {
  kNotifyNoChannel = 0,
  kNotifyKeyspace = 1 << 0,  // K, keyspace channels
  kNotifyKeyevent = 1 << 1,  // E, keyevent channels
};

// Event type flags for notify-keyspace-events, separate from RedisType.
enum KeyspaceEventType {
  kNotifyNoType = 0,
  kNotifyGeneric = 1 << 0,  // g, emits del
  kNotifyString = 1 << 1,   // $, emits set
  // A, supported data classes without K or E.
  kNotifyAll = kNotifyGeneric | kNotifyString,
};

struct KeyspaceEvent {
  KeyspaceEvent(KeyspaceEventType type_flag, std::string_view event, KeyspaceEventChannel channel_flags,
                std::string_view ns, std::string_view key)
      : type_flag(type_flag), channel_flags(channel_flags), event(event), ns(ns), key(key) {}

  KeyspaceEventType type_flag;
  KeyspaceEventChannel channel_flags;
  std::string event;
  std::string ns;
  std::string key;
};

// Parses notify-keyspace-events flags into channel flags followed by event type flags.
StatusOr<std::pair<KeyspaceEventChannel, KeyspaceEventType>> ParseNotifyKeyspaceEventsFlags(std::string_view input);

// Formats the namespace or database scope used in keyspace notification channel names.
// Default namespace maps to 0; database namespaces map back to db indexes when redis-databases is enabled.
// Other namespaces use their original names.
std::string FormatKeyspaceNotificationScope(const std::string &ns, int redis_databases);
