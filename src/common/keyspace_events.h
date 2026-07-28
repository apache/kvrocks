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
#include <vector>

#include "status.h"

// Flags for notify-keyspace-events, separate from RedisType.
enum NotifyKeyspaceEventFlag {
  kNotifyKeyspace = 1 << 0,  // K, keyspace channels
  kNotifyKeyevent = 1 << 1,  // E, keyevent channels
  kNotifyGeneric = 1 << 2,   // g, emits del
  kNotifyString = 1 << 3,    // $, emits set
  // A, supported data classes without K or E.
  kNotifyAll = kNotifyGeneric | kNotifyString,
};

bool ShouldNotifyKeyspaceEvent(int notify_flags, int type_flag);

struct KeyspaceEvent {
  int channel_flags;
  std::string event;
  std::string ns;
  std::string key;
};

// Collects semantic keyspace events for one command; Connection owns publish timing.
class KeyspaceEventCollector {
 public:
  KeyspaceEventCollector(std::string ns, int notify_flags);
  bool IsEnabled(int type_flag) const;
  void Add(int type_flag, std::string_view event, std::string_view key);
  // Moves out events collected during Execute.
  std::vector<KeyspaceEvent> Take();

 private:
  int notify_flags_;
  std::string ns_;
  std::vector<KeyspaceEvent> events_;
};

// Parses notify-keyspace-events flags.
Status ParseNotifyKeyspaceEventsFlags(const std::string &input, int *flags);

// Maps namespaces to notification db names.
// Default namespace maps to 0; database namespaces map back to db indexes when redis-databases is enabled.
// Other namespaces map to ns:encoded-name.
std::string MapNamespaceToKeyspaceDB(const std::string &ns, int redis_databases);
