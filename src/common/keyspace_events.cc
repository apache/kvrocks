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

#include "keyspace_events.h"

#include <cstring>

#include "common/logging.h"
#include "config/config.h"
#include "fmt/format.h"

bool ShouldNotifyKeyspaceEvent(int notify_flags, KeyspaceEventName event) {
  if ((notify_flags & (kNotifyKeyspace | kNotifyKeyevent)) == 0) return false;

  switch (event) {
    case KeyspaceEventName::kSet:
      return (notify_flags & kNotifyString) != 0;
    case KeyspaceEventName::kDel:
      return (notify_flags & kNotifyGeneric) != 0;
  }
  UNREACHABLE();
}

std::string_view KeyspaceEventToString(KeyspaceEventName event) {
  switch (event) {
    case KeyspaceEventName::kSet:
      return "set";
    case KeyspaceEventName::kDel:
      return "del";
  }
  UNREACHABLE();
}

StatusOr<int> ParseNotifyKeyspaceEventsFlags(std::string_view input) {
  int result = 0;
  for (const char c : input) {
    switch (c) {
      case 'K':
        result |= kNotifyKeyspace;
        break;
      case 'E':
        result |= kNotifyKeyevent;
        break;
      case 'A':
        result |= kNotifyAll;
        break;
      case 'g':
        result |= kNotifyGeneric;
        break;
      case '$':
        result |= kNotifyString;
        break;
      default:
        return {Status::NotOK, fmt::format("unsupported notify-keyspace-events flag: '{}'", c)};
    }
  }

  return result;
}

std::string FormatKeyspaceNotificationScope(const std::string &ns, int redis_databases) {
  if (ns == kDefaultNamespace) {
    return "0";
  }
  if (redis_databases > 0 && ns.rfind(kDatabaseNamespacePrefix, 0) == 0) {
    return ns.substr(strlen(kDatabaseNamespacePrefix));
  }
  return ns;
}
