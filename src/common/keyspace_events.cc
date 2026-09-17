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

#include "config/config.h"
#include "fmt/format.h"

StatusOr<std::pair<KeyspaceEventChannel, KeyspaceEventType>> ParseNotifyKeyspaceEventsFlags(std::string_view input) {
  int channel_flags = 0;
  int type_flags = 0;
  for (const char c : input) {
    switch (c) {
      case 'K':
        channel_flags |= kNotifyKeyspace;
        break;
      case 'E':
        channel_flags |= kNotifyKeyevent;
        break;
      case 'A':
        type_flags |= kNotifyAll;
        break;
      case 'g':
        type_flags |= kNotifyGeneric;
        break;
      case '$':
        type_flags |= kNotifyString;
        break;
      default:
        return {Status::NotOK, fmt::format("unsupported notify-keyspace-events flag: '{}'", c)};
    }
  }

  return std::pair{static_cast<KeyspaceEventChannel>(channel_flags), static_cast<KeyspaceEventType>(type_flags)};
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
