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

Status ParseNotifyKeyspaceEventsFlags(const std::string &input, int *flags) {
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

  *flags = result;
  return Status::OK();
}

namespace {
std::string PercentEncode(const std::string &input) {
  std::string output;
  output.reserve(input.size());
  for (const unsigned char c : input) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '.' ||
        c == '-') {
      output += static_cast<char>(c);
    } else {
      output += fmt::format("%{:02X}", c);
    }
  }
  return output;
}
}  // namespace

std::string MapNamespaceToKeyspaceDB(const std::string &ns, int redis_databases) {
  if (ns == kDefaultNamespace) {
    return "0";
  }
  if (redis_databases > 0 && ns.rfind(kDatabaseNamespacePrefix, 0) == 0) {
    return ns.substr(strlen(kDatabaseNamespacePrefix));
  }
  return "ns:" + PercentEncode(ns);
}
