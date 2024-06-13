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

#include "redis_reply.h"

#include <numeric>

namespace redis {

void Reply(evbuffer *output, const std::string &data) { evbuffer_add(output, data.c_str(), data.length()); }

std::string SimpleString(const std::string &data) { return "+" + data + CRLF; }

std::string Error(const Status &s) { return RESP_PREFIX_ERROR + StatusToRedisError(s); }

std::string StatusToRedisError(const Status &s) {
  CHECK(!s.IsOK());
  if (!s.Is<Status::RedisError>()) {
    return "ERR " + s.Msg() + CRLF;
  }
  if (auto iter = ErrorKindMap.find(s.GetErrorKind()); iter != ErrorKindMap.end()) {
    return iter->second + " " + s.Msg() + CRLF;
  }
  return s.Msg() + CRLF;
}

std::string BulkString(const std::string &data) { return "$" + std::to_string(data.length()) + CRLF + data + CRLF; }

std::string Array(const std::vector<std::string> &list) {
  size_t n = std::accumulate(list.begin(), list.end(), 0, [](size_t n, const std::string &s) { return n + s.size(); });
  std::string result = MultiLen(list.size());
  std::string::size_type final_size = result.size() + n;
  result.reserve(final_size);
  for (const auto &i : list) result += i;
  return result;
}

std::string ArrayOfBulkStrings(const std::vector<std::string> &elems) {
  std::string result = MultiLen(elems.size());
  for (const auto &elem : elems) {
    result += BulkString(elem);
  }
  return result;
}

}  // namespace redis
