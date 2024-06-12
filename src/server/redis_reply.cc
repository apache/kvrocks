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

std::string Error(const ErrorKind kind, const std::string &message) {
  std::string prefix;
  switch (kind) {
    case ErrorKind::Loading:
      prefix = "LOADING";
      break;
    case ErrorKind::NoScript:
      prefix = "NOSCRIPT";
      break;
    case ErrorKind::WrongType:
      prefix = "WRONGTYPE";
      break;
    case ErrorKind::NoProto:
      prefix = "NOPROTO";
      break;
    case ErrorKind::NoAuth:
      prefix = "NOAUTH";
      break;
    case ErrorKind::Readonly:
      prefix = "READONLY";
      break;
    case ErrorKind::MasterDown:
      prefix = "MASTERDOWN";
      break;
    case ErrorKind::ExecAbort:
      prefix = "EXECABORT";
      break;
    case ErrorKind::Err:
      prefix = "ERR";
      break;
    default:
      break;
  }
  if (prefix.empty()) {
    return RESP_PREFIX_ERROR + message + CRLF;
  }
  return RESP_PREFIX_ERROR + prefix + " " + message + CRLF;
}

std::string Error(const Status &s) { return Error(ErrorKind::Err, s.Msg()); }

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
