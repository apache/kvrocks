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

#include <event2/buffer.h>
#include <rocksdb/status.h>

#include <string>
#include <type_traits>
#include <vector>

#define CRLF "\r\n"  // NOLINT

namespace redis {

void Reply(evbuffer *output, const std::string &data);
std::string SimpleString(const std::string &data);
std::string Error(const std::string &err);

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string Integer(T data) {
  return ":" + std::to_string(data) + CRLF;
}

std::string BulkString(const std::string &data);
std::string NilString();

template <typename T, std::enable_if_t<std::is_integral_v<T>, int> = 0>
std::string MultiLen(T len) {
  return "*" + std::to_string(len) + CRLF;
}

std::string Array(const std::vector<std::string> &list);
std::string IntegerArray(const std::vector<uint64_t> &values);
std::string MultiBulkString(const std::vector<std::string> &values, bool output_nil_for_empty_string = true);
std::string MultiBulkString(const std::vector<std::string> &values, const std::vector<rocksdb::Status> &statuses);
std::string Command2RESP(const std::vector<std::string> &cmd_args);

}  // namespace redis
