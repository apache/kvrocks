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
#include <vector>
#include <event2/buffer.h>
#include <rocksdb/status.h>

#define CRLF "\r\n"

namespace Redis {
void Reply(evbuffer *output, const std::string &data);
std::string SimpleString(const std::string &data);
std::string Error(const std::string &err);
std::string Integer(int64_t data);
std::string BulkString(const std::string &data);
std::string NilString();
std::string MultiLen(int64_t len);
std::string Array(std::vector<std::string> list);
std::string MultiBulkString(std::vector<std::string> values, bool output_nil_for_empty_string = true);
std::string MultiBulkString(std::vector<std::string> values, const std::vector<rocksdb::Status> &statuses);
std::string Command2RESP(const std::vector<std::string> &cmd_args);
}  // namespace Redis
