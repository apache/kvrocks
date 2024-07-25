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

#include <deque>
#include <string>
#include <vector>

#include "status.h"

class Server;

namespace redis {

constexpr size_t PROTO_INLINE_MAX_SIZE = 16 * 1024L;
constexpr size_t PROTO_MULTI_MAX_SIZE = 1024 * 1024L;

using CommandTokens = std::vector<std::string>;

class Connection;

class Request {
 public:
  explicit Request(Server *srv) : srv_(srv) {}
  ~Request() = default;

  // Not copyable
  Request(const Request &) = delete;
  Request &operator=(const Request &) = delete;

  // Parse the redis requests (bulk string array format)
  Status Tokenize(evbuffer *input);

  std::deque<CommandTokens> *GetCommands() { return &commands_; }

 private:
  // internal states related to parsing

  enum ParserState { ArrayLen, BulkLen, BulkData };
  ParserState state_ = ArrayLen;
  int64_t multi_bulk_len_ = 0;
  size_t bulk_len_ = 0;
  CommandTokens tokens_;
  std::deque<CommandTokens> commands_;

  Server *srv_;
};

}  // namespace redis
