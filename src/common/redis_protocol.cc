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

#include "redis_protocol.h"

#include <memory>

#include "event_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "parse_util.h"
#include "server/redis_reply.h"

namespace util {

Status AuthOnDstNode(int sock_fd, const std::string &password) {
  std::string cmd = redis::ArrayOfBulkStrings({"auth", password});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send AUTH command");
  }

  s = util::CheckSingleResponse(sock_fd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to check the response of AUTH command");
  }

  return Status::OK();
}

Status AuthOnDstNode(int sock_fd, const std::string &username, const std::string &password) {
  std::string cmd = redis::ArrayOfBulkStrings({"auth", username, password});
  auto s = util::SockSend(sock_fd, cmd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to send AUTH command");
  }

  s = util::CheckSingleResponse(sock_fd);
  if (!s.IsOK()) {
    return s.Prefixed("failed to check the response of AUTH command");
  }

  return Status::OK();
}

// Commands  |  Response            |  Instance
// ++++++++++++++++++++++++++++++++++++++++
// set          Redis::Integer         :1\r\n
// hset         Redis::SimpleString    +OK\r\n
// sadd         Redis::Integer
// zadd         Redis::Integer
// siadd        Redis::Integer
// setbit       Redis::Integer
// expire       Redis::Integer
// lpush        Redis::Integer
// rpush        Redis::Integer
// ltrim        Redis::SimpleString    -Err\r\n
// linsert      Redis::Integer
// lset         Redis::SimpleString
// hdel         Redis::Integer
// srem         Redis::Integer
// zrem         Redis::Integer
// lpop         Redis::NilString       $-1\r\n
//          or  Redis::BulkString      $1\r\n1\r\n
// rpop         Redis::NilString
//          or  Redis::BulkString
// lrem         Redis::Integer
// sirem        Redis::Integer
// del          Redis::Integer
// xadd         Redis::BulkString
// bitfield     Redis::Array           *1\r\n:0
Status CheckMultipleResponses(int sock_fd, int total) {
  if (sock_fd < 0 || total <= 0) {
    return {Status::NotOK, fmt::format("invalid arguments: sock_fd={}, count={}", sock_fd, total)};
  }

  // Set socket receive timeout first
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  // Start checking response
  size_t bulk_or_array_len = 0;
  int cnt = 0;
  auto parser_state = ParserState::ArrayLen;
  UniqueEvbuf evbuf;
  while (true) {
    // Read response data from socket buffer to the event buffer
    if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
      return {Status::NotOK, fmt::format("failed to read response: {}", strerror(errno))};
    }

    // Parse response data in event buffer
    bool run = true;
    while (run) {
      switch (parser_state) {
        // Handle single string response
        case ParserState::ArrayLen: {
          UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
          if (!line) {
            LOG(INFO) << "[migrate] Event buffer is empty, read socket again";
            run = false;
            break;
          }

          if (line[0] == '-') {
            return {Status::NotOK, fmt::format("got invalid response of length {}: {}", line.length, line.get())};
          } else if (line[0] == '$' || line[0] == '*') {
            auto parse_result = ParseInt<uint64_t>(std::string(line.get() + 1, line.length - 1), 10);
            if (!parse_result) {
              return {Status::NotOK, "protocol error: expected integer value"};
            }

            bulk_or_array_len = *parse_result;
            if (bulk_or_array_len <= 0) {
              parser_state = ParserState::OneRspEnd;
            } else if (line[0] == '$') {
              parser_state = ParserState::BulkData;
            } else {
              parser_state = ParserState::ArrayData;
            }
          } else if (line[0] == '+' || line[0] == ':') {
            parser_state = ParserState::OneRspEnd;
          } else {
            return {Status::NotOK, fmt::format("got unexpected response of length {}: {}", line.length, line.get())};
          }

          break;
        }
        // Handle bulk string response
        case ParserState::BulkData: {
          if (evbuffer_get_length(evbuf.get()) < bulk_or_array_len + 2) {
            LOG(INFO) << "[migrate] Bulk data in event buffer is not complete, read socket again";
            run = false;
            break;
          }
          // TODO(chrisZMF): Check tail '\r\n'
          evbuffer_drain(evbuf.get(), bulk_or_array_len + 2);
          bulk_or_array_len = 0;
          parser_state = ParserState::OneRspEnd;
          break;
        }
        case ParserState::ArrayData: {
          while (run && bulk_or_array_len > 0) {
            evbuffer_ptr ptr = evbuffer_search_eol(evbuf.get(), nullptr, nullptr, EVBUFFER_EOL_CRLF_STRICT);
            if (ptr.pos < 0) {
              LOG(INFO) << "[migrate] Array data in event buffer is not complete, read socket again";
              run = false;
              break;
            }
            evbuffer_drain(evbuf.get(), ptr.pos + 2);
            --bulk_or_array_len;
          }
          if (run) {
            parser_state = ParserState::OneRspEnd;
          }
          break;
        }
        case ParserState::OneRspEnd: {
          cnt++;
          if (cnt >= total) {
            return Status::OK();
          }

          parser_state = ParserState::ArrayLen;
          break;
        }
        default:
          break;
      }
    }
  }
}

Status CheckSingleResponse(int sock_fd) { return CheckMultipleResponses(sock_fd, 1); }
}  // namespace util