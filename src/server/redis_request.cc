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

#include "redis_request.h"

#include <glog/logging.h>
#include <rocksdb/perf_context.h>

#include <chrono>
#include <memory>
#include <utility>

#include "cluster/redis_slot.h"
#include "event_util.h"
#include "parse_util.h"
#include "redis_connection.h"
#include "redis_reply.h"
#include "server.h"

namespace redis {

Status Request::Tokenize(evbuffer *input) {
  size_t pipeline_size = 0;

  while (true) {
    switch (state_) {
      case ArrayLen: {
        bool is_only_lf = true;
        // We don't use the `EVBUFFER_EOL_CRLF_STRICT` here since only LF is allowed in INLINE protocol.
        // So we need to search LF EOL and figure out current line has CR or not.
        UniqueEvbufReadln line(input, EVBUFFER_EOL_LF);
        if (line && line.length > 0 && line[line.length - 1] == '\r') {
          // remove `\r` if exists
          --line.length;
          is_only_lf = false;
        }

        if (!line || line.length <= 0) {
          if (pipeline_size > 128) {
            LOG(INFO) << "Large pipeline detected: " << pipeline_size;
          }
          if (line) {
            continue;
          }
          return Status::OK();
        }

        pipeline_size++;
        srv_->stats.IncrInboundBytes(line.length);
        if (line[0] == '*') {
          auto parse_result = ParseInt<int64_t>(std::string(line.get() + 1, line.length - 1), 10);
          if (!parse_result) {
            return {Status::NotOK, "Protocol error: invalid multibulk length"};
          }

          multi_bulk_len_ = *parse_result;
          if (is_only_lf || multi_bulk_len_ > (int64_t)PROTO_MULTI_MAX_SIZE) {
            return {Status::NotOK, "Protocol error: invalid multibulk length"};
          }

          if (multi_bulk_len_ <= 0) {
            multi_bulk_len_ = 0;
            continue;
          }

          state_ = BulkLen;
        } else {
          if (line.length > PROTO_INLINE_MAX_SIZE) {
            return {Status::NotOK, "Protocol error: invalid bulk length"};
          }

          tokens_ = util::Split(std::string(line.get(), line.length), " \t");
          if (tokens_.empty()) continue;
          commands_.emplace_back(std::move(tokens_));
          state_ = ArrayLen;
        }
        break;
      }
      case BulkLen: {
        UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
        if (!line || line.length <= 0) return Status::OK();

        srv_->stats.IncrInboundBytes(line.length);
        if (line[0] != '$') {
          return {Status::NotOK, "Protocol error: expected '$'"};
        }

        auto parse_result = ParseInt<uint64_t>(std::string(line.get() + 1, line.length - 1), 10);
        if (!parse_result) {
          return {Status::NotOK, "Protocol error: invalid bulk length"};
        }

        bulk_len_ = *parse_result;
        if (bulk_len_ > srv_->GetConfig()->proto_max_bulk_len) {
          return {Status::NotOK, "Protocol error: invalid bulk length"};
        }

        state_ = BulkData;
        break;
      }
      case BulkData:
        if (evbuffer_get_length(input) < bulk_len_ + 2) return Status::OK();

        char *data = reinterpret_cast<char *>(evbuffer_pullup(input, static_cast<ssize_t>(bulk_len_ + 2)));
        tokens_.emplace_back(data, bulk_len_);
        evbuffer_drain(input, bulk_len_ + 2);
        srv_->stats.IncrInboundBytes(bulk_len_ + 2);
        --multi_bulk_len_;
        if (multi_bulk_len_ == 0) {
          state_ = ArrayLen;
          commands_.emplace_back(std::move(tokens_));
          tokens_.clear();
        } else {
          state_ = BulkLen;
        }
        break;
    }
  }
}

}  // namespace redis
