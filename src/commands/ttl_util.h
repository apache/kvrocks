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

#include <limits>
#include <optional>

#include "commands/command_parser.h"
#include "commands/error_constants.h"
#include "parse_util.h"
#include "status.h"
#include "time_util.h"

template <typename T>
constexpr auto TTL_RANGE = NumericRange<T>{1, std::numeric_limits<T>::max()};

template <typename T>
StatusOr<std::optional<uint64_t>> ParseExpireFlags(CommandParser<T> &parser, std::string_view &curr_flag) {
  uint64_t now_ms = util::GetTimeStampMS();
  if (parser.EatEqICaseFlag("EX", curr_flag)) {
    auto v = GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>));
    if (v > (std::numeric_limits<int64_t>::max() - static_cast<int64_t>(now_ms)) / 1000) {
      return {Status::RedisParseErr, redis::errInvalidExpireTime};
    }
    return static_cast<uint64_t>(v) * 1000 + now_ms;
  } else if (parser.EatEqICaseFlag("EXAT", curr_flag)) {
    auto v = GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>));
    if (v > std::numeric_limits<int64_t>::max() / 1000) {
      return {Status::RedisParseErr, redis::errInvalidExpireTime};
    }
    return static_cast<uint64_t>(v) * 1000;
  } else if (parser.EatEqICaseFlag("PX", curr_flag)) {
    auto v = GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>));
    if (v > std::numeric_limits<int64_t>::max() - static_cast<int64_t>(now_ms)) {
      return {Status::RedisParseErr, redis::errInvalidExpireTime};
    }
    return static_cast<uint64_t>(v) + now_ms;
  } else if (parser.EatEqICaseFlag("PXAT", curr_flag)) {
    auto v = GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>));
    return static_cast<uint64_t>(v);
  } else {
    return std::nullopt;
  }
}
