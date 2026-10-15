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
#include "parse_util.h"
#include "status.h"
#include "time_util.h"

template <typename T>
constexpr auto TTL_RANGE = NumericRange<T>{1, std::numeric_limits<T>::max()};

// Convert a user supplied TTL to an absolute expire time in milliseconds.
// `ttl_in_seconds` selects the EX/PX style scaling, `add_now` whether the current
// time is added (relative) or the value is already absolute (EXAT/PXAT). The
// input is capped so neither the scaling nor the addition of the current time can
// overflow int64, which would wrap the expire time into the past and silently
// delete the key.
inline StatusOr<uint64_t> ToExpireAtMs(int64_t ttl, bool ttl_in_seconds, bool add_now) {
  constexpr int64_t max_ms = std::numeric_limits<int64_t>::max();
  int64_t now_ms = add_now ? util::GetTimeStampMS() : 0;
  int64_t limit = ttl_in_seconds ? (max_ms - now_ms) / 1000 : max_ms - now_ms;
  if (ttl > limit) {
    return {Status::RedisParseErr, "invalid expire time"};
  }
  return static_cast<uint64_t>(ttl_in_seconds ? ttl * 1000 : ttl) + static_cast<uint64_t>(now_ms);
}

template <typename T>
StatusOr<std::optional<uint64_t>> ParseExpireFlags(CommandParser<T> &parser, std::string_view &curr_flag) {
  if (parser.EatEqICaseFlag("EX", curr_flag)) {
    return GET_OR_RET(ToExpireAtMs(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)), true, true));
  } else if (parser.EatEqICaseFlag("EXAT", curr_flag)) {
    return GET_OR_RET(ToExpireAtMs(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)), true, false));
  } else if (parser.EatEqICaseFlag("PX", curr_flag)) {
    return GET_OR_RET(ToExpireAtMs(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)), false, true));
  } else if (parser.EatEqICaseFlag("PXAT", curr_flag)) {
    return GET_OR_RET(ToExpireAtMs(GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)), false, false));
  } else {
    return std::nullopt;
  }
}
