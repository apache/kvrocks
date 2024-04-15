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

#include <optional>

#include "commands/command_parser.h"
#include "parse_util.h"
#include "status.h"
#include "time_util.h"

template <typename T>
constexpr auto TTL_RANGE = NumericRange<T>{1, std::numeric_limits<T>::max()};

template <typename T>
StatusOr<std::optional<uint64_t>> ParseExpireFlags(CommandParser<T> &parser, std::string_view &curr_flag) {
  if (parser.EatEqICaseFlag("EX", curr_flag)) {
    return GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)) * 1000 + util::GetTimeStampMS();
  } else if (parser.EatEqICaseFlag("EXAT", curr_flag)) {
    return GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)) * 1000;
  } else if (parser.EatEqICaseFlag("PX", curr_flag)) {
    return GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>)) + util::GetTimeStampMS();
  } else if (parser.EatEqICaseFlag("PXAT", curr_flag)) {
    return GET_OR_RET(parser.template TakeInt<int64_t>(TTL_RANGE<int64_t>));
  } else {
    return std::nullopt;
  }
}
