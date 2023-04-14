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

#include "parse_util.h"

#include <limits>

// num << bit <= MAX  ->  num <= MAX >> bit
template <typename T, typename U>
StatusOr<T> CheckedShiftLeft(T num, U bit) {
  if (num <= std::numeric_limits<T>::max() >> bit) {
    return num << bit;
  }

  return {Status::NotOK, "arithmetic overflow"};
}

StatusOr<std::uint64_t> ParseSizeAndUnit(const std::string &v) {
  auto [num, rest] = GET_OR_RET(TryParseInt<std::uint64_t>(v.c_str(), 10));

  if (*rest == 0) {
    return num;
  } else if (util::EqualICase(rest, "k")) {
    return CheckedShiftLeft(num, 10);
  } else if (util::EqualICase(rest, "m")) {
    return CheckedShiftLeft(num, 20);
  } else if (util::EqualICase(rest, "g")) {
    return CheckedShiftLeft(num, 30);
  } else if (util::EqualICase(rest, "t")) {
    return CheckedShiftLeft(num, 40);
  } else if (util::EqualICase(rest, "p")) {
    return CheckedShiftLeft(num, 50);
  }

  return {Status::NotOK, "encounter unexpected unit"};
}
