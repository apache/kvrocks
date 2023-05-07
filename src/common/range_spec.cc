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

#include "range_spec.h"

#include "commands/error_constants.h"
#include "parse_util.h"

Status ParseRangeLexSpec(const std::string &min, const std::string &max, CommonRangeLexSpec *spec) {
  if (min == "+" || max == "-") {
    return {Status::NotOK, "min > max"};
  }

  if (min == "-") {
    spec->min = "";
  } else {
    if (min[0] == '(') {
      spec->minex = true;
    } else if (min[0] == '[') {
      spec->minex = false;
    } else {
      return {Status::NotOK, "the min is illegal"};
    }
    spec->min = min.substr(1);
  }

  if (max == "+") {
    spec->max_infinite = true;
  } else {
    if (max[0] == '(') {
      spec->maxex = true;
    } else if (max[0] == '[') {
      spec->maxex = false;
    } else {
      return {Status::NotOK, "the max is illegal"};
    }
    spec->max = max.substr(1);
  }
  return Status::OK();
}

Status ParseRangeRankSpec(const std::string &min, const std::string &max, CommonRangeRankSpec *spec) {
  auto parse_start = ParseInt<int>(min, 10);
  auto parse_stop = ParseInt<int>(max, 10);
  if (!parse_start || !parse_stop) {
    return {Status::RedisParseErr, redis::errValueNotInteger};
  }
  spec->start = *parse_start;
  spec->stop = *parse_stop;
  return Status::OK();
}

Status ParseRangeScoreSpec(const std::string &min, const std::string &max, CommonRangeScoreSpec *spec) {
  char *eptr = nullptr;

  if (min == "+inf" || max == "-inf") {
    return {Status::NotOK, "min > max"};
  }

  if (min == "-inf") {
    spec->min = kMinScore;
  } else {
    const char *sptr = min.data();
    if (!min.empty() && min[0] == '(') {
      spec->minex = true;
      sptr++;
    }
    spec->min = strtod(sptr, &eptr);
    if ((eptr && eptr[0] != '\0') || std::isnan(spec->min)) {
      return {Status::NotOK, "the min isn't double"};
    }
  }

  if (max == "+inf") {
    spec->max = kMaxScore;
  } else {
    const char *sptr = max.data();
    if (!max.empty() && max[0] == '(') {
      spec->maxex = true;
      sptr++;
    }
    spec->max = strtod(sptr, &eptr);
    if ((eptr && eptr[0] != '\0') || std::isnan(spec->max)) {
      return {Status::NotOK, "the max isn't double"};
    }
  }
  return Status::OK();
}