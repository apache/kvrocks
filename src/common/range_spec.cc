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

Status ParseRangeLexSpec(const std::string &min, const std::string &max, CommonRangeLexSpec *spec) {
  if (min == "+" || max == "-") {
    return {Status::NotOK, "min > max"};
  }

  if (min == "-") {
    spec->min_ = "";
  } else {
    if (min[0] == '(') {
      spec->minex_ = true;
    } else if (min[0] == '[') {
      spec->minex_ = false;
    } else {
      return {Status::NotOK, "the min is illegal"};
    }
    spec->min_ = min.substr(1);
  }

  if (max == "+") {
    spec->max_infinite_ = true;
  } else {
    if (max[0] == '(') {
      spec->maxex_ = true;
    } else if (max[0] == '[') {
      spec->maxex_ = false;
    } else {
      return {Status::NotOK, "the max is illegal"};
    }
    spec->max_ = max.substr(1);
  }
  return Status::OK();
}
