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

#include <stdint.h>

#include <string>
#include <vector>

#include "encoding.h"
#include "json.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/redis_string.h"

namespace redis {
class RedisJson : public String {
 public:
  explicit RedisJson(engine::Storage *storage, const std::string &ns) : String(storage, ns) {}
  rocksdb::Status JsonGet(const std::string &user_key, const std::vector<JsonPath> &path, std::string *values);
  rocksdb::Status JsonSet(const std::string &user_key, const JsonPath &path, const std::string &set_value,
                          JsonSetFlags set_flags, bool *set_ok);
  rocksdb::Status JsonDel(const std::string &user_key, const JsonPath &path);
};
}  // namespace redis
