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
#include <map>
#include <string>
#include <vector>

#include "redis_db.h"
#include "redis_metadata.h"
#include "status.h"
#include "storage.h"

// An extractor to extract update from raw writebatch
class WriteBatchExtractor : public rocksdb::WriteBatch::Handler {
 public:
  explicit WriteBatchExtractor(bool is_slotid_encoded, int16_t slot = -1, bool to_redis = false)
      : is_slotid_encoded_(is_slotid_encoded), slot_(slot), to_redis_(to_redis) {}
  void LogData(const rocksdb::Slice &blob) override;
  rocksdb::Status PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) override;

  rocksdb::Status DeleteCF(uint32_t column_family_id, const Slice &key) override;
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const Slice &begin_key, const Slice &end_key) override;
  std::map<std::string, std::vector<std::string>> *GetRESPCommands() { return &resp_commands_; }

  static Status ExtractStreamAddCommand(bool is_slotid_encoded, const Slice &subkey, const Slice &value,
                                        std::vector<std::string> *command_args);

 private:
  std::map<std::string, std::vector<std::string>> resp_commands_;
  Redis::WriteBatchLogData log_data_;
  bool first_seen_ = true;
  bool is_slotid_encoded_ = false;
  int slot_;
  bool to_redis_;
};
