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

#include <mutex>
#include <string>
#include <vector>

#include "cluster_defs.h"
#include "config/config.h"
#include "logging.h"
#include "server/server.h"
#include "storage/redis_db.h"

enum ImportStatus {
  kImportStart,
  kImportSuccess,
  kImportFailed,
  kImportNone,
};

class SlotImport : public redis::Database {
 public:
  explicit SlotImport(Server *srv);
  ~SlotImport() = default;

  Status Start(const SlotRange &slot_range, uint64_t conn_id);
  Status Success(const SlotRange &slot_range, uint64_t conn_id);
  Status Fail(const SlotRange &slot_range, uint64_t conn_id);
  Status StopForLinkError(uint64_t conn_id);
  SlotRange GetSlotRange();
  int GetStatus();
  void GetImportInfo(std::string *info);

 private:
  Server *srv_ = nullptr;
  std::mutex mutex_;
  SlotRange import_slot_range_;
  int import_status_;
  // Connection that started the current import. A later start of the same range does not replace it.
  uint64_t import_conn_id_ = 0;
};
