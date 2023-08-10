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

#include "slot_import.h"

SlotImport::SlotImport(Server *svr)
    : Database(svr->storage, kDefaultNamespace),
      svr_(svr),
      import_slot_(-1),
      import_status_(kImportNone),
      import_fd_(-1) {
  std::lock_guard<std::mutex> guard(mutex_);
  // Let metadata_cf_handle_ be nullptr, then get them in real time while use them.
  // See comments in SlotMigrator::SlotMigrator for detailed reason.
  metadata_cf_handle_ = nullptr;
}

bool SlotImport::Start(int fd, int slot) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_status_ == kImportStart) {
    LOG(ERROR) << "[import] Only one slot importing is allowed"
               << ", current slot is " << import_slot_ << ", cannot import slot " << slot;
    return false;
  }

  // Clean slot data first
  auto s = ClearKeysOfSlot(namespace_, slot);
  if (!s.ok()) {
    LOG(INFO) << "[import] Failed to clear keys of slot " << slot << "current status is importing 'START'"
              << ", Err: " << s.ToString();
    return false;
  }

  import_status_ = kImportStart;
  import_slot_ = slot;
  import_fd_ = fd;

  return true;
}

bool SlotImport::Success(int slot) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_slot_ != slot) {
    LOG(ERROR) << "[import] Wrong slot, importing slot: " << import_slot_ << ", but got slot: " << slot;
    return false;
  }

  Status s = svr_->cluster->SetSlotImported(import_slot_);
  if (!s.IsOK()) {
    LOG(ERROR) << "[import] Failed to set slot, Err: " << s.Msg();
    return false;
  }

  import_status_ = kImportSuccess;
  import_fd_ = -1;

  return true;
}

bool SlotImport::Fail(int slot) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_slot_ != slot) {
    LOG(ERROR) << "[import] Wrong slot, importing slot: " << import_slot_ << ", but got slot: " << slot;
    return false;
  }

  // Clean imported slot data
  auto s = ClearKeysOfSlot(namespace_, slot);
  if (!s.ok()) {
    LOG(INFO) << "[import] Failed to clear keys of slot " << slot << ", current importing status is importing 'FAIL'"
              << ", Err: " << s.ToString();
  }

  import_status_ = kImportFailed;
  import_fd_ = -1;

  return true;
}

void SlotImport::StopForLinkError(int fd) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_status_ != kImportStart) return;

  // Maybe server has failovered
  // Situation:
  // Refer to the situation described in SlotMigrator::SlotMigrator
  // 1. Change server to slave when it is importing data.
  // 2. Source server's migration process end after destination server has finished replication.
  // 3. The migration link closed by source server, then this function will be call by OnEvent.
  // 4. ClearKeysOfSlot can clear data although server is a slave, because ClearKeysOfSlot
  //    deletes data in rocksdb directly. Therefore, it is necessary to avoid clearing data gotten
  //    from new master.
  if (!svr_->IsSlave()) {
    // Clean imported slot data
    auto s = ClearKeysOfSlot(namespace_, import_slot_);
    if (!s.ok()) {
      LOG(WARNING) << "[import] Failed to clear keys of slot " << import_slot_ << " Current status is link error"
                   << ", Err: " << s.ToString();
    }
  }

  LOG(INFO) << "[import] Stop importing for link error, slot: " << import_slot_;
  import_status_ = kImportFailed;
  import_fd_ = -1;
}

int SlotImport::GetSlot() {
  std::lock_guard<std::mutex> guard(mutex_);
  return import_slot_;
}

int SlotImport::GetStatus() {
  std::lock_guard<std::mutex> guard(mutex_);
  return import_status_;
}

void SlotImport::GetImportInfo(std::string *info) {
  std::lock_guard<std::mutex> guard(mutex_);
  info->clear();
  if (import_slot_ < 0) {
    return;
  }

  std::string import_stat;
  switch (import_status_) {
    case kImportNone:
      import_stat = "none";
      break;
    case kImportStart:
      import_stat = "start";
      break;
    case kImportSuccess:
      import_stat = "success";
      break;
    case kImportFailed:
      import_stat = "error";
      break;
    default:
      break;
  }

  *info = fmt::format("importing_slot: {}\r\nimport_state: {}\r\n", import_slot_, import_stat);
}
