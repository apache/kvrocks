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

SlotImport::SlotImport(Server *srv)
    : Database(srv->storage, kDefaultNamespace), srv_(srv), import_slot_(-1), import_status_(kImportNone) {
  std::lock_guard<std::mutex> guard(mutex_);
  // Let metadata_cf_handle_ be nullptr, then get them in real time while use them.
  // See comments in SlotMigrator::SlotMigrator for detailed reason.
  metadata_cf_handle_ = nullptr;
}

Status SlotImport::Start(int slot) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_status_ == kImportStart) {
    // return ok if the same slot is importing
    if (import_slot_ == slot) {
      return Status::OK();
    }
    return {Status::NotOK, fmt::format("only one importing slot is allowed, current slot is: {}", import_slot_)};
  }

  // Clean slot data first
  engine::Context ctx(srv_->storage);
  auto s = ClearKeysOfSlot(ctx, namespace_, slot);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("clear keys of slot error: {}", s.ToString())};
  }

  import_status_ = kImportStart;
  import_slot_ = slot;
  return Status::OK();
}

Status SlotImport::Success(int slot) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_slot_ != slot) {
    return {Status::NotOK, fmt::format("mismatch slot, importing slot: {}, but got: {}", import_slot_, slot)};
  }

  Status s = srv_->cluster->SetSlotImported(import_slot_);
  if (!s.IsOK()) {
    return {Status::NotOK, fmt::format("unable to set imported status: {}", slot)};
  }

  import_status_ = kImportSuccess;
  return Status::OK();
}

Status SlotImport::Fail(int slot) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_slot_ != slot) {
    return {Status::NotOK, fmt::format("mismatch slot, importing slot: {}, but got: {}", import_slot_, slot)};
  }

  // Clean imported slot data
  engine::Context ctx(srv_->storage);
  auto s = ClearKeysOfSlot(ctx, namespace_, slot);
  if (!s.ok()) {
    return {Status::NotOK, fmt::format("clear keys of slot error: {}", s.ToString())};
  }

  import_status_ = kImportFailed;
  return Status::OK();
}

Status SlotImport::StopForLinkError() {
  std::lock_guard<std::mutex> guard(mutex_);
  // We don't need to do anything if the importer is not started yet.
  if (import_status_ != kImportStart) return Status::OK();

  // Maybe server has failovered
  // Situation:
  // Refer to the situation described in SlotMigrator::SlotMigrator
  // 1. Change server to slave when it is importing data.
  // 2. Source server's migration process end after destination server has finished replication.
  // 3. The migration link closed by source server, then this function will be call by OnEvent.
  // 4. ClearKeysOfSlot can clear data although server is a slave, because ClearKeysOfSlot
  //    deletes data in rocksdb directly. Therefore, it is necessary to avoid clearing data gotten
  //    from new master.
  if (!srv_->IsSlave()) {
    // Clean imported slot data
    engine::Context ctx(srv_->storage);
    auto s = ClearKeysOfSlot(ctx, namespace_, import_slot_);
    if (!s.ok()) {
      return {Status::NotOK, fmt::format("clear keys of slot error: {}", s.ToString())};
    }
  }

  import_status_ = kImportFailed;
  return Status::OK();
}

int SlotImport::GetSlot() {
  std::lock_guard<std::mutex> guard(mutex_);
  // import_slot_ only be set when import_status_ is kImportStart
  if (import_status_ != kImportStart) {
    return -1;
  }
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
