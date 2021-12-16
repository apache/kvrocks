#include "slot_import.h"

SlotImport::SlotImport(Server *svr) : Database(svr->storage_, kDefaultNamespace), svr_(svr) {
  std::lock_guard<std::mutex> guard(mutex_);
  // Let db_ and metadata_cf_handle_ be nullptr, then get them in real time while use them.
  // See comments in SlotMigrate::SlotMigrate for detailed reason.
  db_ = nullptr;
  metadata_cf_handle_ = nullptr;

  import_fd_ = -1;
  import_slot_ = -1;
  import_status_ = kImportNone;
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
    LOG(INFO) << "[import] Failed to clear keys of slot " << slot
              << "current status is importing 'START'"
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
      LOG(ERROR) << "[import] Wrong slot, importing slot: " << import_slot_
                 << ", but got slot: " << slot;
      return false;
  }

  Status s = svr_->cluster_->SetSlot(slot, svr_->cluster_->GetMyId());
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
    LOG(ERROR) << "[import] Wrong slot, importing slot: " << import_slot_
               << ", but got slot: " << slot;
    return false;
  }

  // Clean imported slot data
  auto s = ClearKeysOfSlot(namespace_, slot);
  if (!s.ok()) {
    LOG(INFO) << "[import] Failed to clear keys of slot " << slot
              << ", current importing status is importing 'FAIL'"
              << ", Err: " << s.ToString();
  }

  import_status_ = kImportFail;
  import_fd_ = -1;
  return true;
}

void SlotImport::StopForLinkError(int fd) {
  std::lock_guard<std::mutex> guard(mutex_);
  if (import_status_ != kImportStart) return;

  // Maybe server has failovered
  // Situation:
  // Refer to the situation described in SlotMigrate::SlotMigrate
  // 1. Change server to slave when it is importing data.
  // 2. Source server's migration process end after destination server has finished replication.
  // 3. The migration link closed by source server, then this function will be call by OnEvent.
  // 4. ClearKeysOfSlot can clear data although server is a slave, because ClearKeysOfSlot
  //    deletes data in rocksdb directly. Therefor, it is necessary to avoid clearing data gotten
  //    from new master.
  if (!svr_->IsSlave()) {
    // Clean imported slot data
    auto s = ClearKeysOfSlot(namespace_, import_slot_);
    if (!s.ok()) {
      LOG(WARNING) << "[import] Failed to clear keys of slot " << import_slot_
                << " Current status is link error"
                << ", Err: " << s.ToString();
    }
  }

  LOG(INFO) << "[import] Stop importing for link error, slot: " << import_slot_;
  import_status_ = kImportFail;
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

Status SlotImport::GetImportInfo(std::vector<std::string> &info, int slot) {
  info.clear();
  if (import_slot_ < 0) {
    return Status(Status::NotOK, "There is no slot importing");
  }
  if (import_slot_ >= 0) {
    if (import_slot_ != slot) {
      return Status(Status::NotOK, "Input slot is not in importing");
    }
  }

  std::string import_stat;
  switch (import_status_) {
    case kImportNone:
      import_stat = "NONE";
      break;
    case kImportStart:
      import_stat = "START";
      break;
    case kImportSuccess:
      import_stat = "SUCCESS";
      break;
    case kImportFail:
      import_stat = "ERROR";
      break;
    default:
      break;
  }

  info.push_back("# Import Status");
  info.push_back("import_slot: " + std::to_string(import_slot_));
  info.push_back("import_state: " + import_stat);

  return Status::OK();
}

