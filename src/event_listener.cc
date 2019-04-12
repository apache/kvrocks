#include "event_listener.h"
#include <string>

void KvrocksEventListener::OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[Compaction compeleted] column family: " << ci.cf_name
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes
            << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_maunal:" << ci.stats.is_manual_compaction
            << ", elapsed(micro): " << ci.stats.elapsed_micros;

  if (storage_->IsReachSpaceLimit()) {
    storage_->SetReachSpaceLimit(true);
  } else {
    storage_->SetReachSpaceLimit(false);
  }
}

void KvrocksEventListener::OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &ci) {
  if (storage_->IsReachSpaceLimit()) {
    storage_->SetReachSpaceLimit(true);
  }
}

void KvrocksEventListener::OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *status) {
  std::string reason_str;
  switch (reason) {
    case rocksdb::BackgroundErrorReason::kCompaction:reason_str = "compact";
      break;
    case rocksdb::BackgroundErrorReason::kFlush:reason_str = "flush";
      break;
    case rocksdb::BackgroundErrorReason::kMemTable:reason_str = "memtable";
      break;
    case rocksdb::BackgroundErrorReason::kWriteCallback:reason_str = "writecallback";
      break;
    default:
      // Should not arrive here
      break;
  }
  LOG(ERROR) << "[OnBackgroundError] Reason: " << reason_str
             << ", Status: " << status->ToString();
}
