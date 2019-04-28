#include "event_listener.h"
#include <string>

void KvrocksEventListener::OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[event_listener/compaction_completed] column family: " << ci.cf_name
            << ", reason: " << std::to_string(static_cast<int>(ci.compaction_reason))
            << ", compression: " << std::to_string(static_cast<char>(ci.compression))
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes
            << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_maunal:" << ci.stats.is_manual_compaction
            << ", elapsed(micro): " << ci.stats.elapsed_micros;
  storage_->IncrCompactionCount(1);
  storage_->CheckDBSizeLimit();
}

void KvrocksEventListener::OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &ci) {
  storage_->IncrFlushCount(1);
  storage_->CheckDBSizeLimit();
  LOG(INFO) << "[event_listener/flush_completed] column family: " << ci.cf_name
            << ", file: " << ci.file_path
            << ", reason: " << std::to_string(static_cast<int>(ci.flush_reason))
            << ", is_write_slowdown: " << (ci.triggered_writes_slowdown ? "yes" : "no")
            << ", is_write_stall: " << (ci.triggered_writes_stop? "yes" : "no")
            << ", largest seqno: " << ci.largest_seqno
            << ", smallest seqno: " << ci.smallest_seqno;
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
  LOG(ERROR) << "[event_listener/background_error] Reason: " << reason_str
             << ", Status: " << status->ToString();
}

void KvrocksEventListener::OnTableFileDeleted(const rocksdb::TableFileDeletionInfo &info) {
  LOG(INFO) << "[event_listener/table_file_deleted] db: " << info.db_name
            << ", sst file: " << info.file_path
            << ", status: " << info.status.ToString();
}

void KvrocksEventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) {
  const char *stall_condition_strings[] = {"normal", "delay", "stop"};
  LOG(WARNING) << "[event_listner/stall_cond_changed] column family: " << info.cf_name
               << " write stall condition was changed, from "
               << stall_condition_strings[static_cast<int>(info.condition.prev)]
               << " to " << stall_condition_strings[static_cast<int>(info.condition.cur)];
}
