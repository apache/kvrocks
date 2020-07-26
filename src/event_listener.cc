#include "event_listener.h"
#include <string>

std::string compressType2String(const rocksdb::CompressionType type) {
  std::map<rocksdb::CompressionType, std::string>compression_type_string_map = {
      {rocksdb::kNoCompression, "no"},
      {rocksdb::kSnappyCompression, "snappy"},
      {rocksdb::kZlibCompression, "zlib"},
      {rocksdb::kBZip2Compression, "zip2"},
      {rocksdb::kLZ4Compression, "lz4"},
      {rocksdb::kLZ4HCCompression, "lz4hc"},
      {rocksdb::kXpressCompression, "xpress"},
      {rocksdb::kZSTD, "std"},
      {rocksdb::kZSTDNotFinalCompression, "zstd_not_final"},
      {rocksdb::kDisableCompressionOption, "disable"}
  };
  auto iter = compression_type_string_map.find(type);
  if (iter == compression_type_string_map.end()) {
    return "unknown";
  }
  return iter->second;
}

void EventListener::OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[event_listener/compaction_completed] column family: " << ci.cf_name
            << ", compaction reason: " << static_cast<int>(ci.compaction_reason)
            << ", output compression type: " << compressType2String(ci.compression)
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes
            << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_manual_compaction:" << (ci.stats.is_manual_compaction ? "yes":"no")
            << ", elapsed(micro): " << ci.stats.elapsed_micros;
  storage_->IncrCompactionCount(1);
  storage_->CheckDBSizeLimit();
}

void EventListener::OnFlushBegin(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  LOG(INFO) << "[event_listener/flush_begin] column family: " << fi.cf_name
            << ", thread_id: " << fi.thread_id << ", job_id: " << fi.job_id
            << ", reason: " << static_cast<int>(fi.flush_reason);
}

void EventListener::OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  storage_->IncrFlushCount(1);
  storage_->CheckDBSizeLimit();
  LOG(INFO) << "[event_listener/flush_completed] column family: " << fi.cf_name
            << ", thread_id: " << fi.thread_id << ", job_id: " << fi.job_id
            << ", file: " << fi.file_path
            << ", reason: " << static_cast<int>(fi.flush_reason)
            << ", is_write_slowdown: " << (fi.triggered_writes_slowdown ? "yes" : "no")
            << ", is_write_stall: " << (fi.triggered_writes_stop? "yes" : "no")
            << ", largest seqno: " << fi.largest_seqno
            << ", smallest seqno: " << fi.smallest_seqno;
}

void EventListener::OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *status) {
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
  LOG(ERROR) << "[event_listener/background_error] reason: " << reason_str
             << ", status: " << status->ToString();
}

void EventListener::OnTableFileDeleted(const rocksdb::TableFileDeletionInfo &info) {
  LOG(INFO) << "[event_listener/table_file_deleted] db: " << info.db_name
            << ", sst file: " << info.file_path
            << ", status: " << info.status.ToString();
}

void EventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) {
  const char *stall_condition_strings[] = {"normal", "delay", "stop"};
  LOG(WARNING) << "[event_listener/stall_cond_changed] column family: " << info.cf_name
               << " write stall condition was changed, from "
               << stall_condition_strings[static_cast<int>(info.condition.prev)]
               << " to " << stall_condition_strings[static_cast<int>(info.condition.cur)];
}
