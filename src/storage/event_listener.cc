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

#include "event_listener.h"

#include <map>
#include <string>
#include <vector>

std::string fileCreatedReason2String(const rocksdb::TableFileCreationReason reason) {
  std::vector<std::string> file_created_reason = {"flush", "compaction", "recovery", "misc"};
  if (static_cast<size_t>(reason) < file_created_reason.size()) {
    return file_created_reason[static_cast<size_t>(reason)];
  }
  return "unknown";
}

std::string stallConditionType2String(const rocksdb::WriteStallCondition type) {
  std::vector<std::string> stall_condition_strings = {"normal", "delay", "stop"};
  if (static_cast<size_t>(type) < stall_condition_strings.size()) {
    return stall_condition_strings[static_cast<size_t>(type)];
  }
  return "unknown";
}

std::string compressType2String(const rocksdb::CompressionType type) {
  std::map<rocksdb::CompressionType, std::string> compression_type_string_map = {
      {rocksdb::kNoCompression, "no"},
      {rocksdb::kSnappyCompression, "snappy"},
      {rocksdb::kZlibCompression, "zlib"},
      {rocksdb::kBZip2Compression, "zip2"},
      {rocksdb::kLZ4Compression, "lz4"},
      {rocksdb::kLZ4HCCompression, "lz4hc"},
      {rocksdb::kXpressCompression, "xpress"},
      {rocksdb::kZSTD, "zstd"},
      {rocksdb::kZSTDNotFinalCompression, "zstd_not_final"},
      {rocksdb::kDisableCompressionOption, "disable"}};
  auto iter = compression_type_string_map.find(type);
  if (iter == compression_type_string_map.end()) {
    return "unknown";
  }
  return iter->second;
}

bool isDiskQuotaExceeded(const rocksdb::Status &bg_error) {
  // EDQUOT: Disk quota exceeded (POSIX.1-2001)
  std::string exceeded_quota_str = "Disk quota exceeded";
  std::string err_msg = bg_error.ToString();

  return err_msg.find(exceeded_quota_str) != std::string::npos;
}

void EventListener::OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  LOG(INFO) << "[event_listener/compaction_completed] column family: " << ci.cf_name
            << ", compaction reason: " << static_cast<int>(ci.compaction_reason)
            << ", output compression type: " << compressType2String(ci.compression)
            << ", base input level(files): " << ci.base_input_level << "(" << ci.input_files.size() << ")"
            << ", output level(files): " << ci.output_level << "(" << ci.output_files.size() << ")"
            << ", input bytes: " << ci.stats.total_input_bytes << ", output bytes:" << ci.stats.total_output_bytes
            << ", is_manual_compaction:" << (ci.stats.is_manual_compaction ? "yes" : "no")
            << ", elapsed(micro): " << ci.stats.elapsed_micros;
  storage_->IncrCompactionCount(1);
  storage_->CheckDBSizeLimit();
}

void EventListener::OnFlushBegin(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  LOG(INFO) << "[event_listener/flush_begin] column family: " << fi.cf_name << ", thread_id: " << fi.thread_id
            << ", job_id: " << fi.job_id << ", reason: " << static_cast<int>(fi.flush_reason);
}

void EventListener::OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  storage_->IncrFlushCount(1);
  storage_->CheckDBSizeLimit();
  LOG(INFO) << "[event_listener/flush_completed] column family: " << fi.cf_name << ", thread_id: " << fi.thread_id
            << ", job_id: " << fi.job_id << ", file: " << fi.file_path
            << ", reason: " << static_cast<int>(fi.flush_reason)
            << ", is_write_slowdown: " << (fi.triggered_writes_slowdown ? "yes" : "no")
            << ", is_write_stall: " << (fi.triggered_writes_stop ? "yes" : "no")
            << ", largest seqno: " << fi.largest_seqno << ", smallest seqno: " << fi.smallest_seqno;
}

void EventListener::OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *bg_error) {
  std::string reason_str;
  switch (reason) {
    case rocksdb::BackgroundErrorReason::kCompaction:
      reason_str = "compact";
      break;
    case rocksdb::BackgroundErrorReason::kFlush:
      reason_str = "flush";
      break;
    case rocksdb::BackgroundErrorReason::kMemTable:
      reason_str = "memtable";
      break;
    case rocksdb::BackgroundErrorReason::kWriteCallback:
      reason_str = "writecallback";
      break;
    default:
      // Should not arrive here
      break;
  }
  if ((bg_error->IsNoSpace() || isDiskQuotaExceeded(*bg_error)) &&
      bg_error->severity() < rocksdb::Status::kFatalError) {
    storage_->SetDBInRetryableIOError(true);
  }

  LOG(ERROR) << "[event_listener/background_error] reason: " << reason_str << ", bg_error: " << bg_error->ToString();
}

void EventListener::OnTableFileDeleted(const rocksdb::TableFileDeletionInfo &info) {
  LOG(INFO) << "[event_listener/table_file_deleted] db: " << info.db_name << ", sst file: " << info.file_path
            << ", status: " << info.status.ToString();
}

void EventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) {
  LOG(WARNING) << "[event_listener/stall_cond_changed] column family: " << info.cf_name
               << " write stall condition was changed, from " << stallConditionType2String(info.condition.prev)
               << " to " << stallConditionType2String(info.condition.cur);
}

void EventListener::OnTableFileCreated(const rocksdb::TableFileCreationInfo &info) {
  LOG(INFO) << "[event_listener/table_file_created] column family: " << info.cf_name
            << ", file path: " << info.file_path << ", file size: " << info.file_size << ", job id: " << info.job_id
            << ", reason: " << fileCreatedReason2String(info.reason) << ", status: " << info.status.ToString();
}
