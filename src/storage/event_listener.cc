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

#include <string>
#include <vector>

#include "fmt/format.h"

std::string BackgroundErrorReason2String(const rocksdb::BackgroundErrorReason reason) {
  std::vector<std::string> background_error_reason = {
      "flush", "compaction", "write_callback", "memtable", "manifest_write", "flush_no_wal", "manifest_write_no_wal"};
  if (static_cast<size_t>(reason) < background_error_reason.size()) {
    return background_error_reason[static_cast<size_t>(reason)];
  }
  return "unknown";
}

std::string TableFileCreatedReason2String(const rocksdb::TableFileCreationReason reason) {
  std::vector<std::string> file_created_reason = {"flush", "compaction", "recovery", "misc"};
  if (static_cast<size_t>(reason) < file_created_reason.size()) {
    return file_created_reason[static_cast<size_t>(reason)];
  }
  return "unknown";
}

std::string BlobFileCreatedReason2String(const rocksdb::BlobFileCreationReason reason) {
  std::vector<std::string> blob_file_created_reason = {"flush", "compaction", "recovery"};
  if (static_cast<size_t>(reason) < blob_file_created_reason.size()) {
    return blob_file_created_reason[static_cast<size_t>(reason)];
  }
  return "unknown";
}

std::string StallConditionType2String(const rocksdb::WriteStallCondition type) {
  switch (type) {
    case rocksdb::WriteStallCondition::kDelayed:
      return "delay";
    case rocksdb::WriteStallCondition::kStopped:
      return "stop";
    case rocksdb::WriteStallCondition::kNormal:
      return "normal";
  }
  return "unknown";
}

std::string CompressType2String(const rocksdb::CompressionType type) {
  for (const auto &option : engine::CompressionOptions) {
    if (option.type == type) {
      return option.name;
    }
  }
  return "unknown";
}

std::string ExtractSSTFileNameFromError(const std::string &error) {
  auto match_results = util::RegexMatch(error, ".*(/\\w*\\.sst).*");
  if (match_results.size() == 2) {
    return match_results[1];
  }
  return {};
}

bool IsDiskQuotaExceeded(const rocksdb::Status &bg_error) {
  // EDQUOT: Disk quota exceeded (POSIX.1-2001)
  std::string exceeded_quota_str = "Disk quota exceeded";
  std::string err_msg = bg_error.ToString();

  return err_msg.find(exceeded_quota_str) != std::string::npos;
}

void EventListener::OnCompactionBegin([[maybe_unused]] rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  info(
      "[event_listener/compaction_begin] column family: {}, job_id: {}, compaction reason: {}, output compression "
      "type: {}, base input level(files): {}({}), output level(files): {}({}), input bytes: {}, output bytes: {}, "
      "is_manual_compaction: {}",
      ci.cf_name, ci.job_id, rocksdb::GetCompactionReasonString(ci.compaction_reason),
      CompressType2String(ci.compression), ci.base_input_level, ci.input_files.size(), ci.output_level,
      ci.output_files.size(), ci.stats.total_input_bytes, ci.stats.total_output_bytes,
      ci.stats.is_manual_compaction ? "yes" : "no");
}

void EventListener::OnCompactionCompleted([[maybe_unused]] rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) {
  info(
      "[event_listener/compaction_completed] column family: {}, job_id: {}, compaction reason: {}, output compression "
      "type: {}, base input level(files): {}({}), output level(files): {}({}), input bytes: {}, output bytes: {}, "
      "is_manual_compaction: {}, elapsed(micro): {}",
      ci.cf_name, ci.job_id, rocksdb::GetCompactionReasonString(ci.compaction_reason),
      CompressType2String(ci.compression), ci.base_input_level, ci.input_files.size(), ci.output_level,
      ci.output_files.size(), ci.stats.total_input_bytes, ci.stats.total_output_bytes,
      ci.stats.is_manual_compaction ? "yes" : "no", ci.stats.elapsed_micros);
  storage_->RecordStat(engine::StatType::CompactionCount, 1);
  storage_->CheckDBSizeLimit();
}

void EventListener::OnSubcompactionBegin(const rocksdb::SubcompactionJobInfo &si) {
  info(
      "[event_listener/subcompaction_begin] column family: {}, job_id: {}, compaction reason: {}, output compression "
      "type: {}",
      si.cf_name, si.job_id, rocksdb::GetCompactionReasonString(si.compaction_reason),
      CompressType2String(si.compression));
}

void EventListener::OnSubcompactionCompleted(const rocksdb::SubcompactionJobInfo &si) {
  info(
      "[event_listener/subcompaction_completed] column family: {}, job_id: {}, compaction reason: {}, output "
      "compression type: {}, base input level(files): {}, output level(files): {}, input bytes: {}, output bytes: {}, "
      "is_manual_compaction: {}, elapsed(micro): {}",
      si.cf_name, si.job_id, rocksdb::GetCompactionReasonString(si.compaction_reason),
      CompressType2String(si.compression), si.base_input_level, si.output_level, si.stats.total_input_bytes,
      si.stats.total_output_bytes, si.stats.is_manual_compaction ? "yes" : "no", si.stats.elapsed_micros);
}

void EventListener::OnFlushBegin([[maybe_unused]] rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  info("[event_listener/flush_begin] column family: {}, thread_id: {}, job_id: {}, reason: {}", fi.cf_name,
       fi.thread_id, fi.job_id, rocksdb::GetFlushReasonString(fi.flush_reason));
}

void EventListener::OnFlushCompleted([[maybe_unused]] rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) {
  storage_->RecordStat(engine::StatType::FlushCount, 1);
  storage_->CheckDBSizeLimit();
  info(
      "[event_listener/flush_completed] column family: {}, thread_id: {}, job_id: {}, file: {}, reason: {}, "
      "is_write_slowdown: {}, is_write_stall: {}, largest seqno: {}, smallest seqno: {}",
      fi.cf_name, fi.thread_id, fi.job_id, fi.file_path, rocksdb::GetFlushReasonString(fi.flush_reason),
      fi.triggered_writes_slowdown ? "yes" : "no", fi.triggered_writes_stop ? "yes" : "no", fi.largest_seqno,
      fi.smallest_seqno);
}

void EventListener::OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *bg_error) {
  auto reason_str = BackgroundErrorReason2String(reason);
  auto error_str = bg_error->ToString();
  if (bg_error->IsCorruption() || bg_error->IsIOError()) {
    // Background error may occur when SST are generated during flush/compaction. If those files are not applied
    // to Version, we consider them non-fatal background error. We can override bg_error to recover from
    // background error.
    // Note that we cannot call Resume() manually because the error severity is unrecoverable.
    auto corrupt_sst = ExtractSSTFileNameFromError(error_str);
    if (!corrupt_sst.empty()) {
      std::vector<std::string> live_files;
      uint64_t manifest_size = 0;
      auto s = storage_->GetDB()->GetLiveFiles(live_files, &manifest_size, false /* flush_memtable */);
      if (s.ok() && std::find(live_files.begin(), live_files.end(), corrupt_sst) == live_files.end()) {
        *bg_error = rocksdb::Status::OK();
        warn(
            "[event_listener/background_error] ignore no-fatal background error about sst file, reason: {}, bg_error: "
            "{}",
            reason_str, error_str);
        return;
      }
    }
  }

  if ((bg_error->IsNoSpace() || IsDiskQuotaExceeded(*bg_error)) &&
      bg_error->severity() < rocksdb::Status::kFatalError) {
    storage_->SetDBInRetryableIOError(true);
  }

  error("[event_listener/background_error] reason: {}, bg_error: {}", reason_str, error_str);
}

void EventListener::OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) {
  warn("[event_listener/stall_cond_changed] column family: {} write stall condition was changed, from {} to {}",
       info.cf_name, StallConditionType2String(info.condition.prev), StallConditionType2String(info.condition.cur));
}

void EventListener::OnTableFileCreated(const rocksdb::TableFileCreationInfo &table_info) {
  info(
      "[event_listener/table_file_created] column family: {}, file path: {}, file size: {}, job_id: {}, reason: {}, "
      "status: {}",
      table_info.cf_name, table_info.file_path, table_info.file_size, table_info.job_id,
      TableFileCreatedReason2String(table_info.reason), table_info.status.ToString());
}

void EventListener::OnTableFileDeleted(const rocksdb::TableFileDeletionInfo &table_info) {
  info("[event_listener/table_file_deleted] db: {}, sst file: {}, status: {}", table_info.db_name, table_info.file_path,
       table_info.status.ToString());
}

void EventListener::OnBlobFileCreated(const rocksdb::BlobFileCreationInfo &blob_info) {
  info(
      "[event_listener/blob_file_created] column family: {}, file path: {}, blob count: {}, blob bytes: {}, job_id: {}"
      ", reason: {}, status: {}",
      blob_info.cf_name, blob_info.file_path, blob_info.total_blob_count, blob_info.total_blob_bytes, blob_info.job_id,
      BlobFileCreatedReason2String(blob_info.reason), blob_info.status.ToString());
}

void EventListener::OnBlobFileDeleted(const rocksdb::BlobFileDeletionInfo &blob_info) {
  info("[event_listener/blob_file_deleted] db: {}, blob file: {}, status: {}", blob_info.db_name, blob_info.file_path,
       blob_info.status.ToString());
}
