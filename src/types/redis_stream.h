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

#include <rocksdb/status.h>

#include <optional>
#include <string>
#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

using rocksdb::Slice;

namespace Redis {

class Stream : public SubKeyScanner {
 public:
  explicit Stream(Engine::Storage *storage, const std::string &ns)
      : SubKeyScanner(storage, ns), stream_cf_handle_(storage->GetCFHandle("stream")) {}
  rocksdb::Status Add(const Slice &stream_name, const StreamAddOptions &options, const std::vector<std::string> &values,
                      StreamEntryID *id);
  rocksdb::Status DeleteEntries(const Slice &stream_name, const std::vector<StreamEntryID> &ids, uint64_t *ret);
  rocksdb::Status Len(const Slice &stream_name, const StreamLenOptions &options, uint64_t *ret);
  rocksdb::Status GetStreamInfo(const Slice &stream_name, bool full, uint64_t count, StreamInfo *info);
  rocksdb::Status Range(const Slice &stream_name, const StreamRangeOptions &options, std::vector<StreamEntry> *entries);
  rocksdb::Status Trim(const Slice &stream_name, const StreamTrimOptions &options, uint64_t *ret);
  rocksdb::Status GetMetadata(const Slice &stream_name, StreamMetadata *metadata);
  rocksdb::Status GetLastGeneratedID(const Slice &stream_name, StreamEntryID *id);
  rocksdb::Status SetId(const Slice &stream_name, const StreamEntryID &last_generated_id,
                        std::optional<uint64_t> entries_added, std::optional<StreamEntryID> max_deleted_id);

 private:
  rocksdb::ColumnFamilyHandle *stream_cf_handle_;

  rocksdb::Status range(const std::string &ns_key, const StreamMetadata &metadata, const StreamRangeOptions &options,
                        std::vector<StreamEntry> *entries) const;
  rocksdb::Status getEntryRawValue(const std::string &ns_key, const StreamMetadata &metadata, const StreamEntryID &id,
                                   std::string *value) const;
  StreamEntryID entryIDFromInternalKey(const rocksdb::Slice &key) const;
  std::string internalKeyFromEntryID(const std::string &ns_key, const StreamMetadata &metadata,
                                     const StreamEntryID &id) const;
  static rocksdb::Status getNextEntryID(const StreamMetadata &metadata, const StreamAddOptions &options,
                                        bool first_entry, StreamEntryID *next_entry_id);
  uint64_t trim(const std::string &ns_key, const StreamTrimOptions &options, StreamMetadata *metadata,
                rocksdb::WriteBatch *batch);
};

}  // namespace Redis
