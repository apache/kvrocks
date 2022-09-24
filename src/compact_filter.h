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

#include <vector>
#include <memory>
#include <string>

#include <rocksdb/db.h>
#include <rocksdb/compaction_filter.h>

#include "redis_metadata.h"
#include "storage.h"

namespace Engine {
class MetadataFilter : public rocksdb::CompactionFilter {
 public:
  explicit MetadataFilter(Storage *storage): stor_(storage) {}
  const char *Name() const override { return "MetadataFilter"; }
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override;
 private:
  Engine::Storage *stor_;
};

class MetadataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit MetadataFilterFactory(Engine::Storage *storage) {
    stor_ = storage;
  }
  const char *Name() const override { return "MetadataFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new MetadataFilter(stor_));
  }

 private:
  Engine::Storage *stor_ = nullptr;
};

class SubKeyFilter : public rocksdb::CompactionFilter {
 public:
  explicit SubKeyFilter(Storage *storage)
      : cached_key_(""),
        cached_metadata_(""),
        stor_(storage) {}

  const char *Name() const override { return "SubkeyFilter"; }
  Status GetMetadata(const InternalKey &ikey, Metadata* metadata) const;
  bool IsMetadataExpired(const InternalKey &ikey, const Metadata& metadata) const;
  rocksdb::CompactionFilter::Decision FilterBlobByKey(int level, const Slice &key,
                                  std::string *new_value,
                                  std::string *skip_until) const override;
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override;

 protected:
  mutable std::string cached_key_;
  mutable std::string cached_metadata_;
  Engine::Storage *stor_;
};

class SubKeyFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit SubKeyFilterFactory(Engine::Storage *storage) {
    stor_ = storage;
  }

  const char *Name() const override { return "SubKeyFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new SubKeyFilter(stor_));
  }

 private:
  Engine::Storage *stor_ = nullptr;
};

class PropagateFilter : public rocksdb::CompactionFilter {
 public:
  const char *Name() const override { return "PropagateFilter"; }
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override {
    // We propagate Lua commands which don't store data,
    // just in order to implement updating Lua state.
    return key == Engine::kPropagateScriptCommand;
  }
};

class PropagateFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  PropagateFilterFactory() = default;
  const char *Name() const override { return "PropagateFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new PropagateFilter());
  }
};

class PubSubFilter : public rocksdb::CompactionFilter {
 public:
  const char *Name() const override { return "PubSubFilter"; }
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override { return true; }
};

class PubSubFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  PubSubFilterFactory() = default;
  const char *Name() const override { return "PubSubFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new PubSubFilter());
  }
};
}  // namespace Engine
