#pragma once

#include <rocksdb/compaction_filter.h>
#include <vector>
#include <memory>
#include <string>
#include "redis_metadata.h"

namespace Engine {
class MetadataFilter : public rocksdb::CompactionFilter {
 public:
  const char *Name() const override { return "MetadataFilter"; }
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override;
};

class MetadataFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  MetadataFilterFactory() = default;
  const char *Name() const override { return "MetadataFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(new MetadataFilter());
  }
};

class SubKeyFilter : public rocksdb::CompactionFilter {
 public:
  SubKeyFilter(rocksdb::DB **db, std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles)
      : cached_key_(""),
        cached_metadata_(""),
        db_(db),
        cf_handles_(cf_handles) {}

  const char *Name() const override { return "SubkeyFilter"; }
  bool IsKeyExpired(const InternalKey &ikey, const Slice &value) const;
  bool Filter(int level, const Slice &key, const Slice &value,
              std::string *new_value, bool *modified) const override;

 protected:
  mutable std::string cached_key_;
  mutable std::string cached_metadata_;
  rocksdb::DB **db_;
  std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles_;
};

class SubKeyFilterFactory : public rocksdb::CompactionFilterFactory {
 public:
  explicit SubKeyFilterFactory(
      rocksdb::DB **db,
      std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles) {
    db_ = db;
    cf_handles_ = cf_handles;
  }

  const char *Name() const override { return "SubKeyFilterFactory"; }
  std::unique_ptr<rocksdb::CompactionFilter> CreateCompactionFilter(
      const rocksdb::CompactionFilter::Context &context) override {
    return std::unique_ptr<rocksdb::CompactionFilter>(
        new SubKeyFilter(db_, cf_handles_));
  }

 private:
  rocksdb::DB **db_;
  std::vector<rocksdb::ColumnFamilyHandle *> *cf_handles_;
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
