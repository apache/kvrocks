#pragma once

#include <utility>
#include <string>
#include <memory>
#include <rocksdb/table_properties.h>

class CompactOnExpiredCollector : public rocksdb::TablePropertiesCollector {
 public:
  explicit CompactOnExpiredCollector(const std::string &cf_name, float trigger_threshold)
      : cf_name_(cf_name), trigger_threshold_(trigger_threshold) {}
  const char * Name() const override { return "compact_on_expired_collector"; }
  bool NeedCompact() const override;
  rocksdb::Status AddUserKey(const rocksdb::Slice &key, const rocksdb::Slice &value,
      rocksdb::EntryType, rocksdb::SequenceNumber, uint64_t) override;
  rocksdb::Status Finish(rocksdb::UserCollectedProperties *properties) override;
  rocksdb::UserCollectedProperties GetReadableProperties() const override;

 private:
  std::string cf_name_;
  float trigger_threshold_;
  int64_t total_keys_ = 0;
  int64_t deleted_keys_ = 0;
  std::string start_key_;
  std::string stop_key_;
};

class CompactOnExpiredTableCollectorFactory: public rocksdb::TablePropertiesCollectorFactory {
 public:
  explicit CompactOnExpiredTableCollectorFactory(const std::string &cf_name,
                                                 float trigger_threshold) :
                                                 cf_name_(std::move(cf_name)),
                                                 trigger_threshold_(trigger_threshold) {}
  virtual ~CompactOnExpiredTableCollectorFactory() {}
  rocksdb::TablePropertiesCollector* CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override;
  const char * Name() const override {
    return "CompactOnExpiredCollector";
  }

 private:
  friend std::shared_ptr<CompactOnExpiredTableCollectorFactory>
    NewCompactOnExpiredTableCollectorFactory(const std::string &cf_name,
                                             float trigger_threshold);
  std::string cf_name_;
  float trigger_threshold_ = 0.3;
};

extern std::shared_ptr<CompactOnExpiredTableCollectorFactory>
NewCompactOnExpiredTableCollectorFactory(const std::string &cf_name,
                                         float trigger_threshold);
