#pragma once

#include <glog/logging.h>
#include <rocksdb/listener.h>

#include "storage.h"

class EventListener : public rocksdb::EventListener {
 public:
  explicit EventListener(Engine::Storage *storage) : storage_(storage) {}
  ~EventListener() override = default;
  void OnFlushBegin(rocksdb::DB* db, const rocksdb::FlushJobInfo& fi) override;
  void OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) override;
  void OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) override;
  void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *status) override;
  void OnTableFileDeleted(const rocksdb::TableFileDeletionInfo& info) override;
  void OnStallConditionsChanged(const rocksdb::WriteStallInfo& info) override;
  void OnTableFileCreated(const rocksdb::TableFileCreationInfo& info) override;
 private:
  Engine::Storage *storage_ = nullptr;
};
