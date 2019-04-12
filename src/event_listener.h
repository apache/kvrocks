#pragma once

#include <glog/logging.h>
#include <rocksdb/listener.h>

#include "storage.h"

class KvrocksEventListener : public rocksdb::EventListener {
 public:
  explicit KvrocksEventListener(Engine::Storage *storage) : storage_(storage) {}
  ~KvrocksEventListener() override = default;
  void OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) override;
  void OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &ci) override;
  void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *status) override;
 private:
  Engine::Storage *storage_ = nullptr;
};
