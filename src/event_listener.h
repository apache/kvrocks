#pragma once

#include <glog/logging.h>
#include <rocksdb/listener.h>

class CompactionEventListener : public rocksdb::EventListener {
 public:
  CompactionEventListener() = default;
  ~CompactionEventListener() override = default;
  void OnCompactionCompleted(rocksdb::DB* db, const rocksdb::CompactionJobInfo& ci) override;
};
