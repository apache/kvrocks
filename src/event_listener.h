#ifndef KVROCKS_EVENT_LISTENER_H
#define KVROCKS_EVENT_LISTENER_H

#include <glog/logging.h>
#include <rocksdb/listener.h>

class CompactionEventListener : public rocksdb::EventListener {
 public:
  explicit CompactionEventListener() = default;
  ~CompactionEventListener() override = default;
  void OnCompactionCompleted(rocksdb::DB* db,const rocksdb::CompactionJobInfo& ci) override;

};

#endif //KVROCKS_EVENT_LISTENER_H
