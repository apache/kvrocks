#pragma once

#include <string>

#include "redis_metadata.h"

class RedisPubSub : public RedisDB {
 public:
  explicit RedisPubSub(Engine::Storage *storage) :
      RedisDB(storage),
      pubsub_cf_handle_(storage->GetCFHandle("pubsub")) {}
  rocksdb::Status Publish(const Slice &channel, const Slice &value);

 private:
  rocksdb::ColumnFamilyHandle *pubsub_cf_handle_;
};
