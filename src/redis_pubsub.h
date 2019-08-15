#pragma once

#include <string>

#include "redis_db.h"
#include "redis_metadata.h"

namespace Redis {

class PubSub : public Database {
 public:
  explicit PubSub(Engine::Storage *storage) :
      Database(storage),
      pubsub_cf_handle_(storage->GetCFHandle("pubsub")) {}
  rocksdb::Status Publish(const Slice &channel, const Slice &value);

 private:
  rocksdb::ColumnFamilyHandle *pubsub_cf_handle_;
};

}  // namespace Redis
