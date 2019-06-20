#include "redis_pubsub.h"

namespace Redis {
rocksdb::Status PubSub::Publish(const Slice &channel, const Slice &value) {
  rocksdb::WriteBatch batch;
  batch.Put(pubsub_cf_handle_, channel, value);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}
}  // namespace Redis
