#include "redis_pubsub.h"

rocksdb::Status RedisPubSub::Publish(const Slice &channel, const Slice &value) {
  rocksdb::WriteBatch batch;
  batch.Put(pubsub_cf_handle_, channel, value);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}
