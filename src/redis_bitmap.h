#pragma once

#include "redis_metadata.h"

#include <string>

class RedisBitmap : public RedisDB {
 public:
  RedisBitmap(Engine::Storage *storage, std::string ns): RedisDB(storage, std::move(ns)) {}
  rocksdb::Status GetBit(Slice key, uint32_t offset, bool *bit);
  rocksdb::Status SetBit(Slice key, uint32_t offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(Slice key, int start, int stop, uint32_t *cnt);
  rocksdb::Status BitPos(Slice key, bool bit, int start, int stop, int *pos);
 private:
  rocksdb::Status GetMetadata(Slice key, BitmapMetadata *metadata);
};

