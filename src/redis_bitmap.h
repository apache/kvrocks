#pragma once

#include "redis_metadata.h"

#include <string>

const uint32_t kBitmapSegmentBits = 1024 * 8;
const uint32_t kBitmapSegmentBytes = 1024;

class RedisBitmap : public RedisDB {
 public:
  RedisBitmap(Engine::Storage *storage, std::string ns): RedisDB(storage, std::move(ns)) {}
  rocksdb::Status GetBit(Slice key, uint32_t offset, bool *bit);
  rocksdb::Status SetBit(Slice key, uint32_t offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(Slice key, int start, int stop, uint32_t *cnt);
  rocksdb::Status BitPos(Slice key, bool bit, int start, int stop, int *pos);
  static bool GetBitFromValueAndOffset(const std::string &value, const uint32_t offset);
 private:
  rocksdb::Status GetMetadata(Slice key, BitmapMetadata *metadata);
};

