#pragma once

#include "redis_metadata.h"

#include <string>

const uint32_t kBitmapSegmentBits = 1024 * 8;
const uint32_t kBitmapSegmentBytes = 1024;

class RedisBitmap : public RedisDB {
 public:
  RedisBitmap(Engine::Storage *storage, const std::string &ns): RedisDB(storage, ns) {}
  rocksdb::Status GetBit(const Slice &user_key, uint32_t offset, bool *bit);
  rocksdb::Status SetBit(const Slice &user_key, uint32_t offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(const Slice &user_key, int start, int stop, uint32_t *cnt);
  rocksdb::Status BitPos(const Slice &user_key, bool bit, int start, int stop, int *pos);
  static bool GetBitFromValueAndOffset(const std::string &value, const uint32_t offset);
  static bool IsEmptySegment(const Slice &segment);
 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, BitmapMetadata *metadata);
};

