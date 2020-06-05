#pragma once

#include "redis_db.h"
#include "redis_metadata.h"

#include <string>
#include <vector>

namespace Redis {

class BitmapString : public Database {
 public:
  BitmapString(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status GetBit(const std::string &raw_value, uint32_t offset, bool *bit);
  rocksdb::Status SetBit(const Slice &ns_key, std::string *raw_value, uint32_t offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(const std::string &raw_value, int start, int stop, uint32_t *cnt);
  rocksdb::Status BitPos(const std::string &raw_value, bool bit, int start, int stop, bool stop_given, int *pos);
 private:
  size_t redisPopcount(unsigned char *p, int count);
  int redisBitpos(unsigned char *c, int count, int bit);
};

}  // namespace Redis
