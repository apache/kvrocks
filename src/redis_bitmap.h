#pragma once

#include "redis_db.h"
#include "redis_metadata.h"

#include <string>
#include <vector>

#if defined(__sparc__) || defined(__arm__)
#define USE_ALIGNED_ACCESS
#endif

enum BitOpFlags {
  kBitOpAnd,
  kBitOpOr,
  kBitOpXor,
  kBitOpNot,
};

namespace Redis {

class Bitmap : public Database {
 public:
  Bitmap(Engine::Storage *storage, const std::string &ns): Database(storage, ns) {}
  rocksdb::Status GetBit(const Slice &user_key, uint32_t offset, bool *bit);
  rocksdb::Status GetString(const Slice &user_key, const uint32_t max_btos_size, std::string *value);
  rocksdb::Status SetBit(const Slice &user_key, uint32_t offset, bool new_bit, bool *old_bit);
  rocksdb::Status BitCount(const Slice &user_key, int64_t start, int64_t stop, uint32_t *cnt);
  rocksdb::Status BitPos(const Slice &user_key, bool bit, int64_t start, int64_t stop, bool stop_given, int64_t *pos);
  rocksdb::Status BitOp(BitOpFlags op_flag, const Slice &user_key, const std::vector<Slice> &op_keys, int64_t *len);
  static bool GetBitFromValueAndOffset(const std::string &value, const uint32_t offset);
  static bool IsEmptySegment(const Slice &segment);
 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, BitmapMetadata *metadata, std::string *raw_value);
};

}  // namespace Redis
