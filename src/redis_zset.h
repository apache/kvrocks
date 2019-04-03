#pragma once

#include <string>
#include <vector>
#include <limits>

#include "redis_metadata.h"

typedef struct ZRangeSpec {
  double min, max;
  bool minex, maxex; /* are min or max exclusive */
  int offset, count;
  bool removed;
  ZRangeSpec() {
    min = std::numeric_limits<double>::lowest();
    max = std::numeric_limits<double>::max();
    minex = maxex = false;
    offset = -1; count = -1;
    removed = false;
  }
} ZRangeSpec;

typedef struct {
  std::string member;
  double score;
} MemberScore;

#define ZSET_INCR 1
#define ZSET_NX (1<<1)
#define ZSET_XX (1<<2)
#define ZSET_REVERSED (1<<3)
#define ZSET_REMOVED 1<<4

class RedisZSet : public RedisSubKeyScanner {
 public:
  explicit RedisZSet(Engine::Storage *storage, const std::string &ns) :
      RedisSubKeyScanner(storage, ns),
      score_cf_handle_(storage->GetCFHandle("zset_score")) {}
  rocksdb::Status Add(Slice key, uint8_t flags, std::vector<MemberScore> *mscores, int *ret);
  rocksdb::Status Card(Slice key, int *ret);
  rocksdb::Status Count(Slice key, const ZRangeSpec &spec, int *ret);
  rocksdb::Status IncrBy(Slice key, Slice member, double increment, double *score);
  rocksdb::Status Range(Slice key, int start, int stop, uint8_t flags, std::vector<MemberScore> *mscores);
  rocksdb::Status RangeByScore(Slice key, ZRangeSpec spec, std::vector<MemberScore> *mscores, int *size);
  rocksdb::Status Rank(Slice key, Slice member, bool reversed, int *ret);
  rocksdb::Status Remove(Slice key, std::vector<Slice> members, int *ret);
  rocksdb::Status RemoveRangeByScore(Slice key, ZRangeSpec spec, int *ret);
  rocksdb::Status RemoveRangeByRank(Slice key, int start, int stop, int *ret);
  rocksdb::Status Pop(Slice key, int count, bool min, std::vector<MemberScore> *mscores);
  rocksdb::Status Score(Slice key, Slice member, double *score);
  static Status ParseRangeSpec(const std::string &min, const std::string &max, ZRangeSpec *spec);
  uint64_t Scan(Slice key,
                const std::string &cursor,
                const uint64_t &limit,
                const std::string &member_prefix,
                std::vector<std::string> *members);

 private:
  rocksdb::ColumnFamilyHandle *score_cf_handle_;
  rocksdb::Status GetMetadata(Slice key, ZSetMetadata *metadata);
};
