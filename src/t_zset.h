//
// Created by hulk on 2018/10/16.
//

#ifndef KVROCKS_T_ZSET_H
#define KVROCKS_T_ZSET_H

#include "t_metadata.h"

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
#define ZSET_REVERSED 2
#define ZSET_REMOVED 4

class RedisZSet : public RedisDB {
public:
  explicit RedisZSet(Engine::Storage *storage)
          :RedisDB(storage),
           score_cf_handle_(storage->GetCFHandle("zset_score")) {}
  rocksdb::Status Add(Slice key, uint8_t flags, std::vector<MemberScore> &mscores, int *ret);
  rocksdb::Status Card(Slice key, int *ret);
  rocksdb::Status Count(Slice key, ZRangeSpec spec, int *ret);
  rocksdb::Status IncrBy(Slice key, Slice member, double increment, double *score);
  rocksdb::Status Range(Slice key, int start, int stop, uint8_t flags, std::vector<MemberScore> *mscores);
  rocksdb::Status RangeByScore(Slice key, ZRangeSpec spec, std::vector<MemberScore> *mscores, int *size);
  rocksdb::Status Rank(Slice key, Slice member, bool reversed, int *ret);
  rocksdb::Status Remove(Slice key, std::vector<Slice> members, int *ret);
  rocksdb::Status RemoveRangeByScore(Slice key, ZRangeSpec spec, int *ret);
  rocksdb::Status RemoveRangeByRank(Slice key, int start, int stop, bool reversed, int *ret);
  rocksdb::Status Pop(Slice key, int count, bool min, std::vector<MemberScore> *mscores);
  rocksdb::Status Score(Slice key, Slice member, double *score);

private:
  rocksdb::ColumnFamilyHandle *score_cf_handle_;
  rocksdb::Status GetMetadata(Slice key, ZSetMetadata *metadata);
};


#endif //KVROCKS_T_ZSET_H
