#pragma once

#include <map>
#include <string>
#include <vector>
#include <limits>

#include "redis_db.h"
#include "redis_metadata.h"

enum AggregateMethod {
  kAggregateSum,
  kAggregateMin,
  kAggregateMax
};

const double kMinScore = (std::numeric_limits<float>::is_iec559 ?
      -std::numeric_limits<double>::infinity() : std::numeric_limits<double>::lowest());
const double kMaxScore = (std::numeric_limits<float>::is_iec559 ?
      std::numeric_limits<double>::infinity() : std::numeric_limits<double>::max());

typedef struct ZRangeSpec {
  double min, max;
  bool minex, maxex; /* are min or max exclusive */
  int offset, count;
  bool removed, reversed;
  ZRangeSpec() {
    min = kMinScore;
    max = kMaxScore;
    minex = maxex = false;
    offset = -1; count = -1;
    removed = reversed = false;
  }
} ZRangeSpec;

typedef struct ZRangeLexSpec {
  std::string min, max;
  bool minex, maxex; /* are min or max exclusive */
  bool max_infinite; /* are max infinite */
  int offset, count;
  bool removed;
  ZRangeLexSpec() {
    minex = maxex = false;
    max_infinite = false;
    offset = -1;
    count = -1;
    removed = false;
  }
} ZRangeLexSpec;

typedef struct KeyWeight {
  std::string key;
  double weight;
} KeyWeight;

typedef struct {
  std::string member;
  double score;
} MemberScore;

#define ZSET_INCR 1
#define ZSET_NX (1<<1)
#define ZSET_XX (1<<2)
#define ZSET_REVERSED (1<<3)
#define ZSET_REMOVED 1<<4

namespace Redis {

class ZSet : public SubKeyScanner {
 public:
  explicit ZSet(Engine::Storage *storage, const std::string &ns) :
      SubKeyScanner(storage, ns),
      score_cf_handle_(storage->GetCFHandle("zset_score")) {}
  rocksdb::Status Add(const Slice &user_key, uint8_t flags, std::vector<MemberScore> *mscores, int *ret);
  rocksdb::Status Card(const Slice &user_key, int *ret);
  rocksdb::Status Count(const Slice &user_key, const ZRangeSpec &spec, int *ret);
  rocksdb::Status IncrBy(const Slice &user_key, const Slice &member, double increment, double *score);
  rocksdb::Status Range(const Slice &user_key, int start, int stop, uint8_t flags, std::vector<MemberScore> *mscores);
  rocksdb::Status RangeByScore(const Slice &user_key, ZRangeSpec spec, std::vector<MemberScore> *mscores, int *size);
  rocksdb::Status RangeByLex(const Slice &user_key, ZRangeLexSpec spec, std::vector<std::string> *members, int *size);
  rocksdb::Status Rank(const Slice &user_key, const Slice &member, bool reversed, int *ret);
  rocksdb::Status Remove(const Slice &user_key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status RemoveRangeByScore(const Slice &user_key, ZRangeSpec spec, int *ret);
  rocksdb::Status RemoveRangeByLex(const Slice &user_key, ZRangeLexSpec spec, int *ret);
  rocksdb::Status RemoveRangeByRank(const Slice &user_key, int start, int stop, int *ret);
  rocksdb::Status Pop(const Slice &user_key, int count, bool min, std::vector<MemberScore> *mscores);
  rocksdb::Status Score(const Slice &user_key, const Slice &member, double *score);
  static Status ParseRangeSpec(const std::string &min, const std::string &max, ZRangeSpec *spec);
  static Status ParseRangeLexSpec(const std::string &min, const std::string &max, ZRangeLexSpec *spec);
  rocksdb::Status Scan(const Slice &user_key,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &member_prefix,
                       std::vector<std::string> *members,
                       std::vector<double> *scores = nullptr);
  rocksdb::Status Overwrite(const Slice &user_key, const std::vector<MemberScore> &mscores);
  rocksdb::Status InterStore(const Slice &dst,
                             const std::vector<KeyWeight> &keys_weights,
                             AggregateMethod aggregate_method,
                             int *size);
  rocksdb::Status UnionStore(const Slice &dst,
                             const std::vector<KeyWeight> &keys_weights,
                             AggregateMethod aggregate_method,
                             int *size);
  rocksdb::Status MGet(const Slice &user_key,
                       const std::vector<Slice> &members,
                       std::map<std::string, double> *mscores);

  rocksdb::Status GetMetadata(const Slice &ns_key, ZSetMetadata *metadata);

 private:
  rocksdb::ColumnFamilyHandle *score_cf_handle_;
};

}  // namespace Redis
