#pragma once

#include <string>
#include <vector>
#include <limits>

#include "redis_db.h"
#include "redis_metadata.h"

typedef struct SortedintRangeSpec {
  uint64_t min, max;
  bool minex, maxex; /* are min or max exclusive */
  int offset, count;
  bool reversed;
  SortedintRangeSpec() {
    min = std::numeric_limits<uint64_t>::lowest();
    max = std::numeric_limits<uint64_t>::max();
    minex = maxex = false;
    offset = -1;
    count = -1;
    reversed = false;
  }
} SortedintRangeSpec;

namespace Redis {

class Sortedint : public Database {
 public:
  explicit Sortedint(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Card(const Slice &user_key, int *ret);
  rocksdb::Status MExist(const Slice &user_key, std::vector<uint64_t> ids, std::vector<int> *exists);
  rocksdb::Status Add(const Slice &user_key, std::vector<uint64_t> ids, int *ret);
  rocksdb::Status Remove(const Slice &user_key, std::vector<uint64_t> ids, int *ret);
  rocksdb::Status Range(const Slice &user_key,
                        uint64_t cursor_id,
                        uint64_t page,
                        uint64_t limit,
                        bool reversed,
                        std::vector<uint64_t> *ids);
  rocksdb::Status RangeByValue(const Slice &user_key,
                               SortedintRangeSpec spec,
                               std::vector<uint64_t> *ids,
                               int *size);
  static Status ParseRangeSpec(const std::string &min, const std::string &max, SortedintRangeSpec *spec);

 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, SortedintMetadata *metadata);
};

}  // namespace Redis
