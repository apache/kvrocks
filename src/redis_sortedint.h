#pragma once

#include <string>
#include <vector>

#include "redis_db.h"
#include "redis_metadata.h"

namespace Redis {

class Sortedint : public Database {
 public:
  explicit Sortedint(Engine::Storage *storage, const std::string &ns) : Database(storage, ns) {}
  rocksdb::Status Card(const Slice &user_key, int *ret);
  rocksdb::Status Add(const Slice &user_key, std::vector<uint64_t> ids, int *ret);
  rocksdb::Status Remove(const Slice &user_key, std::vector<uint64_t> ids, int *ret);
  rocksdb::Status Range(const Slice &user_key,
                        uint64_t cursor_id,
                        uint64_t page,
                        uint64_t limit,
                        bool reversed,
                        std::vector<uint64_t> *ids);

 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, SortedintMetadata *metadata);
};

}  // namespace Redis
