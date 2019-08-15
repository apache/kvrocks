#pragma once

#include <string>
#include <vector>

#include "redis_db.h"
#include "redis_metadata.h"

namespace Redis {

class Set : public SubKeyScanner {
 public:
  explicit Set(Engine::Storage *storage, const std::string &ns)
      : SubKeyScanner(storage, ns) {}

  rocksdb::Status Card(const Slice &user_key, int *ret);
  rocksdb::Status IsMember(const Slice &user_key, const Slice &member, int *ret);
  rocksdb::Status Add(const Slice &user_key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status Remove(const Slice &user_key, const std::vector<Slice> &members, int *ret);
  rocksdb::Status Members(const Slice &user_key, std::vector<std::string> *members);
  rocksdb::Status Move(const Slice &src, const Slice &dst, const Slice &member, int *ret);
  rocksdb::Status Take(const Slice &user_key, std::vector<std::string> *members, int count, bool pop);
  rocksdb::Status Diff(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Union(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Inter(const std::vector<Slice> &keys, std::vector<std::string> *members);
  rocksdb::Status Overwrite(Slice user_key, const std::vector<std::string> &members);
  rocksdb::Status DiffStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status UnionStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status InterStore(const Slice &dst, const std::vector<Slice> &keys, int *ret);
  rocksdb::Status Scan(const Slice &user_key,
                       const std::string &cursor,
                       uint64_t limit,
                       const std::string &member_prefix,
                       std::vector<std::string> *members);

 private:
  rocksdb::Status GetMetadata(const Slice &ns_key, SetMetadata *metadata);
};

}  // namespace Redis
