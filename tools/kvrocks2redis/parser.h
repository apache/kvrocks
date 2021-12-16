#pragma once

#include <string>
#include <map>
#include <vector>

#include "../../src/redis_db.h"
#include "../../src/status.h"
#include "../../src/storage.h"
#include "../../src/redis_metadata.h"
#include "../../src/batch_parser.h"

#include "config.h"
#include "writer.h"

class LatestSnapShot {
 public:
  explicit LatestSnapShot(rocksdb::DB *db) : db_(db) {
    snapshot_ = db_->GetSnapshot();
  }
  ~LatestSnapShot() {
    db_->ReleaseSnapshot(snapshot_);
  }
  const rocksdb::Snapshot *GetSnapShot() { return snapshot_; }
 private:
  rocksdb::DB *db_ = nullptr;
  const rocksdb::Snapshot *snapshot_ = nullptr;
};

class Parser {
 public:
  explicit Parser(Engine::Storage *storage, Writer *writer)
      : storage_(storage), writer_(writer) {
    lastest_snapshot_ = new LatestSnapShot(storage->GetDB());
    is_slotid_encoded_ = storage_->IsSlotIdEncoded();
  }
  ~Parser() { delete lastest_snapshot_; }
  Status ParseFullDB();
  rocksdb::Status ParseWriteBatch(const std::string &batch_string);

 protected:
  Engine::Storage *storage_ = nullptr;
  Writer *writer_ = nullptr;
  LatestSnapShot *lastest_snapshot_ = nullptr;
  bool is_slotid_encoded_ = false;

  Status parseSimpleKV(const Slice &ns_key, const Slice &value, int expire);
  Status parseComplexKV(const Slice &ns_key, const Metadata &metadata);
  Status parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap);
};
