#pragma once
#include <string>
#include <vector>
#include <map>

#include "redis_db.h"
#include "status.h"
#include "storage.h"
#include "redis_metadata.h"


// An extractor to extract update from raw writebatch
class WriteBatchExtractor : public rocksdb::WriteBatch::Handler {
 public:
  explicit WriteBatchExtractor(bool is_slotid_encoded, int16_t slot = -1, bool to_redis = false)
  : is_slotid_encoded_(is_slotid_encoded), slot_(slot), to_redis_(to_redis) {}
  void LogData(const rocksdb::Slice &blob) override;
  rocksdb::Status PutCF(uint32_t column_family_id, const Slice &key,
                        const Slice &value) override;

  rocksdb::Status DeleteCF(uint32_t column_family_id, const Slice &key) override;
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id,
                                const Slice& begin_key, const Slice& end_key) override;
  std::map<std::string, std::vector<std::string>> *GetAofStrings() { return &resp_commands_; }
 private:
  std::map<std::string, std::vector<std::string>> resp_commands_;
  Redis::WriteBatchLogData log_data_;
  bool firstSeen_ = true;
  bool is_slotid_encoded_ = false;
  int slot_;
  bool to_redis_;
};
