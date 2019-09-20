#pragma once

#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/db.h>

#include <string>
#include <vector>

#include "encoding.h"
#include "redis_db.h"

// crc32
#define HASH_SLOTS_MASK 0x000003ff
#define HASH_SLOTS_SIZE (HASH_SLOTS_MASK + 1)

const uint32_t IEEE_POLY = 0xedb88320;
extern uint32_t crc32tab[256];

typedef struct {
  uint32_t slot_num;
  uint64_t count;
} SlotCount;

typedef struct KeyValue {
  std::string key;
  int ttl;
  std::string value;
} KeyValue;

void CRC32TableInit(uint32_t poly);
void InitCRC32Table();
uint32_t CRC32Update(uint32_t crc, const char *buf, int len);
uint32_t GetSlotNumFromKey(const std::string &key);
std::string GetTagFromKey(const std::string &key);

class SlotInternalKey {
 public:
  explicit SlotInternalKey(rocksdb::Slice key, uint64_t version);
  explicit SlotInternalKey(uint32_t slot_num, uint64_t version);
  explicit SlotInternalKey(rocksdb::Slice input);
  ~SlotInternalKey();

  uint64_t GetSlotNum() const;
  uint64_t GetVersion() const;
  rocksdb::Slice GetKey() const;
  void Encode(std::string *out);
  bool operator==(const SlotInternalKey &that) const;

 private:
  uint32_t slot_num_;
  uint64_t version_;
  rocksdb::Slice key_;
  char *buf_;
  char prealloc_[256];
};

class SlotMetadata {
 public:
  uint64_t version;
  uint32_t size;

 public:
  SlotMetadata();

  void Encode(std::string *dst) const;
  rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const SlotMetadata &that) const;

 private:
  uint64_t generateVersion();
};

namespace Redis {
class Slot : public SubKeyScanner {
 public:
  explicit Slot(Engine::Storage *storage, const std::string &ns = "") :
      SubKeyScanner(storage, ns),
      slot_metadata_cf_handle_(storage->GetCFHandle("slot_metadata")),
      slot_key_cf_handle_(storage->GetCFHandle("slot")) {}

  Status MigrateOne(const std::string &host, uint32_t port, uint64_t timeout, const rocksdb::Slice &key);
  Status MigrateSlotRandomOne(const std::string &host,
                              uint32_t port,
                              uint64_t timeout,
                              uint32_t slot_num);
  Status MigrateTagSlot(const std::string &host,
                        uint32_t port,
                        uint64_t timeout,
                        uint32_t slot_num,
                        int *ret);
  Status MigrateTag(const std::string &host,
                    uint32_t port,
                    uint64_t timeout,
                    const std::string &key,
                    int *ret);
  rocksdb::Status Check();
  rocksdb::Status GetInfo(uint64_t start, uint64_t count, std::vector<SlotCount> *slot_counts);
  rocksdb::Status Del(uint32_t slot_num);
  rocksdb::Status DeleteAll();
  rocksdb::Status Size(uint32_t slot_num, uint32_t *ret);
  rocksdb::Status UpdateKeys(const std::vector<std::string> &put_keys,
                             const std::vector<std::string> &delete_keys,
                             rocksdb::WriteBatch *updates);
  rocksdb::Status IsKeyExist(const Slice &key);
  rocksdb::Status Scan(uint32_t slot_num,
                       const std::string &cursor,
                       uint64_t limit,
                       std::vector<std::string> *keys);
  rocksdb::Status Restore(const std::vector<KeyValue> &key_values);
  Status CheckCodisEnabledStatus(bool enabled);

 private:
  std::string codis_enabled_status_key_ = "codis_enabled";
  rocksdb::ColumnFamilyHandle *slot_metadata_cf_handle_;
  rocksdb::ColumnFamilyHandle *slot_key_cf_handle_;
  rocksdb::Status GetMetadata(uint32_t slot_num, SlotMetadata *metadata);
  Status generateMigrateCommandComplexKV(const Slice &key, const Metadata &metadata, std::string *output);
};
}  // namespace Redis




