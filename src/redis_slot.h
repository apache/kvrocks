#pragma once
#include <string>
#include <vector>
#include <thread>
#include <rocksdb/status.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/db.h>

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
  uint64_t size;

 public:
  explicit SlotMetadata(bool generate_version = true);

  void Encode(std::string *dst) const;
  rocksdb::Status Decode(const std::string &bytes);
  bool operator==(const SlotMetadata &that) const;

 private:
  uint64_t generateVersion();
};

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  void Unlock();
  void AssertHeld() {}

 private:
  friend class CondVar;
  pthread_mutex_t mu_;

  // No copying
  Mutex(const Mutex &);
  void operator=(const Mutex &);
};

class CondVar {
 public:
  explicit CondVar(Mutex *mu);
  ~CondVar();
  void Wait();
  /*
   * timeout is millisecond
   * so if you want to wait for 1 s, you should call
   * TimeWait(1000);
   * return false if timeout
   */
  bool TimedWait(uint32_t timeout);
  void Signal();
  void SignalAll();

 private:
  pthread_cond_t cv_;
  Mutex *mu_;
};

class MutexLock {
 public:
  explicit MutexLock(Mutex *mu)
      : mu_(mu) {
    this->mu_->Lock();
  }
  ~MutexLock() { this->mu_->Unlock(); }

 private:
  Mutex *const mu_;
  // No copying allowed
  MutexLock(const MutexLock &);
  void operator=(const MutexLock &);
};

namespace Redis {
class Slot : public SubKeyScanner {
 public:
  explicit Slot(Engine::Storage *storage) :
      SubKeyScanner(storage, kDefaultNamespace),
      slot_metadata_cf_handle_(storage->GetCFHandle("slot_metadata")),
      slot_key_cf_handle_(storage->GetCFHandle("slot")) {}

  rocksdb::Status GetMetadata(uint32_t slot_num, SlotMetadata *metadata);
  Status MigrateOne(const std::string &host, int port, uint64_t timeout, const rocksdb::Slice &key);
  Status MigrateOneKey(int sock_fd, const rocksdb::Slice &key);
  Status MigrateSlotRandomOne(const std::string &host,
                              int port,
                              uint64_t timeout,
                              uint32_t slot_num);
  Status MigrateTagSlot(const std::string &host,
                        int port,
                        uint64_t timeout,
                        uint32_t slot_num,
                        int *ret);
  Status MigrateTag(const std::string &host,
                    int port,
                    uint64_t timeout,
                    const std::string &key,
                    int *ret);
  rocksdb::Status Check();
  rocksdb::Status GetInfo(uint32_t start, int count, std::vector<SlotCount> *slot_counts);
  rocksdb::Status Del(uint32_t slot_num);
  rocksdb::Status AddKey(const Slice &key);
  rocksdb::Status DeleteKey(const Slice &key);
  rocksdb::Status DeleteAll();
  rocksdb::Status Size(uint32_t slot_num, uint64_t *ret);
  rocksdb::Status UpdateKeys(const std::vector<std::string> &put_keys,
                             const std::vector<std::string> &delete_keys,
                             rocksdb::WriteBatch *updates);
  rocksdb::Status IsKeyExist(const Slice &key);
  rocksdb::Status Scan(uint32_t slot_num,
                       const std::string &cursor,
                       uint64_t limit,
                       std::vector<std::string> *keys);
  rocksdb::Status Restore(const std::vector<KeyValue> &key_values);

 private:
  rocksdb::ColumnFamilyHandle *slot_metadata_cf_handle_;
  rocksdb::ColumnFamilyHandle *slot_key_cf_handle_;
  Status generateMigrateCommandComplexKV(const Slice &key, const Metadata &metadata, std::string *output);
};

class SlotsMgrtSenderThread {
 public:
  explicit SlotsMgrtSenderThread(Engine::Storage *storage) :
      slotsmgrt_cond_(&slotsmgrt_cond_mu_),
      storage_(storage),
      is_migrating_(false) {}
  virtual ~SlotsMgrtSenderThread();

  Status Start();
  void Stop();
  void StopMigrateSlot();
  void Join();
  bool IsStopped() { return stop_; }
  Status SlotsMigrateOne(const std::string &key, int *ret);
  Status SlotsMigrateBatch(const std::string &ip, int port, uint64_t time_out, uint32_t slot, int keys_num);
  Status GetSlotsMigrateResult(uint64_t *moved, uint64_t *remained);
  Status GetSlotsMgrtSenderStatus(std::string *ip,
                                  int *port,
                                  uint32_t *slot_num,
                                  bool *migrating,
                                  uint64_t *moved,
                                  uint64_t *remained);
  Status SlotsMigrateAsyncCancel();

 private:
  std::thread t_;
  std::mutex db_mu_;
  std::mutex ones_mu_;
  Mutex slotsmgrt_cond_mu_;
  CondVar slotsmgrt_cond_;
  bool stop_ = false;
  Engine::Storage *storage_;

  std::string dest_ip_;
  int dest_port_ = 0;
  uint64_t timeout_ms_ = 0;
  uint32_t slot_num_ = 0;
  std::atomic<int> keys_num_{0};
  uint64_t moved_keys_num_ = 0;  // during one batch moved
  uint64_t moved_keys_all_ = 0;  // all keys moved in the slot
  uint64_t remained_keys_num_ = 0;
  bool error_ = false;
  std::vector<std::string> migrating_ones_;
  std::atomic<bool> is_migrating_;

  Status ElectMigrateKeys(std::vector<std::string> *keys);
  void loop();
};

}  // namespace Redis




