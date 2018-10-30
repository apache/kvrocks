#ifndef KVROCKS_T_MUTEX_H
#define KVROCKS_T_MUTEX_H

#include <mutex>
#include <chrono>
#include <vector>
#include <condition_variable>

class RWLock {
 public:
  RWLock();
  ~RWLock();
  RWLock(const RWLock&) = delete;
  RWLock &operator=(const RWLock&)= delete;
  void Lock();
  void UnLock();
  void RLock();
  void RUnLock();

 private:
  typedef std::mutex mutex_t;
  typedef std::condition_variable cond_t;

  mutex_t mu_;
  cond_t gate1_;
  cond_t gate2_;
  unsigned state_;

  static const unsigned write_entered_ = 1U << (sizeof(unsigned)*CHAR_BIT-1);
  static const unsigned readers_mask_ = ~write_entered_;
};

class RWLocks {
 public:
  explicit RWLocks(int hash_power);
  ~RWLocks();

  unsigned Size();
  void Lock(std::string key);
  void UnLock(std::string key);
  void RLock(std::string key);
  void RUnLock(std::string key);
  RWLocks(const RWLock&) = delete;
  RWLocks &operator=(const RWLock&)= delete;
 private:
  int hash_mask_;
  int hash_power_;
  std::vector<RWLock*> locks_;
  unsigned hash(std::string &key);
};
#endif //KVROCKS_T_MUTEX_H
