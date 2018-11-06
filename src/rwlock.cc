#include "rwlock.h"

#include <thread>

RWLock::RWLock(): state_(0) {
}

RWLock::~RWLock() {
  std::lock_guard<mutex_t> _(mu_);
}

void RWLock::Lock() {
  std::unique_lock<mutex_t> lock(mu_);
  gate1_.wait(lock, [this]{ return !(state_ & write_entered_); });
  state_ |= write_entered_;
  gate2_.wait(lock, [this]{ return !(state_ & readers_mask_); });
}

void RWLock::UnLock() {
  std::unique_lock<mutex_t> _(mu_);
  state_ = 0;
  gate1_.notify_all();
}

void RWLock::RLock() {
  std::unique_lock<mutex_t> lock(mu_);
  gate1_.wait(lock, [this]{return !((state_ & write_entered_) || (state_ & readers_mask_) == readers_mask_);});
  unsigned n_readers = (state_ & readers_mask_)+1;
  state_ &= ~readers_mask_;
  state_ |= n_readers;
}

void RWLock::RUnLock() {
  std::unique_lock<mutex_t> lock(mu_);
  unsigned n_readers = (state_&readers_mask_)-1;
  state_ &= ~readers_mask_;
  state_ |= n_readers;
  if (state_ & write_entered_) {
    if (n_readers == 0) { // wakeup the writer when there's no reader
      gate2_.notify_one();
    }
  } else {
    if (n_readers == readers_mask_-1) { // too many readers? wakeup one
      gate1_.notify_one();
    }
  }
}

RWLocks::RWLocks(int hash_power): hash_power_(hash_power){
  hash_mask_ = (1U<<hash_power) - 1;
  unsigned size = Size();
  for (int i = 0; i < size; i++) {
    locks_.emplace_back(new RWLock());
  }
}

RWLocks::~RWLocks() {
  for(auto lock : locks_) {
    delete lock;
  }
}

unsigned RWLocks::Size() {
  return (1U << hash_power_);
}
unsigned RWLocks::hash(std::string &key) {
  return static_cast<unsigned>(std::hash<std::string>{}(key) & hash_mask_);
}

void RWLocks::Lock(std::string key) {
  unsigned slot = hash(key);
  locks_[slot]->Lock();
}

void RWLocks::UnLock(std::string key) {
  unsigned slot = hash(key);
  locks_[slot]->UnLock();
}

void RWLocks::RLock(std::string key) {
  unsigned slot = hash(key);
  locks_[slot]->RLock();
}

void RWLocks::RUnLock(std::string key) {
  unsigned slot = hash(key);
  locks_[slot]->RUnLock();
}