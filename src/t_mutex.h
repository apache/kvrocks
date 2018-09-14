#ifndef KVROCKS_T_MUTEX_H
#define KVROCKS_T_MUTEX_H

#include <mutex>
#include <stdint.h>

class RWLock {
public:
  RWLock() : status_(0), waiting_readers_(0), waiting_writers_(0) {}
  RWLock(const RWLock&) = delete;
  RWLock(RWLock&&) = delete;
  RWLock& operator = (const RWLock&) = delete;
  RWLock& operator = (RWLock&&) = delete;

  void RLock() {
    std::unique_lock<std::mutex> lock(mtx_);
    waiting_readers_ += 1;
    read_cv_.wait(lock, [&]() { return waiting_writers_ == 0 && status_ >= 0; });
    waiting_readers_ -= 1;
    status_ += 1;
  }

  void WLock() {
    std::unique_lock<std::mutex> lock(mtx_);
    waiting_writers_ += 1;
    write_cv_.wait(lock, [&]() { return status_ == 0; });
    waiting_writers_ -= 1;
    status_ = -1;
  }

  void UnLock() {
    std::unique_lock<std::mutex> lock(mtx_);
    if (status_ == -1) {
      status_ = 0;
    } else {
      status_ -= 1;
    }
    if (waiting_writers_ > 0) {
      if (status_ == 0) {
        write_cv_.notify_one();
      }
    } else {
      read_cv_.notify_all();
    }
  }

private:
  // -1    : one writer
  // 0     : no reader and no writer
  // n > 0 : n reader
  int32_t status_;
  int32_t waiting_readers_;
  int32_t waiting_writers_;
  std::mutex mtx_;
  std::condition_variable read_cv_;
  std::condition_variable write_cv_;
};

#endif //KVROCKS_T_MUTEX_H
