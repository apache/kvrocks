#include "task_runner.h"

#include <thread>
#include "util.h"

Status TaskRunner::Publish(Task task) {
  mu_.lock();
  if (stop_) {
    mu_.unlock();
    return Status(Status::NotOK, "the runner was stopped");
  }
  if (task_queue_.size() >= max_queue_size_) {
    mu_.unlock();
    return Status(Status::NotOK, "the task queue was reached max length");
  }
  task_queue_.emplace_back(task);
  cond_.notify_all();
  mu_.unlock();
  return Status::OK();
}

void TaskRunner::Start() {
  stop_ = false;
  for (int i = 0; i < n_thread_; i++) {
    threads_.emplace_back(std::thread([this]() {
      Util::ThreadSetName("task-runner");
      this->run();
    }));
  }
}

void TaskRunner::Stop() {
  mu_.lock();
  stop_ = true;
  cond_.notify_all();
  mu_.unlock();
}

void TaskRunner::Join() {
  for (size_t i = 0; i < threads_.size(); i++) {
    if (threads_[i].joinable()) threads_[i].join();
  }
}

void TaskRunner::Purge() {
  mu_.lock();
  threads_.clear();
  task_queue_.clear();
  mu_.unlock();
}

void TaskRunner::run() {
  Task task;
  std::unique_lock<std::mutex> lock(mu_);
  while (!stop_) {
    cond_.wait(lock, [this]() -> bool { return stop_ || !task_queue_.empty();});
    while (!stop_ && !task_queue_.empty()) {
      task = task_queue_.front();
      task_queue_.pop_front();
      lock.unlock();
      if (task.callback) task.callback(task.arg);
      lock.lock();
    }
  }
  task_queue_.clear();
  lock.unlock();
  // CAUTION: drop the rest of tasks, don't use task runner if the task can't be drop
}
