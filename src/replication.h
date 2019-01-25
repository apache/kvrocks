#pragma once

#include <event2/bufferevent.h>
#include <thread>
#include <vector>
#include <utility>
#include <tuple>
#include <string>
#include <deque>

#include "status.h"
#include "storage.h"

enum ReplState {
  kReplConnecting = 1,
  kReplSendAuth,
  kReplCheckDBName,
  kReplSendPSync,
  kReplFetchMeta,
  kReplFetchSST,
  kReplConnected,
  kReplError,
};

class ReplicationThread {
 public:
  explicit ReplicationThread(std::string host, uint32_t port,
                             Engine::Storage *storage, std::string auth = "");
  void Start(std::function<void()> &&pre_fullsync_cb,
             std::function<void()> &&post_fullsync_cb);
  void Stop();
  ReplState State() { return repl_state_; }
  rocksdb::SequenceNumber Offset() { return seq_; }
  time_t LastIOTime() { return last_io_time_; }

 private:
  std::thread t_;
  event_base *base_ = nullptr;
  bool stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  std::string auth_;
  Engine::Storage *storage_ = nullptr;
  rocksdb::SequenceNumber seq_ = 0;
  ReplState repl_state_;
  time_t last_io_time_ = 0;

  std::function<void()> pre_fullsync_cb_;
  std::function<void()> post_fullsync_cb_;

  // Internal states managed by FullSync procedure
  enum FullSyncState {
    kFetchMetaID,
    kFetchMetaSize,
    kFetchMetaContent,
  } fullsync_state_ = kFetchMetaID;
  rocksdb::BackupID fullsync_meta_id_ = 0;
  size_t fullsync_filesize_ = 0;

  // Internal states managed by IncrementBatchLoop procedure
  enum IncrementBatchLoopState {
    Incr_batch_size,
    Incr_batch_data,
  } incr_state_ = Incr_batch_size;
  size_t incr_bulk_len_ = 0;

  // The state machine to manage the asynchronous steps used in replication
  class CallbacksStateMachine {
   public:
    enum class State {
      NEXT,
      AGAIN,
      QUIT,
      RESTART,
    };
    enum EventType {
      READ,
      WRITE,
    };
    using CallbackType = std::tuple<EventType, std::string, std::function<State(bufferevent *, void *)>>;
    using CallbackList = std::deque<CallbackType>;
    CallbacksStateMachine(ReplicationThread *repl, CallbackList &&handlers);

    void Start();
    void Stop();
    static void EvCallback(bufferevent *bev, void *ctx);
    static void ConnEventCB(bufferevent *bev, int16_t events,
                            void *state_machine_ptr);
    static void SetReadCB(bufferevent *bev, bufferevent_data_cb cb,
                          void *state_machine_ptr);
    static void SetWriteCB(bufferevent *bev, bufferevent_data_cb cb,
                           void *state_machine_ptr);

   private:
    bufferevent *bev_ = nullptr;
    ReplicationThread *repl_;
    CallbackList handlers_;
    CallbackList::size_type handler_idx_ = 0;

    EventType getHandlerEventType(CallbackList::size_type idx) {
      return std::get<0>(handlers_[idx]);
    }
    std::string getHandlerName(CallbackList::size_type idx) {
      return std::get<1>(handlers_[idx]);
    }
    std::function<State(bufferevent *, void *)> getHandlerFunc(CallbackList::size_type idx) {
      return std::get<2>(handlers_[idx]);
    }
  };

  using CBState = CallbacksStateMachine::State;
  CallbacksStateMachine psync_steps_;
  CallbacksStateMachine fullsync_steps_;

  void run();

  static CBState authWriteCB(bufferevent *bev, void *ctx);
  static CBState authReadCB(bufferevent *bev, void *ctx);
  static CBState checkDBNameWriteCB(bufferevent *bev, void *ctx);
  static CBState checkDBNameReadCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncReadCB(bufferevent *bev, void *ctx);
  static CBState incrementBatchLoopCB(bufferevent *bev, void *ctx);
  static CBState fullSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState fullSyncReadCB(bufferevent *bev, void *ctx);

  // Synchronized-Blocking ops
  Status sendAuth(int sock_fd);
  Status fetchFile(int sock_fd, std::string path, uint32_t crc);
  Status parallelFetchFile(const std::vector<std::pair<std::string, uint32_t>> &files);

  static void EventTimerCB(int, int16_t, void *ctx);
};
