#pragma once

#include <thread>
#include <vector>
#include <utility>
#include <memory>
#include <tuple>
#include <string>
#include <deque>
#include <event2/bufferevent.h>

#include "status.h"
#include "storage.h"
#include "redis_connection.h"

class Server;

enum ReplState {
  kReplConnecting = 1,
  kReplSendAuth,
  kReplCheckDBName,
  kReplReplConf,
  kReplSendPSync,
  kReplFetchMeta,
  kReplFetchSST,
  kReplConnected,
  kReplError,
};


class FeedSlaveThread {
 public:
  explicit FeedSlaveThread(Server *srv, Redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq)
      : srv_(srv), conn_(conn), next_repl_seq_(next_repl_seq) {}
  ~FeedSlaveThread();

  Status Start();
  void Stop();
  void Join();
  bool IsStopped() { return stop_; }
  Redis::Connection *GetConn() { return conn_; }
  rocksdb::SequenceNumber GetCurrentReplSeq() { return next_repl_seq_ == 0 ? 0 : next_repl_seq_-1; }

 private:
  uint64_t interval = 0;
  bool stop_ = false;
  Server *srv_ = nullptr;
  Redis::Connection *conn_ = nullptr;
  rocksdb::SequenceNumber next_repl_seq_ = 0;
  std::thread t_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iter_ = nullptr;

  void loop();
  void checkLivenessIfNeed();
};

class ReplicationThread {
 public:
  explicit ReplicationThread(std::string host, uint32_t port,
                             Server *srv, std::string auth = "");
  Status Start(std::function<void()> &&pre_fullsync_cb,
               std::function<void()> &&post_fullsync_cb);
  void Stop();
  ReplState State() { return repl_state_; }
  time_t LastIOTime() { return last_io_time_; }

 protected:
  event_base *base_ = nullptr;

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

 private:
  std::thread t_;
  bool stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  std::string auth_;
  Server *srv_ = nullptr;
  Engine::Storage *storage_ = nullptr;
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

  using CBState = CallbacksStateMachine::State;
  CallbacksStateMachine psync_steps_;
  CallbacksStateMachine fullsync_steps_;

  void run();

  static CBState authWriteCB(bufferevent *bev, void *ctx);
  static CBState authReadCB(bufferevent *bev, void *ctx);
  static CBState checkDBNameWriteCB(bufferevent *bev, void *ctx);
  static CBState checkDBNameReadCB(bufferevent *bev, void *ctx);
  static CBState replConfWriteCB(bufferevent *bev, void *ctx);
  static CBState replConfReadCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncReadCB(bufferevent *bev, void *ctx);
  static CBState incrementBatchLoopCB(bufferevent *bev, void *ctx);
  static CBState fullSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState fullSyncReadCB(bufferevent *bev, void *ctx);

  // Synchronized-Blocking ops
  Status sendAuth(int sock_fd);
  Status fetchFile(int sock_fd, std::string path, uint32_t crc);
  Status parallelFetchFile(const std::vector<std::pair<std::string, uint32_t>> &files);
  static bool isRestoringError(const char *err);

  static void EventTimerCB(int, int16_t, void *ctx);

  rocksdb::Status ParseWriteBatch(const std::string &batch_string);
};

/*
 * An extractor to extract update from raw writebatch
 */
class WriteBatchHandler : public rocksdb::WriteBatch::Handler {
 public:
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice &key,
                        const rocksdb::Slice &value) override;

  rocksdb::Slice GetPublishChannel() { return publish_message_.first; }
  rocksdb::Slice GetPublishValue() { return publish_message_.second; }
  bool IsPublish() { return is_publish_; }
 private:
  std::pair<std::string, std::string> publish_message_;
  bool is_publish_ = false;
};
