/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include <event2/bufferevent.h>

#include <atomic>
#include <deque>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "event_util.h"
#include "server/redis_connection.h"
#include "status.h"
#include "storage/storage.h"

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

enum WriteBatchType {
  kBatchTypeNone = 0,
  kBatchTypePublish,
  kBatchTypePropagate,
  kBatchTypeStream,
};

using FetchFileCallback = std::function<void(const std::string, const uint32_t)>;

class FeedSlaveThread {
 public:
  explicit FeedSlaveThread(Server *srv, redis::Connection *conn, rocksdb::SequenceNumber next_repl_seq)
      : srv_(srv), conn_(conn), next_repl_seq_(next_repl_seq) {}
  ~FeedSlaveThread() = default;

  Status Start();
  void Stop();
  void Join();
  bool IsStopped() { return stop_; }
  redis::Connection *GetConn() { return conn_.get(); }
  rocksdb::SequenceNumber GetCurrentReplSeq() {
    auto seq = next_repl_seq_.load();
    return seq == 0 ? 0 : seq - 1;
  }

 private:
  uint64_t interval_ = 0;
  std::atomic<bool> stop_ = false;
  Server *srv_ = nullptr;
  std::unique_ptr<redis::Connection> conn_ = nullptr;
  std::atomic<rocksdb::SequenceNumber> next_repl_seq_ = 0;
  std::thread t_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iter_ = nullptr;

  static const size_t kMaxDelayUpdates = 16;
  static const size_t kMaxDelayBytes = 16 * 1024;

  void loop();
  void checkLivenessIfNeed();
};

class ReplicationThread : private EventCallbackBase<ReplicationThread> {
 public:
  explicit ReplicationThread(std::string host, uint32_t port, Server *srv);
  Status Start(std::function<void()> &&pre_fullsync_cb, std::function<void()> &&post_fullsync_cb);
  void Stop();
  ReplState State() { return repl_state_.load(std::memory_order_relaxed); }
  time_t LastIOTime() { return last_io_time_.load(std::memory_order_relaxed); }

  void TimerCB(int, int16_t);

 protected:
  event_base *base_ = nullptr;

  // The state machine to manage the asynchronous steps used in replication
  class CallbacksStateMachine {
   public:
    enum class State {
      NEXT,
      PREV,
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
    CallbacksStateMachine(ReplicationThread *repl, CallbackList &&handlers)
        : repl_(repl), handlers_(std::move(handlers)) {}

    void Start();
    void Stop();
    static void EvCallback(bufferevent *bev, void *ctx);
    static void ConnEventCB(bufferevent *bev, int16_t events, void *state_machine_ptr);
    static void SetReadCB(bufferevent *bev, bufferevent_data_cb cb, void *state_machine_ptr);
    static void SetWriteCB(bufferevent *bev, bufferevent_data_cb cb, void *state_machine_ptr);

   private:
    bufferevent *bev_ = nullptr;
    ReplicationThread *repl_;
    CallbackList handlers_;
    CallbackList::size_type handler_idx_ = 0;

    EventType getHandlerEventType(CallbackList::size_type idx) { return std::get<0>(handlers_[idx]); }
    std::string getHandlerName(CallbackList::size_type idx) { return std::get<1>(handlers_[idx]); }
    std::function<State(bufferevent *, void *)> getHandlerFunc(CallbackList::size_type idx) {
      return std::get<2>(handlers_[idx]);
    }
  };

 private:
  std::thread t_;
  std::atomic<bool> stop_flag_ = false;
  std::string host_;
  uint32_t port_;
  Server *srv_ = nullptr;
  engine::Storage *storage_ = nullptr;
  std::atomic<ReplState> repl_state_;
  std::atomic<time_t> last_io_time_ = 0;
  bool next_try_old_psync_ = false;
  bool next_try_without_announce_ip_address_ = false;

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
  Status fetchFile(int sock_fd, evbuffer *evbuf, const std::string &dir, const std::string &file, uint32_t crc,
                   const FetchFileCallback &fn);
  Status fetchFiles(int sock_fd, const std::string &dir, const std::vector<std::string> &files,
                    const std::vector<uint32_t> &crcs, const FetchFileCallback &fn);
  Status parallelFetchFile(const std::string &dir, const std::vector<std::pair<std::string, uint32_t>> &files);
  static bool isRestoringError(const char *err);
  static bool isWrongPsyncNum(const char *err);
  static bool isUnknownOption(const char *err);

  Status parseWriteBatch(const std::string &batch_string);
};

/*
 * An extractor to extract update from raw writebatch
 */
class WriteBatchHandler : public rocksdb::WriteBatch::Handler {
 public:
  rocksdb::Status PutCF(uint32_t column_family_id, const rocksdb::Slice &key, const rocksdb::Slice &value) override;
  rocksdb::Status DeleteCF(uint32_t column_family_id, const rocksdb::Slice &key) override {
    return rocksdb::Status::OK();
  }
  rocksdb::Status DeleteRangeCF(uint32_t column_family_id, const rocksdb::Slice &begin_key,
                                const rocksdb::Slice &end_key) override {
    return rocksdb::Status::OK();
  }
  WriteBatchType Type() { return type_; }
  std::string Key() const { return kv_.first; }
  std::string Value() const { return kv_.second; }

 private:
  std::pair<std::string, std::string> kv_;
  WriteBatchType type_ = kBatchTypeNone;
};
