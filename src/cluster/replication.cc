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

#include "replication.h"

#include <arpa/inet.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>
#include <event2/event.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <csignal>
#include <future>
#include <string>
#include <thread>

#include "event_util.h"
#include "fd_util.h"
#include "fmt/format.h"
#include "io_util.h"
#include "rocksdb_crc32c.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "status.h"
#include "storage/batch_debugger.h"
#include "thread_util.h"
#include "time_util.h"

Status FeedSlaveThread::Start() {
  auto s = Util::CreateThread("feed-replica", [this] {
    sigset_t mask, omask;
    sigemptyset(&mask);
    sigemptyset(&omask);
    sigaddset(&mask, SIGCHLD);
    sigaddset(&mask, SIGHUP);
    sigaddset(&mask, SIGPIPE);
    pthread_sigmask(SIG_BLOCK, &mask, &omask);
    auto s = Util::SockSend(conn_->GetFD(), "+OK\r\n");
    if (!s.IsOK()) {
      LOG(ERROR) << "failed to send OK response to the replica: " << s.Msg();
      return;
    }
    this->loop();
  });

  if (s) {
    t_ = std::move(*s);
  } else {
    conn_ = nullptr;  // prevent connection was freed when failed to start the thread
  }

  return s;
}

void FeedSlaveThread::Stop() {
  stop_ = true;
  LOG(WARNING) << "Slave thread was terminated, would stop feeding the slave: " << conn_->GetAddr();
}

void FeedSlaveThread::Join() {
  if (auto s = Util::ThreadJoin(t_); !s) {
    LOG(WARNING) << "Slave thread operation failed: " << s.Msg();
  }
}

void FeedSlaveThread::checkLivenessIfNeed() {
  if (++interval % 1000) return;
  const auto ping_command = Redis::BulkString("ping");
  auto s = Util::SockSend(conn_->GetFD(), ping_command);
  if (!s.IsOK()) {
    LOG(ERROR) << "Ping slave[" << conn_->GetAddr() << "] err: " << s.Msg() << ", would stop the thread";
    Stop();
  }
}

void FeedSlaveThread::loop() {
  // is_first_repl_batch was used to fix that replication may be stuck in a dead loop
  // when some seqs might be lost in the middle of the WAL log, so forced to replicate
  // first batch here to work around this issue instead of waiting for enough batch size.
  bool is_first_repl_batch = true;
  uint32_t yield_microseconds = 2 * 1000;
  std::string batches_bulk;
  size_t updates_in_batches = 0;
  while (!IsStopped()) {
    auto curr_seq = next_repl_seq_.load();

    if (!iter_ || !iter_->Valid()) {
      if (iter_) LOG(INFO) << "WAL was rotated, would reopen again";
      if (!srv_->storage_->WALHasNewData(curr_seq) || !srv_->storage_->GetWALIter(curr_seq, &iter_).IsOK()) {
        iter_ = nullptr;
        usleep(yield_microseconds);
        checkLivenessIfNeed();
        continue;
      }
    }
    // iter_ would be always valid here
    auto batch = iter_->GetBatch();
    if (batch.sequence != curr_seq) {
      LOG(ERROR) << "Fatal error encountered, WAL iterator is discrete, some seq might be lost"
                 << ", sequence " << curr_seq << " expectd, but got " << batch.sequence;
      Stop();
      return;
    }
    updates_in_batches += batch.writeBatchPtr->Count();
    batches_bulk += Redis::BulkString(batch.writeBatchPtr->Data());
    // 1. We must send the first replication batch, as said above.
    // 2. To avoid frequently calling 'write' system call to send replication stream,
    //    we pack multiple batches into one big bulk if possible, and only send once.
    //    But we should send the bulk of batches if its size exceed kMaxDelayBytes,
    //    16Kb by default. Moreover, we also send if updates count in all bathes is
    //    more that kMaxDelayUpdates, to void too many delayed updates.
    // 3. To avoid master don't send replication stream to slave since of packing
    //    batches strategy, we still send batches if current batch sequence is less
    //    kMaxDelayUpdates than latest sequence.
    if (is_first_repl_batch || batches_bulk.size() >= kMaxDelayBytes || updates_in_batches >= kMaxDelayUpdates ||
        srv_->storage_->LatestSeqNumber() - batch.sequence <= kMaxDelayUpdates) {
      // Send entire bulk which contain multiple batches
      auto s = Util::SockSend(conn_->GetFD(), batches_bulk);
      if (!s.IsOK()) {
        LOG(ERROR) << "Write error while sending batch to slave: " << s.Msg() << ". batches: 0x"
                   << Util::StringToHex(batches_bulk);
        Stop();
        return;
      }
      is_first_repl_batch = false;
      batches_bulk.clear();
      if (batches_bulk.capacity() > kMaxDelayBytes * 2) batches_bulk.shrink_to_fit();
      updates_in_batches = 0;
    }
    curr_seq = batch.sequence + batch.writeBatchPtr->Count();
    next_repl_seq_.store(curr_seq);
    while (!IsStopped() && !srv_->storage_->WALHasNewData(curr_seq)) {
      usleep(yield_microseconds);
      checkLivenessIfNeed();
    }
    iter_->Next();
  }
}

void send_string(bufferevent *bev, const std::string &data) {
  auto output = bufferevent_get_output(bev);
  evbuffer_add(output, data.c_str(), data.length());
}

void ReplicationThread::CallbacksStateMachine::ConnEventCB(bufferevent *bev, int16_t events, void *state_machine_ptr) {
  if (events & BEV_EVENT_CONNECTED) {
    // call write_cb when connected
    bufferevent_data_cb write_cb = nullptr;
    bufferevent_getcb(bev, nullptr, &write_cb, nullptr, nullptr);
    if (write_cb) write_cb(bev, state_machine_ptr);
    return;
  }
  if (events & (BEV_EVENT_ERROR | BEV_EVENT_EOF)) {
    LOG(ERROR) << "[replication] connection error/eof, reconnect the master";
    // Wait a bit and reconnect
    auto state_m = static_cast<CallbacksStateMachine *>(state_machine_ptr);
    state_m->repl_->repl_state_.store(kReplConnecting, std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    state_m->Stop();
    state_m->Start();
  }
}

void ReplicationThread::CallbacksStateMachine::SetReadCB(bufferevent *bev, bufferevent_data_cb cb,
                                                         void *state_machine_ptr) {
  bufferevent_enable(bev, EV_READ);
  bufferevent_setcb(bev, cb, nullptr, ConnEventCB, state_machine_ptr);
}

void ReplicationThread::CallbacksStateMachine::SetWriteCB(bufferevent *bev, bufferevent_data_cb cb,
                                                          void *state_machine_ptr) {
  bufferevent_enable(bev, EV_WRITE);
  bufferevent_setcb(bev, nullptr, cb, ConnEventCB, state_machine_ptr);
}

void ReplicationThread::CallbacksStateMachine::EvCallback(bufferevent *bev, void *ctx) {
  auto self = static_cast<CallbacksStateMachine *>(ctx);
LOOP_LABEL:
  assert(self->handler_idx_ <= self->handlers_.size());
  DLOG(INFO) << "[replication] Execute handler[" << self->getHandlerName(self->handler_idx_) << "]";
  auto st = self->getHandlerFunc(self->handler_idx_)(bev, self->repl_);
  self->repl_->last_io_time_.store(Util::GetTimeStamp(), std::memory_order_relaxed);
  switch (st) {
    case CBState::NEXT:
      ++self->handler_idx_;
      [[fallthrough]];
    case CBState::PREV:
      if (st == CBState::PREV) --self->handler_idx_;
      if (self->getHandlerEventType(self->handler_idx_) == WRITE) {
        SetWriteCB(bev, EvCallback, ctx);
      } else {
        SetReadCB(bev, EvCallback, ctx);
      }
      // invoke the read handler (of next step) directly, as the bev might
      // have the data already.
      goto LOOP_LABEL;
    case CBState::AGAIN:
      break;
    case CBState::QUIT:  // state that can not be retry, or all steps are executed.
      bufferevent_free(bev);
      self->bev_ = nullptr;
      self->repl_->repl_state_.store(kReplError, std::memory_order_relaxed);
      break;
    case CBState::RESTART:  // state that can be retried some time later
      self->Stop();
      if (self->repl_->stop_flag_) {
        LOG(INFO) << "[replication] Wouldn't restart while the replication thread was stopped";
        break;
      }
      self->repl_->repl_state_.store(kReplConnecting, std::memory_order_relaxed);
      LOG(INFO) << "[replication] Retry in 10 seconds";
      std::this_thread::sleep_for(std::chrono::seconds(10));
      self->Start();
  }
}

void ReplicationThread::CallbacksStateMachine::Start() {
  struct bufferevent *bev = nullptr;

  if (handlers_.empty()) {
    return;
  }

  // Note: It may cause data races to use 'masterauth' directly.
  // It is acceptable because password change is a low frequency operation.
  if (!repl_->srv_->GetConfig()->masterauth.empty()) {
    handlers_.emplace_front(CallbacksStateMachine::READ, "auth read", authReadCB);
    handlers_.emplace_front(CallbacksStateMachine::WRITE, "auth write", authWriteCB);
  }

  uint64_t last_connect_timestamp = 0;
  int connect_timeout_ms = 3100;

  while (!repl_->stop_flag_ && bev == nullptr) {
    if (Util::GetTimeStampMS() - last_connect_timestamp < 1000) {
      // prevent frequent re-connect when the master is down with the connection refused error
      sleep(1);
    }
    last_connect_timestamp = Util::GetTimeStampMS();
    auto cfd = Util::SockConnect(repl_->host_, repl_->port_, connect_timeout_ms);
    if (!cfd) {
      LOG(ERROR) << "[replication] Failed to connect the master, err: " << cfd.Msg();
      continue;
    }
    bev = bufferevent_socket_new(repl_->base_, *cfd, BEV_OPT_CLOSE_ON_FREE);
    if (bev == nullptr) {
      close(*cfd);
      LOG(ERROR) << "[replication] Failed to create the event socket";
      continue;
    }
  }
  if (bev == nullptr) {  // failed to connect the master and received the stop signal
    return;
  }

  handler_idx_ = 0;
  repl_->incr_state_ = Incr_batch_size;
  if (getHandlerEventType(0) == WRITE) {
    SetWriteCB(bev, EvCallback, this);
  } else {
    SetReadCB(bev, EvCallback, this);
  }
  bev_ = bev;
}

void ReplicationThread::CallbacksStateMachine::Stop() {
  if (bev_) {
    bufferevent_free(bev_);
    bev_ = nullptr;
  }
}

ReplicationThread::ReplicationThread(std::string host, uint32_t port, Server *srv)
    : host_(std::move(host)),
      port_(port),
      srv_(srv),
      storage_(srv->storage_),
      repl_state_(kReplConnecting),
      psync_steps_(
          this,
          CallbacksStateMachine::CallbackList{
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::WRITE, "dbname write", checkDBNameWriteCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::READ, "dbname read", checkDBNameReadCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::WRITE, "replconf write", replConfWriteCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::READ, "replconf read", replConfReadCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::WRITE, "psync write", tryPSyncWriteCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::READ, "psync read", tryPSyncReadCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::READ, "batch loop", incrementBatchLoopCB}}),
      fullsync_steps_(
          this,
          CallbacksStateMachine::CallbackList{
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::WRITE, "fullsync write", fullSyncWriteCB},
              CallbacksStateMachine::CallbackType{CallbacksStateMachine::READ, "fullsync read", fullSyncReadCB}}) {}

Status ReplicationThread::Start(std::function<void()> &&pre_fullsync_cb, std::function<void()> &&post_fullsync_cb) {
  pre_fullsync_cb_ = std::move(pre_fullsync_cb);
  post_fullsync_cb_ = std::move(post_fullsync_cb);

  // Clean synced checkpoint from old master because replica starts to follow new master
  auto s = rocksdb::DestroyDB(srv_->GetConfig()->sync_checkpoint_dir, rocksdb::Options());
  if (!s.ok()) {
    LOG(WARNING) << "Can't clean synced checkpoint from master, error: " << s.ToString();
  } else {
    LOG(WARNING) << "Clean old synced checkpoint successfully";
  }

  // cleanup the old backups, so we can start replication in a clean state
  storage_->PurgeOldBackups(0, 0);

  t_ = GET_OR_RET(Util::CreateThread("master-repl", [this] {
    this->run();
    assert(stop_flag_);
  }));

  return Status::OK();
}

void ReplicationThread::Stop() {
  if (stop_flag_) return;

  stop_flag_ = true;  // Stopping procedure is asynchronous,
                      // handled by timer
  if (auto s = Util::ThreadJoin(t_); !s) {
    LOG(WARNING) << "Replication thread operation failed: " << s.Msg();
  }
  LOG(INFO) << "[replication] Stopped";
}

/*
 * Run connect to master, and start the following steps
 * asynchronously
 *  - CheckDBName
 *  - TryPsync
 *  - - if ok, IncrementBatchLoop
 *  - - not, FullSync and restart TryPsync when done
 */
void ReplicationThread::run() {
  base_ = event_base_new();
  if (base_ == nullptr) {
    LOG(ERROR) << "[replication] Failed to create new ev base";
    return;
  }
  psync_steps_.Start();

  auto timer = event_new(base_, -1, EV_PERSIST, EventTimerCB, this);
  timeval tmo{0, 100000};  // 100 ms
  evtimer_add(timer, &tmo);

  event_base_dispatch(base_);
  event_free(timer);
  event_base_free(base_);
}

ReplicationThread::CBState ReplicationThread::authWriteCB(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  send_string(bev, Redis::MultiBulkString({"AUTH", self->srv_->GetConfig()->masterauth}));
  LOG(INFO) << "[replication] Auth request was sent, waiting for response";
  self->repl_state_.store(kReplSendAuth, std::memory_order_relaxed);
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::authReadCB(bufferevent *bev, void *ctx) {
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;
  if (strncmp(line.get(), "+OK", 3) != 0) {
    // Auth failed
    LOG(ERROR) << "[replication] Auth failed: " << line.get();
    return CBState::RESTART;
  }
  LOG(INFO) << "[replication] Auth response was received, continue...";
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::checkDBNameWriteCB(bufferevent *bev, void *ctx) {
  send_string(bev, Redis::MultiBulkString({"_db_name"}));
  auto self = static_cast<ReplicationThread *>(ctx);
  self->repl_state_.store(kReplCheckDBName, std::memory_order_relaxed);
  LOG(INFO) << "[replication] Check db name request was sent, waiting for response";
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::checkDBNameReadCB(bufferevent *bev, void *ctx) {
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  if (line[0] == '-') {
    if (isRestoringError(line.get())) {
      LOG(WARNING) << "The master was restoring the db, retry later";
    } else {
      LOG(ERROR) << "Failed to get the db name, " << line.get();
    }
    return CBState::RESTART;
  }
  auto self = static_cast<ReplicationThread *>(ctx);
  std::string db_name = self->storage_->GetName();
  if (line.length == db_name.size() && !strncmp(line.get(), db_name.data(), line.length)) {
    // DB name match, we should continue to next step: TryPsync
    LOG(INFO) << "[replication] DB name is valid, continue...";
    return CBState::NEXT;
  }
  LOG(ERROR) << "[replication] Mismatched the db name, local: " << db_name << ", remote: " << line.get();
  return CBState::RESTART;
}

ReplicationThread::CBState ReplicationThread::replConfWriteCB(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  auto config = self->srv_->GetConfig();

  auto port = config->replica_announce_port > 0 ? config->replica_announce_port : config->port;
  std::vector<std::string> data_to_send{"replconf", "listening-port", std::to_string(port)};
  if (!self->next_try_without_announce_ip_address_ && !config->replica_announce_ip.empty()) {
    data_to_send.emplace_back("ip-address");
    data_to_send.emplace_back(config->replica_announce_ip);
  }
  send_string(bev, Redis::MultiBulkString(data_to_send));
  self->repl_state_.store(kReplReplConf, std::memory_order_relaxed);
  LOG(INFO) << "[replication] replconf request was sent, waiting for response";
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::replConfReadCB(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  // on unknown option: first try without announce ip, if it fails again - do nothing (to prevent infinite loop)
  if (isUnknownOption(line.get()) && !self->next_try_without_announce_ip_address_) {
    self->next_try_without_announce_ip_address_ = true;
    LOG(WARNING) << "The old version master, can't handle ip-address, "
                 << "try without it again";
    // Retry previous state, i.e. send replconf again
    return CBState::PREV;
  }
  if (line[0] == '-' && isRestoringError(line.get())) {
    LOG(WARNING) << "The master was restoring the db, retry later";
    return CBState::RESTART;
  }
  if (strncmp(line.get(), "+OK", 3) != 0) {
    LOG(WARNING) << "[replication] Failed to replconf: " << line.get() + 1;
    //  backward compatible with old version that doesn't support replconf cmd
    return CBState::NEXT;
  } else {
    LOG(INFO) << "[replication] replconf is ok, start psync";
    return CBState::NEXT;
  }
}

ReplicationThread::CBState ReplicationThread::tryPSyncWriteCB(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  auto cur_seq = self->storage_->LatestSeqNumber();
  auto next_seq = cur_seq + 1;
  std::string replid;

  // Get replication id
  std::string replid_in_wal = self->storage_->GetReplIdFromWalBySeq(cur_seq);
  // Set if valid replication id
  if (replid_in_wal.length() == kReplIdLength) {
    replid = replid_in_wal;
  } else {
    // Maybe there is no WAL, we can get replication id from db since master
    // always write replication id into db before any operation when starting
    // new "replication history".
    std::string replid_in_db = self->storage_->GetReplIdFromDbEngine();
    if (replid_in_db.length() == kReplIdLength) {
      replid = replid_in_db;
    }
  }

  // To adapt to old master using old PSYNC, i.e. only use next sequence id.
  // Also use old PSYNC if replica can't find replication id from WAL and DB.
  if (!self->srv_->GetConfig()->use_rsid_psync || self->next_try_old_psync_ || replid.length() != kReplIdLength) {
    self->next_try_old_psync_ = false;  // Reset next_try_old_psync_
    send_string(bev, Redis::MultiBulkString({"PSYNC", std::to_string(next_seq)}));
    LOG(INFO) << "[replication] Try to use psync, next seq: " << next_seq;
  } else {
    // NEW PSYNC "Unique Replication Sequence ID": replication id and sequence id
    send_string(bev, Redis::MultiBulkString({"PSYNC", replid, std::to_string(next_seq)}));
    LOG(INFO) << "[replication] Try to use new psync, current unique replication sequence id: " << replid << ":"
              << cur_seq;
  }
  self->repl_state_.store(kReplSendPSync, std::memory_order_relaxed);
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::tryPSyncReadCB(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  auto input = bufferevent_get_input(bev);
  UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
  if (!line) return CBState::AGAIN;

  if (line[0] == '-' && isRestoringError(line.get())) {
    LOG(WARNING) << "The master was restoring the db, retry later";
    return CBState::RESTART;
  }

  if (line[0] == '-' && isWrongPsyncNum(line.get())) {
    self->next_try_old_psync_ = true;
    LOG(WARNING) << "The old version master, can't handle new PSYNC, "
                 << "try old PSYNC again";
    // Retry previous state, i.e. send PSYNC again
    return CBState::PREV;
  }

  if (strncmp(line.get(), "+OK", 3) != 0) {
    // PSYNC isn't OK, we should use FullSync
    // Switch to fullsync state machine
    self->fullsync_steps_.Start();
    LOG(INFO) << "[replication] Failed to psync, error: " << line.get() << ", switch to fullsync";
    return CBState::QUIT;
  } else {
    // PSYNC is OK, use IncrementBatchLoop
    LOG(INFO) << "[replication] PSync is ok, start increment batch loop";
    return CBState::NEXT;
  }
}

ReplicationThread::CBState ReplicationThread::incrementBatchLoopCB(bufferevent *bev, void *ctx) {
  char *bulk_data = nullptr;
  auto self = static_cast<ReplicationThread *>(ctx);
  self->repl_state_.store(kReplConnected, std::memory_order_relaxed);
  auto input = bufferevent_get_input(bev);
  while (true) {
    switch (self->incr_state_) {
      case Incr_batch_size: {
        // Read bulk length
        UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return CBState::AGAIN;
        self->incr_bulk_len_ = line.length > 0 ? std::strtoull(line.get() + 1, nullptr, 10) : 0;
        if (self->incr_bulk_len_ == 0) {
          LOG(ERROR) << "[replication] Invalid increment data size";
          return CBState::RESTART;
        }
        self->incr_state_ = Incr_batch_data;
        break;
      }
      case Incr_batch_data:
        // Read bulk data (batch data)
        if (self->incr_bulk_len_ + 2 <= evbuffer_get_length(input)) {  // We got enough data
          bulk_data = reinterpret_cast<char *>(evbuffer_pullup(input, static_cast<ssize_t>(self->incr_bulk_len_ + 2)));
          std::string bulk_string = std::string(bulk_data, self->incr_bulk_len_);
          // master would send the ping heartbeat packet to check whether the slave was alive or not,
          // don't write ping to db here.
          if (bulk_string != "ping") {
            auto s = self->storage_->ReplicaApplyWriteBatch(std::string(bulk_data, self->incr_bulk_len_));
            if (!s.IsOK()) {
              LOG(ERROR) << "[replication] CRITICAL - Failed to write batch to local, " << s.Msg() << ". batch: 0x"
                         << Util::StringToHex(bulk_string);
              return CBState::RESTART;
            }

            s = self->ParseWriteBatch(bulk_string);
            if (!s.IsOK()) {
              LOG(ERROR) << "[replication] CRITICAL - failed to parse write batch 0x" << Util::StringToHex(bulk_string)
                         << ": " << s.Msg();
              return CBState::RESTART;
            }
          }
          evbuffer_drain(input, self->incr_bulk_len_ + 2);
          self->incr_state_ = Incr_batch_size;
        } else {
          return CBState::AGAIN;
        }
        break;
    }
  }
}

ReplicationThread::CBState ReplicationThread::fullSyncWriteCB(bufferevent *bev, void *ctx) {
  send_string(bev, Redis::MultiBulkString({"_fetch_meta"}));
  auto self = static_cast<ReplicationThread *>(ctx);
  self->repl_state_.store(kReplFetchMeta, std::memory_order_relaxed);
  LOG(INFO) << "[replication] Start syncing data with fullsync";
  return CBState::NEXT;
}

ReplicationThread::CBState ReplicationThread::fullSyncReadCB(bufferevent *bev, void *ctx) {
  auto self = static_cast<ReplicationThread *>(ctx);
  auto input = bufferevent_get_input(bev);
  switch (self->fullsync_state_) {
    case kFetchMetaID: {
      // New version master only sends meta file content
      if (!self->srv_->GetConfig()->master_use_repl_port) {
        self->fullsync_state_ = kFetchMetaContent;
        return CBState::AGAIN;
      }
      UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return CBState::AGAIN;
      if (line[0] == '-') {
        LOG(ERROR) << "[replication] Failed to fetch meta id: " << line.get();
        return CBState::RESTART;
      }
      self->fullsync_meta_id_ =
          static_cast<rocksdb::BackupID>(line.length > 0 ? std::strtoul(line.get(), nullptr, 10) : 0);
      if (self->fullsync_meta_id_ == 0) {
        LOG(ERROR) << "[replication] Invalid meta id received";
        return CBState::RESTART;
      }
      self->fullsync_state_ = kFetchMetaSize;
      LOG(INFO) << "[replication] Succeed fetching meta id: " << self->fullsync_meta_id_;
    }
    case kFetchMetaSize: {
      UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
      if (!line) return CBState::AGAIN;
      if (line[0] == '-') {
        LOG(ERROR) << "[replication] Failed to fetch meta size: " << line.get();
        return CBState::RESTART;
      }
      self->fullsync_filesize_ = line.length > 0 ? std::strtoull(line.get(), nullptr, 10) : 0;
      if (self->fullsync_filesize_ == 0) {
        LOG(ERROR) << "[replication] Invalid meta file size received";
        return CBState::RESTART;
      }
      self->fullsync_state_ = kFetchMetaContent;
      LOG(INFO) << "[replication] Succeed fetching meta size: " << self->fullsync_filesize_;
    }
    case kFetchMetaContent: {
      std::string target_dir;
      Engine::Storage::ReplDataManager::MetaInfo meta;
      // Master using old version
      if (self->srv_->GetConfig()->master_use_repl_port) {
        if (evbuffer_get_length(input) < self->fullsync_filesize_) {
          return CBState::AGAIN;
        }
        auto s =
            Engine::Storage::ReplDataManager::ParseMetaAndSave(self->storage_, self->fullsync_meta_id_, input, &meta);
        if (!s.IsOK()) {
          LOG(ERROR) << "[replication] Failed to parse meta and save: " << s.Msg();
          return CBState::AGAIN;
        }
        target_dir = self->srv_->GetConfig()->backup_sync_dir;
      } else {
        // Master using new version
        UniqueEvbufReadln line(input, EVBUFFER_EOL_CRLF_STRICT);
        if (!line) return CBState::AGAIN;
        if (line[0] == '-') {
          LOG(ERROR) << "[replication] Failed to fetch meta info: " << line.get();
          return CBState::RESTART;
        }
        std::vector<std::string> need_files = Util::Split(std::string(line.get()), ",");
        for (const auto &f : need_files) {
          meta.files.emplace_back(f, 0);
        }

        target_dir = self->srv_->GetConfig()->sync_checkpoint_dir;
        // Clean invalid files of checkpoint, "CURRENT" file must be invalid
        // because we identify one file by its file number but only "CURRENT"
        // file doesn't have number.
        auto iter = std::find(need_files.begin(), need_files.end(), "CURRENT");
        if (iter != need_files.end()) need_files.erase(iter);
        auto s = Engine::Storage::ReplDataManager::CleanInvalidFiles(self->storage_, target_dir, need_files);
        if (!s.IsOK()) {
          LOG(WARNING) << "[replication] Failed to clean up invalid files of the old checkpoint,"
                       << " error: " << s.Msg();
          LOG(WARNING) << "[replication] Try to clean all checkpoint files";
          auto s = rocksdb::DestroyDB(target_dir, rocksdb::Options());
          if (!s.ok()) {
            LOG(WARNING) << "[replication] Failed to clean all checkpoint files, error: " << s.ToString();
          }
        }
      }
      assert(evbuffer_get_length(input) == 0);
      self->fullsync_state_ = kFetchMetaID;
      LOG(INFO) << "[replication] Succeeded fetching full data files info, fetching files in parallel";

      // If 'slave-empty-db-before-fullsync' is yes, we call 'pre_fullsync_cb_'
      // just like reloading database. And we don't want slave to occupy too much
      // disk space, so we just empty entire database rudely.
      if (self->srv_->GetConfig()->slave_empty_db_before_fullsync) {
        self->pre_fullsync_cb_();
        self->storage_->EmptyDB();
      }

      self->repl_state_.store(kReplFetchSST, std::memory_order_relaxed);
      auto s = self->parallelFetchFile(target_dir, meta.files);
      if (!s.IsOK()) {
        LOG(ERROR) << "[replication] Failed to parallel fetch files while " + s.Msg();
        return CBState::RESTART;
      }
      LOG(INFO) << "[replication] Succeeded fetching files in parallel, restoring the backup";

      // Restore DB from backup
      // We already call 'pre_fullsync_cb_' if 'slave-empty-db-before-fullsync' is yes
      if (!self->srv_->GetConfig()->slave_empty_db_before_fullsync) self->pre_fullsync_cb_();
      // For old version, master uses rocksdb backup to implement data snapshot
      if (self->srv_->GetConfig()->master_use_repl_port) {
        s = self->storage_->RestoreFromBackup();
      } else {
        s = self->storage_->RestoreFromCheckpoint();
      }
      if (!s.IsOK()) {
        LOG(ERROR) << "[replication] Failed to restore backup while " + s.Msg() + ", restart fullsync";
        return CBState::RESTART;
      }
      LOG(INFO) << "[replication] Succeeded restoring the backup, fullsync was finish";
      self->post_fullsync_cb_();

      // Switch to psync state machine again
      self->psync_steps_.Start();
      return CBState::QUIT;
    }
  }

  LOG(ERROR) << "Should not arrive here";
  assert(false);
  return CBState::QUIT;
}

Status ReplicationThread::parallelFetchFile(const std::string &dir,
                                            const std::vector<std::pair<std::string, uint32_t>> &files) {
  size_t concurrency = 1;
  if (files.size() > 20) {
    // Use 4 threads to download files in parallel
    concurrency = 4;
  }
  std::atomic<uint32_t> fetch_cnt = {0};
  std::atomic<uint32_t> skip_cnt = {0};
  std::vector<std::future<Status>> results;
  for (size_t tid = 0; tid < concurrency; ++tid) {
    results.push_back(
        std::async(std::launch::async, [this, dir, &files, tid, concurrency, &fetch_cnt, &skip_cnt]() -> Status {
          if (this->stop_flag_) {
            return {Status::NotOK, "replication thread was stopped"};
          }
          int sock_fd = GET_OR_RET(Util::SockConnect(this->host_, this->port_).Prefixed("connect the server err"));
          UniqueFD unique_fd{sock_fd};
          auto s = this->sendAuth(sock_fd);
          if (!s.IsOK()) {
            return s.Prefixed("send the auth command err");
          }
          std::vector<std::string> fetch_files;
          std::vector<uint32_t> crcs;
          for (auto f_idx = tid; f_idx < files.size(); f_idx += concurrency) {
            if (this->stop_flag_) {
              return {Status::NotOK, "replication thread was stopped"};
            }
            const auto &f_name = files[f_idx].first;
            const auto &f_crc = files[f_idx].second;
            // Don't fetch existing files
            if (Engine::Storage::ReplDataManager::FileExists(this->storage_, dir, f_name, f_crc)) {
              skip_cnt.fetch_add(1);
              uint32_t cur_skip_cnt = skip_cnt.load();
              uint32_t cur_fetch_cnt = fetch_cnt.load();
              LOG(INFO) << "[skip] " << f_name << " " << f_crc << ", skip count: " << cur_skip_cnt
                        << ", fetch count: " << cur_fetch_cnt << ", progress: " << cur_skip_cnt + cur_fetch_cnt << "/"
                        << files.size();
              continue;
            }
            fetch_files.push_back(f_name);
            crcs.push_back(f_crc);
          }
          unsigned files_count = files.size();
          fetch_file_callback fn = [&fetch_cnt, &skip_cnt, files_count](const std::string &fetch_file,
                                                                        const uint32_t fetch_crc) {
            fetch_cnt.fetch_add(1);
            uint32_t cur_skip_cnt = skip_cnt.load();
            uint32_t cur_fetch_cnt = fetch_cnt.load();
            LOG(INFO) << "[fetch] "
                      << "Fetched " << fetch_file << ", crc32: " << fetch_crc << ", skip count: " << cur_skip_cnt
                      << ", fetch count: " << cur_fetch_cnt << ", progress: " << cur_skip_cnt + cur_fetch_cnt << "/"
                      << files_count;
          };
          // For master using old version, it only supports to fetch a single file by one
          // command, so we need to fetch all files by multiple command interactions.
          if (srv_->GetConfig()->master_use_repl_port) {
            for (unsigned i = 0; i < fetch_files.size(); i++) {
              s = this->fetchFiles(sock_fd, dir, {fetch_files[i]}, {crcs[i]}, fn);
              if (!s.IsOK()) break;
            }
          } else {
            if (!fetch_files.empty()) {
              s = this->fetchFiles(sock_fd, dir, fetch_files, crcs, fn);
            }
          }
          return s;
        }));
  }

  // Wait til finish
  for (auto &f : results) {
    Status s = f.get();
    if (!s.IsOK()) return s;
  }
  return Status::OK();
}

Status ReplicationThread::sendAuth(int sock_fd) {
  // Send auth when needed
  std::string auth = srv_->GetConfig()->masterauth;
  if (!auth.empty()) {
    UniqueEvbuf evbuf;
    const auto auth_command = Redis::MultiBulkString({"AUTH", auth});
    auto s = Util::SockSend(sock_fd, auth_command);
    if (!s.IsOK()) return s.Prefixed("send auth command err");
    while (true) {
      if (evbuffer_read(evbuf.get(), sock_fd, -1) <= 0) {
        return Status::FromErrno("read auth response err");
      }
      UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
      if (!line) continue;
      if (strncmp(line.get(), "+OK", 3) != 0) {
        return {Status::NotOK, "auth got invalid response"};
      }
      break;
    }
  }
  return Status::OK();
}

Status ReplicationThread::fetchFile(int sock_fd, evbuffer *evbuf, const std::string &dir, const std::string &file,
                                    uint32_t crc, const fetch_file_callback &fn) {
  size_t file_size = 0;

  // Read file size line
  while (true) {
    UniqueEvbufReadln line(evbuf, EVBUFFER_EOL_CRLF_STRICT);
    if (!line) {
      if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
        return {Status::NotOK, fmt::format("read size: {}", strerror(errno))};
      }
      continue;
    }
    if (line[0] == '-') {
      std::string msg(line.get());
      return {Status::NotOK, msg};
    }
    file_size = line.length > 0 ? std::strtoull(line.get(), nullptr, 10) : 0;
    break;
  }

  // Write to tmp file
  auto tmp_file = Engine::Storage::ReplDataManager::NewTmpFile(storage_, dir, file);
  if (!tmp_file) {
    return {Status::NotOK, "unable to create tmp file"};
  }

  size_t remain = file_size;
  uint32_t tmp_crc = 0;
  char data[16 * 1024];
  while (remain != 0) {
    if (evbuffer_get_length(evbuf) > 0) {
      auto data_len = evbuffer_remove(evbuf, data, remain > 16 * 1024 ? 16 * 1024 : remain);
      if (data_len == 0) continue;
      if (data_len < 0) {
        return {Status::NotOK, "read sst file data error"};
      }
      tmp_file->Append(rocksdb::Slice(data, data_len));
      tmp_crc = rocksdb::crc32c::Extend(tmp_crc, data, data_len);
      remain -= data_len;
    } else {
      if (evbuffer_read(evbuf, sock_fd, -1) <= 0) {
        return {Status::NotOK, fmt::format("read sst file: {}", strerror(errno))};
      }
    }
  }
  // Verify file crc checksum if crc is not 0
  if (crc && crc != tmp_crc) {
    return {Status::NotOK, fmt::format("CRC mismatched, {} was expected but got {}", crc, tmp_crc)};
  }
  // File is OK, rename to formal name
  auto s = Engine::Storage::ReplDataManager::SwapTmpFile(storage_, dir, file);
  if (!s.IsOK()) return s;

  // Call fetch file callback function
  fn(file, crc);
  return Status::OK();
}

Status ReplicationThread::fetchFiles(int sock_fd, const std::string &dir, const std::vector<std::string> &files,
                                     const std::vector<uint32_t> &crcs, const fetch_file_callback &fn) {
  std::string files_str;
  for (const auto &file : files) {
    files_str += file;
    files_str.push_back(',');
  }
  files_str.pop_back();

  const auto fetch_command = Redis::MultiBulkString({"_fetch_file", files_str});
  auto s = Util::SockSend(sock_fd, fetch_command);
  if (!s.IsOK()) return s.Prefixed("send fetch file command");

  UniqueEvbuf evbuf;
  for (unsigned i = 0; i < files.size(); i++) {
    DLOG(INFO) << "[fetch] Start to fetch file " << files[i];
    s = fetchFile(sock_fd, evbuf.get(), dir, files[i], crcs[i], fn);
    if (!s.IsOK()) {
      s = Status(Status::NotOK, "fetch file err: " + s.Msg());
      LOG(WARNING) << "[fetch] Fail to fetch file " << files[i] << ", err: " << s.Msg();
      break;
    }
    DLOG(INFO) << "[fetch] Succeed fetching file " << files[i];

    // Just for tests
    if (srv_->GetConfig()->fullsync_recv_file_delay) {
      sleep(srv_->GetConfig()->fullsync_recv_file_delay);
    }
  }
  return s;
}

// Check if stop_flag_ is set, when do, tear down replication
void ReplicationThread::EventTimerCB(int, int16_t, void *ctx) {
  // DLOG(INFO) << "[replication] timer";
  auto self = static_cast<ReplicationThread *>(ctx);
  if (self->stop_flag_) {
    LOG(INFO) << "[replication] Stop ev loop";
    event_base_loopbreak(self->base_);
    self->psync_steps_.Stop();
    self->fullsync_steps_.Stop();
  }
}

Status ReplicationThread::ParseWriteBatch(const std::string &batch_string) {
  rocksdb::WriteBatch write_batch(batch_string);
  WriteBatchHandler write_batch_handler;

  auto db_status = write_batch.Iterate(&write_batch_handler);
  if (!db_status.ok()) return {Status::NotOK, "failed to iterate over write batch: " + db_status.ToString()};

  switch (write_batch_handler.Type()) {
    case kBatchTypePublish:
      srv_->PublishMessage(write_batch_handler.Key(), write_batch_handler.Value());
      break;
    case kBatchTypePropagate:
      if (write_batch_handler.Key() == Engine::kPropagateScriptCommand) {
        std::vector<std::string> tokens = Util::TokenizeRedisProtocol(write_batch_handler.Value());
        if (!tokens.empty()) {
          auto s = srv_->ExecPropagatedCommand(tokens);
          if (!s.IsOK()) {
            return s.Prefixed("failed to execute propagate command");
          }
        }
      }
      break;
    case kBatchTypeStream: {
      auto key = write_batch_handler.Key();
      InternalKey ikey(key, storage_->IsSlotIdEncoded());
      Slice entry_id = ikey.GetSubKey();
      Redis::StreamEntryID id;
      GetFixed64(&entry_id, &id.ms);
      GetFixed64(&entry_id, &id.seq);
      srv_->OnEntryAddedToStream(ikey.GetNamespace().ToString(), ikey.GetKey().ToString(), id);
      break;
    }
    case kBatchTypeNone:
      break;
  }
  return Status::OK();
}

bool ReplicationThread::isRestoringError(const char *err) {
  return std::string(err) == "-ERR restoring the db from backup";
}

bool ReplicationThread::isWrongPsyncNum(const char *err) {
  return std::string(err) == "-ERR wrong number of arguments";
}

bool ReplicationThread::isUnknownOption(const char *err) { return std::string(err) == "-ERR unknown option"; }

rocksdb::Status WriteBatchHandler::PutCF(uint32_t column_family_id, const rocksdb::Slice &key,
                                         const rocksdb::Slice &value) {
  type_ = kBatchTypeNone;
  if (column_family_id == kColumnFamilyIDPubSub) {
    type_ = kBatchTypePublish;
    kv_ = std::make_pair(key.ToString(), value.ToString());
    return rocksdb::Status::OK();
  } else if (column_family_id == kColumnFamilyIDPropagate) {
    type_ = kBatchTypePropagate;
    kv_ = std::make_pair(key.ToString(), value.ToString());
    return rocksdb::Status::OK();
  } else if (column_family_id == kColumnFamilyIDStream) {
    type_ = kBatchTypeStream;
    kv_ = std::make_pair(key.ToString(), value.ToString());
  }
  return rocksdb::Status::OK();
}
