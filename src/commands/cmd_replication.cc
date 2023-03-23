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

#include "commander.h"
#include "error_constants.h"
#include "fd_util.h"
#include "io_util.h"
#include "scope_exit.h"
#include "server/server.h"
#include "thread_util.h"
#include "time_util.h"

namespace Redis {

class CommandPSync : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t seq_arg = 1;
    if (args.size() == 3) {
      seq_arg = 2;
      new_psync = true;
    }

    auto parse_result = ParseInt<uint64_t>(args[seq_arg], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "value is not an unsigned long long or out of range"};
    }

    next_repl_seq = static_cast<rocksdb::SequenceNumber>(*parse_result);
    if (new_psync) {
      assert(args.size() == 3);
      replica_replid = args[1];
      if (replica_replid.size() != kReplIdLength) {
        return {Status::RedisParseErr, "Wrong replication id length"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    LOG(INFO) << "Slave " << conn->GetAddr() << ", listening port: " << conn->GetListeningPort()
              << ", announce ip: " << conn->GetAnnounceIP() << " asks for synchronization"
              << " with next sequence: " << next_repl_seq
              << " replication id: " << (replica_replid.length() ? replica_replid : "not supported")
              << ", and local sequence: " << svr->storage_->LatestSeqNumber();

    bool need_full_sync = false;

    // Check replication id of the last sequence log
    if (new_psync && svr->GetConfig()->use_rsid_psync) {
      std::string replid_in_wal = svr->storage_->GetReplIdFromWalBySeq(next_repl_seq - 1);
      LOG(INFO) << "Replication id in WAL: " << replid_in_wal;

      // We check replication id only when WAL has this sequence, since there may be no WAL,
      // Or WAL may have nothing when starting from db of old version kvrocks.
      if (replid_in_wal.length() == kReplIdLength && replid_in_wal != replica_replid) {
        *output = "wrong replication id of the last log";
        need_full_sync = true;
      }
    }

    // Check Log sequence
    if (!need_full_sync && !checkWALBoundary(svr->storage_, next_repl_seq).IsOK()) {
      *output = "sequence out of range, please use fullsync";
      need_full_sync = true;
    }

    if (need_full_sync) {
      svr->stats_.IncrPSyncErrCounter();
      return {Status::RedisExecErr, *output};
    }

    // Server would spawn a new thread to sync the batch, and connection would
    // be taken over, so should never trigger any event in worker thread.
    conn->Detach();
    conn->EnableFlag(Redis::Connection::kSlave);
    auto s = Util::SockSetBlocking(conn->GetFD(), 1);
    if (!s.IsOK()) {
      conn->EnableFlag(Redis::Connection::kCloseAsync);
      return s.Prefixed("failed to set blocking mode on socket");
    }

    svr->stats_.IncrPSyncOKCounter();
    s = svr->AddSlave(conn, next_repl_seq);
    if (!s.IsOK()) {
      std::string err = "-ERR " + s.Msg() + "\r\n";
      s = Util::SockSend(conn->GetFD(), err);
      if (!s.IsOK()) {
        LOG(WARNING) << "failed to send error message to the replica: " << s.Msg();
      }
      conn->EnableFlag(Redis::Connection::kCloseAsync);
      LOG(WARNING) << "Failed to add replica: " << conn->GetAddr() << " to start incremental syncing";
    } else {
      LOG(INFO) << "New replica: " << conn->GetAddr() << " was added, start incremental syncing";
    }
    return s;
  }

 private:
  rocksdb::SequenceNumber next_repl_seq = 0;
  bool new_psync = false;
  std::string replica_replid;

  // Return OK if the seq is in the range of the current WAL
  static Status checkWALBoundary(Engine::Storage *storage, rocksdb::SequenceNumber seq) {
    if (seq == storage->LatestSeqNumber() + 1) {
      return Status::OK();
    }

    // Upper bound
    if (seq > storage->LatestSeqNumber() + 1) {
      return {Status::NotOK};
    }

    // Lower bound
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = storage->GetWALIter(seq, &iter);
    if (s.IsOK() && iter->Valid()) {
      auto batch = iter->GetBatch();
      if (seq != batch.sequence) {
        if (seq > batch.sequence) {
          LOG(ERROR) << "checkWALBoundary with sequence: " << seq
                     << ", but GetWALIter return older sequence: " << batch.sequence;
        }
        return {Status::NotOK};
      }
      return Status::OK();
    }
    return {Status::NotOK};
  }
};

class CommandReplConf : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = 1; i < args.size(); i += 2) {
      Status s = ParseParam(Util::ToLower(args[i]), args[i + 1]);
      if (!s.IsOK()) {
        return s;
      }
    }

    return Commander::Parse(args);
  }

  Status ParseParam(const std::string &option, const std::string &value) {
    if (option == "listening-port") {
      auto parse_result = ParseInt<int>(value, NumericRange<int>{1, PORT_LIMIT - 1}, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "listening-port should be number or out of range"};
      }

      port_ = *parse_result;
    } else if (option == "ip-address") {
      if (value == "") {
        return {Status::RedisParseErr, "ip-address should not be empty"};
      }
      ip_address_ = value;
    } else {
      return {Status::RedisParseErr, errUnknownOption};
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (port_ != 0) {
      conn->SetListeningPort(port_);
    }
    if (!ip_address_.empty()) {
      conn->SetAnnounceIP(ip_address_);
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int port_ = 0;
  std::string ip_address_;
};

class CommandFetchMeta : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int repl_fd = conn->GetFD();
    std::string ip = conn->GetAnnounceIP();

    auto s = Util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();
    conn->EnableFlag(Redis::Connection::kCloseAsync);
    svr->stats_.IncrFullSyncCounter();

    // Feed-replica-meta thread
    auto t = GET_OR_RET(Util::CreateThread("feed-repl-info", [svr, repl_fd, ip, bev = conn->GetBufferEvent()] {
      svr->IncrFetchFileThread();
      auto exit = MakeScopeExit([svr, bev] {
        bufferevent_free(bev);
        svr->DecrFetchFileThread();
      });

      std::string files;
      auto s = Engine::Storage::ReplDataManager::GetFullReplDataInfo(svr->storage_, &files);
      if (!s.IsOK()) {
        s = Util::SockSend(repl_fd, "-ERR can't create db checkpoint");
        if (!s.IsOK()) {
          LOG(WARNING) << "[replication] Failed to send error response: " << s.Msg();
        }
        LOG(WARNING) << "[replication] Failed to get full data file info: " << s.Msg();
        return;
      }
      // Send full data file info
      if (Util::SockSend(repl_fd, files + CRLF).IsOK()) {
        LOG(INFO) << "[replication] Succeed sending full data file info to " << ip;
      } else {
        LOG(WARNING) << "[replication] Fail to send full data file info " << ip << ", error: " << strerror(errno);
      }
      auto now = static_cast<time_t>(Util::GetTimeStamp());
      svr->storage_->SetCheckpointAccessTime(now);
    }));

    if (auto s = Util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }
};

class CommandFetchFile : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    files_str_ = args[1];
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<std::string> files = Util::Split(files_str_, ",");

    int repl_fd = conn->GetFD();
    std::string ip = conn->GetAnnounceIP();

    auto s = Util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();  // Feed-replica-file thread will close the replica bufferevent
    conn->EnableFlag(Redis::Connection::kCloseAsync);

    auto t = GET_OR_RET(Util::CreateThread("feed-repl-file", [svr, repl_fd, ip, files, bev = conn->GetBufferEvent()]() {
      auto exit = MakeScopeExit([bev] { bufferevent_free(bev); });
      svr->IncrFetchFileThread();

      for (const auto &file : files) {
        if (svr->IsStopped()) break;

        uint64_t file_size = 0, max_replication_bytes = 0;
        if (svr->GetConfig()->max_replication_mb > 0) {
          max_replication_bytes = (svr->GetConfig()->max_replication_mb * MiB) / svr->GetFetchFileThreadNum();
        }
        auto start = std::chrono::high_resolution_clock::now();
        auto fd = UniqueFD(Engine::Storage::ReplDataManager::OpenDataFile(svr->storage_, file, &file_size));
        if (!fd) break;

        // Send file size and content
        if (Util::SockSend(repl_fd, std::to_string(file_size) + CRLF).IsOK() &&
            Util::SockSendFile(repl_fd, *fd, file_size).IsOK()) {
          LOG(INFO) << "[replication] Succeed sending file " << file << " to " << ip;
        } else {
          LOG(WARNING) << "[replication] Fail to send file " << file << " to " << ip << ", error: " << strerror(errno);
          break;
        }
        fd.Close();

        // Sleep if the speed of sending file is more than replication speed limit
        auto end = std::chrono::high_resolution_clock::now();
        uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        auto shortest = static_cast<uint64_t>(static_cast<double>(file_size) /
                                              static_cast<double>(max_replication_bytes) * (1000 * 1000));
        if (max_replication_bytes > 0 && duration < shortest) {
          LOG(INFO) << "[replication] Need to sleep " << (shortest - duration) / 1000
                    << " ms since of sending files too quickly";
          usleep(shortest - duration);
        }
      }
      auto now = static_cast<time_t>(Util::GetTimeStamp());
      svr->storage_->SetCheckpointAccessTime(now);
      svr->DecrFetchFileThread();
    }));

    if (auto s = Util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }

 private:
  std::string files_str_;
};

class CommandDBName : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    conn->Reply(svr->storage_->GetName() + CRLF);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandReplConf>("replconf", -3, "read-only replication no-script", 0, 0, 0),
                        MakeCmdAttr<CommandPSync>("psync", -2, "read-only replication no-multi no-script", 0, 0, 0),
                        MakeCmdAttr<CommandFetchMeta>("_fetch_meta", 1, "read-only replication no-multi no-script", 0,
                                                      0, 0),
                        MakeCmdAttr<CommandFetchFile>("_fetch_file", 2, "read-only replication no-multi no-script", 0,
                                                      0, 0),
                        MakeCmdAttr<CommandDBName>("_db_name", 1, "read-only replication no-multi", 0, 0, 0), )

}  // namespace Redis
