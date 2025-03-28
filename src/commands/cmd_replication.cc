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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "cluster/cluster_defs.h"
#include "commander.h"
#include "error_constants.h"
#include "fmt/format.h"
#include "io_util.h"
#include "scope_exit.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "stats/log_collector.h"
#include "status.h"
#include "thread_util.h"
#include "time_util.h"
#include "unique_fd.h"

namespace redis {

class CommandPSync : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t seq_arg = 1;
    if (args.size() == 3) {
      seq_arg = 2;
      new_psync_ = true;
    }

    auto parse_result = ParseInt<uint64_t>(args[seq_arg], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "value is not an unsigned long long or out of range"};
    }

    next_repl_seq_ = static_cast<rocksdb::SequenceNumber>(*parse_result);
    if (new_psync_) {
      assert(args.size() == 3);
      replica_replid_ = args[1];
      if (replica_replid_.size() != kReplIdLength) {
        return {Status::RedisParseErr, "Wrong replication id length"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn, std::string *output) override {
    auto peer_info = conn->GetPeerInfo().ToString();

    LOG(INFO) << fmt::format(
        "Slave {} asks for synchronization with next sequence: {} "
        "replication id: {} and local sequence: {}",
        peer_info, next_repl_seq_, (new_psync_ ? replica_replid_ : "not supported"), srv->storage->LatestSeqNumber());

    bool need_full_sync = false;

    // Check replication id of the last sequence log
    if (new_psync_ && srv->GetConfig()->use_rsid_psync) {
      std::string replid_in_wal = srv->storage->GetReplIdFromWalBySeq(next_repl_seq_ - 1);
      LOG(INFO) << "Replication id in WAL: " << replid_in_wal;

      // We check replication id only when WAL has this sequence, since there may be no WAL,
      // Or WAL may have nothing when starting from db of old version kvrocks.
      if (replid_in_wal.length() == kReplIdLength && replid_in_wal != replica_replid_) {
        *output = "wrong replication id of the last log";
        need_full_sync = true;
      }
    }

    // Check Log sequence
    if (!need_full_sync && !checkWALBoundary(srv->storage, next_repl_seq_).IsOK()) {
      *output = "sequence out of range, please use fullsync";
      need_full_sync = true;
    }

    if (need_full_sync) {
      srv->stats.IncrPSyncErrCount();
      return {Status::RedisExecErr, *output};
    }

    // Server would spawn a new thread to sync the batch, and connection would
    // be taken over, so should never trigger any event in worker thread.
    conn->Detach();
    conn->EnableFlag(redis::Connection::kSlave);
    auto s = util::SockSetBlocking(conn->GetFD(), 1);
    if (!s.IsOK()) {
      conn->EnableFlag(redis::Connection::kCloseAsync);
      return s.Prefixed("failed to set blocking mode on socket");
    }

    srv->stats.IncrPSyncOKCount();
    s = srv->AddSlave(conn, next_repl_seq_);
    if (!s.IsOK()) {
      std::string err = redis::Error(s);
      s = util::SockSend(conn->GetFD(), err, conn->GetBufferEvent());
      if (!s.IsOK()) {
        LOG(WARNING) << "failed to send error message to the replica: " << s.Msg();
      }
      conn->EnableFlag(redis::Connection::kCloseAsync);
      LOG(WARNING) << "Failed to add replica: " << conn->GetAddr() << " to start incremental syncing";
    } else {
      LOG(INFO) << "New replica: " << conn->GetAddr() << " was added, start incremental syncing";
    }
    return s;
  }

 private:
  rocksdb::SequenceNumber next_repl_seq_ = 0;
  bool new_psync_ = false;
  std::string replica_replid_;

  // Return OK if the seq is in the range of the current WAL
  static Status checkWALBoundary(engine::Storage *storage, rocksdb::SequenceNumber seq) {
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
      Status s = ParseParam(util::ToLower(args[i]), args[i + 1]);
      if (!s.IsOK()) {
        return s;
      }
    }

    return Commander::Parse(args);
  }

  Status ParseParam(std::string_view option, std::string_view value) {
    if (option == "listening-port") {
      auto parse_result = ParseInt<int>(value, NumericRange<int>{1, PORT_LIMIT - 1}, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "listening-port should be number or out of range"};
      }
      port_ = *parse_result;
      return Status::OK();
    }

    if (option == "ip-address") {
      if (value.empty()) {
        return {Status::RedisParseErr, "ip-address should not be empty"};
      }
      ip_ = value;
      return Status::OK();
    }

    if (option == "peer-id") {
      if (value.empty()) {
        return {Status::RedisParseErr, "peer-id should not be empty"};
      }
      peer_id_ = value;
      return Status::OK();
    }

    if (option == "version") {
      auto parse_result = ParseInt<int64_t>(value, 10);
      if (!parse_result) {
        return {Status::RedisParseErr, "version should be number"};
      }
      peer_version_ = *parse_result;
      return Status::OK();
    }

    return {Status::RedisParseErr, errUnknownOption};
  }

  Status Execute([[maybe_unused]] engine::Context &ctx, [[maybe_unused]] Server *srv, Connection *conn,
                 std::string *output) override {
    if (srv->GetConfig()->cluster_enabled && peer_version_ >= 0 &&
        !srv->cluster->IsInCluster(peer_id_, peer_version_)) {
      return {Status::NotOK, errYouAreFired};
    }

    if (ip_.empty()) {
      ip_ = conn->GetIP();
    }

    auto peer_info = std::make_unique<PeerInfo>(ip_, port_, peer_id_, peer_version_);
    conn->SetPeerInfo(std::move(peer_info));

    *output = redis::RESP_OK;
    return Status::OK();
  }

 private:
  std::string ip_;
  uint32_t port_;
  std::string peer_id_;
  int64_t peer_version_ = -1;
};

class CommandFetchMeta : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    int repl_fd = conn->GetFD();
    auto peer_info = conn->GetPeerInfo().ToString();

    auto s = util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();
    conn->EnableFlag(redis::Connection::kCloseAsync);
    srv->stats.IncrFullSyncCount();

    // Feed-replica-meta thread
    auto t = GET_OR_RET(util::CreateThread("feed-repl-info", [srv, repl_fd, peer_info, bev = conn->GetBufferEvent()] {
      srv->IncrFetchFileThread();
      auto exit = MakeScopeExit([srv, bev] {
        bufferevent_free(bev);
        srv->DecrFetchFileThread();
      });

      std::string files;
      auto s = engine::Storage::ReplDataManager::GetFullReplDataInfo(srv->storage, &files);
      if (!s.IsOK()) {
        LOG(WARNING) << "[replication] Failed to get full data file info: " << s.Msg();
        s = util::SockSend(repl_fd, redis::Error({Status::RedisErrorNoPrefix, "can't create db checkpoint"}), bev);
        if (!s.IsOK()) {
          LOG(WARNING) << "[replication] Failed to send error response: " << s.Msg();
        }
        return;
      }
      // Send full data file info
      if (auto s = util::SockSend(repl_fd, files + CRLF, bev)) {
        LOG(INFO) << fmt::format("[replication] Succeed sending full data file info to {}: {}", peer_info, files);
      } else {
        LOG(WARNING) << fmt::format("[replication] Failed to send full data file info to {}: {}", peer_info, s.Msg());
      }
      auto now_secs = static_cast<time_t>(util::GetTimeStamp());
      srv->storage->SetCheckpointAccessTimeSecs(now_secs);
    }));

    if (auto s = util::ThreadDetach(t); !s) {
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

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    std::vector<std::string> files = util::Split(files_str_, ",");

    int repl_fd = conn->GetFD();
    auto peer_info = conn->GetPeerInfo().ToString();

    auto s = util::SockSetBlocking(repl_fd, 1);
    if (!s.IsOK()) {
      return s.Prefixed("failed to set blocking mode on socket");
    }

    conn->NeedNotFreeBufferEvent();  // Feed-replica-file thread will close the replica bufferevent
    conn->EnableFlag(redis::Connection::kCloseAsync);

    auto t = GET_OR_RET(
        util::CreateThread("feed-repl-file", [srv, repl_fd, peer_info, files, bev = conn->GetBufferEvent()]() {
          auto exit = MakeScopeExit([bev] { bufferevent_free(bev); });
          srv->IncrFetchFileThread();

          for (const auto &file : files) {
            if (srv->IsStopped()) break;

            uint64_t file_size = 0, max_replication_bytes = 0;
            if (srv->GetConfig()->max_replication_mb > 0 && srv->GetFetchFileThreadNum() != 0) {
              max_replication_bytes = (srv->GetConfig()->max_replication_mb * MiB) / srv->GetFetchFileThreadNum();
            }
            auto start = std::chrono::high_resolution_clock::now();
            auto fd = UniqueFD(engine::Storage::ReplDataManager::OpenDataFile(srv->storage, file, &file_size));
            if (!fd) break;

            // Send file size and content
            auto s = util::SockSend(repl_fd, std::to_string(file_size) + CRLF, bev);
            if (s) {
              s = util::SockSendFile(repl_fd, *fd, file_size, bev);
            }
            if (s) {
              LOG(INFO) << fmt::format("[replication] Succeed sending file {} to {} with size: {}", file, peer_info,
                                       file_size);
            } else {
              LOG(WARNING) << fmt::format("[replication] Fail to send file {} to {}: {}", file, peer_info, s.Msg());
              break;
            }
            fd.Close();

            // Sleep if the speed of sending file is more than replication speed limit
            auto end = std::chrono::high_resolution_clock::now();
            uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
            if (max_replication_bytes > 0) {
              auto shortest = static_cast<uint64_t>(static_cast<double>(file_size) /
                                                    static_cast<double>(max_replication_bytes) * (1000 * 1000));
              if (duration < shortest) {
                LOG(INFO) << "[replication] Need to sleep " << (shortest - duration) / 1000
                          << " ms since of sending files too quickly";
                usleep(shortest - duration);
              }
            }
          }
          auto now_secs = util::GetTimeStamp<std::chrono::seconds>();
          srv->storage->SetCheckpointAccessTimeSecs(now_secs);
          srv->DecrFetchFileThread();
        }));

    if (auto s = util::ThreadDetach(t); !s) {
      return s;
    }

    return Status::OK();
  }

 private:
  std::string files_str_;
};

class CommandDBName : public Commander {
 public:
  Status Parse([[maybe_unused]] const std::vector<std::string> &args) override { return Status::OK(); }

  Status Execute([[maybe_unused]] engine::Context &ctx, Server *srv, Connection *conn,
                 [[maybe_unused]] std::string *output) override {
    conn->Reply(srv->storage->GetName() + CRLF);
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(Replication, MakeCmdAttr<CommandReplConf>("replconf", -3, "read-only no-script", NO_KEY),
                        MakeCmdAttr<CommandPSync>("psync", -2, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandFetchMeta>("_fetch_meta", 1, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandFetchFile>("_fetch_file", 2, "read-only no-multi no-script", NO_KEY),
                        MakeCmdAttr<CommandDBName>("_db_name", 1, "read-only no-multi", NO_KEY), )

}  // namespace redis
