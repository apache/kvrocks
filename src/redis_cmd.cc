#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <thread>

#include "redis_cmd.h"
#include "redis_replication.h"
#include "server.h"
#include "sock_util.h"
#include "string_util.h"

namespace Redis {
std::map<std::string, CommanderFactory> command_table = {
    {"get",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandGet);
     }},
    {"set",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSet);
     }},
    {"slaveof",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSlaveOf);
     }},
    {"psync", []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandPSync);
     }}};

Status LookupCommand(const std::string &cmd_name,
                     std::unique_ptr<Commander> *cmd) {
  if (cmd_name.empty()) return Status(Status::RedisUnknownCmd);
  auto cmd_factory = command_table.find(Util::ToLower(cmd_name));
  if (cmd_factory == command_table.end()) {
    return Status(Status::RedisUnknownCmd);
  }
  *cmd = cmd_factory->second();
  return Status::OK();
}

/*
 * Redis Commands
 */

Status CommandGet::Parse(const std::list<std::string> &args) {
  name_ = "get";
  if (args.size() != 1) {
    return Status(Status::RedisInvalidCmd);
  }
  key_ = args.front();
  return Status::OK();
}

Status CommandGet::Execute(Server *svr, std::string *output) {
  *output = SimpleString(svr->storage_.Get(key_));
  return Status::OK();
}

Status CommandSet::Parse(const std::list<std::string> &args) {
  name_ = "set";
  if (args.size() < 2) {
    return Status(Status::RedisInvalidCmd);
  }
  auto it = args.cbegin();
  key_ = *it++;
  value_ = *it++;
  std::string ex, px;
  bool nx = false, xx = false;
  for (auto end = args.cend(); it != end; ++it) {
    if (Util::ToLower(*it) == "ex") {
      ex = *++it;
    } else if (Util::ToLower(*it) == "px") {
      px = *++it;
    } else if (Util::ToLower(*it) == "nx") {
      nx = true;
    } else if (Util::ToLower(*it) == "xx") {
      xx = true;
    } else {
      break;
    }
  }
  DLOG(INFO) << "SET " << key_ << "=" << value_ << ",ex=" << ex << ",px=" << px
             << ",nx=" << nx << ",xx=" << xx;
  return Status::OK();
}

Status CommandSet::Execute(Server *svr, std::string *output) {
  svr->storage_.Set(key_, value_);
  *output = SimpleString("set");
  return Status::OK();
}

Status CommandSlaveOf::Parse(const std::list<std::string> &args) {
  name_ = "slaveof";
  if (args.size() != 2) {
    return Status(Status::RedisInvalidCmd);
  }
  auto it = args.cbegin();
  host_ = *it++;
  auto port = *it++;
  try {
    auto p = std::stoul(port);
    if (p > UINT32_MAX) {
      throw std::overflow_error("port out of range");
    }
    port_ = static_cast<uint32_t>(p);
  } catch (const std::exception &e) {
    return Status(Status::RedisParseErr);
  }
  return Status::OK();
}

Status CommandSlaveOf::Execute(Server *svr, std::string *output) {
  auto s = svr->AddMaster(host_, port_);
  if (s.IsOK()) {
    *output = Redis::SimpleString("OK");
  }
  return s;
}

Status CommandPSync::Parse(const std::list<std::string> &args) {
  name_ = "psync";
  if (args.size() != 1) {
    return Status(Status::RedisInvalidCmd);
  }
  auto seq = args.front();
  try {
    auto s = std::stoull(seq);
    seq_ = static_cast<rocksdb::SequenceNumber>(s);
  } catch (const std::exception &e) {
    return Status(Status::RedisParseErr);
  }
  return Status::OK();
}

Status CommandPSync::SidecarExecute(Server *svr, int out_fd) {
  using namespace std::chrono_literals;
  std::unique_ptr<rocksdb::TransactionLogIterator> iter;

  // If seq_ is larger than storage's seq, return error
  if (seq_ > svr->storage_.LatestSeq()) {
    sock_send(out_fd, Redis::Error("sequence out of range"));
    return Status(Status::RedisExecErr);
  } else {
    if (sock_send(out_fd, Redis::SimpleString("OK")) < 0) {
      return Status(Status::NetSendErr);
    }
  }

  while (true) {
    // TODO: test if out_fd is closed on the other side, HEARTBEAT
    int sock_err = 0;
    socklen_t sock_err_len = sizeof(sock_err);
    if (getsockopt(out_fd, SOL_SOCKET, SO_ERROR, (void *)&sock_err,
                   &sock_err_len) < 0 ||
        sock_err) {
      LOG(ERROR) << "Socket err: " << evutil_socket_error_to_string(sock_err);
      return Status(Status::NetSendErr);
    }

    auto s = svr->storage_.GetWALIter(seq_, &iter);
    if (!s.IsOK()) {
      // LOG(ERROR) << "Failed to get WAL iter: " << s.msg();
      std::this_thread::sleep_for(1s);
      continue;
    }

    while (iter->Valid()) {
      LOG(INFO) << "WAL send batch";
      auto batch = iter->GetBatch();
      auto data = batch.writeBatchPtr->Data();
      // Send data in redis bulk string format
      std::string bulk_str = "$" + std::to_string(data.length()) + CRLF
          + data + CRLF;
      if (sock_send(out_fd, bulk_str) < 0) {
        return Status(Status::NetSendErr);
      }
      seq_ = batch.sequence + 1;
      iter->Next();
    }
    // if arrived here, means the wal file is rotated, a reopen is needed.
    LOG(INFO) << "WAL rotate";
    std::this_thread::sleep_for(1s);
  }
}

/*
 * Sidecar Thread
 */

void make_socket_blocking(int fd) {
  int flag = fcntl(fd, F_GETFL);
  if (!(flag & O_NONBLOCK)) {
    LOG(ERROR) << "Expected fd to be non-blocking";
  }
  fcntl(fd, F_SETFL, flag & ~O_NONBLOCK);  // remove NONBLOCK
}

void TakeOverBufferEvent(bufferevent *bev) {
  auto base = bufferevent_get_base(bev);
  auto fd = bufferevent_getfd(bev);
  // 1. remove FD' events
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  // 2. change FD to blocking mode
  make_socket_blocking(fd);
}

Status SidecarCommandThread::Start() {
  t_ = std::thread([this]() { this->Run(); });
  return Status::OK();
}

void SidecarCommandThread::Run() {
  std::string reply;
  int fd = bufferevent_getfd(bev_);
  cmd_->SidecarExecute(svr_, fd);
  Stop();
}

}  // namespace Redis
