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
#include "server/server.h"
#include "types/redis_list.h"

namespace Redis {

class CommandPush : public Commander {
 public:
  CommandPush(bool create_if_missing, bool left) : left_(left), create_if_missing_(create_if_missing) {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> elems;
    for (size_t i = 2; i < args_.size(); i++) {
      elems.emplace_back(args_[i]);
    }

    int ret = 0;
    rocksdb::Status s;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    if (create_if_missing_) {
      s = list_db.Push(args_[1], elems, left_, &ret);
    } else {
      s = list_db.PushX(args_[1], elems, left_, &ret);
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    svr->WakeupBlockingConns(args_[1], elems.size());

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool left_;
  bool create_if_missing_;
};

class CommandLPush : public CommandPush {
 public:
  CommandLPush() : CommandPush(true, true) {}
};

class CommandRPush : public CommandPush {
 public:
  CommandRPush() : CommandPush(true, false) {}
};

class CommandLPushX : public CommandPush {
 public:
  CommandLPushX() : CommandPush(false, true) {}
};

class CommandRPushX : public CommandPush {
 public:
  CommandRPushX() : CommandPush(false, false) {}
};

class CommandPop : public Commander {
 public:
  explicit CommandPop(bool left) : left_(left) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 3) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    if (args.size() == 2) {
      return Status::OK();
    }

    auto v = ParseInt<int32_t>(args[2], 10);
    if (!v) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    if (*v < 0) {
      return {Status::RedisParseErr, errValueMustBePositive};
    }

    count_ = *v;
    with_count_ = true;

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    if (with_count_) {
      std::vector<std::string> elems;
      auto s = list_db.PopMulti(args_[1], left_, count_, &elems);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = Redis::MultiLen(-1);
      } else {
        *output = Redis::MultiBulkString(elems);
      }
    } else {
      std::string elem;
      auto s = list_db.Pop(args_[1], left_, &elem);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = Redis::NilString();
      } else {
        *output = Redis::BulkString(elem);
      }
    }

    return Status::OK();
  }

 private:
  bool left_;
  bool with_count_ = false;
  uint32_t count_ = 1;
};

class CommandLPop : public CommandPop {
 public:
  CommandLPop() : CommandPop(true) {}
};

class CommandRPop : public CommandPop {
 public:
  CommandRPop() : CommandPop(false) {}
};

class CommandBPop : public Commander {
 public:
  explicit CommandBPop(bool left) : left_(left) {}

  CommandBPop(const CommandBPop &) = delete;
  CommandBPop &operator=(const CommandBPop &) = delete;

  ~CommandBPop() override {
    if (timer_) {
      event_free(timer_);
      timer_ = nullptr;
    }
  }

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[args.size() - 1], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, "timeout is not an integer or out of range"};
    }

    if (*parse_result < 0) {
      return {Status::RedisParseErr, "timeout should not be negative"};
    }

    timeout_ = *parse_result;

    keys_ = std::vector<std::string>(args.begin() + 1, args.end() - 1);
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr_ = svr;
    conn_ = conn;

    auto bev = conn->GetBufferEvent();
    auto s = TryPopFromList();
    if (s.ok() || !s.IsNotFound()) {
      return Status::OK();  // error has already output in TryPopFromList
    }

    if (conn->IsInExec()) {
      *output = Redis::MultiLen(-1);
      return Status::OK();  // No blocking in multi-exec
    }

    for (const auto &key : keys_) {
      svr_->BlockOnKey(key, conn_);
    }

    bufferevent_setcb(bev, nullptr, WriteCB, EventCB, this);

    if (timeout_) {
      timer_ = evtimer_new(bufferevent_get_base(bev), TimerCB, this);
      timeval tm = {timeout_, 0};
      evtimer_add(timer_, &tm);
    }

    return {Status::BlockingCmd};
  }

  rocksdb::Status TryPopFromList() {
    Redis::List list_db(svr_->storage_, conn_->GetNamespace());
    std::string elem;
    const std::string *last_key_ptr = nullptr;
    rocksdb::Status s;
    for (const auto &key : keys_) {
      last_key_ptr = &key;
      s = list_db.Pop(key, left_, &elem);
      if (s.ok() || !s.IsNotFound()) {
        break;
      }
    }

    if (s.ok()) {
      if (!last_key_ptr) {
        conn_->Reply(Redis::MultiBulkString({"", ""}));
      } else {
        conn_->GetServer()->UpdateWatchedKeysManually({*last_key_ptr});
        conn_->Reply(Redis::MultiBulkString({*last_key_ptr, std::move(elem)}));
      }
    } else if (!s.IsNotFound()) {
      conn_->Reply(Redis::Error("ERR " + s.ToString()));
      LOG(ERROR) << "Failed to execute redis command: " << conn_->current_cmd_->GetAttributes()->name_
                 << ", err: " << s.ToString();
    }

    return s;
  }

  static void WriteCB(bufferevent *bev, void *ctx) {
    auto self = reinterpret_cast<CommandBPop *>(ctx);
    auto s = self->TryPopFromList();
    if (s.IsNotFound()) {
      // The connection may be waked up but can't pop from list. For example,
      // connection A is blocking on list and connection B push a new element
      // then wake up the connection A, but this element may be token by other connection C.
      // So we need to wait for the wake event again by disabling the WRITE event.
      bufferevent_disable(bev, EV_WRITE);
      return;
    }

    if (self->timer_) {
      event_free(self->timer_);
      self->timer_ = nullptr;
    }

    self->unBlockingAll();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      self->conn_);
    bufferevent_enable(bev, EV_READ);
    // We need to manually trigger the read event since we will stop processing commands
    // in connection after the blocking command, so there may have some commands to be processed.
    // Related issue: https://github.com/apache/incubator-kvrocks/issues/831
    bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
  }

  static void EventCB(bufferevent *bev, int16_t events, void *ctx) {
    auto self = static_cast<CommandBPop *>(ctx);
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (self->timer_ != nullptr) {
        event_free(self->timer_);
        self->timer_ = nullptr;
      }
      self->unBlockingAll();
    }
    Redis::Connection::OnEvent(bev, events, self->conn_);
  }

  static void TimerCB(int, int16_t events, void *ctx) {
    auto self = reinterpret_cast<CommandBPop *>(ctx);
    self->conn_->Reply(Redis::NilString());
    event_free(self->timer_);
    self->timer_ = nullptr;
    self->unBlockingAll();
    auto bev = self->conn_->GetBufferEvent();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      self->conn_);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  bool left_ = false;
  int timeout_ = 0;  // seconds
  std::vector<std::string> keys_;
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  event *timer_ = nullptr;

  void unBlockingAll() {
    for (const auto &key : keys_) {
      svr_->UnblockOnKey(key, conn_);
    }
  }
};

class CommandBLPop : public CommandBPop {
 public:
  CommandBLPop() : CommandBPop(true) {}
};

class CommandBRPop : public CommandBPop {
 public:
  CommandBRPop() : CommandBPop(false) {}
};

class CommandLRem : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    count_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Rem(args_[1], count_, args_[3], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int count_ = 0;
};

class CommandLInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if ((Util::ToLower(args[2]) == "before")) {
      before_ = true;
    } else if ((Util::ToLower(args[2]) == "after")) {
      before_ = false;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Insert(args_[1], args_[3], args_[4], before_, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool before_ = false;
};

class CommandLRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> elems;
    auto s = list_db.Range(args_[1], start_, stop_, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(elems, false);
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandLLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    uint32_t count = 0;
    auto s = list_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(count);
    return Status::OK();
  }
};

class CommandLIndex : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    auto s = list_db.Index(args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(elem);
    }
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLSet : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseInt<int>(args[2], 10);
    if (!parse_result) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    index_ = *parse_result;
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Set(args_[1], index_, args_[3]);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int index_ = 0;
};

class CommandLTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto parse_start = ParseInt<int>(args[2], 10);
    auto parse_stop = ParseInt<int>(args[3], 10);
    if (!parse_start || !parse_stop) {
      return {Status::RedisParseErr, errValueNotInteger};
    }

    start_ = *parse_start;
    stop_ = *parse_stop;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    auto s = list_db.Trim(args_[1], start_, stop_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandRPopLPUSH : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    auto s = list_db.RPopLPush(args_[1], args_[2], &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }
};

class CommandLMove : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto arg_val = Util::ToLower(args_[3]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    src_left_ = arg_val == "left";
    arg_val = Util::ToLower(args_[4]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    dst_left_ = arg_val == "left";
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::List list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    auto s = list_db.LMove(args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }

 private:
  bool src_left_;
  bool dst_left_;
};

REDIS_REGISTER_COMMANDS(
    MakeCmdAttr<CommandLPush>("lpush", -3, "write", 1, 1, 1), MakeCmdAttr<CommandRPush>("rpush", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandLPushX>("lpushx", -3, "write", 1, 1, 1),
    MakeCmdAttr<CommandRPushX>("rpushx", -3, "write", 1, 1, 1), MakeCmdAttr<CommandLPop>("lpop", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandRPop>("rpop", -2, "write", 1, 1, 1),
    MakeCmdAttr<CommandBLPop>("blpop", -3, "write no-script", 1, -2, 1),
    MakeCmdAttr<CommandBRPop>("brpop", -3, "write no-script", 1, -2, 1),
    MakeCmdAttr<CommandLRem>("lrem", 4, "write", 1, 1, 1), MakeCmdAttr<CommandLInsert>("linsert", 5, "write", 1, 1, 1),
    MakeCmdAttr<CommandLRange>("lrange", 4, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandLIndex>("lindex", 3, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandLTrim>("ltrim", 4, "write", 1, 1, 1), MakeCmdAttr<CommandLLen>("llen", 2, "read-only", 1, 1, 1),
    MakeCmdAttr<CommandLSet>("lset", 4, "write", 1, 1, 1),
    MakeCmdAttr<CommandRPopLPUSH>("rpoplpush", 3, "write", 1, 2, 1),
    MakeCmdAttr<CommandLMove>("lmove", 5, "write", 1, 2, 1), )

}  // namespace Redis
