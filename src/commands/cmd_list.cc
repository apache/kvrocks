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
#include "commands/command_parser.h"
#include "error_constants.h"
#include "event_util.h"
#include "server/server.h"
#include "types/redis_list.h"

namespace redis {

class CommandPush : public Commander {
 public:
  CommandPush(bool create_if_missing, bool left) : left_(left), create_if_missing_(create_if_missing) {}

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<Slice> elems;
    for (size_t i = 2; i < args_.size(); i++) {
      elems.emplace_back(args_[i]);
    }

    uint64_t ret = 0;
    rocksdb::Status s;
    redis::List list_db(svr->storage, conn->GetNamespace());
    if (create_if_missing_) {
      s = list_db.Push(args_[1], elems, left_, &ret);
    } else {
      s = list_db.PushX(args_[1], elems, left_, &ret);
    }
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    svr->WakeupBlockingConns(args_[1], elems.size());

    *output = redis::Integer(ret);
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
    redis::List list_db(svr->storage, conn->GetNamespace());
    if (with_count_) {
      std::vector<std::string> elems;
      auto s = list_db.PopMulti(args_[1], left_, count_, &elems);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = redis::MultiLen(-1);
      } else {
        *output = redis::MultiBulkString(elems);
      }
    } else {
      std::string elem;
      auto s = list_db.Pop(args_[1], left_, &elem);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (s.IsNotFound()) {
        *output = redis::NilString();
      } else {
        *output = redis::BulkString(elem);
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

class CommandLMPop : public Commander {
 public:
  /// format: LMPOP #numkeys key0 [key1 ...] <LEFT | RIGHT> [COUNT count]
  Status Parse(const std::vector<std::string> &args) override {
    auto v = ParseInt<int32_t>(args[1], 10);
    if (!v) {
      return {Status::RedisParseErr, errValueNotInteger};
    }
    if (*v <= 0) {
      return {Status::RedisParseErr, errValueMustBePositive};
    }
    num_keys_ = *v;

    if ((args.size() != num_keys_ + 3) && (args.size() != num_keys_ + 5)) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    std::string left_or_right = util::ToLower(args[num_keys_ + 2]);
    if (left_or_right == "left") {
      left_ = true;
    }
    else if (left_or_right == "right") {
      left_ = false;
    }
    else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if (args.size() == num_keys_ + 5) {
      if (util::ToLower(args[num_keys_ + 3]) != "count") {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
      auto c = ParseInt<int32_t>(args[num_keys_ + 4]);
      if (!c) {
        return {Status::RedisParseErr, errValueNotInteger};
      }
      if (*c < 0) {
        return {Status::RedisParseErr, errValueMustBePositive};
      }
      count_ = *c;
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::List list_db(svr->storage, conn->GetNamespace());

    std::vector<std::string> elems;
    int key_id = 0;
    for (; key_id < num_keys_; ++key_id) {
      auto s = list_db.PopMulti(args_[key_id + 2], left_, count_, &elems);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }
      if (!elems.empty()) {
        break;
      }
    }

    if (elems.empty()) {
      *output = redis::NilString();
    }
    else {
      std::string elems_bulk = redis::MultiBulkString(elems);
      *output = redis::Array({redis::BulkString(args_[key_id + 2]), elems_bulk});
    }

    return Status::OK();
  }

  static const inline CommandKeyRangeGen keyRangeGen = [](const std::vector<std::string> &args) {
    CommandKeyRange range;
    range.first_key = 2;
    range.key_step = 1;
    auto v = ParseInt<int32_t>(args[1], 10);
    range.last_key = range.first_key + (*v) - 1;
    return range;
  };

 private:
  bool left_;
  uint32_t num_keys_ = 1;
  uint32_t count_ = 1;
};

class CommandBPop : public Commander,
                    private EvbufCallbackBase<CommandBPop, false>,
                    private EventCallbackBase<CommandBPop> {
 public:
  explicit CommandBPop(bool left) : left_(left) {}

  CommandBPop(const CommandBPop &) = delete;
  CommandBPop &operator=(const CommandBPop &) = delete;

  ~CommandBPop() override = default;

  Status Parse(const std::vector<std::string> &args) override {
    auto parse_result = ParseFloat(args[args.size() - 1]);
    if (!parse_result) {
      return {Status::RedisParseErr, errTimeoutIsNotFloat};
    }

    if (*parse_result < 0) {
      return {Status::RedisParseErr, "timeout should not be negative"};
    }

    timeout_ = static_cast<int64_t>(*parse_result * 1000 * 1000);

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
      *output = redis::MultiLen(-1);
      return Status::OK();  // No blocking in multi-exec
    }

    for (const auto &key : keys_) {
      svr_->BlockOnKey(key, conn_);
    }

    SetCB(bev);

    if (timeout_) {
      timer_.reset(NewTimer(bufferevent_get_base(bev)));
      int64_t timeout_second = timeout_ / 1000 / 1000;
      int64_t timeout_microsecond = timeout_ % (1000 * 1000);
      timeval tm = {timeout_second, static_cast<int>(timeout_microsecond)};
      evtimer_add(timer_.get(), &tm);
    }

    return {Status::BlockingCmd};
  }

  rocksdb::Status TryPopFromList() {
    redis::List list_db(svr_->storage, conn_->GetNamespace());
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
        conn_->Reply(redis::MultiBulkString({"", ""}));
      } else {
        conn_->GetServer()->UpdateWatchedKeysManually({*last_key_ptr});
        conn_->Reply(redis::MultiBulkString({*last_key_ptr, std::move(elem)}));
      }
    } else if (!s.IsNotFound()) {
      conn_->Reply(redis::Error("ERR " + s.ToString()));
    }

    return s;
  }

  void OnWrite(bufferevent *bev) {
    auto s = TryPopFromList();
    if (s.IsNotFound()) {
      // The connection may be waked up but can't pop from list. For example,
      // connection A is blocking on list and connection B push a new element
      // then wake up the connection A, but this element may be token by other connection C.
      // So we need to wait for the wake event again by disabling the WRITE event.
      bufferevent_disable(bev, EV_WRITE);
      return;
    }

    if (timer_) {
      timer_.reset();
    }

    unBlockingAll();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
    // We need to manually trigger the read event since we will stop processing commands
    // in connection after the blocking command, so there may have some commands to be processed.
    // Related issue: https://github.com/apache/kvrocks/issues/831
    bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
  }

  void OnEvent(bufferevent *bev, int16_t events) {
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (timer_ != nullptr) {
        timer_.reset();
      }
      unBlockingAll();
    }
    conn_->OnEvent(bev, events);
  }

  void TimerCB(int, int16_t events) {
    conn_->Reply(redis::NilString());
    timer_.reset();
    unBlockingAll();
    auto bev = conn_->GetBufferEvent();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  bool left_ = false;
  int64_t timeout_ = 0;  // microseconds
  std::vector<std::string> keys_;
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  UniqueEvent timer_;

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
    uint64_t ret = 0;
    redis::List list_db(svr->storage, conn->GetNamespace());
    auto s = list_db.Rem(args_[1], count_, args_[3], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }

 private:
  int count_ = 0;
};

class CommandLInsert : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if ((util::ToLower(args[2]) == "before")) {
      before_ = true;
    } else if ((util::ToLower(args[2]) == "after")) {
      before_ = false;
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    redis::List list_db(svr->storage, conn->GetNamespace());
    auto s = list_db.Insert(args_[1], args_[3], args_[4], before_, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
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
    redis::List list_db(svr->storage, conn->GetNamespace());
    std::vector<std::string> elems;
    auto s = list_db.Range(args_[1], start_, stop_, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::MultiBulkString(elems, false);
    return Status::OK();
  }

 private:
  int start_ = 0, stop_ = 0;
};

class CommandLLen : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::List list_db(svr->storage, conn->GetNamespace());
    uint64_t count = 0;
    auto s = list_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(count);
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
    redis::List list_db(svr->storage, conn->GetNamespace());
    std::string elem;
    auto s = list_db.Index(args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = redis::NilString();
    } else {
      *output = redis::BulkString(elem);
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
    redis::List list_db(svr->storage, conn->GetNamespace());
    auto s = list_db.Set(args_[1], index_, args_[3]);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    *output = redis::SimpleString("OK");
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
    redis::List list_db(svr->storage, conn->GetNamespace());
    auto s = list_db.Trim(args_[1], start_, stop_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int start_ = 0;
  int stop_ = 0;
};

class CommandRPopLPUSH : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::List list_db(svr->storage, conn->GetNamespace());
    std::string elem;
    auto s = list_db.LMove(args_[1], args_[2], /*src_left=*/false, /*dst_left=*/true, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::NilString() : redis::BulkString(elem);
    return Status::OK();
  }
};

class CommandLMove : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto arg_val = util::ToLower(args_[3]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    src_left_ = arg_val == "left";
    arg_val = util::ToLower(args_[4]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    dst_left_ = arg_val == "left";
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::List list_db(svr->storage, conn->GetNamespace());
    std::string elem;
    auto s = list_db.LMove(args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = s.IsNotFound() ? redis::NilString() : redis::BulkString(elem);
    return Status::OK();
  }

 private:
  bool src_left_;
  bool dst_left_;
};

class CommandBLMove : public Commander,
                      private EvbufCallbackBase<CommandBLMove, false>,
                      private EventCallbackBase<CommandBLMove> {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto arg_val = util::ToLower(args_[3]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    src_left_ = arg_val == "left";
    arg_val = util::ToLower(args_[4]);
    if (arg_val != "left" && arg_val != "right") {
      return {Status::RedisParseErr, errInvalidSyntax};
    }
    dst_left_ = arg_val == "left";

    auto parse_result = ParseFloat(args[args.size() - 1]);
    if (!parse_result) {
      return {Status::RedisParseErr, errTimeoutIsNotFloat};
    }
    if (*parse_result < 0) {
      return {Status::RedisParseErr, errTimeoutIsNegative};
    }
    timeout_ = static_cast<int64_t>(*parse_result * 1000 * 1000);

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr_ = svr;
    conn_ = conn;

    redis::List list_db(svr->storage, conn->GetNamespace());
    std::string elem;
    auto s = list_db.LMove(args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }
    if (!elem.empty()) {
      *output = redis::BulkString(elem);
      return Status::OK();
    }

    if (conn->IsInExec()) {
      *output = redis::MultiLen(-1);
      return Status::OK();  // no blocking in multi-exec
    }

    svr_->BlockOnKey(args_[1], conn_);
    auto bev = conn->GetBufferEvent();
    SetCB(bev);

    if (timeout_) {
      timer_.reset(NewTimer(bufferevent_get_base(bev)));
      int64_t timeout_second = timeout_ / 1000 / 1000;
      int64_t timeout_microsecond = timeout_ % (1000 * 1000);
      timeval tm = {timeout_second, static_cast<int>(timeout_microsecond)};
      evtimer_add(timer_.get(), &tm);
    }

    return {Status::BlockingCmd};
  }

  void OnWrite(bufferevent *bev) {
    redis::List list_db(svr_->storage, conn_->GetNamespace());
    std::string elem;
    auto s = list_db.LMove(args_[1], args_[2], src_left_, dst_left_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      conn_->Reply(redis::Error("ERR " + s.ToString()));
      return;
    }

    if (elem.empty()) {
      // The connection may be waked up but can't pop from a zset. For example, connection A is blocked on zset and
      // connection B added a new element; then connection A was unblocked, but this element may be taken by
      // another connection C. So we need to block connection A again and wait for the element being added
      // by disabling the WRITE event.
      bufferevent_disable(bev, EV_WRITE);
      return;
    }

    conn_->Reply(redis::BulkString(elem));

    if (timer_) {
      timer_.reset();
    }

    unblockOnSrc();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
    // We need to manually trigger the read event since we will stop processing commands
    // in connection after the blocking command, so there may have some commands to be processed.
    // Related issue: https://github.com/apache/kvrocks/issues/831
    bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
  }

  void OnEvent(bufferevent *bev, int16_t events) {
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (timer_ != nullptr) {
        timer_.reset();
      }
      unblockOnSrc();
    }
    conn_->OnEvent(bev, events);
  }

  void TimerCB(int, int16_t) {
    conn_->Reply(redis::MultiLen(-1));
    timer_.reset();
    unblockOnSrc();
    auto bev = conn_->GetBufferEvent();
    conn_->SetCB(bev);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  bool src_left_;
  bool dst_left_;
  int64_t timeout_ = 0;  // microseconds
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  UniqueEvent timer_;

  void unblockOnSrc() { svr_->UnblockOnKey(args_[1], conn_); }
};

class CommandLPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 9) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    CommandParser parser(args, 3);
    while (parser.Good()) {
      if (parser.EatEqICase("rank")) {
        spec_.rank = GET_OR_RET(parser.TakeInt());
        if (spec_.rank == 0) {
          return {Status::RedisParseErr,
                  "RANK can't be zero: use 1 to start from "
                  "the first match, 2 from the second ... "
                  "or use negative to start from the end of the list"};
        } else if (spec_.rank == LLONG_MIN) {
          // Negating LLONG_MIN will cause an overflow, and is effectively be the same as passing -1.
          return {Status::RedisParseErr, "rank would overflow"};
        }
      } else if (parser.EatEqICase("count")) {
        spec_.count = GET_OR_RET(parser.TakeInt());
        if (spec_.count < 0) {
          return {Status::RedisParseErr, "COUNT can't be negative"};
        }
      } else if (parser.EatEqICase("maxlen")) {
        spec_.max_len = GET_OR_RET(parser.TakeInt());
        if (spec_.max_len < 0) {
          return {Status::RedisParseErr, "MAXLEN can't be negative"};
        }
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    redis::List list_db(svr->storage, conn->GetNamespace());
    std::vector<int64_t> indexes;
    auto s = list_db.Pos(args_[1], args_[2], spec_, &indexes);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    // We return nil or a single value if `COUNT` option is not given.
    if (!spec_.count.has_value()) {
      if (s.IsNotFound() || indexes.empty()) {
        *output = redis::NilString();
      } else {
        assert(indexes.size() == 1);
        *output = redis::Integer(indexes[0]);
      }
    }
    // Otherwise we return an array.
    else {
      std::vector<std::string> values;
      values.reserve(indexes.size());
      for (const auto &index : indexes) {
        values.emplace_back(std::to_string(index));
      }
      *output = redis::MultiBulkString(values, false);
    }
    return Status::OK();
  }

 private:
  PosSpec spec_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandBLPop>("blpop", -3, "write no-script", 1, -2, 1),
                        MakeCmdAttr<CommandBRPop>("brpop", -3, "write no-script", 1, -2, 1),
                        MakeCmdAttr<CommandLIndex>("lindex", 3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLInsert>("linsert", 5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLLen>("llen", 2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLMove>("lmove", 5, "write", 1, 2, 1),
                        MakeCmdAttr<CommandBLMove>("blmove", 6, "write", 1, 2, 1),
                        MakeCmdAttr<CommandLPop>("lpop", -2, "write", 1, 1, 1),  //
                        MakeCmdAttr<CommandLPos>("lpos", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLPush>("lpush", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLPushX>("lpushx", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLRange>("lrange", 4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandLRem>("lrem", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLSet>("lset", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLTrim>("ltrim", 4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandLMPop>("lmpop", -4, "write", CommandLMPop::keyRangeGen),
                        MakeCmdAttr<CommandRPop>("rpop", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandRPopLPUSH>("rpoplpush", 3, "write", 1, 2, 1),
                        MakeCmdAttr<CommandRPush>("rpush", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandRPushX>("rpushx", -3, "write", 1, 1, 1), )

}  // namespace redis
