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
#include "types/redis_stream.h"

namespace Redis {

class CommandXAdd : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool entry_id_found = false;
    stream_name_ = args[1];

    for (size_t i = 2; i < args.size();) {
      auto val = entry_id_found ? args[i] : Util::ToLower(args[i]);

      if (val == "nomkstream" && !entry_id_found) {
        nomkstream_ = true;
        ++i;
        continue;
      }

      if (val == "maxlen" && !entry_id_found) {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        size_t max_len_idx = 0;
        bool eq_sign_found = false;
        if (args[i + 1] == "=") {
          max_len_idx = i + 2;
          eq_sign_found = true;
        } else {
          max_len_idx = i + 1;
        }

        if (max_len_idx >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        auto parse_result = ParseInt<uint64_t>(args[max_len_idx], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        max_len_ = *parse_result;
        with_max_len_ = true;

        i += eq_sign_found ? 3 : 2;
        continue;
      }

      if (val == "minid" && !entry_id_found) {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        size_t min_id_idx = 0;
        bool eq_sign_found = false;
        if (args[i + 1] == "=") {
          min_id_idx = i + 2;
          eq_sign_found = true;
        } else {
          min_id_idx = i + 1;
        }

        if (min_id_idx >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        auto s = ParseStreamEntryID(args[min_id_idx], &min_id_);
        if (!s.IsOK()) {
          return {Status::RedisParseErr, s.Msg()};
        }

        with_min_id_ = true;
        i += eq_sign_found ? 3 : 2;
        continue;
      }

      if (val == "limit" && !entry_id_found) {
        return {Status::RedisParseErr, errLimitOptionNotAllowed};
      }

      if (val == "*" && !entry_id_found) {
        entry_id_found = true;
        ++i;
        continue;
      } else if (!entry_id_found) {
        auto s = ParseNewStreamEntryID(val, &entry_id_);
        if (!s.IsOK()) {
          return {Status::RedisParseErr, s.Msg()};
        }

        entry_id_found = true;
        with_entry_id_ = true;
        ++i;
        continue;
      }

      if (entry_id_found) {
        name_value_pairs_.push_back(val);
        ++i;
      }
    }

    if (name_value_pairs_.empty() || name_value_pairs_.size() % 2 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::StreamAddOptions options;
    options.nomkstream = nomkstream_;
    if (with_max_len_) {
      options.trim_options.strategy = StreamTrimStrategy::MaxLen;
      options.trim_options.max_len = max_len_;
    }
    if (with_min_id_) {
      options.trim_options.strategy = StreamTrimStrategy::MinID;
      options.trim_options.min_id = min_id_;
    }
    if (with_entry_id_) {
      options.with_entry_id = true;
      options.entry_id = entry_id_;
    }

    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    StreamEntryID entry_id;
    auto s = stream_db.Add(stream_name_, options, name_value_pairs_, &entry_id);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound() && nomkstream_) {
      *output = Redis::NilString();
      return Status::OK();
    }

    *output = Redis::BulkString(entry_id.ToString());

    svr->OnEntryAddedToStream(conn->GetNamespace(), stream_name_, entry_id);

    return Status::OK();
  }

 private:
  std::string stream_name_;
  uint64_t max_len_ = 0;
  Redis::StreamEntryID min_id_;
  Redis::NewStreamEntryID entry_id_;
  std::vector<std::string> name_value_pairs_;
  bool nomkstream_ = false;
  bool with_max_len_ = false;
  bool with_min_id_ = false;
  bool with_entry_id_ = false;
};

class CommandXDel : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); ++i) {
      Redis::StreamEntryID id;
      auto s = ParseStreamEntryID(args[i], &id);
      if (!s.IsOK()) {
        return s;
      }

      ids_.push_back(id);
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    uint64_t deleted = 0;
    auto s = stream_db.DeleteEntries(args_[1], ids_, &deleted);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(deleted);

    return Status::OK();
  }

 private:
  std::vector<Redis::StreamEntryID> ids_;
};

class CommandXLen : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 2) {
      Redis::StreamEntryID id;
      auto s = Redis::ParseStreamEntryID(args[2], &id);
      if (!s.IsOK()) return s;

      options_.with_entry_id = true;
      options_.entry_id = id;

      if (args.size() > 3) {
        if (args[3] == "-") {
          options_.to_first = true;
        } else if (args[3] != "+") {
          return {Status::RedisParseErr, errInvalidSyntax};
        }
      }
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    uint64_t len = 0;
    auto s = stream_db.Len(args_[1], options_, &len);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(len);

    return Status::OK();
  }

 private:
  Redis::StreamLenOptions options_;
};

class CommandXInfo : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    auto val = Util::ToLower(args[1]);
    if (val == "stream" && args.size() >= 2) {
      stream_ = true;

      if (args.size() > 3 && Util::ToLower(args[3]) == "full") {
        full_ = true;
      }

      if (args.size() > 5 && Util::ToLower(args[4]) == "count") {
        auto parse_result = ParseInt<uint64_t>(args[5], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
      }
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (stream_) {
      return getStreamInfo(svr, conn, output);
    }
    return Status::OK();
  }

 private:
  uint64_t count_ = 10;  // default Redis value
  bool stream_ = false;
  bool full_ = false;

  Status getStreamInfo(Server *svr, Connection *conn, std::string *output) {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());
    Redis::StreamInfo info;
    auto s = stream_db.GetStreamInfo(args_[2], full_, count_, &info);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      return {Status::RedisExecErr, errNoSuchKey};
    }

    if (!full_) {
      output->append(Redis::MultiLen(14));
    } else {
      output->append(Redis::MultiLen(12));
    }
    output->append(Redis::BulkString("length"));
    output->append(Redis::Integer(info.size));
    output->append(Redis::BulkString("last-generated-id"));
    output->append(Redis::BulkString(info.last_generated_id.ToString()));
    output->append(Redis::BulkString("max-deleted-entry-id"));
    output->append(Redis::BulkString(info.max_deleted_entry_id.ToString()));
    output->append(Redis::BulkString("entries-added"));
    output->append(Redis::Integer(info.entries_added));
    output->append(Redis::BulkString("recorded-first-entry-id"));
    output->append(Redis::BulkString(info.recorded_first_entry_id.ToString()));
    if (!full_) {
      output->append(Redis::BulkString("first-entry"));
      if (info.first_entry) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(info.first_entry->key));
        output->append(Redis::MultiBulkString(info.first_entry->values));
      } else {
        output->append(Redis::NilString());
      }
      output->append(Redis::BulkString("last-entry"));
      if (info.last_entry) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(info.last_entry->key));
        output->append(Redis::MultiBulkString(info.last_entry->values));
      } else {
        output->append(Redis::NilString());
      }
    } else {
      output->append(Redis::BulkString("entries"));
      output->append(Redis::MultiLen(info.entries.size()));
      for (const auto &e : info.entries) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(e.key));
        output->append(Redis::MultiBulkString(e.values));
      }
    }

    return Status::OK();
  }
};

class CommandXRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    if (args[2] == "-") {
      start_ = Redis::StreamEntryID::Minimum();
    } else if (args[2][0] == '(') {
      exclude_start_ = true;
      auto s = ParseRangeStart(args[2].substr(1), &start_);
      if (!s.IsOK()) return s;
    } else if (args[2] == "+") {
      start_ = Redis::StreamEntryID::Maximum();
    } else {
      auto s = ParseRangeStart(args[2], &start_);
      if (!s.IsOK()) return s;
    }

    if (args[3] == "+") {
      end_ = Redis::StreamEntryID::Maximum();
    } else if (args[3][0] == '(') {
      exclude_end_ = true;
      auto s = ParseRangeEnd(args[3].substr(1), &end_);
      if (!s.IsOK()) return s;
    } else if (args[3] == "-") {
      end_ = Redis::StreamEntryID::Minimum();
    } else {
      auto s = ParseRangeEnd(args[3], &end_);
      if (!s.IsOK()) return s;
    }

    if (args.size() >= 5 && Util::ToLower(args[4]) == "count") {
      if (args.size() != 6) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      with_count_ = true;

      auto parse_result = ParseInt<uint64_t>(args[5], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (with_count_ && count_ == 0) {
      *output = Redis::NilString();
      return Status::OK();
    }

    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    Redis::StreamRangeOptions options;
    options.reverse = false;
    options.start = start_;
    options.end = end_;
    options.with_count = with_count_;
    options.count = count_;
    options.exclude_start = exclude_start_;
    options.exclude_end = exclude_end_;

    std::vector<StreamEntry> result;
    auto s = stream_db.Range(stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(result.size()));

    for (const auto &e : result) {
      output->append(Redis::MultiLen(2));
      output->append(Redis::BulkString(e.key));
      output->append(Redis::MultiBulkString(e.values));
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_ = 0;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
  bool with_count_ = false;
};

class CommandXRevRange : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    if (args[2] == "+") {
      start_ = Redis::StreamEntryID::Maximum();
    } else if (args[2][0] == '(') {
      exclude_start_ = true;
      auto s = ParseRangeEnd(args[2].substr(1), &start_);
      if (!s.IsOK()) return s;
    } else if (args[2] == "-") {
      start_ = Redis::StreamEntryID::Minimum();
    } else {
      auto s = ParseRangeEnd(args[2], &start_);
      if (!s.IsOK()) return s;
    }

    if (args[3] == "-") {
      end_ = Redis::StreamEntryID::Minimum();
    } else if (args[3][0] == '(') {
      exclude_end_ = true;
      auto s = ParseRangeStart(args[3].substr(1), &end_);
      if (!s.IsOK()) return s;
    } else if (args[3] == "+") {
      end_ = Redis::StreamEntryID::Maximum();
    } else {
      auto s = ParseRangeStart(args[3], &end_);
      if (!s.IsOK()) return s;
    }

    if (args.size() >= 5 && Util::ToLower(args[4]) == "count") {
      if (args.size() != 6) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      with_count_ = true;

      auto parse_result = ParseInt<uint64_t>(args[5]);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      count_ = *parse_result;
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (with_count_ && count_ == 0) {
      *output = Redis::NilString();
      return Status::OK();
    }

    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    Redis::StreamRangeOptions options;
    options.reverse = true;
    options.start = start_;
    options.end = end_;
    options.with_count = with_count_;
    options.count = count_;
    options.exclude_start = exclude_start_;
    options.exclude_end = exclude_end_;

    std::vector<StreamEntry> result;
    auto s = stream_db.Range(stream_name_, options, &result);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    output->append(Redis::MultiLen(result.size()));

    for (const auto &e : result) {
      output->append(Redis::MultiLen(2));
      output->append(Redis::BulkString(e.key));
      output->append(Redis::MultiBulkString(e.values));
    }

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID start_;
  StreamEntryID end_;
  uint64_t count_ = 0;
  bool exclude_start_ = false;
  bool exclude_end_ = false;
  bool with_count_ = false;
};

class CommandXRead : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    size_t streams_word_idx = 0;

    for (size_t i = 1; i < args.size();) {
      auto arg = Util::ToLower(args[i]);

      if (arg == "streams") {
        streams_word_idx = i;
        break;
      }

      if (arg == "count") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        with_count_ = true;

        auto parse_result = ParseInt<uint64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
        continue;
      }

      if (arg == "block") {
        if (i + 1 >= args.size()) {
          return {Status::RedisParseErr, errInvalidSyntax};
        }

        block_ = true;

        auto parse_result = ParseInt<int64_t>(args[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        if (*parse_result < 0) {
          return {Status::RedisParseErr, errTimeoutIsNegative};
        }

        block_timeout_ = *parse_result;
        i += 2;
        continue;
      }

      ++i;
    }

    if (streams_word_idx == 0) {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    if ((args.size() - streams_word_idx - 1) % 2 != 0) {
      return {Status::RedisParseErr, errUnbalancedStreamList};
    }

    size_t number_of_streams = (args.size() - streams_word_idx - 1) / 2;

    for (size_t i = streams_word_idx + 1; i <= streams_word_idx + number_of_streams; ++i) {
      streams_.push_back(args[i]);
      const auto &id_str = args[i + number_of_streams];
      bool get_latest = id_str == "$";
      latest_marks_.push_back(get_latest);
      StreamEntryID id;
      if (!get_latest) {
        auto s = ParseStreamEntryID(id_str, &id);
        if (!s.IsOK()) {
          return s;
        }
      }
      ids_.push_back(id);
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    std::vector<Redis::StreamReadResult> results;

    for (size_t i = 0; i < streams_.size(); ++i) {
      if (latest_marks_[i]) {
        continue;
      }

      Redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = ids_[i];
      options.end = StreamEntryID{UINT64_MAX, UINT64_MAX};
      options.with_count = with_count_;
      options.count = count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.Range(streams_[i], options, &result);
      if (!s.ok() && !s.IsNotFound()) {
        return {Status::RedisExecErr, s.ToString()};
      }

      if (result.size() > 0) {
        results.emplace_back(streams_[i], result);
      }
    }

    if (block_ && results.empty()) {
      if (conn->IsInExec()) {
        *output = Redis::MultiLen(-1);
        return Status::OK();  // No blocking in multi-exec
      }

      return blockingRead(svr, conn, &stream_db);
    }

    if (!block_ && results.empty()) {
      *output = Redis::MultiLen(-1);
      return Status::OK();
    }

    return sendResults(output, results);
  }

  static Status sendResults(std::string *output, const std::vector<StreamReadResult> &results) {
    output->append(Redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output->append(Redis::MultiLen(2));
      output->append(Redis::BulkString(result.name));
      output->append(Redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output->append(Redis::MultiLen(2));
        output->append(Redis::BulkString(entry.key));
        output->append(Redis::MultiBulkString(entry.values));
      }
    }

    return Status::OK();
  }

  Status blockingRead(Server *svr, Connection *conn, Redis::Stream *stream_db) {
    if (!with_count_) {
      with_count_ = true;
      count_ = blocked_default_count_;
    }

    for (size_t i = 0; i < streams_.size(); ++i) {
      if (latest_marks_[i]) {
        StreamEntryID last_generated_id;
        auto s = stream_db->GetLastGeneratedID(streams_[i], &last_generated_id);
        if (!s.ok()) {
          return {Status::RedisExecErr, s.ToString()};
        }

        ids_[i] = last_generated_id;
      }
    }

    svr_ = svr;
    conn_ = conn;

    svr_->BlockOnStreams(streams_, ids_, conn_);

    auto bev = conn->GetBufferEvent();
    bufferevent_setcb(bev, nullptr, WriteCB, EventCB, this);

    if (block_timeout_ > 0) {
      timer_ = evtimer_new(bufferevent_get_base(bev), TimerCB, this);
      timeval tm;
      if (block_timeout_ > 1000) {
        tm.tv_sec = block_timeout_ / 1000;
        tm.tv_usec = (block_timeout_ % 1000) * 1000;
      } else {
        tm.tv_sec = 0;
        tm.tv_usec = block_timeout_ * 1000;
      }

      evtimer_add(timer_, &tm);
    }

    return {Status::BlockingCmd};
  }

  static void WriteCB(bufferevent *bev, void *ctx) {
    auto command = reinterpret_cast<CommandXRead *>(ctx);

    if (command->timer_ != nullptr) {
      event_free(command->timer_);
      command->timer_ = nullptr;
    }

    command->unblockAll();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      command->conn_);
    bufferevent_enable(bev, EV_READ);

    Redis::Stream stream_db(command->svr_->storage_, command->conn_->GetNamespace());

    std::vector<StreamReadResult> results;

    for (size_t i = 0; i < command->streams_.size(); ++i) {
      Redis::StreamRangeOptions options;
      options.reverse = false;
      options.start = command->ids_[i];
      options.end = StreamEntryID{UINT64_MAX, UINT64_MAX};
      options.with_count = command->with_count_;
      options.count = command->count_;
      options.exclude_start = true;
      options.exclude_end = false;

      std::vector<StreamEntry> result;
      auto s = stream_db.Range(command->streams_[i], options, &result);
      if (!s.ok()) {
        command->conn_->Reply(Redis::MultiLen(-1));
        LOG(ERROR) << "ERR executing XRANGE for stream " << command->streams_[i] << " from "
                   << command->ids_[i].ToString() << " to " << options.end.ToString() << " with count "
                   << command->count_ << ": " << s.ToString();
      }

      if (result.size() > 0) {
        results.emplace_back(command->streams_[i], result);
      }
    }

    if (results.empty()) {
      command->conn_->Reply(Redis::MultiLen(-1));
    }

    command->sendReply(results);
  }

  void sendReply(const std::vector<StreamReadResult> &results) {
    std::string output;

    output.append(Redis::MultiLen(results.size()));

    for (const auto &result : results) {
      output.append(Redis::MultiLen(2));
      output.append(Redis::BulkString(result.name));
      output.append(Redis::MultiLen(result.entries.size()));
      for (const auto &entry : result.entries) {
        output.append(Redis::MultiLen(2));
        output.append(Redis::BulkString(entry.key));
        output.append(Redis::MultiBulkString(entry.values));
      }
    }

    conn_->Reply(output);
  }

  static void EventCB(bufferevent *bev, int16_t events, void *ctx) {
    auto command = static_cast<CommandXRead *>(ctx);

    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      if (command->timer_ != nullptr) {
        event_free(command->timer_);
        command->timer_ = nullptr;
      }
      command->unblockAll();
    }
    Redis::Connection::OnEvent(bev, events, command->conn_);
  }

  static void TimerCB(int, int16_t events, void *ctx) {
    auto command = reinterpret_cast<CommandXRead *>(ctx);

    command->conn_->Reply(Redis::NilString());

    event_free(command->timer_);
    command->timer_ = nullptr;

    command->unblockAll();

    auto bev = command->conn_->GetBufferEvent();
    bufferevent_setcb(bev, Redis::Connection::OnRead, Redis::Connection::OnWrite, Redis::Connection::OnEvent,
                      command->conn_);
    bufferevent_enable(bev, EV_READ);
  }

 private:
  std::vector<std::string> streams_;
  std::vector<StreamEntryID> ids_;
  std::vector<bool> latest_marks_;
  Server *svr_ = nullptr;
  Connection *conn_ = nullptr;
  event *timer_ = nullptr;
  uint64_t count_ = 0;
  int64_t block_timeout_ = 0;
  int blocked_default_count_ = 1000;
  bool with_count_ = false;
  bool block_ = false;

  void unblockAll() { svr_->UnblockOnStreams(streams_, conn_); }
};

class CommandXTrim : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    bool eq_sign_found = false;

    auto trim_strategy = Util::ToLower(args[2]);
    if (trim_strategy == "maxlen") {
      strategy_ = StreamTrimStrategy::MaxLen;

      size_t max_len_idx = 0;
      if (args[3] != "=") {
        max_len_idx = 3;
      } else {
        max_len_idx = 4;
        eq_sign_found = true;
      }

      if (max_len_idx >= args.size()) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto parse_result = ParseInt<uint64_t>(args[max_len_idx], 10);
      if (!parse_result) {
        return {Status::RedisParseErr, errValueNotInteger};
      }

      max_len_ = *parse_result;
    } else if (trim_strategy == "minid") {
      strategy_ = StreamTrimStrategy::MinID;

      size_t min_id_idx = 0;
      if (args[3] != "=") {
        min_id_idx = 3;
      } else {
        min_id_idx = 4;
        eq_sign_found = true;
      }

      if (min_id_idx >= args.size()) {
        return {Status::RedisParseErr, errInvalidSyntax};
      }

      auto s = ParseStreamEntryID(args[min_id_idx], &min_id_);
      if (!s.IsOK()) {
        return s;
      }
    } else {
      return {Status::RedisParseErr, errInvalidSyntax};
    }

    bool limit_option_found = false;
    if (eq_sign_found) {
      if (args.size() > 6 && Util::ToLower(args[5]) == "limit") {
        limit_option_found = true;
      }
    } else {
      if (args.size() > 5 && Util::ToLower(args[4]) == "limit") {
        limit_option_found = true;
      }
    }

    if (limit_option_found) {
      return {Status::RedisParseErr, errLimitOptionNotAllowed};
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    StreamTrimOptions options;
    options.strategy = strategy_;
    options.max_len = max_len_;
    options.min_id = min_id_;

    uint64_t removed = 0;
    auto s = stream_db.Trim(args_[1], options, &removed);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(removed);

    return Status::OK();
  }

 private:
  uint64_t max_len_ = 0;
  StreamEntryID min_id_;
  StreamTrimStrategy strategy_ = StreamTrimStrategy::None;
};

class CommandXSetId : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    stream_name_ = args[1];

    auto s = Redis::ParseStreamEntryID(args[2], &last_id_);
    if (!s.IsOK()) {
      return {Status::RedisParseErr, s.Msg()};
    }

    if (args.size() == 3) {
      return Status::OK();
    }

    for (size_t i = 3; i < args.size(); /* manual increment */) {
      if (Util::EqualICase(args[i], "entriesadded") && i + 1 < args.size()) {
        auto parse_result = ParseInt<uint64_t>(args[i + 1]);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        entries_added_ = *parse_result;
        i += 2;
      } else if (Util::EqualICase(args[i], "maxdeletedid") && i + 1 < args.size()) {
        StreamEntryID id;
        s = Redis::ParseStreamEntryID(args[i + 1], &id);
        if (!s.IsOK()) {
          return {Status::RedisParseErr, s.Msg()};
        }

        max_deleted_id_ = std::make_optional<StreamEntryID>(id.ms, id.seq);
        i += 2;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }

    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Redis::Stream stream_db(svr->storage_, conn->GetNamespace());

    auto s = stream_db.SetId(stream_name_, last_id_, entries_added_, max_deleted_id_);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::SimpleString("OK");

    return Status::OK();
  }

 private:
  std::string stream_name_;
  StreamEntryID last_id_;
  std::optional<StreamEntryID> max_deleted_id_;
  std::optional<uint64_t> entries_added_;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandXAdd>("xadd", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandXDel>("xdel", -3, "write", 1, 1, 1),
                        MakeCmdAttr<CommandXLen>("xlen", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXInfo>("xinfo", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandXRange>("xrange", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXRevRange>("xrevrange", -2, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandXRead>("xread", -4, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandXTrim>("xtrim", -4, "write", 1, 1, 1),
                        MakeCmdAttr<CommandXSetId>("xsetid", -3, "write", 1, 1, 1))

}  // namespace Redis
