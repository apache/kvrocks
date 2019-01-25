#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <thread>

#include "redis_cmd.h"
#include "redis_hash.h"
#include "redis_list.h"
#include "redis_request.h"
#include "redis_set.h"
#include "redis_string.h"
#include "redis_zset.h"
#include "replication.h"
#include "util.h"
#include "storage.h"
#include "worker.h"

namespace Redis {
class CommandAuth : public Commander {
 public:
  explicit CommandAuth() : Commander("auth", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Config *config = svr->GetConfig();
    auto iter = config->tokens.find(args_[1]);
    if (iter == config->tokens.end()) {
      *output = Redis::Error("ERR invalid password");
    } else {
      conn->SetNamespace(iter->second);
      if (args_[1] == config->requirepass) {
        conn->BecomeAdmin();
      } else {
        conn->BecomeUser();
      }
      *output = Redis::SimpleString("OK");
    }
    return Status::OK();
  }
};

class CommandNamespace : public Commander {
 public:
  explicit CommandNamespace() : Commander("namespace", -3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error("only administrator can use namespace command");
      return Status::OK();
    }
    Config *config = svr->GetConfig();
    if (args_.size() == 3 && args_[1] == "get") {
      if (args_[2] == "*") {
        std::vector<std::string> namespaces;
        auto tokens = config->tokens;
        for (auto iter = tokens.begin(); iter != tokens.end(); iter++) {
          namespaces.emplace_back(iter->second);  // namespace
          namespaces.emplace_back(iter->first);   // token
        }
        *output = Redis::MultiBulkString(namespaces);
      } else {
        std::string token;
        config->GetNamespace(args_[2], &token);
        *output = Redis::BulkString(token);
      }
    } else if (args_.size() == 4 && args_[1] == "set") {
      Status s = config->SetNamepsace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
    } else if (args_.size() == 4 && args_[1] == "add") {
      Status s = config->AddNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
    } else if (args_.size() == 3 && args_[1] == "del") {
      Status s = config->DelNamespace(args_[2]);
      *output = s.IsOK() ? Redis::SimpleString("OK") : Redis::Error(s.Msg());
    } else {
      *output = Redis::Error(
          "NAMESPACE subcommand must be one of GET, SET, DEL, ADD");
    }
    return Status::OK();
  }
};

class CommandKeys : public Commander {
 public:
  explicit CommandKeys() : Commander("keys", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string prefix = args_[1];
    std::vector<std::string> keys;
    RedisDB redis(svr->storage_, conn->GetNamespace());
    if (prefix == "*") {
      redis.Keys(std::string(), &keys);
    } else {
      if (prefix[prefix.size() - 1] != '*') {
        *output = Redis::Error("ERR only keys prefix match was supported");
        return Status::OK();
      }
      redis.Keys(prefix.substr(0, prefix.size() - 1), &keys);
    }
    *output = Redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandFlushAll : public Commander {
 public:
  explicit CommandFlushAll() : Commander("flushall", 1, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisDB redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.FlushAll();
    if (s.ok()) {
      *output = Redis::SimpleString("OK");
      return Status::OK();
    }
    return Status(Status::RedisExecErr, s.ToString());
  }
};

class CommandPing : public Commander {
 public:
  explicit CommandPing() : Commander("ping", 1, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = Redis::SimpleString("PONG");
    return Status::OK();
  }
};

class CommandConfig : public Commander {
 public:
  explicit CommandConfig() : Commander("config", -2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error("only administrator can use config command");
      return Status::OK();
    }

    std::string err;
    Config *config = svr->GetConfig();
    if (args_.size() == 2 && Util::ToLower(args_[1]) == "rewrite") {
      Status s = config->Rewrite();
      if (!s.IsOK()) return Status(Status::RedisExecErr, s.Msg());
      *output = Redis::SimpleString("OK");
      LOG(INFO) << "# CONFIG REWRITE executed with success.";
    } else if (args_.size() == 3 && Util::ToLower(args_[1]) == "get") {
      std::vector<std::string> values;
      config->Get(args_[2], &values);
      *output = Redis::MultiBulkString(values);
    } else if (args_.size() == 4 && Util::ToLower(args_[1]) == "set") {
      Status s = config->Set(args_[2], args_[3]);
      if (!s.IsOK()) {
        return Status(Status::NotOK, s.Msg() + ", key: " + args_[2]);
      }
      *output = Redis::SimpleString("OK");
    } else {
      *output = Redis::Error("CONFIG subcommand must be one of GET, SET, REWRITE");
    }
    return Status::OK();
  }
};

class CommandGet : public Commander {
 public:
  explicit CommandGet() : Commander("get", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string value;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Get(args_[1], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }
};

class CommandMGet : public Commander {
 public:
  explicit CommandMGet() : Commander("mget", -2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisString string_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> keys;
    for (size_t i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    std::vector<std::string> values;
    // always return OK
    string_db.MGet(keys, &values);
    *output = Redis::MultiBulkString(values);
    return Status::OK();
  }
};

class CommandSet : public Commander {
 public:
  explicit CommandSet() : Commander("set", 3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Set(args_[1], args_[2]);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandMSet : public Commander {
 public:
  explicit CommandMSet() : Commander("mset", -3, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 1) {
      return Status(Status::RedisParseErr, "wrong number of arguments");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisString string_db(svr->storage_, conn->GetNamespace());
    std::vector<StringPair> kvs;
    for(size_t i = 1; i < args_.size(); i+=2) {
      kvs.emplace_back(StringPair{args_[i], args_[i+1]});
    }
    rocksdb::Status s = string_db.MSet(kvs);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandSetNX : public Commander {
 public:
  explicit CommandSetNX() : Commander("setnx", 3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.SetNX(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncr : public Commander {
 public:
  explicit CommandIncr() : Commander("incr", 2, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], 1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr : public Commander {
 public:
  explicit CommandDecr() : Commander("decr", 2, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], -1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncrBy : public Commander {
 public:
  explicit CommandIncrBy() : Commander("incrby", 3, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], increment_, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_;
};

class CommandDecrBy : public Commander {
 public:
  explicit CommandDecrBy() : Commander("decrby", 3, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], -1 * increment_, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_;
};

class CommandDel : public Commander {
 public:
  explicit CommandDel() : Commander("del", -2, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int cnt = 0;
    RedisDB redis(svr->storage_, conn->GetNamespace());
    for (unsigned int i = 1; i < args_.size(); i++) {
      rocksdb::Status s = redis.Del(args_[i]);
      if (s.ok()) cnt++;
    }
    *output = Redis::Integer(cnt);
    return Status::OK();
  }
};

class CommandType : public Commander {
 public:
  explicit CommandType() : Commander("type", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisDB redis(svr->storage_, conn->GetNamespace());
    RedisType type;
    rocksdb::Status s = redis.Type(args_[1], &type);
    if (s.ok()) {
      std::vector<std::string> type_names = {"none", "string", "hash",
                                             "list", "set",    "zset"};
      *output = Redis::BulkString(type_names[type]);
      return Status::OK();
    }
    return Status(Status::RedisExecErr, s.ToString());
  }
};

class CommandTTL : public Commander {
 public:
  explicit CommandTTL() : Commander("ttl", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisDB redis(svr->storage_, conn->GetNamespace());
    int ttl;
    rocksdb::Status s = redis.TTL(args_[1], &ttl);
    if (s.ok()) {
      *output = Redis::Integer(ttl);
      return Status::OK();
    } else {
      return Status(Status::RedisExecErr, s.ToString());
    }
  }
};

class CommandExists : public Commander {
 public:
  explicit CommandExists() : Commander("exists", -2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int cnt = 0;
    RedisDB redis(svr->storage_, conn->GetNamespace());
    std::vector<rocksdb::Slice> keys;
    for (unsigned i = 1; i < args_.size(); i++) {
      keys.emplace_back(args_[i]);
    }
    redis.Exists(keys, &cnt);
    *output = Redis::Integer(cnt);
    return Status::OK();
  }
};

class CommandExpire : public Commander {
 public:
  explicit CommandExpire() : Commander("expire", 3, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    try {
      seconds_ = std::stoi(args[2]);
      if (seconds_ >= INT32_MAX - now) {
        return Status(Status::RedisParseErr, "the expire time was overflow");
      }
      seconds_ += now;
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisDB redis(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = redis.Expire(args_[1], seconds_);
    if (s.ok()) {
      *output = Redis::Integer(1);
    } else {
      *output = Redis::Integer(0);
    }
    return Status::OK();
  }

 private:
  int seconds_;
};

class CommandHGet : public Commander {
 public:
  explicit CommandHGet() : Commander("hget", 3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    rocksdb::Status s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(value);
    return Status::OK();
  }
};

class CommandHSet : public Commander {
 public:
  explicit CommandHSet() : Commander("hset", 4, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.Set(args_[1], args_[2], args_[3], &ret);
    if (s.ok()) {
      *output = Redis::Integer(ret);
      return Status::OK();
    } else {
      return Status(Status::RedisExecErr, s.ToString());
    }
  }
};

class CommandHSetNX : public Commander {
 public:
  explicit CommandHSetNX() : Commander("hsetnx", 4, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.SetNX(args_[1], args_[2], args_[3], &ret);
    if (s.ok()) {
      *output = Redis::Integer(ret);
      return Status::OK();
    } else {
      return Status(Status::RedisExecErr, s.ToString());
    }
  }
};

class CommandHStrlen : public Commander {
 public:
  explicit CommandHStrlen() : Commander("hstrlen", 3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    rocksdb::Status s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(int(value.size()));
    return Status::OK();
  }
};

class CommandHDel : public Commander {
 public:
  explicit CommandHDel() : Commander("hdel", -3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> fields;
    for (unsigned int i = 2; i < args_.size(); i++) {
      fields.emplace_back(Slice(args_[i]));
    }
    rocksdb::Status s = hash_db.Delete(args_[1], fields, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHExists : public Commander {
 public:
  explicit CommandHExists() : Commander("hexists", 3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::string value;
    rocksdb::Status s = hash_db.Get(args_[1], args_[2], &value);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::Integer(0) : Redis::Integer(1);
    return Status::OK();
  }
};

class CommandHLen : public Commander {
 public:
  explicit CommandHLen() : Commander("hlen", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint32_t count;
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = hash_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::Integer(0) : Redis::Integer(count);
    return Status::OK();
  }
};

class CommandHIncrBy : public Commander {
 public:
  explicit CommandHIncrBy() : Commander("hincrby", 4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    long long ret;
    rocksdb::Status s = hash_db.IncrBy(args_[1], args_[2], increment_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  long long increment_;
};

class CommandHMGet : public Commander {
 public:
  explicit CommandHMGet() : Commander("hmget", -3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> fields;
    for (unsigned int i = 2; i < args_.size(); i++) {
      fields.emplace_back(Slice(args_[i]));
    }
    std::vector<std::string> values;
    rocksdb::Status s = hash_db.MGet(args_[1], fields, &values);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      values.resize(fields.size(), "");
    }
    *output = Redis::MultiBulkString(values);
    return Status::OK();
  }
};

class CommandHMSet : public Commander {
 public:
  explicit CommandHMSet() : Commander("hmset", -4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, "wrong number of arguments");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    for (unsigned int i = 2; i < args_.size(); i += 2) {
      field_values.push_back(FieldValue{args_[i], args_[i + 1]});
    }
    rocksdb::Status s = hash_db.MSet(args_[1], field_values, false, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHKeys : public Commander {
 public:
  explicit CommandHKeys() : Commander("hkeys", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values, 1);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    std::vector<std::string> keys;
    for (const auto fv : field_values) {
      keys.emplace_back(fv.field);
    }
    *output = Redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandHVals : public Commander {
 public:
  explicit CommandHVals() : Commander("hvals", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values, 2);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    std::vector<std::string> values;
    for (const auto fv : field_values) {
      values.emplace_back(fv.value);
    }
    *output = Redis::MultiBulkString(values);
    return Status::OK();
  }
};

class CommandHGetAll : public Commander {
 public:
  explicit CommandHGetAll() : Commander("hgetall", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = "*" + std::to_string(field_values.size() * 2) + CRLF;
    for (const auto fv : field_values) {
      *output += Redis::BulkString(fv.field);
      *output += Redis::BulkString(fv.value);
    }
    return Status::OK();
  }
};

class CommandPush : public Commander {
 public:
  explicit CommandPush(bool create_if_missing, bool left)
      : Commander("push", -3, true) {
    left_ = left;
    create_if_missing_ = create_if_missing;
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> elems;
    for (unsigned int i = 2; i < args_.size(); i++) {
      elems.emplace_back(args_[i]);
    }
    int ret;
    rocksdb::Status s;
    if (create_if_missing_) {
      s = list_db.Push(args_[1], elems, left_, &ret);
    } else {
      s = list_db.PushX(args_[1], elems, left_, &ret);
    }
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  bool left_;
  bool create_if_missing_;
};

class CommandLPush : public CommandPush {
 public:
  CommandLPush() : CommandPush(true, true) { name_ = "lpush"; }
};

class CommandRPush : public CommandPush {
 public:
  CommandRPush() : CommandPush(true, false) { name_ = "rpush"; }
};

class CommandLPushX : public CommandPush {
 public:
  CommandLPushX() : CommandPush(true, true) { name_ = "lpushx"; }
};

class CommandRPushX : public CommandPush {
 public:
  CommandRPushX() : CommandPush(true, false) { name_ = "rpushx"; }
};

class CommandPop : public Commander {
 public:
  explicit CommandPop(bool left) : Commander("pop", 2, true) { left_ = left; }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    rocksdb::Status s = list_db.Pop(args_[1], &elem, left_);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(elem);
    }
    return Status::OK();
  }

 private:
  bool left_;
};

class CommandLPop : public CommandPop {
 public:
  explicit CommandLPop() : CommandPop(true) { name_ = "lpop"; }
};

class CommandRPop : public CommandPop {
 public:
  explicit CommandRPop() : CommandPop(false) { name_ = "rpop"; }
};

class CommandLRange : public Commander {
 public:
  explicit CommandLRange() : Commander("lrange", 4, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> elems;
    rocksdb::Status s = list_db.Range(args_[1], start_, stop_, &elems);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(elems);
    return Status::OK();
  }

 private:
  int start_, stop_;
};

class CommandLLen : public Commander {
 public:
  explicit CommandLLen() : Commander("llen", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    uint32_t count;
    rocksdb::Status s = list_db.Size(args_[1], &count);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(count);
    return Status::OK();
  }
};

class CommandLIndex : public Commander {
 public:
  explicit CommandLIndex() : Commander("lindex", 3, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    rocksdb::Status s = list_db.Index(args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::BulkString(elem);
    return Status::OK();
  }

 private:
  int index_;
};

class CommandLSet : public Commander {
 public:
  explicit CommandLSet() : Commander("lset", 4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = list_db.Set(args_[1], index_, args_[3]);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    return Status::OK();
  }

 private:
  int index_;
};

class CommandLTrim : public Commander {
 public:
  explicit CommandLTrim() : Commander("ltrim", 4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = list_db.Trim(args_[1], start_, stop_);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  int start_, stop_;
};

class CommandLPushRPop : public Commander {
 public:
  explicit CommandLPushRPop() : Commander("lpushrpop", 3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisList list_db(svr->storage_, conn->GetNamespace());
    std::string elem;
    rocksdb::Status s = list_db.RPopLPush(args_[1], args_[2], &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }
};

class CommandSAdd : public Commander {
 public:
  explicit CommandSAdd() : Commander("sadd", -3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    int ret;
    rocksdb::Status s = set_db.Add(args_[1], members, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSRem : public Commander {
 public:
  explicit CommandSRem() : Commander("srem", -3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    std::vector<Slice> members;
    for (unsigned int i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    int ret;
    rocksdb::Status s = set_db.Remove(args_[1], members, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSCard : public Commander {
 public:
  explicit CommandSCard() : Commander("scard", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = set_db.Card(args_[1], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSMembers : public Commander {
 public:
  explicit CommandSMembers() : Commander("smembers", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = set_db.Members(args_[1], &members);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members);
    return Status::OK();
  }
};

class CommandSIsMember : public Commander {
 public:
  explicit CommandSIsMember() : Commander("sismmeber", 3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = set_db.IsMember(args_[1], args_[2], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandSPop : public Commander {
 public:
  explicit CommandSPop() : Commander("spop", -2, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() == 3) {
        count_ = std::stoi(args[2]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = set_db.Take(args_[1], &members, count_, true);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members);
    return Status::OK();
  }

 private:
  int count_ = 1;
};

class CommandSRandMember : public Commander {
 public:
  explicit CommandSRandMember() : Commander("srandmember", -2, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() == 3) {
        count_ = std::stoi(args[2]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr,
                    "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    rocksdb::Status s = set_db.Take(args_[1], &members, count_, false);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::MultiBulkString(members);
    return Status::OK();
  }

 private:
  int count_ = 1;
};

class CommandSMove : public Commander {
 public:
  explicit CommandSMove() : Commander("smove", 4, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    int ret;
    rocksdb::Status s = set_db.Move(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZAdd : public Commander {
 public:
  explicit CommandZAdd() : Commander("zadd", -4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, "syntax error");
    }

    try {
      double score;
      for (unsigned i = 2; i < args.size(); i += 2) {
        score = std::stod(args[i]);
        member_scores_.emplace_back(MemberScore{args[i + 1], score});
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Add(args_[1], 0, member_scores_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<MemberScore> member_scores_;
};

class CommandZCount : public Commander {
 public:
  explicit CommandZCount() : Commander("zcount", 4, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    Status s = RedisZSet::ParseRangeSpec(args[2], args[3], &spec_);
    if (!s.IsOK()) {
      return Status(Status::RedisParseErr, s.Msg());
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;

    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Count(args_[1], spec_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZCard : public Commander {
 public:
  explicit CommandZCard() : Commander("zcard", 2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;

    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Card(args_[1], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandZIncrBy : public Commander {
 public:
  explicit CommandZIncrBy() : Commander("zincrby", 4, true) {}

  Status Parse(const std::vector<std::string> &args) override {
    try {
      incr_ = std::stod(args[2]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score;

    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.IncrBy(args_[1], args_[3], incr_, &score);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::BulkString(std::to_string(score));
    return Status::OK();
  }

 private:
  double incr_;
};

class CommandZPop : public Commander {
 public:
  explicit CommandZPop(bool min) : Commander("zpop", -2, true), min_(min) {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() > 2) {
      try {
        count_ = std::stoi(args[2]);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr);
      }
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> memeber_scores;
    rocksdb::Status s = zset_db.Pop(args_[1], count_, min_, &memeber_scores);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    output->append(Redis::MultiLen(memeber_scores.size() * 2));
    for (const auto ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      output->append(Redis::BulkString(std::to_string(ms.score)));
    }
    return Status::OK();
  }

 private:
  bool min_;
  int count_ = 1;
};

class CommandZPopMin : public CommandZPop {
 public:
  explicit CommandZPopMin() : CommandZPop(true) { name_ = "zpopmin"; }
};

class CommandZPopMax : public CommandZPop {
 public:
  explicit CommandZPopMax() : CommandZPop(false) { name_ = "zpopmax"; }
};

class CommandZRange : public Commander {
 public:
  explicit CommandZRange(bool reversed = false)
      : Commander("zrange", -4, false), reversed_(reversed) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    if (args.size() > 4 && (Util::ToLower(args[4]) == "withscores")) {
      with_scores_ = true;
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> memeber_scores;
    uint8_t flags = !reversed_ ? 0 : ZSET_REVERSED;
    rocksdb::Status s =
        zset_db.Range(args_[1], start_, stop_, flags, &memeber_scores);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (!with_scores_) {
      output->append(Redis::MultiLen(memeber_scores.size()));
    } else {
      output->append(Redis::MultiLen(memeber_scores.size() * 2));
    }
    for (const auto ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_)
        output->append(Redis::BulkString(std::to_string(ms.score)));
    }
    return Status::OK();
  }

 private:
  int start_;
  int stop_;
  bool reversed_;
  bool with_scores_ = false;
};

class CommandZRevRange : public CommandZRange {
 public:
  explicit CommandZRevRange() : CommandZRange(true) { name_ = "zrevrange"; }
};

class CommandZRangeByScore : public Commander {
 public:
  explicit CommandZRangeByScore() : Commander("zrangebyscore", -4, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      Status s = RedisZSet::ParseRangeSpec(args[2], args[3], &spec_);
      if (!s.IsOK()) {
        return Status(Status::RedisParseErr, s.Msg());
      }
      if (args.size() == 8 && Util::ToLower(args[5]) == "limit") {
        spec_.offset = std::stoi(args[6]);
        spec_.count = std::stoi(args[7]);
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    if (args.size() > 4 && (Util::ToLower(args[4]) == "withscores")) {
      with_scores_ = true;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<MemberScore> memeber_scores;
    rocksdb::Status s =
        zset_db.RangeByScore(args_[1], spec_, &memeber_scores, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (!with_scores_) {
      output->append(Redis::MultiLen(memeber_scores.size()));
    } else {
      output->append(Redis::MultiLen(memeber_scores.size() * 2));
    }
    for (const auto ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_)
        output->append(Redis::BulkString(std::to_string(ms.score)));
    }
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
  bool with_scores_;
};

class CommandZRank : public Commander {
 public:
  explicit CommandZRank(bool reversed = false)
      : Commander("zrank", 3, false), reversed_(reversed) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int rank;
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Rank(args_[1], args_[2], reversed_, &rank);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(rank);
    return Status::OK();
  }

 private:
  bool reversed_;
};

class CommandZRevRank : public CommandZRank {
 public:
  explicit CommandZRevRank() : CommandZRank(true) { name_ = "zrevrank"; }
};

class CommandZRem : public Commander {
 public:
  explicit CommandZRem() : Commander("zrem", -3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<rocksdb::Slice> members;
    for (unsigned i = 2; i < args_.size(); i++) {
      members.emplace_back(args_[i]);
    }
    rocksdb::Status s = zset_db.Remove(args_[1], members, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }
};

class CommandZRemRangeByRank : public Commander {
 public:
  explicit CommandZRemRangeByRank() : Commander("zremrangebyrank", 4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;

    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s =
        zset_db.RemoveRangeByRank(args_[1], start_, stop_, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int start_;
  int stop_;
};

class CommandZRemRangeByScore : public Commander {
 public:
  explicit CommandZRemRangeByScore()
      : Commander("zremrangebyscore", -4, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      Status s = RedisZSet::ParseRangeSpec(args[2], args[3], &spec_);
      if (!s.IsOK()) {
        return Status(Status::RedisParseErr, s.Msg());
      }
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int size;
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.RemoveRangeByScore(args_[1], spec_, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(size);
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
};

class CommandZScore : public Commander {
 public:
  explicit CommandZScore() : Commander("zscore", 3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double score;
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = zset_db.Score(args_[1], args_[2], &score);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(std::to_string(score));
    }
    return Status::OK();
  }
};

class CommandInfo : public Commander {
 public:
  explicit CommandInfo() : Commander("info", -1, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string section = "all";
    if (args_.size() == 2) {
      section = Util::ToLower(args_[1]);
    }
    std::string info;
    svr->GetInfo(conn->GetNamespace(), section, info);
    *output = Redis::BulkString(info);
    return Status::OK();
  }
};

class CommandCompact : public Commander {
 public:
  explicit CommandCompact() : Commander("compact", 1, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error("only administrator can compact the db");
      return Status::OK();
    }
    Status s = svr->AsyncCompactDB();
    if (!s.IsOK()) return s;
    *output = Redis::SimpleString("OK");
    LOG(INFO) << "Commpact was triggered by manual with executed success.";
    return Status::OK();
  }
};

class CommandDBSize : public Commander {
 public:
  explicit CommandDBSize() : Commander("dbsize", -1, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string ns = conn->GetNamespace();
    if (args_.size() == 1) {
      *output = Redis::Integer(svr->GetLastKeyNum(ns));
    } else if (args_.size() == 2 && args_[1] == "scan") {
      Status s = svr->AsyncScanDBSize(ns);
      if (s.IsOK()) {
        *output = Redis::SimpleString("OK");
      } else {
        *output = Redis::Error(s.Msg());
      }
    } else {
      *output = Redis::Error("DBSIZE subcommand only supports scan");
    }
    return Status::OK();
  }
};

class CommandPublish : public Commander {
 public:
  explicit CommandPublish() : Commander("publish", 3, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int receivers = svr->PublishMessage(args_[1], args_[2]);
    *output = Redis::Integer(receivers);
    return Status::OK();
  }
};

class CommandSubscribe : public Commander {
 public:
  explicit CommandSubscribe() : Commander("subcribe", -2, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (unsigned i = 1; i < args_.size(); i++) {
      conn->SubscribeChannel(args_[i]);
      output->append(Redis::MultiLen(3));
      output->append(Redis::BulkString("subscribe"));
      output->append(Redis::BulkString(args_[i]));
      output->append(Redis::Integer(conn->SubscriptionsCount()));
    }
    return Status::OK();
  }
};

class CommandUnSubscribe : public Commander {
 public:
  explicit CommandUnSubscribe() : Commander("unsubcribe", -1, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() > 1) {
      conn->UnSubscribeChannel(args_[1]);
    } else {
      conn->UnSubscribeAll();
    }
    return Status::OK();
  }
};

class CommandSlaveOf : public Commander {
 public:
  explicit CommandSlaveOf() : Commander("slaveof", 3, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    host_ = args[1];
    auto port = args[2];
    if (Util::ToLower(host_) == "no" && Util::ToLower(port) == "one") {
      host_.clear();
      return Status::OK();
    }
    try {
      auto p = std::stoul(port);
      if (p > UINT32_MAX) {
        throw std::overflow_error("port out of range");
      }
      // we use port + 1 as repl port, so incr the slaveof port here
      port_ = static_cast<uint32_t>(p)+1;
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr, "port should be number");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error("only administrator can use slaveof command");
      return Status::OK();
    }
    Status s;
    if (host_.empty()) {
      s = svr->RemoveMaster();
    } else {
      s = svr->AddMaster(host_, port_);
    }
    if (s.IsOK()) {
      *output = Redis::SimpleString("OK");
    }
    return s;
  }

 private:
  std::string host_;
  uint32_t port_;
};

class CommandPSync : public Commander {
 public:
  explicit CommandPSync() : Commander("psync", 2, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    try {
      auto s = std::stoull(args[1]);
      next_seq_ = static_cast<rocksdb::SequenceNumber>(s);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  // PSync is a long-polling op, but the Execute method must return to make the
  // event loop being able to handle new requests. so we change the event
  // callbacks of this connection:
  // 1. the writable callback is set to `StreamingBatch`. this cb sends at most
  //    500 batches, and then yield, wait for next writable event.
  // 2. also add a new timer event to the ev base, to re-trigger the WRITE event
  //    when `StreamingBatch` needs to wait for new data in WAL.
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr_ = svr;
    conn_ = conn;

    if (!checkWALBoundary(svr->storage_, next_seq_).IsOK()) {
      svr->stats_.IncrPSyncErrCounter();
      *output = "sequence out of range";
      return Status(Status::RedisExecErr, *output);
    } else {
      svr->stats_.IncrPSyncOKCounter();
      conn->Reply(Redis::SimpleString("OK"));
    }

    std::string peer_addr;
    uint32_t port;
    int sock_fd = conn->GetFD();
    if (Util::GetPeerAddr(sock_fd, &peer_addr, &port) < 0) {
      peer_addr = "unknown";
    }
    slave_info_pos_ = svr->AddSlave(peer_addr, port);
    if (next_seq_ == 0) {
      svr->UpdateSlaveStats(slave_info_pos_, next_seq_);
    } else {
      // the seq_ is the client's next seq, so it current seq should be seq_ - 1
      svr->UpdateSlaveStats(slave_info_pos_, next_seq_ - 1);
    }

    state_ = State::GetWALIter;
    auto bev = conn->GetBufferEvent();
    StreamingBatch(bev, this);
    bufferevent_setcb(bev, nullptr, StreamingBatch, EventCB, this);
    timer_ = event_new(bufferevent_get_base(bev), -1, EV_PERSIST, TimerCB, bev);
    timeval tm = {0, 5000};  // 5ms
    evtimer_add(timer_, &tm);
    return Status::OK();
  }

  static void StreamingBatch(bufferevent *bev, void *ctx) {
    auto self = reinterpret_cast<CommandPSync *>(ctx);
    auto output = bufferevent_get_output(bev);
    while (true) {
      switch (self->state_) {
        case State::GetWALIter:
          if (!self->svr_->storage_->GetWALIter(self->next_seq_, &self->iter_)
                   .IsOK()) {
            return;  // Try again next time, the timer will notify me.
          }
          self->state_ = State::SendBatch;
          break;
        case State::SendBatch:
          // Everything is OK, streaming the batch data
          // Every 500 batches, yield and wait for next WRITE event
          {
            int count = 500;
            while (self->iter_->Valid()) {
              auto batch = self->iter_->GetBatch();
              auto data = batch.writeBatchPtr->Data();
              // Send data in redis bulk string format
              std::string bulk_str =
                  "$" + std::to_string(data.length()) + CRLF + data + CRLF;
              evbuffer_add(output, bulk_str.c_str(), bulk_str.size());
              self->svr_->UpdateSlaveStats(self->slave_info_pos_, self->next_seq_);
              self->next_seq_ = batch.sequence + batch.writeBatchPtr->Count();
              if (!DoesWALHaveNewData(self->next_seq_, self->svr_->storage_)) {
                self->state_ = State::WaitWAL;
                return;
              }
              self->iter_->Next();
              --count;
              if (count <= 0) {
                return;  // Send enough batches, yield
              }
            }
          }

          // if arrived here, means the wal file is rotated, a reopen is needed.
          LOG(INFO) << "WAL rotate";
          self->state_ = State::GetWALIter;
          break;
        case State::WaitWAL:
          if (!DoesWALHaveNewData(self->next_seq_, self->svr_->storage_)) {
            return;  // Try again next time, the timer will notify me.
          }
          self->iter_->Next();
          self->state_ = State::SendBatch;
          break;
      }
    }
  }

  static void EventCB(bufferevent *bev, short events, void *ctx) {
    auto self = static_cast<CommandPSync *>(ctx);
    if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
      int slave_fd = self->conn_->GetFD();
      self->svr_->RemoveSlave(self->slave_info_pos_);
      event_free(self->timer_);
      self->conn_->Owner()->RemoveConnection(slave_fd);

      std::string addr;
      uint32_t port;
      Util::GetPeerAddr(slave_fd, &addr, &port);
      if (events & BEV_EVENT_EOF) {
        LOG(WARNING) << "Disconnect the slave[" << addr << ":" << port << "], "
                     << "while the connection was closed";
      } else {
        LOG(ERROR) << "Disconnect the slave[" << addr << ":" << port << "], "
                   << " while encounter err: " << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
      }
      return;
    }
    if (events & BEV_EVENT_TIMEOUT) {
      LOG(INFO) << "timeout, fd=" << self->conn_->GetFD();
      bufferevent_enable(bev, EV_WRITE);
    }
  }

  static void TimerCB(int, short events, void *ctx) {
    auto bev = reinterpret_cast<bufferevent *>(ctx);
    bufferevent_enable(bev, EV_WRITE);
    bufferevent_trigger(bev, EV_WRITE, 0);
  }

 private:
  rocksdb::SequenceNumber next_seq_;
  Server *svr_;
  Connection *conn_;
  event *timer_;
  std::unique_ptr<rocksdb::TransactionLogIterator> iter_;
  enum class State {
    GetWALIter,
    SendBatch,
    WaitWAL,
  };
  State state_;
  Server::SlaveInfoPos slave_info_pos_;

  // Return OK if the seq is in the range of the current WAL
  Status checkWALBoundary(Engine::Storage *storage,
                          rocksdb::SequenceNumber seq) {
    if (seq == storage->LatestSeq() + 1) {
      return Status::OK();
    }
    // Upper bound
    if (seq > storage->LatestSeq() + 1) {
      return Status(Status::NotOK);
    }
    // Lower bound
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    auto s = storage->GetWALIter(seq, &iter);
    if (s.IsOK() && iter->Valid()) {
      auto batch = iter->GetBatch();
      if (seq < batch.sequence) {
        return Status(Status::NotOK);
      }
    }
    return Status::OK();
  }

  inline static bool DoesWALHaveNewData(rocksdb::SequenceNumber seq,
                                        Engine::Storage *storage) {
    return seq <= storage->LatestSeq();
  }
};

class CommandSlowlog : public Commander {
 public:
  explicit CommandSlowlog() : Commander("slowlog", -2, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if ((subcommand_ == "reset" || subcommand_ == "len" ||
         subcommand_ == "get") &&
        args.size() == 2) {
      return Status::OK();
    }
    if (subcommand_ == "get" && args.size() == 3) {
      try {
        auto c = std::stoul(args[2]);
        count_ = static_cast<uint32_t>(c);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr);
      }
      return Status::OK();
    }
    return Status(
        Status::RedisInvalidCmd,
        "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "reset") {
      srv->SlowlogReset();
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "len") {
      *output = Redis::Integer(srv->SlowlogLen());
      return Status::OK();
    } else if (subcommand_ == "get") {
      srv->CreateSlowlogReply(output, count_);
      return Status::OK();
    }
    return Status(
        Status::RedisInvalidCmd,
        "Unknown SLOWLOG subcommand or wrong # of args. Try GET, RESET, LEN.");
  }

 private:
  std::string subcommand_;
  uint32_t count_ = 10;
};

class CommandClient : public Commander {
 public:
  explicit CommandClient() : Commander("client", -2, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    //subcommand: getname id kill list setname
    if ((subcommand_ == "id" || subcommand_ == "getname" ||  subcommand_ == "list") && args.size() == 2) {
      return Status::OK();
    }
    if ((subcommand_ == "setname") && args.size() == 3) {
      name_ = args[2];
      return Status::OK();
    }
    if ((subcommand_ == "kill")) {
      if (args.size() == 2) {
        return Status(Status::RedisParseErr,"syntax error");
      }
      if (args.size() == 3 ) {
        addr_ = args[2];
        new_format_ = false;
        return Status::OK();
      }

      uint i = 2;
      new_format_ = true;
      while(i < args.size()) {
        bool moreargs = i < args.size();
        if (args[i] == "addr" && moreargs) {
          addr_ = args[i+1];
        } else if (args[i] == "id" && moreargs) {
          try {
            id_ = std::stoll(args[i+1]);
          } catch (std::exception &e) {
            return Status(Status::RedisParseErr, "value is not an integer or out of range");
          }
        } else if (args[i] == "skipme" && moreargs) {
          if (args[i+1] == "yes") {
            skipme_ = true;
          } else if (args[i+1] == "no") {
            skipme_ = false;
          } else {
            return Status(Status::RedisParseErr,"syntax error");
          }
        } else {
          return Status(Status::RedisParseErr,"syntax error");
        }
        i += 2;
      }
      return Status::OK();
    }
    return Status(Status::RedisInvalidCmd,"Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name");

  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "list") {
      std::string o;
      *output = Redis::BulkString(srv->GetClientsStr());
      return Status::OK();
    } else if (subcommand_ == "setname") {
      conn->SetName(name_);
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else if (subcommand_ == "getname") {
      std::string name = conn->GetName();
      *output = name== ""? Redis::NilString(): Redis::BulkString(name);
      return Status::OK();
    } else if (subcommand_ == "id") {
      *output = Redis::Integer(conn->GetID());
      return Status::OK();
    } else if (subcommand_ == "kill") {
      int64_t killed = 0;
      srv->KillClient(&killed, addr_, id_, skipme_, conn);
      if (new_format_) {
        *output = Redis::Integer(killed);
      } else {
        if (killed == 0)
          *output = Redis::Error("No such client");
        else
          *output = Redis::SimpleString("OK");
      }
      return Status::OK();
    }

    return Status(Status::RedisInvalidCmd,"Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name");
  }

 private:
  std::string subcommand_;
  std::string name_;
  std::string addr_ = "";
  bool skipme_ = false;
  uint64_t id_ = 0;
  bool new_format_;
};

class CommandShutdown : public Commander {
 public:
  explicit CommandShutdown() : Commander("shutdown", -1, false) {}
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error("only administrator can use namespace command");
      return Status::OK();
    }
    if (!srv->IsStopped()) {
      LOG(INFO) << "bye bye";
      srv->Stop();
    }
    return Status::OK();
  }
};

class CommandScanBase : public Commander {
 public:
  explicit CommandScanBase(std::string name, int arity, bool is_write = false) : Commander(name, arity, is_write) {}
  Status ParseMatchAndCountParam(const std::string &type, const std::string &value) {
    if (type == "match") {
      prefix = std::move(value);
      if (!prefix.empty() && prefix[prefix.size() - 1] == '*') {
        prefix = prefix.substr(0, prefix.size() - 1);
        return Status::OK();
      }
      return Status(Status::RedisParseErr, "only keys prefix match was supported");
    } else if (type == "count") {
      try {
        limit = std::stoi(value);
      } catch (const std::exception &e) {
        return Status(Status::RedisParseErr, "ERR count param should be type int");
      }
    }
    return Status::OK();
  }

  void ParseCursor(const std::string &param) {
    cursor = param;
    if (cursor == "0") {
      cursor = std::string();
    }
  }

  std::string GenerateOutput(const std::vector<std::string> &keys) {
    std::vector<std::string> list;
    if (!keys.empty()) {
      list.emplace_back(Redis::BulkString(keys.back()));
    } else {
      list.emplace_back(Redis::BulkString("0"));
    }

    list.emplace_back(Redis::MultiBulkString(keys));

    return Redis::Array(list);
  }

 protected:
  std::string cursor;
  std::string prefix;
  int limit = 20;
};

class CommandSubkeyScanBase : public CommandScanBase {
 public:
  explicit CommandSubkeyScanBase(std::string name, int arity, bool is_write = false) : CommandScanBase(name,
                                                                                                       arity,
                                                                                                       is_write) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 == 0) {
      return Status(Status::RedisParseErr, "wrong number of arguments");
    }
    key = args[1];
    ParseCursor(args[2]);
    if (args.size() >= 5) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[3]), args_[4]);
      if (!s.IsOK()) {
        return s;
      }
    }
    if (args.size() >= 7) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[5]), args_[6]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }

 protected:
  std::string key;
};

class CommandScan : public CommandScanBase {
 public:
  explicit CommandScan() : CommandScanBase("scan", -2, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, "wrong number of arguments");
    }

    ParseCursor(args[1]);
    if (args.size() >= 4) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[2]), args_[3]);
      if (!s.IsOK()) {
        return s;
      }
    }
    if (args.size() >= 6) {
      Status s = ParseMatchAndCountParam(Util::ToLower(args[4]), args_[5]);
      if (!s.IsOK()) {
        return s;
      }
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisDB redis_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> keys;
    redis_db.Scan(cursor, limit, prefix, &keys);

    *output = GenerateOutput(keys);
    return Status::OK();
  }
};

class CommandHScan : public CommandSubkeyScanBase {
 public:
  explicit CommandHScan() : CommandSubkeyScanBase("hscan", -3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> fields;
    hash_db.Scan(key, cursor, limit, prefix, &fields);

    *output = GenerateOutput(fields);
    return Status::OK();
  }
};

class CommandSScan : public CommandSubkeyScanBase {
 public:
  explicit CommandSScan() : CommandSubkeyScanBase("sscan", -3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisSet set_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    set_db.Scan(key, cursor, limit, prefix, &members);

    *output = GenerateOutput(members);
    return Status::OK();
  }
};

class CommandZScan : public CommandSubkeyScanBase {
 public:
  explicit CommandZScan() : CommandSubkeyScanBase("zscan", -3, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisZSet zset_db(svr->storage_, conn->GetNamespace());
    std::vector<std::string> members;
    zset_db.Scan(key, cursor, limit, prefix, &members);

    *output = GenerateOutput(members);
    return Status::OK();
  }
};

class CommandFetchMeta : public Commander {
 public:
  explicit CommandFetchMeta() : Commander("_fetch_meta", 1, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint64_t file_size;
    rocksdb::BackupID meta_id;
    int fd;
    auto s = Engine::Storage::BackupManager::OpenLatestMeta(
        svr->storage_, &fd, &meta_id, &file_size);
    if (!s.IsOK()) {
      LOG(ERROR) << "Failed to open latest meta, err: " << s.Msg();
      return Status(Status::DBBackupFileErr, "can't create db backup");
    }
    // Send the meta ID
    conn->Reply(std::to_string(meta_id) + CRLF);
    // Send meta file size
    conn->Reply(std::to_string(file_size) + CRLF);
    // Send meta content
    conn->SendFile(fd);
    svr->stats_.IncrFullSyncCounter();
    return Status::OK();
  }
};

class CommandFetchFile : public Commander {
 public:
  CommandFetchFile() : Commander("_fetch_file", 2, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    path_ = args[1];
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    uint64_t file_size = 0;
    auto fd = Engine::Storage::BackupManager::OpenDataFile(svr->storage_, path_,
                                                           &file_size);
    if (fd < 0) {
      return Status(Status::DBBackupFileErr);
    }

    conn->Reply(std::to_string(file_size) + CRLF);
    conn->SendFile(fd);

    // TODO: we could keep receiving _fetch_file cmd and send file
    // so to avoid creating thread for every _fetch_file
    return Status::OK();
  }

 private:
  std::string path_;
};

class CommandDBName : public Commander {
 public:
  CommandDBName() : Commander("_db_name", 1, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    conn->Reply(svr->storage_->GetName() + CRLF);
    return Status::OK();
  }
};

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;
std::map<std::string, CommanderFactory> command_table = {
    {"auth",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandAuth);
     }},
    {"ping",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandPing);
     }},
    {"info",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandInfo);
     }},
    {"config",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandConfig);
     }},
    {"namespace",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandNamespace);
     }},
    {"keys",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandKeys);
     }},
    {"flushall",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandFlushAll);
     }},
    {"dbsize",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandDBSize);
     }},
    {"slowlog",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSlowlog);
     }},
    {"client",
     []()->std::unique_ptr<Commander> {
        return std::unique_ptr<Commander>(new CommandClient);
    }},
    {"shutdown",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandShutdown);
     }},
    {"scan",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandScan);
     }},
    // key command
    {"ttl",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandTTL);
     }},
    {"type",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandType);
     }},
    {"exists",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandExists);
     }},
    {"expire",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandExpire);
     }},
    {"del",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandDel);
     }},
    // string command
    {"get",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandGet);
     }},
    {"mget",
          []() -> std::unique_ptr<Commander> {
            return std::unique_ptr<Commander>(new CommandMGet);
     }},
    {"set",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSet);
     }},
    {"setnx",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSetNX);
     }},
    {"mset",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandMSet);
     }},
    {"incrby",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandIncrBy);
     }},
    {"incr",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandIncr);
     }},
    {"decrby",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandDecrBy);
     }},
    {"decr",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandDecr);
     }},
    // hash command
    {"hget",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHGet);
     }},
    {"hincrby",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHIncrBy);
     }},
    {"hset",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHSet);
     }},
    {"hsetnx",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHSetNX);
     }},
    {"hdel",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHDel);
     }},
    {"hstrlen",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHStrlen);
     }},
    {"hexists",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHExists);
     }},
    {"hlen",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHLen);
     }},
    {"hmget",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHMGet);
     }},
    {"hmset",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHMSet);
     }},
    {"hkeys",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHKeys);
     }},
    {"hvals",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHVals);
     }},
    {"hgetall",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHGetAll);
     }},
    {"hscan",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandHScan);
     }},
    // list command
    {"lpush",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLPush);
     }},
    {"rpush",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandRPush);
     }},
    {"lpushx",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLPushX);
     }},
    {"rpushx",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandRPushX);
     }},
    {"lpop",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLPop);
     }},
    {"rpop",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandRPop);
     }},
    {"lrange",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLRange);
     }},
    {"lindex",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLIndex);
     }},
    {"ltrim",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLTrim);
     }},
    {"llen",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLLen);
     }},
    {"lset",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLSet);
     }},
    {"lpushrpop",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandLPushRPop);
     }},
    // set command
    {"sadd",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSAdd);
     }},
    {"srem",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSRem);
     }},
    {"scard",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSCard);
     }},
    {"smembers",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSMembers);
     }},
    {"sismember",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSIsMember);
     }},
    {"spop",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSPop);
     }},
    {"srandmember",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSRandMember);
     }},
    {"smove",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSMove);
     }},
    {"sscan",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSScan);
     }},
    // zset command
    {"zadd",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZAdd);
     }},
    {"zcard",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZCard);
     }},
    {"zcount",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZCount);
     }},
    {"zincrby",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZIncrBy);
     }},
    {"zpopmax",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZPopMax);
     }},
    {"zpopmin",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZPopMin);
     }},
    {"zrange",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRange);
     }},
    {"zrevrange",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRevRange);
     }},
    {"zrangebyscore",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRangeByScore);
     }},
    {"zrank",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRank);
     }},
    {"zrem",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRem);
     }},
    {"zremrangebyrank",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRemRangeByRank);
     }},
    {"zremrangebyscore",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRemRangeByScore);
     }},
    {"zrevrank",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZRevRank);
     }},
    {"zscore",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZScore);
     }},
    {"zscan",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandZScan);
     }},
    // pub/sub command
    {"publish",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandPublish);
     }},
    {"subscribe",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSubscribe);
     }},
    {"unsubscribe",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandUnSubscribe);
     }},

    // internal management cmd
    {"compact",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandCompact);
     }},
    {"slaveof",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandSlaveOf);
     }},
};

// Replication related commands, which are received by workers listening on
// `repl-port`
std::map<std::string, CommanderFactory> repl_command_table = {
    {"auth",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandAuth);
     }},
    {"psync",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandPSync);
     }},
    {"_fetch_meta",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandFetchMeta);
     }},
    {"_fetch_file",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandFetchFile);
     }},
    {"_db_name",
     []() -> std::unique_ptr<Commander> {
       return std::unique_ptr<Commander>(new CommandDBName);
     }},
};

Status LookupCommand(const std::string &cmd_name,
                     std::unique_ptr<Commander> *cmd, bool is_repl) {
  if (cmd_name.empty()) return Status(Status::RedisUnknownCmd);
  if (is_repl) {
    auto cmd_factory = repl_command_table.find(Util::ToLower(cmd_name));
    if (cmd_factory == repl_command_table.end()) {
      return Status(Status::RedisUnknownCmd);
    }
    *cmd = cmd_factory->second();
  } else {
    auto cmd_factory = command_table.find(Util::ToLower(cmd_name));
    if (cmd_factory == command_table.end()) {
      return Status(Status::RedisUnknownCmd);
    }
    *cmd = cmd_factory->second();
  }
  return Status::OK();
}

}  // namespace Redis
