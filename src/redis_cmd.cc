#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <algorithm>
#include <cctype>
#include <chrono>
#include <thread>

#include "redis_cmd.h"
#include "replication.h"
#include "worker.h"
#include "redis_request.h"
#include "sock_util.h"
#include "storage.h"
#include "string_util.h"
#include "t_string.h"
#include "t_hash.h"
#include "t_list.h"
#include "t_set.h"
#include "t_zset.h"

namespace Redis {
class CommandAuth : public Commander {
 public:
  explicit CommandAuth() : Commander("auth", 2, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    Config *config = svr->GetConfig();
    auto iter = config->tokens.find(args_[1]);
    if (iter == config->tokens.end()) {
      *output = Redis::Error("ERR invalid password");
    } else {
      conn->SetNamespace(iter->second);
      if (args_[1] == config->require_passwd) {
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
  explicit CommandNamespace() : Commander("namespace", -3, false, false) {}
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
          namespaces.emplace_back(iter->second); // namespace
          namespaces.emplace_back(iter->first); // token
        }
        *output = Redis::MultiBulkString(namespaces);
      } else {
        std::string token;
        config->GetNamespace(args_[2], &token);
        *output = Redis::BulkString(token);
      }
    } else if (args_.size() == 4 && args_[1] == "set") {
      Status s = config->SetNamepsace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK"):Redis::Error(s.Msg());
    } else if (args_.size() == 4 && args_[1] == "add") {
      Status s = config->AddNamespace(args_[2], args_[3]);
      *output = s.IsOK() ? Redis::SimpleString("OK"):Redis::Error(s.Msg());
    } else if (args_.size() == 3 && args_[1] == "del") {
      Status s = config->DelNamespace(args_[2]);
      *output = s.IsOK() ? Redis::SimpleString("OK"):Redis::Error(s.Msg());
    } else {
      *output = Redis::Error("NAMESPACE subcommand must be one of GET, SET, DEL, ADD");
    }
    return Status::OK();
  }
};

class CommandKeys: public Commander {
 public:
  explicit CommandKeys() : Commander("keys", 2, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string prefix = args_[1];
    std::vector<std::string> keys;
    RedisDB redis(svr->storage_, conn->GetNamespace());
    if (prefix == "*") {
      redis.Keys(std::string(), &keys);
    } else {
      if (prefix[prefix.size()-1]!= '*') {
        *output = Redis::Error("ERR only keys prefix match was supported");
        return Status::OK();
      }
      redis.Keys(prefix.substr(0, prefix.size()-1), &keys);
    }
    *output = Redis::MultiBulkString(keys);
    return Status::OK();
  }
};

class CommandFlushAll: public Commander {
 public:
  explicit CommandFlushAll() : Commander("flushall", 1, false, false) {}
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
  explicit CommandPing() : Commander("ping", 1, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    *output = Redis::SimpleString("PONG");
    return Status::OK();
  }
};

class CommandConfig : public Commander {
 public:
  explicit CommandConfig() : Commander("config", -2, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsAdmin()) {
      *output = Redis::Error("only administrator can use config command");
      return Status::OK();
    }

    std::string err;
    Config *config = svr->GetConfig();
    if (args_.size() == 2 && Util::ToLower(args_[1]) == "rewrite") {
      if (!config->Rewrite(&err)) {
        return Status(Status::RedisExecErr, err);
      }
      *output = Redis::SimpleString("OK");
      LOG(INFO) << "# CONFIG REWRITE executed with success.";
    } else if (args_.size() == 3 && Util::ToLower(args_[1]) == "get") {
      std::vector<std::string> values;
      config->Get(args_[2], &values);
      *output = Redis::MultiBulkString(values);
    } else if (args_.size() == 4 && Util::ToLower(args_[1]) == "set") {
      Status s = config->Set(args_[2], args_[3]);
      if (!s.IsOK()) {
        return Status(Status::NotOK, s.Msg()+":"+args_[2]);
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
  explicit CommandGet() : Commander("get", 2, false, false) {}
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

class CommandSet : public Commander {
 public:
  explicit CommandSet() : Commander("set", 3, false, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.Set(args_[1], args_[2]);
    if (s.ok()) {
      *output = Redis::SimpleString("OK");
      return Status::OK();
    } else {
      return Status(Status::RedisExecErr, s.ToString());
    }
  }
};

class CommandIncr : public Commander {
 public:
  explicit CommandIncr() : Commander("incr", 2, false, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], 1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr: public Commander {
 public:
  explicit CommandDecr() : Commander("decr", 2, false, true) {}
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
  explicit CommandIncrBy() : Commander("incrby", 3, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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
  explicit CommandDecrBy() : Commander("decrby", 3, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_, conn->GetNamespace());
    rocksdb::Status s = string_db.IncrBy(args_[1], -1*increment_, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  int64_t increment_;
};

class CommandDel : public Commander {
 public:
  explicit CommandDel() : Commander("del", -2, false, true) {}
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

class CommandType: public Commander {
 public:
  explicit CommandType () : Commander("type", 2, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisDB redis(svr->storage_, conn->GetNamespace());
    RedisType type;
    rocksdb::Status s = redis.Type(args_[1], &type);
    if (s.ok()) {
      std::vector<std::string> type_names = {"none", "string", "hash", "list", "set", "zset"};
      *output = Redis::BulkString(type_names[type]);
      return Status::OK();
    }
    return Status(Status::RedisExecErr, s.ToString());
  }
};

class CommandTTL: public Commander {
 public:
  explicit CommandTTL() : Commander("ttl", 2, false, false) {}
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

class CommandExists: public Commander {
 public:
  explicit CommandExists() : Commander("exists", -2, false, false) {}
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

class CommandExpire: public Commander {
 public:
  explicit CommandExpire() : Commander("expire", 3, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    try {
      seconds_ = std::stoi(args[2]);
      if (seconds_ >= INT32_MAX-now) {
        return Status(Status::RedisParseErr, "the expire time was overflow");
      }
      seconds_ += now;
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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
  explicit CommandHGet() : Commander("hget", 3, false, false) {}
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
  explicit CommandHSet() : Commander("hset", 4, false, true) {}
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
  explicit CommandHSetNX() : Commander("hsetnx", 4, false, true) {}
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
  explicit CommandHStrlen() : Commander("hstrlen", 3, false, false) {}
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

class CommandHDel: public Commander {
 public:
  explicit CommandHDel() : Commander("hdel", -3, false, true) {}
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

class CommandHExists: public Commander {
 public:
  explicit CommandHExists() : Commander("hexists", 3, false, false) {}
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

class CommandHLen: public Commander {
 public:
  explicit CommandHLen() : Commander("hlen", 2, false, false) {}
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
  explicit CommandHIncrBy() : Commander("hincrby", 4, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandHMGet: public Commander {
 public:
  explicit CommandHMGet() : Commander("hmget", -3, false, false) {}
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

class CommandHMSet: public Commander {
 public:
  explicit CommandHMSet() : Commander("hmset", -4, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    if(args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, "wrong number of arguments");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    for (unsigned int i = 2; i < args_.size(); i+=2) {
      field_values.push_back(FieldValue{args_[i], args_[i+1]});
    }
    rocksdb::Status s = hash_db.MSet(args_[1], field_values, false, &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandHKeys: public Commander {
 public:
  explicit CommandHKeys() : Commander("hkeys", 2, false, false) {}
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

class CommandHVals: public Commander {
 public:
  explicit CommandHVals() : Commander("hvals", 2, false, false) {}
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

class CommandHGetAll: public Commander {
 public:
  explicit CommandHGetAll() : Commander("hgetall", 2, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    RedisHash hash_db(svr->storage_, conn->GetNamespace());
    std::vector<FieldValue> field_values;
    rocksdb::Status s = hash_db.GetAll(args_[1], &field_values);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = "*"+std::to_string(field_values.size()*2)+CRLF;
    for (const auto fv : field_values) {
      *output += Redis::BulkString(fv.field);
      *output += Redis::BulkString(fv.value);
    }
    return Status::OK();
  }
};

class CommandPush: public Commander {
 public:
  explicit CommandPush(bool create_if_missing, bool left)
      : Commander("push", -3, false, true) {
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
  CommandLPush():CommandPush(true, true) {name_="lpush";}
};

class CommandRPush : public CommandPush{
 public:
  CommandRPush():CommandPush(true, false) {name_="rpush";}
};

class CommandLPushX : public CommandPush{
 public:
  CommandLPushX():CommandPush(true, true) {name_="lpushx";}
};

class CommandRPushX : public CommandPush{
 public:
  CommandRPushX():CommandPush(true, false) {name_="rpushx";}
};

class CommandPop: public Commander {
 public:
  explicit CommandPop(bool left) : Commander("pop", 2, false, true) {
    left_ = left;
  }
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
   explicit CommandLPop():CommandPop(true) {name_ = "lpop";}
 };

class CommandRPop : public CommandPop {
 public:
  explicit CommandRPop():CommandPop(false) {name_ = "rpop";}
};

class CommandLRange : public Commander {
 public:
  explicit CommandLRange() : Commander("lrange", 4, false, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandLLen : public  Commander {
 public:
  explicit CommandLLen() : Commander("llen", 2, false, false) {}
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

class CommandLIndex: public Commander {
 public:
  explicit CommandLIndex() : Commander("lindex", 3, false, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandLSet: public Commander {
 public:
  explicit CommandLSet() : Commander("lset", 4, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandLTrim: public Commander {
 public:
  explicit CommandLTrim() : Commander("ltrim", 4, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandLPushRPop: public  Commander {
 public:
  explicit CommandLPushRPop() : Commander("lpushrpop", 3, false, true) {}
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

class CommandSAdd : public  Commander {
 public:
  explicit CommandSAdd() : Commander("sadd", -3, false, true) {}
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

class CommandSRem: public  Commander {
 public:
  explicit CommandSRem() : Commander("srem", -3, false, true) {}
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

class CommandSCard: public  Commander {
 public:
  explicit CommandSCard() : Commander("scard", 2, false, false) {}
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

class CommandSMembers: public  Commander {
 public:
  explicit CommandSMembers() : Commander("smembers", 2, false, false) {}
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

class CommandSIsMember: public  Commander {
 public:
  explicit CommandSIsMember() : Commander("sismmeber", 3, false, false) {}
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

class CommandSPop: public Commander {
 public:
  explicit CommandSPop() : Commander("spop", -2, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() == 3) {
        count_ = std::stoi(args[2]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandSRandMember: public Commander {
 public:
  explicit CommandSRandMember() : Commander("srandmember", -2, false, false) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      if (args.size() == 3) {
        count_ = std::stoi(args[2]);
      }
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
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

class CommandSMove: public  Commander {
 public:
  explicit CommandSMove() : Commander("smove", 4, false, true) {}
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
  explicit CommandZAdd() : Commander("zadd", -4, false, true) {}
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size()%2 != 0) {
      return Status(Status::RedisParseErr, "syntax error");
    }

    try {
      double score;
      for (unsigned i = 2; i < args.size(); i+=2) {
        score = std::stod(args[i]);
        member_scores_.emplace_back(MemberScore{args[i+1], score});
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

class CommandZCount: public Commander {
 public:
  explicit CommandZCount() : Commander("zcount", 4, false, false) {}
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

class CommandZCard: public Commander {
 public:
  explicit CommandZCard() : Commander("zcard", 2, false, false) {}
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

class CommandZIncrBy: public Commander {
 public:
  explicit CommandZIncrBy(): Commander("zincrby", 4, false, true) {}

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
  explicit CommandZPop(bool min): Commander("zpop", -2, false, true), min_(min) {}

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
    output->append(Redis::MultiLen(memeber_scores.size()*2));
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

class CommandZPopMin: public CommandZPop {
 public:
  explicit CommandZPopMin():CommandZPop(true) {name_ = "zpopmin";};
};

class CommandZPopMax: public CommandZPop {
 public:
  explicit CommandZPopMax():CommandZPop(false) {name_ = "zpopmax";};
};

class CommandZRange: public Commander {
 public:
  explicit CommandZRange(bool reversed=false): Commander("zrange", -4, false, false), reversed_(reversed){}
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
    rocksdb::Status s = zset_db.Range(args_[1], start_, stop_, flags, &memeber_scores);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (!with_scores_) {
      output->append(Redis::MultiLen(memeber_scores.size()));
    } else {
      output->append(Redis::MultiLen(memeber_scores.size()*2));
    }
    for (const auto ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_) output->append(Redis::BulkString(std::to_string(ms.score)));
    }
    return Status::OK();
  }
 private:
  int start_;
  int stop_;
  bool reversed_;
  bool with_scores_ = false;
};

class CommandZRevRange: public CommandZRange {
 public:
  explicit CommandZRevRange(): CommandZRange(true){ name_ = "zrevrange";}
};

class CommandZRangeByScore: public Commander {
 public:
  explicit CommandZRangeByScore(): Commander("zrangebyscore", -4, false, false) {}
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
    rocksdb::Status s = zset_db.RangeByScore(args_[1], spec_, &memeber_scores, &size);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    if (!with_scores_) {
      output->append(Redis::MultiLen(memeber_scores.size()));
    } else {
      output->append(Redis::MultiLen(memeber_scores.size()*2));
    }
    for (const auto ms : memeber_scores) {
      output->append(Redis::BulkString(ms.member));
      if (with_scores_) output->append(Redis::BulkString(std::to_string(ms.score)));
    }
    return Status::OK();
  }

 private:
  ZRangeSpec spec_;
  bool with_scores_;
};

class CommandZRank: public Commander {
 public:
  explicit CommandZRank(bool reversed = false) : Commander("zrank", 3, false, false), reversed_(reversed) {}
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
  explicit CommandZRevRank():CommandZRank(true) {name_ = "zrevrank";}
};

class CommandZRem: public Commander {
 public:
  explicit CommandZRem(): Commander("zrem", -3, false, true) {}
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

class CommandZRemRangeByRank: public Commander {
 public:
  explicit CommandZRemRangeByRank(): Commander("zremrangebyrank", 4, false, true) {}
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
    rocksdb::Status s = zset_db.RemoveRangeByRank(args_[1], start_, stop_, &ret);
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

class CommandZRemRangeByScore: public Commander {
 public:
  explicit CommandZRemRangeByScore() : Commander("zremrangebyscore", -4, false, true) {}
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

class CommandZScore: public Commander {
 public:
  explicit CommandZScore() : Commander("zscore", 3, false, false) {}
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

class CommandInfo: public Commander {
 public:
  explicit CommandInfo(): Commander("info", -1, false, false) {}
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

class CommandCompact: public  Commander {
 public:
  explicit CommandCompact() : Commander("compact", 1, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    rocksdb::Status s = svr->storage_->Compact();
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    LOG(INFO) << "Commpact was triggered by manual with executed success.";
    return Status::OK();
  }
};

class CommandDBSize: public  Commander {
 public:
  explicit CommandDBSize() : Commander("dbsize", -1, false, false) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::string ns = conn->GetNamespace();
    if(args_.size() == 1) {
      *output = Redis::Integer(svr->GetLastKeyNum(ns));
    } else if (args_.size() == 2 && args_[1] == "scan"){
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

class CommandPublish: public Commander {
 public:
  explicit CommandPublish() : Commander("publish", 3, false, true) {}
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int receivers = svr->PublishMessage(args_[1], args_[2]);
    *output = Redis::Integer(receivers);
    return Status::OK();
  }
};

class CommandSubscribe : public Commander {
 public:
  explicit CommandSubscribe() : Commander("subcribe", -2, false, false) {}
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
  explicit CommandUnSubscribe() : Commander("unsubcribe", -1, false, false) {}
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
  explicit CommandSlaveOf() : Commander("slaveof", 3, false, false) {}
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
      port_ = static_cast<uint32_t>(p);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
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
  explicit CommandPSync() : Commander("psync", 2, true, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    try {
      auto s = std::stoull(args[1]);
      seq_ = static_cast<rocksdb::SequenceNumber>(s);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  Status SidecarExecute(Server *svr, Connection *conn) override {
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;
    int sock_fd = conn->GetFD();

    // If seq_ is larger than storage_'s seq, return error
    if (!checkWALBoundary(svr->storage_, seq_).IsOK()) {
      svr->stats_.IncrPSyncErrCounter();
      sock_send(sock_fd, Redis::Error("sequence out of range"));
      return Status(Status::RedisExecErr);
    } else {
      svr->stats_.IncrPSyncOKCounter();
      if (sock_send(sock_fd, Redis::SimpleString("OK")) < 0) {
        return Status(Status::NetSendErr);
      }
    }

    while (true) {
      // FIXME: check socket errors
      //if (!sock_check_liveness(sock_fd)) {
      //  LOG(ERROR) << "Connection was closed by peer";
      //  return Status(Status::NetSendErr);
      //}
      auto s = svr->storage_->GetWALIter(seq_, &iter);
      if (!s.IsOK()) {
        waitTilWALHasNewData(svr->storage_);
        continue;
      }

      // Everything is OK, streaming the batch data
      while (iter->Valid()) {
        LOG(INFO) << "WAL send batch";
        auto batch = iter->GetBatch();
        auto data = batch.writeBatchPtr->Data();
        // Send data in redis bulk string format
        std::string bulk_str =
            "$" + std::to_string(data.length()) + CRLF + data + CRLF;
        if (sock_send(sock_fd, bulk_str) < 0) {
          return Status(Status::NetSendErr);
        }
        seq_ = batch.sequence + 1;
        waitTilWALHasNewData(svr->storage_);
        iter->Next();
      }

      // if arrived here, means the wal file is rotated, a reopen is needed.
      LOG(INFO) << "WAL rotate";
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

 private:
  rocksdb::SequenceNumber seq_;

  // Return OK if the seq is in the range of the current WAL
  Status checkWALBoundary(Engine::Storage *storage, rocksdb::SequenceNumber seq) {
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

  void waitTilWALHasNewData(Engine::Storage *storage) {
    while (seq_ > storage->GetDB()->GetLatestSequenceNumber()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }
};

class CommandFetchMeta : public Commander {
 public:
  explicit CommandFetchMeta() : Commander("_fetch_meta", 1, true, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    return Status::OK();
  }

  Status SidecarExecute(Server *svr, Connection *conn) override {
    uint64_t file_size;
    rocksdb::BackupID meta_id;
    int sock_fd = conn->GetFD();
    auto fd = Engine::Storage::BackupManager::OpenLatestMeta(svr->storage_,
                                                             &meta_id,
                                                             &file_size);
    if (fd < 0) {
      sock_send(sock_fd, Redis::Error("failed to open"));
      return Status(Status::DBBackupFileErr);
    }
    off_t offset;
    // Send the meta ID
    sock_send(sock_fd, std::to_string(meta_id) + CRLF);
    // Send meta file size
    sock_send(sock_fd, std::to_string(file_size) + CRLF);
    // Send meta content
    if (sendfile(fd, sock_fd, 0, &offset, nullptr, 0) < 0) {
      LOG(ERROR) << "Failed to send meta file";
      return Status(Status::NetSendErr);
    }
    svr->stats_.IncrFullSyncCounter();
    return Status::OK();
  }
};

class CommandFetchFile: public Commander {
 public:
  CommandFetchFile() : Commander("_fetch_file", 2, true, false) {}

  Status Parse(const std::vector<std::string> &args) override {
    path_ = args[1];
    return Status::OK();
  }

  Status SidecarExecute(Server *svr, Connection *conn) override {
    uint64_t file_size = 0;
    auto fd = Engine::Storage::BackupManager::OpenDataFile(svr->storage_, path_,
                                                           &file_size);
    if (fd < 0) {
      sock_send(conn->GetFD(), Redis::Error("failed to open"));
      return Status(Status::DBBackupFileErr);
    }

    off_t offset;
    sock_send(conn->GetFD(), std::to_string(file_size) + CRLF);
    if (sendfile(fd, conn->GetFD(), 0, &offset, nullptr, 0) < 0) {
      LOG(ERROR) << "Failed to send data file";
      return Status(Status::NetSendErr);
    }

    // TODO: we could keep receiving _fetch_file cmd and send file
    // so to avoid creating thread for every _fetch_file
    return Status::OK();
  }

 private:
  std::string path_;
};

class CommandDBName: public Commander {
 public:
  CommandDBName(): Commander("_db_name", 1, false, false) {}

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
    {"auth", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandAuth); }},
    {"ping", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandPing); }},
    {"info", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandInfo); }},
    {"config", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandConfig); }},
    {"namespace", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandNamespace); }},
    {"keys", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandKeys); }},
    {"flushall", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandFlushAll); }},
    {"dbsize", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandDBSize); }},
    // key command
    {"ttl", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandTTL); }},
    {"type", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandType); }},
    {"exists", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandExists); }},
    {"expire", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandExpire); }},
    {"del", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandDel); }},
    //string command
    {"get", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandGet); }},
    {"set", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSet); }},
    {"incrby", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandIncrBy); }},
    {"incr", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandIncr); }},
    {"decrby", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandDecrBy); }},
    {"decr", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandDecr); }},
    // hash command
    {"hget", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHGet); }},
    {"hincrby", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHIncrBy); }},
    {"hset", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHSet); }},
    {"hsetnx", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHSetNX); }},
    {"hdel", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHDel); }},
    {"hstrlen", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHStrlen); }},
    {"hexists", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHExists); }},
    {"hlen", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHLen); }},
    {"hmget", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHMGet); }},
    {"hmset", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHMSet); }},
    {"hkeys", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHKeys); }},
    {"hvals", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHVals); }},
    {"hgetall", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandHGetAll); }},
    // list command
    {"lpush", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLPush); }},
    {"rpush", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandRPush); }},
    {"lpushx", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLPushX); }},
    {"rpushx", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandRPushX); }},
    {"lpop", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLPop); }},
    {"rpop", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandRPop); }},
    {"lrange", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLRange); }},
    {"lindex", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLIndex); }},
    {"ltrim", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLTrim); }},
    {"llen", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLLen); }},
    {"lset", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLSet); }},
    {"lpushrpop", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandLPushRPop); }},
    // set command
    {"sadd", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSAdd); }},
    {"srem", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSRem); }},
    {"scard", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSCard); }},
    {"smembers", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSMembers); }},
    {"sismember", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSIsMember); }},
    {"spop", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSPop); }},
    {"srandmember", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSRandMember); }},
    {"smove", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSMove); }},
    // zset command
    {"zadd", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZAdd); }},
    {"zcard", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZCard); }},
    {"zcount", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZCount); }},
    {"zincrby", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZIncrBy); }},
    {"zpopmax", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZPopMax); }},
    {"zpopmin", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZPopMin); }},
    {"zrange", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRange); }},
    {"zrangebyscore", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRangeByScore); }},
    {"zrank", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRank); }},
    {"zrem", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRem); }},
    {"zremrangebyrank", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRemRangeByRank); }},
    {"zremrangebyscore", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRemRangeByScore); }},
    {"zrevrank", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZRevRank); }},
    {"zscore", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandZScore); }},
    // pub/sub command
    {"publish", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandPublish); }},
    {"subscribe", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSubscribe); }},
    {"unsubscribe", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandUnSubscribe); }},
    // admin command
    {"slaveof", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSlaveOf); }},
    {"psync", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandPSync); }},
    {"compact", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandCompact); }},
    {"_fetch_meta", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandFetchMeta); }},
    {"_fetch_file", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandFetchFile); }},
    {"_db_name", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandDBName); }},
};

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
  auto fd = bufferevent_getfd(bev);
  // 1. remove FD' events
  bufferevent_setcb(bev, nullptr, nullptr, nullptr, nullptr);
  bufferevent_disable(bev, EV_READ | EV_WRITE);
  // 2. change FD to blocking mode
  make_socket_blocking(fd);
}

SidecarCommandThread::SidecarCommandThread(std::unique_ptr<Redis::Commander> cmd,
                                           Server *svr,
                                           Redis::Connection *conn)
    : cmd_(std::move(cmd)), svr_(svr), conn_(conn) {
  // NOTE: from now on, the bev is managed by the replication thread.
  // start the replication thread
    TakeOverBufferEvent(conn->GetBufferEvent());
}

void SidecarCommandThread::Stop() {
  delete conn_;
}

Status SidecarCommandThread::Start() {
  try {
    t_ = std::thread([this]() { this->Run(); });
  } catch (const std::system_error &e) {
    LOG(ERROR) << "Failed to create thread";
    Stop();
  }
  return Status::OK();
}

void SidecarCommandThread::Run() {
  cmd_->SidecarExecute(svr_, conn_);
  Stop();
}

}  // namespace Redis
