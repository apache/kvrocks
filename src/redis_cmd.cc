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
#include "server.h"
#include "sock_util.h"
#include "storage.h"
#include "string_util.h"
#include "t_string.h"
#include "t_hash.h"
#include "t_list.h"
#include "t_set.h"
#include "t_zset.h"

namespace Redis {
class CommandPing : public Commander {
 public:
  explicit CommandPing() : Commander("ping", 1) {}
  Status Execute(Server *svr, std::string *output) override {
    *output = Redis::SimpleString("PONG");
    return Status::OK();
  }
};

class CommandGet : public Commander {
 public:
  explicit CommandGet() :Commander("get", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    std::string value;
    RedisString string_db(svr->storage_);
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
  explicit CommandSet() :Commander("set", 3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisString string_db(svr->storage_);
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
  explicit CommandIncr() : Commander("incr", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_);
    rocksdb::Status s = string_db.IncrBy(args_[1], 1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandDecr: public Commander {
 public:
  explicit CommandDecr() : Commander("decr", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_);
    rocksdb::Status s = string_db.IncrBy(args_[1], -1, &ret);
    if (!s.ok()) return Status(Status::RedisExecErr, s.ToString());
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandIncrBy : public Commander {
 public:
  explicit CommandIncrBy() : Commander("incrby", 3) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisExecErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_);
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
  explicit CommandDecrBy() : Commander("decrby", 3) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisExecErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, std::string *output) override {
    int64_t ret;
    RedisString string_db(svr->storage_);
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
  explicit CommandDel() :Commander("del", -2) {}
  Status Execute(Server *svr, std::string *output) override {
    int cnt = 0;
    RedisDB redis(svr->storage_);
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
  explicit CommandType () : Commander("type", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisDB redis(svr->storage_);
    RedisType type;
    rocksdb::Status s = redis.Type(args_[1], &type);
    if (s.ok()) {
      std::vector<std::string> type_names = {"none", "string", "hash", "list", "set", "zset"};
      *output = Redis::BulkString(type_names[type]);
      return Status::OK();
    } else {
      return Status(Status::RedisExecErr, s.ToString());
    }
  }
};

class CommandTTL: public Commander {
 public:
  explicit CommandTTL() : Commander("ttl", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisDB redis(svr->storage_);
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

class CommandExpire: public Commander {
 public:
  explicit CommandExpire() : Commander("expire", 2) {}
  Status Parse(const std::vector<std::string> &args) override {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    try {
      seconds_ = std::stoi(args[2]);
      // FIXME: seconds_ may overflow
      seconds_ += now;
    } catch (std::exception &e) {
      return Status(Status::RedisExecErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, std::string *output) override {
    RedisDB redis(svr->storage_);
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
  explicit CommandHGet() :Commander("hget", 3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHSet() : Commander("hset", 4) {}
  Status Execute(Server *svr, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHSetNX() : Commander("hsetnx", 4) {}
  Status Execute(Server *svr, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHStrlen() : Commander("hstrlen", 3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHDel() : Commander("hdel", -3) {}
  Status Execute(Server *svr, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHExists() : Commander("hexists", 3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHLen() : Commander("hlen", 3) {}
  Status Execute(Server *svr, std::string *output) override {
    uint32_t count;
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHIncrBy() : Commander("hincrby", 4) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      increment_ = std::stoll(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisExecErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHMGet() : Commander("hmget", -3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHMSet() : Commander("hmset", -4) {}
  Status Parse(const std::vector<std::string> &args) override {
    if(args.size() % 2 != 0) {
      return Status(Status::RedisParseErr, "wrong number of arguments");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, std::string *output) override {
    int ret;
    RedisHash hash_db(svr->storage_);
    std::vector<FieldValue> field_values;
    for (unsigned int i = 2; i < args_.size(); i++) {
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
  explicit CommandHKeys() : Commander("hkeys", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHVals() : Commander("hvals", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
  explicit CommandHGetAll() : Commander("hgetall", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisHash hash_db(svr->storage_);
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
      :Commander("push", -3) {
    left_ = left;
    create_if_missing_ = create_if_missing;
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandPop(bool left) : Commander("pop", 2) {
    left_ = left;
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandLRange() : Commander("lrange", 4) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandLLen() : Commander("llen", 2){}
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandLIndex() : Commander("lindex", 3) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
    std::string elem;
    rocksdb::Status s = list_db.Index(args_[1], index_, &elem);
    if (!s.ok() && !s.IsNotFound()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = s.IsNotFound() ? Redis::NilString() : Redis::BulkString(elem);
    return Status::OK();
  }

 private:
  int index_;
};

class CommandLSet: public Commander {
 public:
  explicit CommandLSet() : Commander("lset", 4) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      index_ = std::stoi(args[2]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandLTrim() : Commander("ltrim", 4) {}
  Status Parse(const std::vector<std::string> &args) override {
    try {
      start_ = std::stoi(args[2]);
      stop_ = std::stoi(args[3]);
    } catch (std::exception &e) {
      return Status(Status::RedisParseErr, "value is not an integer or out of range");
    }
    return Commander::Parse(args);
  }
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandLPushRPop() : Commander("lpushrpop", 3){}
  Status Execute(Server *svr, std::string *output) override {
    RedisList list_db(svr->storage_);
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
  explicit CommandSAdd() : Commander("sadd", -3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSRem() : Commander("srem", -3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSCard() : Commander("scard", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSMembers() : Commander("smembers", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSIsMember() : Commander("sismmeber", 3) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSPop() : Commander("spop", -2) {}
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
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSRandMember() : Commander("srandmember", -2) {}
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
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
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
  explicit CommandSMove() : Commander("smove", 4) {}
  Status Execute(Server *svr, std::string *output) override {
    RedisSet set_db(svr->storage_);
    int ret;
    rocksdb::Status s = set_db.Move(args_[1], args_[2], args_[3], &ret);
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::Integer(ret);
    return Status::OK();
  }
};

class CommandCompact: public  Commander {
 public:
  explicit CommandCompact() : Commander("compact", 2) {}
  Status Execute(Server *svr, std::string *output) override {
    rocksdb::Status s = svr->storage_->Compact();
    if (!s.ok()) {
      return Status(Status::RedisExecErr, s.ToString());
    }
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandSlaveOf : public Commander {
 public:
  explicit CommandSlaveOf() : Commander("slaveof", 3) {}
  Status Parse(const std::vector<std::string> &args) override {
    host_ = args[1];
    auto port = args[2];
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
  Status Execute(Server *svr, std::string *output) override {
    auto s = svr->AddMaster(host_, port_);
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
  explicit CommandPSync() : Commander("psync", 2, true) {}

  Status Parse(const std::vector<std::string> &args) override {
    try {
      auto s = std::stoull(args[1]);
      seq_ = static_cast<rocksdb::SequenceNumber>(s);
    } catch (const std::exception &e) {
      return Status(Status::RedisParseErr);
    }
    return Commander::Parse(args);
  }

  Status SidecarExecute(Server *svr, int sock_fd) override {
    std::unique_ptr<rocksdb::TransactionLogIterator> iter;

    // If seq_ is larger than storage's seq, return error
    if (!checkWALBoundary(svr->storage_, seq_).IsOK()) {
      sock_send(sock_fd, Redis::Error("sequence out of range"));
      return Status(Status::RedisExecErr);
    } else {
      if (sock_send(sock_fd, Redis::SimpleString("OK")) < 0) {
        return Status(Status::NetSendErr);
      }
    }

    while (true) {
      // TODO: test if sock_fd is closed on the other side, HEARTBEAT
      int sock_err = 0;
      socklen_t sock_err_len = sizeof(sock_err);
      if (getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, (void *)&sock_err,
                     &sock_err_len) < 0 ||
          sock_err) {
        LOG(ERROR) << "Socket err: " << evutil_socket_error_to_string(sock_err);
        return Status(Status::NetSendErr);
      }

      auto s = svr->storage_->GetWALIter(seq_, &iter);
      if (!s.IsOK()) {
        // LOG(ERROR) << "Failed to get WAL iter: " << s.msg();
        std::this_thread::sleep_for(std::chrono::seconds(1));
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
};

class CommandFetchMeta : public Commander {
 public:
  explicit CommandFetchMeta() : Commander("_fetch_meta", 1, true) {}

  Status Parse(const std::vector<std::string> &args) override {
    return Status::OK();
  }

  Status SidecarExecute(Server *svr, int sock_fd) override {
    uint64_t file_size;
    rocksdb::BackupID meta_id;
    auto fd = Engine::Storage::BackupManager::OpenLatestMeta(svr->storage_,
                                                             &meta_id,
                                                             &file_size);
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
    return Status::OK();
  }

 private:
  rocksdb::BackupID meta_id_;
};

class CommandFetchFile: public Commander {
 public:
  CommandFetchFile() : Commander("_fetch_file", 2, true) {}

  Status Parse(const std::vector<std::string> &args) override {
    path_ = args[1];
    return Status::OK();
  }

  Status SidecarExecute(Server *svr, int sock_fd) override {
    uint64_t file_size = 0;
    auto fd = Engine::Storage::BackupManager::OpenDataFile(svr->storage_, path_,
                                                           &file_size);
    off_t offset;
    sock_send(sock_fd, std::to_string(file_size) + CRLF);
    if (sendfile(fd, sock_fd, 0, &offset, nullptr, 0) < 0) {
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

using CommanderFactory = std::function<std::unique_ptr<Commander>()>;
std::map<std::string, CommanderFactory> command_table = {
    {"ping", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandPing); }},
    // key command
    {"ttl", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandTTL); }},
    {"type", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandType); }},
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
    // set commadn
    {"sadd", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSAdd); }},
    {"srem", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSRem); }},
    {"scard", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSCard); }},
    {"smembers", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSMembers); }},
    {"sismember", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSIsMember); }},
    {"spop", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSPop); }},
    {"srandmember", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSRandMember); }},
    {"smove", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSMove); }},
    // admin command
    {"slaveof", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandSlaveOf); }},
    {"psync", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandPSync); }},
    {"compact", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandCompact); }},
    {"_fetch_meta", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandFetchMeta); }},
    {"_fetch_file", []() -> std::unique_ptr<Commander> { return std::unique_ptr<Commander>(new CommandFetchFile); }},
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
