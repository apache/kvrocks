#include <types/redis_cuckoo.h>

#include "commander.h"
#include "commands/command_parser.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

/// CF.ADD key item
class CommandCFAdd final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    int ret{};
    auto s = cf.Add(ctx, args_[1], args_[2], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// CF.ADDNX key item
class CommandCFAddNX final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    int ret{};
    auto s = cf.Add(ctx, args_[1], args_[2], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// CF.COUNT key item
class CommandCFCount final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    uint64_t ret{};
    auto s = cf.Count(ctx, args_[1], args_[2], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// CF.DEL key item
class CommandCFDel final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    int ret{};
    auto s = cf.Del(ctx, args_[1], args_[2], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// CF.EXISTS key item
class CommandCFExists final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    int ret{};
    auto s = cf.Exists(ctx, args_[1], args_[2], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Integer(ret);
    return Status::OK();
  }
};

/// CF.INFO key
class CommandCFInfo final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    redis::CuckooFilterInfo ret{};
    auto s = cf.Info(ctx, args_[1], &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::Array(
        {redis::BulkString("Size"), redis::Integer(ret.size), redis::BulkString("Number of buckets"),
         redis::Integer(ret.num_buckets), redis::BulkString("Number of filters"), redis::Integer(ret.num_filters),
         redis::BulkString("Number of items inserted"), redis::Integer(ret.num_items),
         redis::BulkString("Number of items deleted"), redis::Integer(ret.num_deletes),
         redis::BulkString("Bucket size"), redis::Integer(ret.bucket_size), redis::BulkString("Expansion rate"),
         redis::Integer(ret.expansion), redis::BulkString("Max iterations"), redis::Integer(ret.max_iterations)});
    return Status::OK();
  }
};

/// CF.INSERT key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
class CommandCFInsert final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    while (parser.Good()) {
      if (parser.EatEqICase("CAPACITY")) {
        if (capacity_set_) {
          return {Status::RedisParseErr, "CAPACITY option cannot be specified multiple times"};
        }
        StatusOr<uint64_t> parsed_value = parser.TakeInt<uint64_t>();
        if (!parsed_value) {
          return {Status::RedisParseErr, "CAPACITY option requires an integer value"};
        }
        if (*parsed_value <= 0) {
          return {Status::RedisParseErr, "CAPACITY must be greater than zero"};
        } else {
          capacity_ = *parsed_value;
          capacity_set_ = true;
        }
      } else if (parser.EatEqICase("NOCREATE")) {
        no_create_ = true;
      } else if (parser.EatEqICase("ITEMS")) {
        items_found_ = true;
        while (parser.Good()) {
          auto result = parser.TakeStr();
          if (!result) {
            return {Status::RedisParseErr, "Error parsing item"};
          }
          std::string item = std::move(*result);
          items_.emplace_back(std::move(item));
        }
        break;
      } else {
        return {Status::RedisParseErr, "Syntax error: unexpected token "};
      }
    }

    if (!items_found_) {
      return {Status::RedisParseErr, "Syntax error: ITEMS keyword not found"};
    }
    if (items_.empty()) {
      return {Status::RedisParseErr, "No items specified for insertion"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    std::vector<int> ret{};

    auto s = cf.Insert(ctx, args_[1], items_, &ret, capacity_, no_create_);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_array;
    result_array.reserve(ret.size());
    for (const auto &item : ret) {
      result_array.emplace_back(redis::Integer(item));
    }

    *output = redis::Array(result_array);
    return Status::OK();
  }

 private:
  uint64_t capacity_ = 1024;
  bool no_create_ = false;
  bool capacity_set_ = false;
  bool items_found_ = false;
  std::vector<std::string> items_;
};

/// CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
class CommandCFInsertNX final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    while (parser.Good()) {
      if (parser.EatEqICase("CAPACITY")) {
        if (capacity_set_) {
          return {Status::RedisParseErr, "CAPACITY option cannot be specified multiple times"};
        }
        StatusOr<uint64_t> parsed_value = parser.TakeInt<uint64_t>();
        if (!parsed_value) {
          return {Status::RedisParseErr, "CAPACITY option requires an integer value"};
        }
        if (*parsed_value <= 0) {
          return {Status::RedisParseErr, "CAPACITY must be a positive integer"};
        } else {
          capacity_ = *parsed_value;
          capacity_set_ = true;
        }
      } else if (parser.EatEqICase("NOCREATE")) {
        no_create_ = true;
      } else if (parser.EatEqICase("ITEMS")) {
        items_found_ = true;
        while (parser.Good()) {
          auto result = parser.TakeStr();
          if (!result) {
            return {Status::RedisParseErr, "Error parsing item"};
          }
          std::string item = std::move(*result);
          items_.emplace_back(std::move(item));
        }
        break;
      } else {
        return {Status::RedisParseErr, "Syntax error: unexpected token "};
      }
    }

    if (!items_found_) {
      return {Status::RedisParseErr, "Syntax error: ITEMS keyword not found"};
    }
    if (items_.empty()) {
      return {Status::RedisParseErr, "No items specified for insertion"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    std::vector<int> ret{};

    auto s = cf.InsertNX(ctx, args_[1], items_, &ret, capacity_, no_create_);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_array;
    result_array.reserve(ret.size());
    for (const auto &item : ret) {
      result_array.emplace_back(redis::Integer(item));
    }

    *output = redis::Array(result_array);
    return Status::OK();
  }

 private:
  uint64_t capacity_ = 1024;
  bool no_create_ = false;
  bool capacity_set_ = false;
  bool items_found_ = false;
  std::vector<std::string> items_;
};

/// CF.MEXISTS key item [item ...]
class CommandCFMExists final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);
    while (parser.Good()) {
      auto result = parser.TakeStr();
      if (!result) {
        return {Status::RedisParseErr, "Error parsing item"};
      }
      std::string item = std::move(*result);
      items_.emplace_back(std::move(item));
    }
    if (items_.empty()) {
      return {Status::RedisParseErr, "No items specified for existence check"};
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    std::vector<int> ret{};
    auto s = cf.MExists(ctx, args_[1], items_, &ret);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> result_array;
    result_array.reserve(ret.size());
    for (const auto &item : ret) {
      result_array.emplace_back(redis::Integer(item));
    }

    *output = redis::Array(result_array);
    return Status::OK();
  }

 private:
  std::vector<std::string> items_;
};

/// CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
class CommandCFReserve final : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 2);

    StatusOr<uint64_t> parsed_value = parser.TakeInt<uint64_t>();
    if (!parsed_value) {
      return {Status::RedisParseErr, "CAPACITY option requires an integer value"};
    }
    if (*parsed_value <= 0) {
      return {Status::RedisParseErr, "CAPACITY must be a positive integer"};
    }
    capacity_ = *parsed_value;

    while (parser.Good()) {
      if (parser.EatEqICase("BUCKETSIZE")) {
        if (bucket_size_set_) {
          return {Status::RedisParseErr, "BUCKETSIZE option cannot be specified multiple times"};
        }
        StatusOr<uint64_t> bucket_value = parser.TakeInt<uint64_t>();
        if (!bucket_value) {
          return {Status::RedisParseErr, "Invalid BUCKETSIZE value"};
        }
        if (*bucket_value <= 0) {
          return {Status::RedisParseErr, "BUCKETSIZE must be a positive integer"};
        }
        bucket_size_ = *bucket_value;
        bucket_size_set_ = true;

      } else if (parser.EatEqICase("MAXITERATIONS")) {
        if (max_iterations_set_) {
          return {Status::RedisParseErr, "MAXITERATIONS option cannot be specified multiple times"};
        }
        StatusOr<uint64_t> max_iterations_value = parser.TakeInt<uint64_t>();
        if (!max_iterations_value) {
          return {Status::RedisParseErr, "Invalid MAXITERATIONS value"};
        }
        if (*max_iterations_value <= 0) {
          return {Status::RedisParseErr, "MAXITERATIONS must be a positive integer"};
        }
        max_iterations_ = *max_iterations_value;
        max_iterations_set_ = true;

      } else if (parser.EatEqICase("EXPANSION")) {
        if (expansion_set_) {
          return {Status::RedisParseErr, "EXPANSION option cannot be specified multiple times"};
        }
        StatusOr<uint64_t> expansion_value = parser.TakeInt<uint64_t>();
        if (!expansion_value) {
          return {Status::RedisParseErr, "Invalid EXPANSION value"};
        }
        if (*expansion_value <= 0) {
          return {Status::RedisParseErr, "EXPANSION must be a positive integer"};
        }
        expansion_ = *expansion_value;
        expansion_set_ = true;

      } else {
        return {Status::RedisParseErr, "Syntax error: unexpected argument"};
      }
    }

    return Commander::Parse(args);
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    redis::CFilter cf(srv->storage, conn->GetNamespace());
    engine::Context ctx(srv->storage);

    uint64_t bucket_size = bucket_size_set_ ? bucket_size_ : 2;
    uint64_t max_iterations = max_iterations_set_ ? max_iterations_ : 20;
    uint64_t expansion = expansion_set_ ? expansion_ : 1;

    auto s = cf.Reserve(ctx, args_[1], capacity_, bucket_size, max_iterations, expansion);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = redis::SimpleString("OK");
    return Status::OK();
  }

 private:
  uint64_t capacity_ = 0;
  uint64_t bucket_size_ = 0;
  uint64_t max_iterations_ = 0;
  uint64_t expansion_ = 0;

  bool bucket_size_set_ = false;
  bool max_iterations_set_ = false;
  bool expansion_set_ = false;
};

REDIS_REGISTER_COMMANDS(CuckooFilter, MakeCmdAttr<CommandCFAdd>("cf.add", 3, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCFAddNX>("cf.addnx", 3, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCFCount>("cf.count", 3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandCFDel>("cf.del", 3, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCFExists>("cf.exists", 3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandCFInfo>("cf.info", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandCFInsert>("cf.insert", -4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCFInsertNX>("cf.insertnx", -4, "write", 0, 0, 0),
                        MakeCmdAttr<CommandCFMExists>("cf.mexists", -3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandCFReserve>("cf.reserve", -3, "write", 0, 0, 0), );

}  // namespace redis
