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

#include <memory>
#include <variant>

#include "commander.h"
#include "commands/command_parser.h"
#include "search/index_info.h"
#include "search/ir_plan.h"
#include "search/ir_sema_checker.h"
#include "search/passes/manager.h"
#include "search/plan_executor.h"
#include "search/search_encoding.h"
#include "search/sql_transformer.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "tao/pegtl/string_input.hpp"

namespace redis {

class CommandFTCreate : public Commander {
  Status Parse(const std::vector<std::string> &args) override {
    CommandParser parser(args, 1);

    auto index_name = GET_OR_RET(parser.TakeStr());
    if (index_name.empty()) {
      return {Status::RedisParseErr, "index name cannot be empty"};
    }

    index_info_ = std::make_unique<kqir::IndexInfo>(index_name, redis::IndexMetadata{}, "");
    auto data_type = IndexOnDataType(0);

    while (parser.Good()) {
      if (parser.EatEqICase("ON")) {
        if (parser.EatEqICase("HASH")) {
          data_type = IndexOnDataType::HASH;
        } else if (parser.EatEqICase("JSON")) {
          data_type = IndexOnDataType::JSON;
        } else {
          return {Status::RedisParseErr, "expect HASH or JSON after ON"};
        }
      } else if (parser.EatEqICase("PREFIX")) {
        size_t count = GET_OR_RET(parser.TakeInt<size_t>());

        for (size_t i = 0; i < count; ++i) {
          index_info_->prefixes.prefixes.push_back(GET_OR_RET(parser.TakeStr()));
        }
      } else {
        break;
      }
    }

    if (int(data_type) == 0) {
      return {Status::RedisParseErr, "expect ON HASH | JSON"};
    } else {
      index_info_->metadata.on_data_type = data_type;
    }

    if (parser.EatEqICase("SCHEMA")) {
      while (parser.Good()) {
        auto field_name = GET_OR_RET(parser.TakeStr());
        if (field_name.empty()) {
          return {Status::RedisParseErr, "field name cannot be empty"};
        }

        std::unique_ptr<redis::IndexFieldMetadata> field_meta;
        if (parser.EatEqICase("TAG")) {
          field_meta = std::make_unique<redis::TagFieldMetadata>();
        } else if (parser.EatEqICase("NUMERIC")) {
          field_meta = std::make_unique<redis::NumericFieldMetadata>();
        } else {
          return {Status::RedisParseErr, "expect field type TAG or NUMERIC"};
        }

        while (parser.Good()) {
          if (parser.EatEqICase("NOINDEX")) {
            field_meta->noindex = true;
          } else if (auto tag = dynamic_cast<redis::TagFieldMetadata *>(field_meta.get())) {
            if (parser.EatEqICase("CASESENSITIVE")) {
              tag->case_sensitive = true;
            } else if (parser.EatEqICase("SEPARATOR")) {
              auto sep = GET_OR_RET(parser.TakeStr());

              if (sep.size() != 1) {
                return {Status::NotOK, "only one character separator is supported"};
              }

              tag->separator = sep[0];
            } else {
              break;
            }
          } else {
            break;
          }
        }

        kqir::FieldInfo field_info(field_name, std::move(field_meta));

        index_info_->Add(std::move(field_info));
      }
    } else {
      return {Status::RedisParseErr, "expect SCHEMA section for this index"};
    }

    if (parser.Good()) {
      return {Status::RedisParseErr, "more token than expected in command arguments"};
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    index_info_->ns = conn->GetNamespace();

    GET_OR_RET(srv->index_mgr.Create(std::move(index_info_)));

    output->append(redis::SimpleString("OK"));
    return Status::OK();
  };

 private:
  std::unique_ptr<kqir::IndexInfo> index_info_;
};

class CommandFTSearchSQL : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    const auto &index_map = srv->index_mgr.index_map;
    const auto &sql = args_[1];

    auto ir = GET_OR_RET(kqir::sql::ParseToIR(kqir::peg::string_input(sql, "ft.searchsql")));

    kqir::SemaChecker sema_checker(index_map);
    sema_checker.ns = conn->GetNamespace();

    GET_OR_RET(sema_checker.Check(ir.get()));

    auto plan_ir = kqir::PassManager::Execute(kqir::PassManager::Default(), std::move(ir));
    std::unique_ptr<kqir::PlanOperator> plan_op;
    if (plan_op = kqir::Node::As<kqir::PlanOperator>(std::move(plan_ir)); !plan_op) {
      return {Status::NotOK, "failed to convert the SQL query to plan operators"};
    }

    kqir::ExecutorContext executor_ctx(plan_op.get(), srv->storage);

    std::vector<kqir::ExecutorContext::RowType> results;

    auto iter_res = GET_OR_RET(executor_ctx.Next());
    while (!std::holds_alternative<kqir::ExecutorNode::End>(iter_res)) {
      results.push_back(std::get<kqir::ExecutorContext::RowType>(iter_res));

      iter_res = GET_OR_RET(executor_ctx.Next());
    }

    output->append(MultiLen(results.size()));
    for (const auto &[key, fields, _] : results) {
      output->append(MultiLen(2));
      output->append(redis::BulkString(key));
      output->append(MultiLen(fields.size()));
      for (const auto &[_, field] : fields) {
        output->append(redis::BulkString(field));
      }
    }

    return Status::OK();
  };
};

class CommandFTInfo : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    const auto &index_map = srv->index_mgr.index_map;
    const auto &index_name = args_[1];

    auto iter = index_map.Find(index_name, conn->GetNamespace());
    if (iter == index_map.end()) {
      return {Status::RedisExecErr, "index not found"};
    }

    const auto &info = iter->second;
    output->append(MultiLen(8));

    output->append(redis::SimpleString("index_name"));
    output->append(redis::BulkString(info->name));

    output->append(redis::SimpleString("on_data_type"));
    output->append(redis::BulkString(RedisTypeNames[(size_t)info->metadata.on_data_type]));

    output->append(redis::SimpleString("prefixes"));
    output->append(redis::ArrayOfBulkStrings(info->prefixes.prefixes));

    output->append(redis::SimpleString("fields"));
    output->append(MultiLen(info->fields.size()));
    for (const auto &[_, field] : info->fields) {
      output->append(MultiLen(2));
      output->append(redis::BulkString(field.name));
      auto type = field.metadata->Type();
      output->append(redis::BulkString(std::string(type.begin(), type.end())));
    }

    return Status::OK();
  };
};

class CommandFTList : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    const auto &index_map = srv->index_mgr.index_map;

    std::vector<std::string> results;
    for (const auto &[_, index] : index_map) {
      if (index->ns == conn->GetNamespace()) {
        results.push_back(index->name);
      }
    }

    output->append(ArrayOfBulkStrings(results));

    return Status::OK();
  };
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandFTCreate>("ft.create", -2, "write exclusive no-multi no-script", 0, 0, 0),
                        MakeCmdAttr<CommandFTSearchSQL>("ft.searchsql", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTInfo>("ft.info", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTList>("ft._list", 1, "read-only", 0, 0, 0));

}  // namespace redis
