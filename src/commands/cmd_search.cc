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
#include <sstream>
#include <variant>

#include "commander.h"
#include "commands/command_parser.h"
#include "search/index_info.h"
#include "search/ir.h"
#include "search/ir_dot_dumper.h"
#include "search/plan_executor.h"
#include "search/redis_query_transformer.h"
#include "search/search_encoding.h"
#include "search/sql_transformer.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "string_util.h"
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

static void DumpQueryResult(const std::vector<kqir::ExecutorContext::RowType> &rows, std::string *output) {
  output->append(MultiLen(rows.size() * 2 + 1));
  output->append(Integer(rows.size()));
  for (const auto &[key, fields, _] : rows) {
    output->append(redis::BulkString(key));
    output->append(MultiLen(fields.size() * 2));
    for (const auto &[info, field] : fields) {
      output->append(redis::BulkString(info->name));
      output->append(redis::BulkString(field));
    }
  }
}

class CommandFTExplainSQL : public Commander {
  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 3) {
      if (util::EqualICase(args[2], "simple")) {
        format_ = SIMPLE;
      } else if (util::EqualICase(args[2], "dot")) {
        format_ = DOT_GRAPH;
      } else {
        return {Status::NotOK, "output format should be SIMPLE or DOT"};
      }
    }

    if (args.size() > 3) {
      return {Status::NotOK, "more arguments than expected"};
    }

    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    const auto &sql = args_[1];

    auto ir = GET_OR_RET(kqir::sql::ParseToIR(kqir::peg::string_input(sql, "ft.explainsql")));

    auto plan = GET_OR_RET(srv->index_mgr.GeneratePlan(std::move(ir), conn->GetNamespace()));

    if (format_ == SIMPLE) {
      output->append(BulkString(plan->Dump()));
    } else if (format_ == DOT_GRAPH) {
      std::ostringstream ss;
      kqir::DotDumper dumper(ss);

      dumper.Dump(plan.get());
      output->append(BulkString(ss.str()));
    }

    return Status::OK();
  };

  enum OutputFormat { SIMPLE, DOT_GRAPH } format_ = SIMPLE;
};

class CommandFTSearchSQL : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    const auto &sql = args_[1];

    auto ir = GET_OR_RET(kqir::sql::ParseToIR(kqir::peg::string_input(sql, "ft.searchsql")));

    auto results = GET_OR_RET(srv->index_mgr.Search(std::move(ir), conn->GetNamespace()));

    DumpQueryResult(results, output);

    return Status::OK();
  };
};

static StatusOr<std::unique_ptr<kqir::Node>> ParseRediSearchQuery(const std::vector<std::string> &args) {
  CommandParser parser(args, 1);

  auto index_name = GET_OR_RET(parser.TakeStr());
  auto query_str = GET_OR_RET(parser.TakeStr());

  auto index_ref = std::make_unique<kqir::IndexRef>(index_name);
  auto query = kqir::Node::MustAs<kqir::QueryExpr>(
      GET_OR_RET(kqir::redis_query::ParseToIR(kqir::peg::string_input(query_str, "ft.search"))));

  auto select = std::make_unique<kqir::SelectClause>(std::vector<std::unique_ptr<kqir::FieldRef>>{});
  std::unique_ptr<kqir::SortByClause> sort_by;
  std::unique_ptr<kqir::LimitClause> limit;
  while (parser.Good()) {
    if (parser.EatEqICase("RETURNS")) {
      auto count = GET_OR_RET(parser.TakeInt<size_t>());

      for (size_t i = 0; i < count; ++i) {
        auto field = GET_OR_RET(parser.TakeStr());
        select->fields.push_back(std::make_unique<kqir::FieldRef>(field));
      }
    } else if (parser.EatEqICase("SORTBY")) {
      auto field = GET_OR_RET(parser.TakeStr());
      auto order = kqir::SortByClause::ASC;
      if (parser.EatEqICase("ASC")) {
        // NOOP
      } else if (parser.EatEqICase("DESC")) {
        order = kqir::SortByClause::DESC;
      }

      sort_by = std::make_unique<kqir::SortByClause>(order, std::make_unique<kqir::FieldRef>(field));
    } else if (parser.EatEqICase("LIMIT")) {
      auto offset = GET_OR_RET(parser.TakeInt<size_t>());
      auto count = GET_OR_RET(parser.TakeInt<size_t>());

      limit = std::make_unique<kqir::LimitClause>(offset, count);
    } else {
      return parser.InvalidSyntax();
    }
  }

  return std::make_unique<kqir::SearchExpr>(std::move(index_ref), std::move(query), std::move(limit),
                                            std::move(sort_by), std::move(select));
}

class CommandFTExplain : public Commander {
  Status Parse(const std::vector<std::string> &args) override {
    ir_ = GET_OR_RET(ParseRediSearchQuery(args));
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    CHECK(ir_);
    auto plan = GET_OR_RET(srv->index_mgr.GeneratePlan(std::move(ir_), conn->GetNamespace()));

    output->append(redis::BulkString(plan->Dump()));

    return Status::OK();
  };

 private:
  std::unique_ptr<kqir::Node> ir_;
};

class CommandFTSearch : public Commander {
  Status Parse(const std::vector<std::string> &args) override {
    ir_ = GET_OR_RET(ParseRediSearchQuery(args));
    return Status::OK();
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    CHECK(ir_);
    auto results = GET_OR_RET(srv->index_mgr.Search(std::move(ir_), conn->GetNamespace()));

    DumpQueryResult(results, output);

    return Status::OK();
  };

 private:
  std::unique_ptr<kqir::Node> ir_;
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

class CommandFTDrop : public Commander {
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    const auto &index_name = args_[1];

    GET_OR_RET(srv->index_mgr.Drop(index_name, conn->GetNamespace()));

    output->append(SimpleString("OK"));

    return Status::OK();
  };
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandFTCreate>("ft.create", -2, "write exclusive no-multi no-script", 0, 0, 0),
                        MakeCmdAttr<CommandFTSearchSQL>("ft.searchsql", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTSearch>("ft.search", -3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTExplainSQL>("ft.explainsql", -2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTExplain>("ft.explain", -3, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTInfo>("ft.info", 2, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTList>("ft._list", 1, "read-only", 0, 0, 0),
                        MakeCmdAttr<CommandFTDrop>("ft.dropindex", 2, "write exclusive no-multi no-script", 0, 0, 0));

}  // namespace redis
