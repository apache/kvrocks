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
#include "server/redis_connection.h"

namespace Redis {

class CommandMulti : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = Redis::Error("ERR MULTI calls can not be nested");
      return Status::OK();
    }
    conn->ResetMultiExec();
    // Client starts into MULTI-EXEC
    conn->EnableFlag(Connection::kMultiExec);
    *output = Redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandDiscard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = Redis::Error("ERR DISCARD without MULTI");
      return Status::OK();
    }

    conn->ResetMultiExec();
    *output = Redis::SimpleString("OK");

    return Status::OK();
  }
};

class CommandExec : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = Redis::Error("ERR EXEC without MULTI");
      return Status::OK();
    }

    if (conn->IsMultiError()) {
      conn->ResetMultiExec();
      *output = Redis::Error("EXECABORT Transaction discarded");
      return Status::OK();
    }

    // Reply multi length first
    conn->Reply(Redis::MultiLen(conn->GetMultiExecCommands()->size()));
    // Execute multi-exec commands
    conn->SetInExec();
    conn->ExecuteCommands(conn->GetMultiExecCommands());
    conn->ResetMultiExec();
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandMulti>("multi", 1, "multi", 0, 0, 0),
                        MakeCmdAttr<CommandDiscard>("discard", 1, "multi", 0, 0, 0),
                        MakeCmdAttr<CommandExec>("exec", 1, "exclusive multi", 0, 0, 0), )

}  // namespace Redis
