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
#include "scope_exit.h"
#include "server/redis_connection.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

class CommandMulti : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = redis::Error("ERR MULTI calls can not be nested");
      return Status::OK();
    }
    conn->ResetMultiExec();
    // Client starts into MULTI-EXEC
    conn->EnableFlag(Connection::kMultiExec);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandDiscard : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    auto reset_watch = MakeScopeExit([svr, conn] { svr->ResetWatchedKeys(conn); });

    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = redis::Error("ERR DISCARD without MULTI");
      return Status::OK();
    }

    conn->ResetMultiExec();
    *output = redis::SimpleString("OK");

    return Status::OK();
  }
};

class CommandExec : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    auto reset_watch = MakeScopeExit([svr, conn] { svr->ResetWatchedKeys(conn); });

    if (!conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = redis::Error("ERR EXEC without MULTI");
      return Status::OK();
    }

    auto reset_multiexec = MakeScopeExit([conn] { conn->ResetMultiExec(); });

    if (conn->IsMultiError()) {
      *output = redis::Error("EXECABORT Transaction discarded");
      return Status::OK();
    }

    if (svr->IsWatchedKeysModified(conn)) {
      *output = redis::NilString();
      return Status::OK();
    }

    auto storage = svr->storage;
    // Reply multi length first
    conn->Reply(redis::MultiLen(conn->GetMultiExecCommands()->size()));
    // Execute multi-exec commands
    conn->SetInExec();
    auto s = storage->BeginTxn();
    if (s.IsOK()) {
      conn->ExecuteCommands(conn->GetMultiExecCommands());
      s = storage->CommitTxn();
    }
    return s;
  }
};

class CommandWatch : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (conn->IsFlagEnabled(Connection::kMultiExec)) {
      *output = redis::Error("ERR WATCH inside MULTI is not allowed");
      return Status::OK();
    }

    svr->WatchKey(conn, std::vector<std::string>(args_.begin() + 1, args_.end()));
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

class CommandUnwatch : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    svr->ResetWatchedKeys(conn);
    *output = redis::SimpleString("OK");
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandDiscard>("discard", 1, "multi", 0, 0, 0),
                        MakeCmdAttr<CommandExec>("exec", 1, "exclusive multi", 0, 0, 0),
                        MakeCmdAttr<CommandMulti>("multi", 1, "multi", 0, 0, 0),
                        MakeCmdAttr<CommandUnwatch>("unwatch", 1, "multi", 0, 0, 0),
                        MakeCmdAttr<CommandWatch>("watch", -2, "multi", 1, -1, 1), )

}  // namespace redis
