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

#include <types/redis_cuckoo.h>

#include "commander.h"
#include "commands/command_parser.h"
#include "server/redis_reply.h"
#include "server/server.h"

namespace redis {

class CommandCFAdd final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFAddNX final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFCount final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFDel final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFExists final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFInfo final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFInsert final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFInsertNX final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFLoadChunk final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFMExists final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFReserve final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

class CommandCFScanDump final : public Commander {
 public:
  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    
    return Status::OK();
  }
};

REDIS_REGISTER_COMMANDS(CuckooFilter, MakeCmdAttr<CommandCFAdd>("cf.add", -2, "write", 1, 1, 1),
                        MakeCmdAttr<CommandCFAddNX>("cf.addnx", -2, "read-only", 1, -1, 1),
                        MakeCmdAttr<CommandCFCount>("cf.count", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFDel>("cf.del", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFExists>("cf.exists", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFInfo>("cf.info", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFInsert>("cf.insert", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFInsertNX>("cf.insertnx", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFLoadChunk>("cf.loadchunk", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFMExists>("cf.mexists", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFReserve>("cf.reserve", -2, "write", 1, -1, 1),
                        MakeCmdAttr<CommandCFScanDump>("cf.scandump", -2, "write", 1, -1, 1), );

}  // namespace redis
