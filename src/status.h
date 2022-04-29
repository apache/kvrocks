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

#pragma once

#include <string>
#include <utility>

class Status {
 public:
  enum Code {
    cOK,
    NotOK,
    NotFound,

    // DB
    DBOpenErr,
    DBBackupErr,
    DBGetWALErr,
    DBBackupFileErr,

    // Replication
    DBMismatched,

    // Redis
    RedisUnknownCmd,
    RedisInvalidCmd,
    RedisParseErr,
    RedisExecErr,
    RedisReplicationConflict,

    // Cluster
    ClusterDown,
    ClusterInvalidInfo,

    // Slot
    SlotImport,

    // Network
    NetSendErr,
  };

  Status() : Status(cOK, "ok") {}
  explicit Status(Code code, std::string msg = "") : code_(code), msg_(std::move(msg)) {}
  bool IsOK() { return code_ == cOK; }
  bool IsNotFound() { return code_ == NotFound; }
  bool IsImorting() { return code_ == SlotImport; }
  std::string Msg() { return msg_; }
  static Status OK() { return Status(cOK, "ok"); }

 private:
  Code code_;
  std::string msg_;
};
