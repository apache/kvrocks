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

#include <map>

namespace redis {

enum class ErrorKind {
  None,
  Err,
  WrongType,
  NoScript,
  NoProto,
  NoAuth,
  Loading,
  Readonly,
  MasterDown,
  ExecAbort,
  Moved,
  TryAgain,
  ClusterDown,
  CrossSlot,
};

const std::map<ErrorKind, std::string> ErrorKindMap = {{ErrorKind::Err, "ERR"},
                                                       {ErrorKind::WrongType, "WRONGTYPE"},
                                                       {ErrorKind::NoScript, "NOSCRIPT"},
                                                       {ErrorKind::NoProto, "NOPROTO"},
                                                       {ErrorKind::NoAuth, "NOAUTH"},
                                                       {ErrorKind::Loading, "LOADING"},
                                                       {ErrorKind::Readonly, "READONLY"},
                                                       {ErrorKind::MasterDown, "MASTERDOWN"},
                                                       {ErrorKind::ExecAbort, "EXECABORT"},
                                                       {ErrorKind::Moved, "MOVED"},
                                                       {ErrorKind::TryAgain, "TRYAGAIN"},
                                                       {ErrorKind::ClusterDown, "CLUSTERDOWN"},
                                                       {ErrorKind::CrossSlot, "CROSSSLOT"}};

}  // namespace redis
