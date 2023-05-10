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
#include "event_util.h"
#include "server/server.h"

class SyncMigrateContext : private EvbufCallbackBase<SyncMigrateContext, false>,
                           private EventCallbackBase<SyncMigrateContext> {
 public:
  SyncMigrateContext(Server *svr, redis::Connection *conn, float timeout) : svr_(svr), conn_(conn), timeout_(timeout){};

  void StartBlock();
  void Wakeup(const Status &migrate_result);
  void OnWrite(bufferevent *bev);
  void OnEvent(bufferevent *bev, int16_t events);
  void TimerCB(int, int16_t events);

 private:
  Server *svr_;
  redis::Connection *conn_;
  float timeout_ = 0;
  UniqueEvent timer_;

  Status migrate_result_;
};
