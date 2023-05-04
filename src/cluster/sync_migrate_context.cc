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

#include "cluster/sync_migrate_context.h"

SyncMigrateContext::~SyncMigrateContext() {
  if (timer_) {
    event_free(timer_);
    timer_ = nullptr;
  }
}

void SyncMigrateContext::StartBlock() {
  auto bev = conn_->GetBufferEvent();
  bufferevent_setcb(bev, nullptr, WriteCB, EventCB, this);

  if (timeout_) {
    timer_ = evtimer_new(bufferevent_get_base(bev), TimerCB, this);
    timeval tm = {timeout_, 0};
    evtimer_add(timer_, &tm);
  }
}

void SyncMigrateContext::Wakeup(const Status &migrate_result) {
  migrate_result_ = migrate_result;
  auto s = conn_->Owner()->EnableWriteEvent(conn_->GetFD());
  if (!s.IsOK()) {
    LOG(ERROR) << "[server] Failed to enable write event on the sync migrate connection " << conn_->GetFD() << ": "
               << s.Msg();
  }
}

void SyncMigrateContext::EventCB(bufferevent *bev, int16_t events, void *ctx) {
  auto self = reinterpret_cast<SyncMigrateContext *>(ctx);
  auto &&slot_migrator = self->svr_->slot_migrator;

  if (events & (BEV_EVENT_EOF | BEV_EVENT_ERROR)) {
    if (self->timer_ != nullptr) {
      event_free(self->timer_);
      self->timer_ = nullptr;
    }

    slot_migrator->CancelBlocking();
  }
  redis::Connection::OnEvent(bev, events, self->conn_);
}

void SyncMigrateContext::TimerCB(int, int16_t events, void *ctx) {
  auto self = reinterpret_cast<SyncMigrateContext *>(ctx);
  auto &&slot_migrator = self->svr_->slot_migrator;

  self->conn_->Reply(redis::NilString());
  event_free(self->timer_);
  self->timer_ = nullptr;

  slot_migrator->CancelBlocking();

  auto bev = self->conn_->GetBufferEvent();
  bufferevent_setcb(bev, redis::Connection::OnRead, redis::Connection::OnWrite, redis::Connection::OnEvent,
                    self->conn_);
  bufferevent_enable(bev, EV_READ);
}

void SyncMigrateContext::WriteCB(bufferevent *bev, void *ctx) {
  auto self = reinterpret_cast<SyncMigrateContext *>(ctx);

  if (self->migrate_result_) {
    self->conn_->Reply(redis::SimpleString("OK"));
  } else {
    self->conn_->Reply(redis::Error("ERR " + self->migrate_result_.Msg()));
  }

  if (self->timer_) {
    event_free(self->timer_);
    self->timer_ = nullptr;
  }

  bufferevent_setcb(bev, redis::Connection::OnRead, redis::Connection::OnWrite, redis::Connection::OnEvent,
                    self->conn_);
  bufferevent_enable(bev, EV_READ);

  bufferevent_trigger(bev, EV_READ, BEV_TRIG_IGNORE_WATERMARKS);
}
