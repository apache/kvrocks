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

#include <event2/bufferevent.h>
#include <unistd.h>

#include <fstream>

#include "cluster/replication.h"
#include "config.h"
#include "parser.h"
#include "server/server.h"
#include "status.h"
#include "storage/storage.h"
#include "writer.h"

class Sync {
 public:
  explicit Sync(Engine::Storage *storage, Writer *writer, Parser *parser, Kvrocks2redis::Config *config);
  ~Sync();

  Sync(const Sync &) = delete;
  Sync &operator=(const Sync &) = delete;

  void Start();
  void Stop();
  bool IsStopped() const { return stop_flag_; }

 private:
  int sock_fd_;
  bool stop_flag_ = false;
  Engine::Storage *storage_ = nullptr;
  Writer *writer_ = nullptr;
  Parser *parser_ = nullptr;
  Kvrocks2redis::Config *config_ = nullptr;
  int next_seq_fd_;
  rocksdb::SequenceNumber next_seq_ = static_cast<rocksdb::SequenceNumber>(0);

  // Internal states managed by IncrementBatchLoop procedure
  enum IncrementBatchLoopState {
    Incr_batch_size,
    Incr_batch_data,
  } incr_state_ = Incr_batch_size;

  size_t incr_bulk_len_ = 0;

  Status auth();
  Status tryPSync();
  Status incrementBatchLoop();

  void parseKVFromLocalStorage();

  Status updateNextSeq(rocksdb::SequenceNumber seq);
  Status readNextSeqFromFile(rocksdb::SequenceNumber *seq);
  Status writeNextSeqToFile(rocksdb::SequenceNumber seq) const;
};
