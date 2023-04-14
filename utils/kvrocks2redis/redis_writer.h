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

#include <glog/logging.h>

#include <map>
#include <string>
#include <thread>
#include <vector>

#include "writer.h"

class RedisWriter : public Writer {
 public:
  explicit RedisWriter(kvrocks2redis::Config *config);

  RedisWriter(const RedisWriter &) = delete;
  RedisWriter &operator=(const RedisWriter &) = delete;

  ~RedisWriter();
  Status Write(const std::string &ns, const std::vector<std::string> &aofs) override;
  Status FlushDB(const std::string &ns) override;

  void Stop() override;

 private:
  std::thread t_;
  bool stop_flag_ = false;
  std::map<std::string, int> next_offset_fds_;
  std::map<std::string, std::istream::off_type> next_offsets_;
  std::map<std::string, int> redis_fds_;

  void sync();
  Status getRedisConn(const std::string &ns, const std::string &host, uint32_t port, const std::string &auth,
                      int db_index);
  Status authRedis(const std::string &ns, const std::string &auth);
  Status selectDB(const std::string &ns, int db_number);

  Status updateNextOffset(const std::string &ns, std::istream::off_type offset);
  Status readNextOffsetFromFile(const std::string &ns, std::istream::off_type *offset);
  Status writeNextOffsetToFile(const std::string &ns, std::istream::off_type offset);
  std::string getNextOffsetFilePath(const std::string &ns);
};
