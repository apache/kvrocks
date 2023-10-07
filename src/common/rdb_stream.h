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

#include <stdint.h>

#include <fstream>
#include <string>

#include "status.h"

class RdbStream {
 public:
  RdbStream() = default;
  virtual ~RdbStream() = default;

  virtual StatusOr<size_t> Read(char *buf, size_t len) = 0;
  virtual StatusOr<uint64_t> GetCheckSum() const = 0;
  StatusOr<uint8_t> ReadByte() {
    uint8_t value = 0;
    auto s = Read(reinterpret_cast<char *>(&value), 1);
    if (!s.IsOK()) {
      return s;
    }
    return value;
  }
};

class RdbStringStream : public RdbStream {
 public:
  explicit RdbStringStream(std::string_view input) : input_(input){};
  RdbStringStream(const RdbStringStream &) = delete;
  RdbStringStream &operator=(const RdbStringStream &) = delete;
  ~RdbStringStream() override = default;

  StatusOr<size_t> Read(char *buf, size_t len) override;
  StatusOr<uint64_t> GetCheckSum() const override;

 private:
  std::string input_;
  size_t pos_ = 0;
};

class RdbFileStream : public RdbStream {
 public:
  explicit RdbFileStream(std::string file_name, size_t chunk_size = 1024 *1024) : file_name_(std::move(file_name)), check_sum_(0), total_read_bytes_(0), 
    max_read_chunk_size_(chunk_size){};
  RdbFileStream(const RdbFileStream &) = delete;
  RdbFileStream &operator=(const RdbFileStream &) = delete;
  ~RdbFileStream() override = default;

  Status Open();
  StatusOr<size_t> Read(char *buf, size_t len) override;
  StatusOr<uint64_t> GetCheckSum() const override { return check_sum_; }

 private:
  std::ifstream ifs_;
  std::string file_name_;
  uint64_t check_sum_;
  size_t total_read_bytes_;
  size_t max_read_chunk_size_; // maximum single read chunk size
};