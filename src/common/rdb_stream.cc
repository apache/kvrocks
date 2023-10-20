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

#include "rdb_stream.h"

#include "fmt/format.h"
#include "vendor/crc64.h"

Status RdbStringStream::Read(char *buf, size_t n) {
  if (pos_ + n > input_.size()) {
    return {Status::NotOK, "unexpected EOF"};
  }
  memcpy(buf, input_.data() + pos_, n);
  pos_ += n;
  return Status::OK();
}

StatusOr<uint64_t> RdbStringStream::GetCheckSum() const {
  if (input_.size() < 8) {
    return {Status::NotOK, "invalid payload length"};
  }
  uint64_t crc = crc64(0, reinterpret_cast<const unsigned char *>(input_.data()), input_.size() - 8);
  memrev64ifbe(&crc);
  return crc;
}

Status RdbFileStream::Open() {
  ifs_.open(file_name_, std::ifstream::in | std::ifstream::binary);
  if (!ifs_.is_open()) {
    return {Status::NotOK, fmt::format("failed to open rdb file: '{}': {}", file_name_, strerror(errno))};
  }

  return Status::OK();
}

Status RdbFileStream::Read(char *buf, size_t len) {
  while (len) {
    size_t read_bytes = std::min(max_read_chunk_size_, len);
    ifs_.read(buf, static_cast<std::streamsize>(read_bytes));
    if (!ifs_.good()) {
      if (!ifs_.eof()) {
        return {Status::NotOK, fmt::format("read failed: {}:", strerror(errno))};
      }
      auto eof_read_bytes = static_cast<size_t>(ifs_.gcount());
      if (read_bytes != eof_read_bytes) {
        return {Status::NotOK, fmt::format("read failed: {}:", strerror(errno))};
      }
    }
    check_sum_ = crc64(check_sum_, reinterpret_cast<const unsigned char *>(buf), read_bytes);
    buf = buf + read_bytes;
    DCHECK(len >= read_bytes);
    len -= read_bytes;
    total_read_bytes_ += read_bytes;
  }
  return Status::OK();
}
