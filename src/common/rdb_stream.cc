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
#include "vendor/endianconv.h"

StatusOr<size_t> RdbStringStream::Read(char *buf, size_t n) {
  if (pos_ + n > input_.size()) {  // TODO why not >= ?
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

StatusOr<size_t> RdbFileStream::Read(char *buf, size_t len) {
  size_t n = 0;
  while (len) {
    size_t read_bytes = max_read_chunk_size_ < len ? max_read_chunk_size_ : len;
    ifs_.read(buf, static_cast<std::streamsize>(read_bytes));
    if (!ifs_.good()) {
      return Status(Status::NotOK, fmt::format("read failed: {}:", strerror(errno)));
    }
    check_sum_ = crc64(check_sum_, (const unsigned char *)buf, read_bytes);
    buf = (char *)buf + read_bytes;
    len -= read_bytes;
    total_read_bytes_ += read_bytes;
    n += read_bytes;
  }

  return n;
}
