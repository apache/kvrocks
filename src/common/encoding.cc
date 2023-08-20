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

#include "encoding.h"

#include <float.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>

#include <string>
#include <utility>

inline uint64_t EncodeDoubleToUInt64(double value) {
  uint64_t result = 0;

  __builtin_memcpy(&result, &value, sizeof(value));

  if ((result >> 63) == 1) {
    // signed bit would be zero
    result ^= 0xffffffffffffffff;
  } else {
    // signed bit would be one
    result |= 0x8000000000000000;
  }

  return result;
}

inline double DecodeDoubleFromUInt64(uint64_t value) {
  if ((value >> 63) == 0) {
    value ^= 0xffffffffffffffff;
  } else {
    value &= 0x7fffffffffffffff;
  }

  double result = 0;
  __builtin_memcpy(&result, &value, sizeof(result));

  return result;
}

char *EncodeDouble(char *buf, double value) { return EncodeFixed64(buf, EncodeDoubleToUInt64(value)); }

void PutDouble(std::string *dst, double value) { PutFixed64(dst, EncodeDoubleToUInt64(value)); }

double DecodeDouble(const char *ptr) { return DecodeDoubleFromUInt64(DecodeFixed64(ptr)); }

bool GetDouble(rocksdb::Slice *input, double *value) {
  if (input->size() < sizeof(double)) return false;
  *value = DecodeDouble(input->data());
  input->remove_prefix(sizeof(double));
  return true;
}

char *EncodeVarint32(char *dst, uint32_t v) {
  // Operate on characters as unsigneds
  auto *ptr = reinterpret_cast<unsigned char *>(dst);
  do {
    *ptr = 0x80 | v;
    v >>= 7, ++ptr;
  } while (v != 0);
  *(ptr - 1) &= 0x7F;
  return reinterpret_cast<char *>(ptr);
}

void PutVarint32(std::string *dst, uint32_t v) {
  char buf[5];
  char *ptr = EncodeVarint32(buf, v);
  dst->append(buf, static_cast<size_t>(ptr - buf));
}

const char *GetVarint32PtrFallback(const char *p, const char *limit, uint32_t *value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = static_cast<unsigned char>(*p);
    p++;
    if (byte & 0x80) {
      // More bytes are present
      result |= ((byte & 0x7F) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

const char *GetVarint32Ptr(const char *p, const char *limit, uint32_t *value) {
  if (p < limit) {
    uint32_t result = static_cast<unsigned char>(*p);
    if ((result & 0x80) == 0) {
      *value = result;
      return p + 1;
    }
  }
  return GetVarint32PtrFallback(p, limit, value);
}

bool GetVarint32(rocksdb::Slice *input, uint32_t *value) {
  const char *p = input->data();
  const char *limit = p + input->size();
  const char *q = GetVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = rocksdb::Slice(q, static_cast<size_t>(limit - q));
    return true;
  }
}
