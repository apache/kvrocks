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

#include <rocksdb/slice.h>
#include <unistd.h>

#include <cstdint>
#include <string>

enum class Endian {
  LITTLE = __ORDER_LITTLE_ENDIAN__,
  BIG = __ORDER_BIG_ENDIAN__,
  NATIVE = __BYTE_ORDER__,
};

constexpr inline bool IsLittleEndian() { return Endian::NATIVE == Endian::LITTLE; }
constexpr inline bool IsBigEndian() { return Endian::NATIVE == Endian::BIG; }

constexpr inline uint8_t BitSwap(uint8_t x) { return x; }

constexpr inline uint16_t BitSwap(uint16_t x) { return __builtin_bswap16(x); }

constexpr inline uint32_t BitSwap(uint32_t x) { return __builtin_bswap32(x); }

constexpr inline uint64_t BitSwap(uint64_t x) { return __builtin_bswap64(x); }

template <typename T>
constexpr char *EncodeFixed(char *buf, T value) {
  if constexpr (IsLittleEndian()) {
    value = BitSwap(value);
  }
  __builtin_memcpy(buf, &value, sizeof(value));
  return buf + sizeof(value);
}

inline char *EncodeFixed8(char *buf, uint8_t value) { return EncodeFixed(buf, value); }
inline char *EncodeFixed16(char *buf, uint16_t value) { return EncodeFixed(buf, value); }
inline char *EncodeFixed32(char *buf, uint32_t value) { return EncodeFixed(buf, value); }
inline char *EncodeFixed64(char *buf, uint64_t value) { return EncodeFixed(buf, value); }

inline char *EncodeBuffer(char *buf, rocksdb::Slice value) {
  __builtin_memcpy(buf, value.data(), value.size());

  return buf + value.size();
}

template <typename T>
void PutFixed(std::string *dst, T value) {
  char buf[sizeof(value)];
  EncodeFixed(buf, value);
  dst->append(buf, sizeof(buf));
}

inline void PutFixed8(std::string *dst, uint8_t value) { PutFixed(dst, value); }
inline void PutFixed16(std::string *dst, uint16_t value) { PutFixed(dst, value); }
inline void PutFixed32(std::string *dst, uint32_t value) { PutFixed(dst, value); }
inline void PutFixed64(std::string *dst, uint64_t value) { PutFixed(dst, value); }

template <typename T>
constexpr T DecodeFixed(const char *ptr) {
  T value = 0;

  __builtin_memcpy(&value, ptr, sizeof(value));

  return IsLittleEndian() ? BitSwap(value) : value;
}

inline uint8_t DecodeFixed8(const char *ptr) { return DecodeFixed<uint8_t>(ptr); }
inline uint16_t DecodeFixed16(const char *ptr) { return DecodeFixed<uint16_t>(ptr); }
inline uint32_t DecodeFixed32(const char *ptr) { return DecodeFixed<uint32_t>(ptr); }
inline uint64_t DecodeFixed64(const char *ptr) { return DecodeFixed<uint64_t>(ptr); }

template <typename T>
bool GetFixed(rocksdb::Slice *input, T *value) {
  if (input->size() < sizeof(T)) return false;
  *value = DecodeFixed<T>(input->data());
  input->remove_prefix(sizeof(T));
  return true;
}

inline bool GetFixed8(rocksdb::Slice *input, uint8_t *value) { return GetFixed(input, value); }
inline bool GetFixed16(rocksdb::Slice *input, uint16_t *value) { return GetFixed(input, value); }
inline bool GetFixed32(rocksdb::Slice *input, uint32_t *value) { return GetFixed(input, value); }
inline bool GetFixed64(rocksdb::Slice *input, uint64_t *value) { return GetFixed(input, value); }

char *EncodeDouble(char *buf, double value);
void PutDouble(std::string *dst, double value);
double DecodeDouble(const char *ptr);
bool GetDouble(rocksdb::Slice *input, double *value);

char *EncodeVarint32(char *dst, uint32_t v);
void PutVarint32(std::string *dst, uint32_t v);
bool GetVarint32(rocksdb::Slice *input, uint32_t *value);
