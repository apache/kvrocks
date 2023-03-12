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
#include <sys/types.h> /* This will likely define BYTE_ORDER */
#include <unistd.h>

#include <string>
#include <utility>

/* Byte ordering detection */

#ifndef BYTE_ORDER
#if (BSD >= 199103)
#include <machine/endian.h>
#else
#if defined(linux) || defined(__linux__)
#include <endian.h>
#else
#define LITTLE_ENDIAN 1234 /* least-significant byte first (vax, pc) */
#define BIG_ENDIAN 4321    /* most-significant byte first (IBM, net) */
#define PDP_ENDIAN 3412    /* LSB first in word, MSW first in long (pdp)*/

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || defined(vax) || defined(ns32000) ||         \
    defined(sun386) || defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || defined(__alpha__) || \
    defined(__alpha)
#define BYTE_ORDER LITTLE_ENDIAN
#endif

#if defined(__i386__) || defined(__x86_64__) || defined(__amd64__) || defined(vax) || defined(ns32000) ||         \
    defined(sun386) || defined(MIPSEL) || defined(_MIPSEL) || defined(BIT_ZERO_ON_RIGHT) || defined(__alpha__) || \
    defined(__alpha)
#define BYTE_ORDER LITTLE_ENDIAN
#endif

#if defined(sel) || defined(pyr) || defined(mc68000) || defined(sparc) || defined(is68k) || defined(tahoe) ||        \
    defined(ibm032) || defined(ibm370) || defined(MIPSEB) || defined(_MIPSEB) || defined(_IBMR2) || defined(DGUX) || \
    defined(apollo) || defined(__convex__) || defined(_CRAY) || defined(__hppa) || defined(__hp9000) ||              \
    defined(__hp9000s300) || defined(__hp9000s700) || defined(BIT_ZERO_ON_LEFT) || defined(m68k) || defined(__sparc)
#define BYTE_ORDER BIG_ENDIAN
#endif
#endif /* linux */
#endif /* BSD */
#endif /* BYTE_ORDER */

/* Sometimes after including an OS-specific header that defines the
 * endianess we end with __BYTE_ORDER but not with BYTE_ORDER that is what
 * the Redis code uses. In this case let's define everything without the
 * underscores. */
#ifndef BYTE_ORDER
#ifdef __BYTE_ORDER
#if defined(__LITTLE_ENDIAN) && defined(__BIG_ENDIAN)
#ifndef LITTLE_ENDIAN
#define LITTLE_ENDIAN __LITTLE_ENDIAN
#endif
#ifndef BIG_ENDIAN
#define BIG_ENDIAN __BIG_ENDIAN
#endif
#if (__BYTE_ORDER == __LITTLE_ENDIAN)
#define BYTE_ORDER LITTLE_ENDIAN
#else
#define BYTE_ORDER BIG_ENDIAN
#endif
#endif
#endif
#endif

#if !defined(BYTE_ORDER) || (BYTE_ORDER != BIG_ENDIAN && BYTE_ORDER != LITTLE_ENDIAN)
/* you must determine what the correct bit order is for
 * your compiler - the next line is an intentional error
 * which will force your compiles to bomb until you fix
 * the above macros.
 */
#error "Undefined or invalid BYTE_ORDER"
#endif

void EncodeFixed8(char *buf, uint8_t value) { buf[0] = static_cast<char>(value & 0xff); }

void EncodeFixed16(char *buf, uint16_t value) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = static_cast<char>((value >> 8) & 0xff);
    buf[1] = static_cast<char>(value & 0xff);
  }
}

void EncodeFixed32(char *buf, uint32_t value) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = static_cast<char>((value >> 24) & 0xff);
    buf[1] = static_cast<char>((value >> 16) & 0xff);
    buf[2] = static_cast<char>((value >> 8) & 0xff);
    buf[3] = static_cast<char>(value & 0xff);
  }
}

void EncodeFixed64(char *buf, uint64_t value) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    memcpy(buf, &value, sizeof(value));
  } else {
    buf[0] = static_cast<char>((value >> 56) & 0xff);
    buf[1] = static_cast<char>((value >> 48) & 0xff);
    buf[2] = static_cast<char>((value >> 40) & 0xff);
    buf[3] = static_cast<char>((value >> 32) & 0xff);
    buf[4] = static_cast<char>((value >> 24) & 0xff);
    buf[5] = static_cast<char>((value >> 16) & 0xff);
    buf[6] = static_cast<char>((value >> 8) & 0xff);
    buf[7] = static_cast<char>(value & 0xff);
  }
}

void PutFixed8(std::string *dst, uint8_t value) {
  char buf[1];
  buf[0] = static_cast<char>(value & 0xff);
  dst->append(buf, 1);
}

void PutFixed16(std::string *dst, uint16_t value) {
  char buf[sizeof(value)];
  EncodeFixed16(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed32(std::string *dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string *dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutDouble(std::string *dst, double value) {
  uint64_t u64 = 0;
  memcpy(&u64, &value, sizeof(value));
  auto ptr = &u64;
  if ((*ptr >> 63) == 1) {
    // signed bit would be zero
    *ptr ^= 0xffffffffffffffff;
  } else {
    // signed bit would be one
    *ptr |= 0x8000000000000000;
  }
  PutFixed64(dst, *ptr);
}

bool GetFixed8(rocksdb::Slice *input, uint8_t *value) {
  const char *data = nullptr;
  if (input->size() < sizeof(uint8_t)) {
    return false;
  }
  data = input->data();
  *value = static_cast<uint8_t>(data[0] & 0xff);
  input->remove_prefix(sizeof(uint8_t));
  return true;
}

bool GetFixed64(rocksdb::Slice *input, uint64_t *value) {
  if (input->size() < sizeof(uint64_t)) {
    return false;
  }
  *value = DecodeFixed64(input->data());
  input->remove_prefix(sizeof(uint64_t));
  return true;
}

bool GetFixed32(rocksdb::Slice *input, uint32_t *value) {
  if (input->size() < sizeof(uint32_t)) {
    return false;
  }
  *value = DecodeFixed32(input->data());
  input->remove_prefix(sizeof(uint32_t));
  return true;
}

bool GetFixed16(rocksdb::Slice *input, uint16_t *value) {
  if (input->size() < sizeof(uint16_t)) {
    return false;
  }
  *value = DecodeFixed16(input->data());
  input->remove_prefix(sizeof(uint16_t));
  return true;
}

bool GetDouble(rocksdb::Slice *input, double *value) {
  if (input->size() < sizeof(double)) return false;
  *value = DecodeDouble(input->data());
  input->remove_prefix(sizeof(double));
  return true;
}

uint16_t DecodeFixed16(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint16_t value = 0;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    return ((static_cast<uint16_t>(static_cast<uint8_t>(ptr[1]))) |
            (static_cast<uint16_t>(static_cast<uint8_t>(ptr[0])) << 8));
  }
}

uint32_t DecodeFixed32(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint32_t value = 0;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    return ((static_cast<uint32_t>(static_cast<uint8_t>(ptr[3]))) |
            (static_cast<uint32_t>(static_cast<uint8_t>(ptr[2])) << 8) |
            (static_cast<uint32_t>(static_cast<uint8_t>(ptr[1])) << 16) |
            (static_cast<uint32_t>(static_cast<uint8_t>(ptr[0])) << 24));
  }
}

uint64_t DecodeFixed64(const char *ptr) {
  if (BYTE_ORDER == BIG_ENDIAN) {
    uint64_t value = 0;
    memcpy(&value, ptr, sizeof(value));
    return value;
  } else {
    uint64_t hi = DecodeFixed32(ptr);
    uint64_t lo = DecodeFixed32(ptr + 4);
    return (hi << 32) | lo;
  }
}

double DecodeDouble(const char *ptr) {
  uint64_t decoded = DecodeFixed64(ptr);
  if ((decoded >> 63) == 0) {
    decoded ^= 0xffffffffffffffff;
  } else {
    decoded &= 0x7fffffffffffffff;
  }
  double value = 0;
  memcpy(&value, &decoded, sizeof(value));
  return value;
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
