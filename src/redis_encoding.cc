#include "redis_encoding.h"

#include <limits.h>
#include <float.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <string>

// -- Implementation of the functions declared above
void EncodeFixed8(char *buf, uint8_t value) {
  buf[0] = static_cast<uint8_t>(value & 0xff);
}

void EncodeFixed32(char *buf, uint32_t value) {
  buf[0] = static_cast<uint8_t>((value >> 24) & 0xff);
  buf[1] = static_cast<uint8_t>((value >> 16) & 0xff);
  buf[2] = static_cast<uint8_t>((value >> 8) & 0xff);
  buf[3] = static_cast<uint8_t>(value & 0xff);
}

void EncodeFixed64(char *buf, uint64_t value) {
  buf[0] = static_cast<uint8_t>((value >> 56) & 0xff);
  buf[1] = static_cast<uint8_t>((value >> 48) & 0xff);
  buf[2] = static_cast<uint8_t>((value >> 40) & 0xff);
  buf[3] = static_cast<uint8_t>((value >> 32) & 0xff);
  buf[4] = static_cast<uint8_t>((value >> 24) & 0xff);
  buf[5] = static_cast<uint8_t>((value >> 16) & 0xff);
  buf[6] = static_cast<uint8_t>((value >> 8) & 0xff);
  buf[7] = static_cast<uint8_t>(value & 0xff);
}

void PutFixed8(std::string *dst, uint8_t value) {
  char buf[1];
  buf[0] = static_cast<uint8_t>(value & 0xff);
  dst->append(buf, 1);
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
  uint64_t *ptr = (uint64_t*) &value;
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
  const char *data;
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

bool GetDouble(rocksdb::Slice *input, double *value) {
  if (input->size() < sizeof(double)) return false;
  *value = DecodeDouble(input->data());
  input->remove_prefix(sizeof(double));
  return true;
}

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.

uint8_t DecodeFixed8(const char *ptr) {
  return (static_cast<uint8_t>(ptr[0])) & 0xff;
}

uint32_t DecodeFixed32(const char *ptr) {
  return ((static_cast<uint32_t>(static_cast<uint8_t>(ptr[3])))
          | (static_cast<uint32_t>(static_cast<uint8_t>(ptr[2])) << 8)
          | (static_cast<uint32_t>(static_cast<uint8_t>(ptr[1])) << 16)
          | (static_cast<uint32_t>(static_cast<uint8_t>(ptr[0])) << 24));
}

uint64_t DecodeFixed64(const char *ptr) {
  uint64_t hi = DecodeFixed32(ptr);
  uint64_t lo = DecodeFixed32(ptr + 4);
  return (hi << 32) | lo;
}

double DecodeDouble(const char *ptr) {
  uint64_t decoded = DecodeFixed64(ptr);
  if ((decoded>>63) == 0) {
    decoded ^= 0xffffffffffffffff;
  } else {
    decoded &= 0x7fffffffffffffff;
  }
  return *((double *)&decoded);
}
