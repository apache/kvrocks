#pragma once

#include <unistd.h>
#include <string>
#include <rocksdb/slice.h>

bool GetFixed8(rocksdb::Slice *input, uint8_t *value);
bool GetFixed32(rocksdb::Slice *input, uint32_t *value);
bool GetFixed64(rocksdb::Slice *input, uint64_t *value);
bool GetDouble(rocksdb::Slice *input, double *value);
void PutFixed8(std::string *dst, uint8_t value);
void PutFixed32(std::string *dst, uint32_t value);
void PutFixed64(std::string *dst, uint64_t value);
void PutDouble(std::string *dst, double value);

void EncodeFixed8(char *buf, uint8_t value);
void EncodeFixed32(char *buf, uint32_t value);
void EncodeFixed64(char *buf, uint64_t value);
uint32_t DecodeFixed32(const char *ptr);
uint64_t DecodeFixed64(const char *ptr);
double DecodeDouble(const char *ptr);
