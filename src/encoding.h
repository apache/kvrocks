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

#include <unistd.h>
#include <string>
#include <rocksdb/slice.h>

bool GetFixed8(rocksdb::Slice *input, uint8_t *value);
bool GetFixed16(rocksdb::Slice *input, uint16_t *value);
bool GetFixed32(rocksdb::Slice *input, uint32_t *value);
bool GetFixed64(rocksdb::Slice *input, uint64_t *value);
bool GetDouble(rocksdb::Slice *input, double *value);
void PutFixed8(std::string *dst, uint8_t value);
void PutFixed16(std::string *dst, uint16_t value);
void PutFixed32(std::string *dst, uint32_t value);
void PutFixed64(std::string *dst, uint64_t value);
void PutDouble(std::string *dst, double value);

void EncodeFixed8(char *buf, uint8_t value);
void EncodeFixed16(char *buf, uint16_t value);
void EncodeFixed32(char *buf, uint32_t value);
void EncodeFixed64(char *buf, uint64_t value);
uint16_t DecodeFixed16(const char *ptr);
uint32_t DecodeFixed32(const char *ptr);
uint64_t DecodeFixed64(const char *ptr);
double DecodeDouble(const char *ptr);
char *EncodeVarint32(char *dst, uint32_t v);
void PutVarint32(std::string *dst, uint32_t v);
const char* GetVarint32PtrFallback(const char *p, const char *limit, uint32_t *value);
const char* GetVarint32Ptr(const char *p, const char *limit, uint32_t *value);
bool GetVarint32(rocksdb::Slice *input, uint32_t *value);
