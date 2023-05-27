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

#include "status.h"

namespace util {

std::string Float2String(double d);
std::string ToLower(std::string in);
bool EqualICase(std::string_view lhs, std::string_view rhs);
std::string BytesToHuman(uint64_t n);
std::string Trim(std::string in, const std::string &chars);
std::vector<std::string> Split(const std::string &in, const std::string &delim);
std::vector<std::string> Split2KV(const std::string &in, const std::string &delim);
bool HasPrefix(const std::string &str, const std::string &prefix);
int StringMatch(const std::string &pattern, const std::string &in, int nocase);
int StringMatchLen(const char *p, size_t plen, const char *s, size_t slen, int nocase);
std::string StringToHex(const std::string &input);
std::vector<std::string> TokenizeRedisProtocol(const std::string &value);
std::string StringRepr(const std::string &s);

}  // namespace util
