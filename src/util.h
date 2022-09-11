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

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <arpa/inet.h>

#include <cctype>
#include <string>
#include <vector>
#include <utility>
#include <memory>
#include <chrono>

#include "status.h"
#include "solarisfixes.h"

namespace Util {
// sock util
sockaddr_in NewSockaddrInet(const std::string &host, uint32_t port);
Status SockConnect(const std::string &host, uint32_t port, int *fd);
Status SockConnect(const std::string &host, uint32_t port, int *fd, uint64_t conn_timeout, uint64_t timeout = 0);
Status SockSetTcpNoDelay(int fd, int val);
Status SockSetTcpKeepalive(int fd, int interval);
Status SockSend(int fd, const std::string &data);
Status SockReadLine(int fd, std::string *data);
Status SockSendFile(int out_fd, int in_fd, size_t size);
Status SockSetBlocking(int fd, int blocking);
int GetPeerAddr(int fd, std::string *addr, uint32_t *port);
int GetLocalPort(int fd);
bool IsPortInUse(int port);

// string util
Status DecimalStringToNum(const std::string &str, int64_t *n, int64_t min = INT64_MIN, int64_t max = INT64_MAX);
Status OctalStringToNum(const std::string &str, int64_t *n, int64_t min = INT64_MIN, int64_t max = INT64_MAX);
const std::string Float2String(double d);
std::string ToLower(std::string in);
void BytesToHuman(char *buf, size_t size, uint64_t n);
std::string Trim(std::string in, const std::string &chars);
std::vector<std::string> Split(const std::string &in, const std::string &delim);
std::vector<std::string> Split2KV(const std::string &in, const std::string &delim);
bool HasPrefix(const std::string &str, const std::string &prefix);
int StringMatch(const std::string &pattern, const std::string &in, int nocase);
int StringMatchLen(const char *p, int plen, const char *s, int slen, int nocase);
std::string StringToHex(const std::string &input);
std::vector<std::string> TokenizeRedisProtocol(const std::string &value);

void ThreadSetName(const char *name);
int aeWait(int fd, int mask, uint64_t milliseconds);
uint64_t GetTimeStampMS(void);
uint64_t GetTimeStampUS(void);

// define std::make_unique in c++14
// refer to https://en.cppreference.com/w/cpp/memory/unique_ptr/make_unique
template <typename T, typename... Args>
std::unique_ptr<T> MakeUnique(Args&& ... args) {
  return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

template <typename T, typename... Args>
std::shared_ptr<T> MakeShared(Args&& ... args) {
  return std::make_shared<T>(std::forward<Args>(args)...);
}
}  // namespace Util
