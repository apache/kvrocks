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

#include <netinet/in.h>

#include "status.h"

namespace util {

sockaddr_in NewSockaddrInet(const std::string &host, uint32_t port);
StatusOr<std::vector<std::string>> LookupHostByName(const std::string &host);
StatusOr<int> SockConnect(const std::string &host, uint32_t port, int conn_timeout = 0, int timeout = 0);
Status SockSetTcpNoDelay(int fd, int val);
Status SockSetTcpKeepalive(int fd, int interval);
Status SockSend(int fd, const std::string &data);
StatusOr<std::string> SockReadLine(int fd);
Status SockSendFile(int out_fd, int in_fd, size_t size);
Status SockSetBlocking(int fd, int blocking);
StatusOr<std::tuple<std::string, uint32_t>> GetPeerAddr(int fd);
int GetLocalPort(int fd);
bool IsPortInUse(uint32_t port);

bool MatchListeningIP(std::vector<std::string> &binds, const std::string &ip);
std::vector<std::string> GetLocalIPAddresses();

int AeWait(int fd, int mask, int milliseconds);
Status Write(int fd, const std::string &data);
Status Pwrite(int fd, const std::string &data, off_t offset);

}  // namespace util
