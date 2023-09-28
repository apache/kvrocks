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

#include <config/config.h>
#include <glog/logging.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>

#include <cstdlib>

#include "unique_fd.h"

inline bool SupervisedUpstart() {
  const char *upstart_job = getenv("UPSTART_JOB");
  if (!upstart_job) {
    LOG(WARNING) << "upstart supervision requested, but UPSTART_JOB not found";
    return false;
  }
  LOG(INFO) << "supervised by upstart, will stop to signal readiness";
  raise(SIGSTOP);
  unsetenv("UPSTART_JOB");
  return true;
}

inline bool SupervisedSystemd() {
  const char *notify_socket = getenv("NOTIFY_SOCKET");
  if (!notify_socket) {
    LOG(WARNING) << "systemd supervision requested, but NOTIFY_SOCKET not found";
    return false;
  }

  auto fd = UniqueFD(socket(AF_UNIX, SOCK_DGRAM, 0));
  if (!fd) {
    LOG(WARNING) << "Cannot connect to systemd socket " << notify_socket;
    return false;
  }

  sockaddr_un su;
  memset(&su, 0, sizeof(su));
  su.sun_family = AF_UNIX;
  strncpy(su.sun_path, notify_socket, sizeof(su.sun_path) - 1);
  su.sun_path[sizeof(su.sun_path) - 1] = '\0';
  if (notify_socket[0] == '@') su.sun_path[0] = '\0';

  iovec iov;
  memset(&iov, 0, sizeof(iov));
  std::string ready = "READY=1";
  iov.iov_base = &ready[0];
  iov.iov_len = ready.size();

  msghdr hdr;
  memset(&hdr, 0, sizeof(hdr));
  hdr.msg_name = &su;
  hdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) + strlen(notify_socket);
  hdr.msg_iov = &iov;
  hdr.msg_iovlen = 1;

  int sendto_flags = 0;
  unsetenv("NOTIFY_SOCKET");
#ifdef HAVE_MSG_NOSIGNAL
  sendto_flags |= MSG_NOSIGNAL;
#endif
  if (sendmsg(*fd, &hdr, sendto_flags) < 0) {
    LOG(WARNING) << "Cannot send notification to systemd";
    return false;
  }
  return true;
}

inline bool IsSupervisedMode(SupervisedMode mode) {
  if (mode == kSupervisedAutoDetect) {
    const char *upstart_job = getenv("UPSTART_JOB");
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    if (upstart_job) {
      mode = kSupervisedUpStart;
    } else if (notify_socket) {
      mode = kSupervisedSystemd;
    }
  }
  if (mode == kSupervisedUpStart) {
    return SupervisedUpstart();
  } else if (mode == kSupervisedSystemd) {
    return SupervisedSystemd();
  }
  return false;
}

inline void Daemonize() {
  pid_t pid = fork();
  if (pid < 0) {
    LOG(ERROR) << "Failed to fork the process, err: " << strerror(errno);
    exit(1);
  }

  if (pid > 0) exit(EXIT_SUCCESS);  // parent process
  // change the file mode
  umask(0);
  if (setsid() < 0) {
    LOG(ERROR) << "Failed to setsid, err: %s" << strerror(errno);
    exit(1);
  }

  close(STDIN_FILENO);
  close(STDOUT_FILENO);
  close(STDERR_FILENO);
}
