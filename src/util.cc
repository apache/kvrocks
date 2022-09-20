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

#define __STDC_FORMAT_MACROS
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <netdb.h>
#include <poll.h>
#include <errno.h>
#include <pthread.h>
#include <fcntl.h>
#include <math.h>
#include <string>
#include <algorithm>
#include <event2/util.h>
#include <event2/buffer.h>
#include <glog/logging.h>
#ifdef __linux__
#include <sys/sendfile.h>
#endif


#include "util.h"
#include "status.h"
#include "event_util.h"
#include "parse_util.h"

#ifndef POLLIN
# define POLLIN      0x0001    /* There is data to read */
# define POLLPRI     0x0002    /* There is urgent data to read */
# define POLLOUT     0x0004    /* Writing now will not block */
# define POLLERR     0x0008    /* Error condition */
# define POLLHUP     0x0010    /* Hung up */
# define POLLNVAL    0x0020    /* Invalid request: fd not open */
#endif

#define AE_READABLE 1
#define AE_WRITABLE 2
#define AE_ERROR 4
#define AE_HUP 8

namespace Util {
Status SockConnect(const std::string &host, uint32_t port, int *fd) {
  int rv, cfd;
  char portstr[6];  /* strlen("65535") + 1; */
  addrinfo hints, *servinfo, *p;

  snprintf(portstr, sizeof(portstr), "%u", port);
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host.c_str(), portstr, &hints, &servinfo)) != 0) {
    return Status(Status::NotOK, gai_strerror(rv));
  }

  for (p = servinfo; p != nullptr ; p = p->ai_next) {
    if ((cfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
      continue;
    if (connect(cfd, p->ai_addr, p->ai_addrlen) == -1) {
      close(cfd);
      continue;
    }
    Status s = SockSetTcpKeepalive(cfd, 120);
    if (s.IsOK()) {
      s = SockSetTcpNoDelay(cfd, 1);
    }
    if (!s.IsOK()) {
      close(cfd);
      continue;
    }

    *fd = cfd;
    freeaddrinfo(servinfo);
    return Status::OK();
  }

  freeaddrinfo(servinfo);
  return Status(Status::NotOK, strerror(errno));
}

const std::string Float2String(double d) {
  if (isinf(d)) {
    return d > 0 ? "inf" : "-inf";
  }

  char buf[128];
  snprintf(buf, sizeof(buf), "%.17g", d);
  return buf;
}

Status SockSetTcpNoDelay(int fd, int val) {
  if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val)) == -1) {
    return Status(Status::NotOK, strerror(errno));
  }
  return Status::OK();
}

Status SockSetTcpKeepalive(int fd, int interval) {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1) {
    return Status(Status::NotOK, strerror(errno));
  }

#ifdef __linux__
  // Default settings are more or less garbage, with the keepalive time
  // set to 7200 by default on Linux. Modify settings to make the feature
  // actually useful.

  // Send first probe after interval.
  val = interval;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    return Status(Status::NotOK, std::string("setsockopt TCP_KEEPIDLE: ")+strerror(errno));
  }

  // Send next probes after the specified interval. Note that we set the
  // delay as interval / 3, as we send three probes before detecting
  // an error (see the next setsockopt call).
  val = interval / 3;
  if (val == 0) val = 1;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    return Status(Status::NotOK, std::string("setsockopt TCP_KEEPINTVL: ")+strerror(errno));
  }

  // Consider the socket in error state after three we send three ACK
  // probes without getting a reply.
  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    return Status(Status::NotOK, std::string("setsockopt TCP_KEEPCNT: ")+strerror(errno));
  }
#else
  ((void) interval);  // Avoid unused var warning for non Linux systems.
#endif

  return Status::OK();
}

Status SockConnect(const std::string &host, uint32_t port, int *fd, uint64_t conn_timeout, uint64_t timeout) {
  if (conn_timeout == 0) {
    auto s = SockConnect(host, port, fd);
    if (!s.IsOK()) return s;
  } else {
    sockaddr_in sin{};
    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = inet_addr(host.c_str());
    sin.sin_port = htons(port);
    *fd = socket(AF_INET, SOCK_STREAM, 0);

    fcntl(*fd, F_SETFL, O_NONBLOCK);
    connect(*fd, reinterpret_cast<sockaddr *>(&sin), sizeof(sin));

    auto retmask = Util::aeWait(*fd, AE_WRITABLE, conn_timeout);
    if ((retmask & AE_WRITABLE) == 0 ||
        (retmask & AE_ERROR) != 0 ||
        (retmask & AE_HUP) != 0
        ) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, strerror(errno));
    }

    int socket_arg;
    // Set to blocking mode again...
    if ((socket_arg = fcntl(*fd, F_GETFL, NULL)) < 0) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, strerror(errno));
    }
    socket_arg &= (~O_NONBLOCK);
    if (fcntl(*fd, F_SETFL, socket_arg) < 0) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, strerror(errno));
    }
    SockSetTcpNoDelay(*fd, 1);
  }
  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;
    tv.tv_usec = (timeout % 1000) * 1000;
    if (setsockopt(*fd, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<char *>(&tv), sizeof(tv)) < 0) {
      close(*fd);
      *fd = -1;
      return Status(Status::NotOK, std::string("setsockopt failed: ") + strerror(errno));
    }
  }
  return Status::OK();
}

// NOTE: fd should be blocking here
Status SockSend(int fd, const std::string &data) {
  ssize_t n = 0;
  while (n < static_cast<ssize_t>(data.size())) {
    ssize_t nwritten = write(fd, data.c_str()+n, data.size()-n);
    if (nwritten == -1) {
      return Status(Status::NotOK, strerror(errno));
    }
    n += nwritten;
  }
  return Status::OK();
}

// Implements SockSendFileCore to transfer data between file descriptors and
// avoid transferring data to and from user space.
//
// The function prototype is just like sendfile(2) on Linux. in_fd is a file
// descriptor opened for reading and out_fd is a descriptor opened for writing.
// offset specifies where to start reading data from in_fd. count is the number
// of bytes to copy between the file descriptors.
//
// The return value is the number of bytes written to out_fd, if the transfer
// was successful. On error, -1 is returned, and errno is set appropriately.
ssize_t SockSendFileCore(int out_fd, int in_fd, off_t offset, size_t count) {
#if defined(__linux__)
    return sendfile(out_fd, in_fd, &offset, count);

#elif defined(__APPLE__)
    off_t len = count;
    if (sendfile(in_fd, out_fd, offset, &len, NULL, 0) == -1)
      return -1;
    else
      return (ssize_t)len;

#endif
    errno = ENOSYS;
    return -1;
}

// Send file by sendfile actually according to different operation systems,
// please note that, the out socket fd should be in blocking mode.
Status SockSendFile(int out_fd, int in_fd, size_t size) {
  ssize_t nwritten = 0;
  off_t  offset = 0;
  while (size != 0) {
    size_t n = size <= 16*1024 ? size : 16*1024;
    nwritten = SockSendFileCore(out_fd, in_fd, offset, n);
    if (nwritten == -1) {
      if (errno == EINTR)
        continue;
      else
        return Status(Status::NotOK, strerror(errno));
    }
    size -= nwritten;
    offset += nwritten;
  }
  return Status::OK();
}

Status SockSetBlocking(int fd, int blocking) {
  int flags;
  // Old flags
  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    return Status(Status::NotOK,
           std::string("fcntl(F_GETFL): ") + strerror(errno));
  }

  // New flags
  if (blocking)
    flags &= ~O_NONBLOCK;
  else
    flags |= O_NONBLOCK;

  if (fcntl(fd, F_SETFL, flags) == -1) {
    return Status(Status::NotOK,
           std::string("fcntl(F_SETFL,O_BLOCK): ") + strerror(errno));
  }
  return Status::OK();
}

Status SockReadLine(int fd, std::string *data) {
  UniqueEvbuf evbuf;
  if (evbuffer_read(evbuf.get(), fd, -1) <= 0) {
    return Status(Status::NotOK, std::string("read response err: ") + strerror(errno));
  }
  UniqueEvbufReadln line(evbuf.get(), EVBUFFER_EOL_CRLF_STRICT);
  if (!line) {
    return Status(Status::NotOK, std::string("read response err(empty): ") + strerror(errno));
  }
  *data = std::string(line.get(), line.length);
  return Status::OK();
}

int GetPeerAddr(int fd, std::string *addr, uint32_t *port) {
  addr->clear();

  sockaddr_storage sa{};
  socklen_t sa_len = sizeof(sa);
  if (getpeername(fd, reinterpret_cast<sockaddr *>(&sa), &sa_len) < 0) {
    return -1;
  }
  if (sa.ss_family == AF_INET6) {
    char buf[INET6_ADDRSTRLEN];
    auto sa6 = reinterpret_cast<sockaddr_in6 *>(&sa);
    inet_ntop(AF_INET6, reinterpret_cast<void *>(&sa6->sin6_addr), buf, INET_ADDRSTRLEN);
    addr->append(buf);
    *port = ntohs(sa6->sin6_port);
  } else {
    auto sa4 = reinterpret_cast<sockaddr_in *>(&sa);
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, reinterpret_cast<void *>(&sa4->sin_addr), buf, INET_ADDRSTRLEN);
    addr->append(buf);
    *port = ntohs(sa4->sin_port);
  }
  return 0;
}

int GetLocalPort(int fd) {
  sockaddr_in6 address;
  socklen_t len = sizeof(address);
  if (getsockname(fd, (struct sockaddr *)&address, &len) == -1) {
    return 0;
  }

  if (address.sin6_family == AF_INET) {
    return ntohs(reinterpret_cast<sockaddr_in *>(&address)->sin_port);
  } else if (address.sin6_family == AF_INET6) {
    return ntohs(address.sin6_port);
  }

  return 0;
}

Status DecimalStringToNum(const std::string &str, int64_t *n, int64_t min, int64_t max) {
  auto parse_result = ParseInt<int64_t>(str, NumericRange<int64_t>{min, max}, 10);
  if (!parse_result) {
    return parse_result.ToStatus();
  }
  *n = *parse_result;
  return Status::OK();
}

Status OctalStringToNum(const std::string &str, int64_t *n, int64_t min, int64_t max) {
  auto parse_result = ParseInt<int64_t>(str, NumericRange<int64_t>{min, max}, 8);
  if (!parse_result) {
    return parse_result.ToStatus();
  }
  *n = *parse_result;
  return Status::OK();
}

std::string ToLower(std::string in) {
  std::transform(in.begin(), in.end(), in.begin(),
                 [](char c) -> char { return static_cast<char>(std::tolower(c)); });
  return in;
}

std::string Trim(std::string in, const std::string &chars) {
  if (in.empty()) return in;

  in.erase(0, in.find_first_not_of(chars));
  in.erase(in.find_last_not_of(chars) + 1);

  return in;
}

std::vector<std::string> Split(const std::string &in, const std::string &delim) {
  std::vector<std::string> out;

  if (in.empty()) {
    return out;
  }

  if (delim.empty()) {
    out.resize(in.size());
    std::transform(in.begin(), in.end(), out.begin(),
      [](char c) -> std::string { return {c}; });
    return out;
  }

  size_t begin = 0, end = in.find_first_of(delim);
  do {
    std::string elem = in.substr(begin, end - begin);
    if (!elem.empty()) out.push_back(std::move(elem));
    if (end == std::string::npos) break;
    begin = end + 1;
    end = in.find_first_of(delim, begin);
  } while (true);

  return out;
}

std::vector<std::string> Split2KV(const std::string &in, const std::string &delim) {
  std::vector<std::string> out;

  std::string::size_type pos = in.find_first_of(delim);
  if (pos != std::string::npos) {
    std::string key = in.substr(0, pos);
    if (!key.empty()) out.push_back(std::move(key));

    std::string value = Trim(in.substr(pos + 1), delim);
    if (!value.empty()) out.push_back(std::move(value));
  }

  return out;
}

bool HasPrefix(const std::string &str, const std::string &prefix) {
  if (str.empty() || prefix.empty()) return false;
  return !strncasecmp(str.data(), prefix.data(), prefix.size());
}

int StringMatch(const std::string &pattern, const std::string &in, int nocase) {
  return StringMatchLen(pattern.c_str(), pattern.length(), in.c_str(), in.length(), nocase);
}

// Glob-style pattern matching.
int StringMatchLen(const char *pattern, int patternLen,
                   const char *string, int stringLen, int nocase) {
  while (patternLen && stringLen) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while (stringLen) {
          if (StringMatchLen(pattern + 1, patternLen - 1,
                             string, stringLen, nocase))
            return 1; /* match */
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (stringLen == 0)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      case '[': {
        int not_symbol, match;

        pattern++;
        patternLen--;
        not_symbol = pattern[0] == '^';
        if (not_symbol) {
          pattern++;
          patternLen--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\' && patternLen >= 2) {
            pattern++;
            patternLen--;
            if (pattern[0] == string[0])
              match = 1;
          } else if (pattern[0] == ']') {
            break;
          } else if (patternLen == 0) {
            pattern--;
            patternLen++;
            break;
          } else if (pattern[1] == '-' && patternLen >= 3) {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            patternLen -= 2;
            if (c >= start && c <= end)
              match = 1;
          } else {
            if (!nocase) {
              if (pattern[0] == string[0])
                match = 1;
            } else {
              if (tolower(static_cast<int>(pattern[0])) == tolower(static_cast<int>(string[0])))
                match = 1;
            }
          }
          pattern++;
          patternLen--;
        }
        if (not_symbol)
          match = !match;
        if (!match)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0])
            return 0; /* no match */
        } else {
          if (tolower(static_cast<int>(pattern[0])) != tolower(static_cast<int>(string[0])))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0)
    return 1;
  return 0;
}

std::string StringToHex(const std::string &input) {
  static const char hex_digits[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(input.length() * 2);
  for (unsigned char c : input) {
    output.push_back(hex_digits[c >> 4]);
    output.push_back(hex_digits[c & 15]);
  }
  return output;
}

void BytesToHuman(char *buf, size_t size, uint64_t n) {
  double d;

  if (n < 1024) {
    snprintf(buf, size, "%" PRIu64 "B", n);
  } else if (n < (1024*1024)) {
    d = static_cast<double>(n)/(1024);
    snprintf(buf, size, "%.2fK", d);
  } else if (n < (1024LL*1024*1024)) {
    d = static_cast<double>(n)/(1024*1024);
    snprintf(buf, size, "%.2fM", d);
  } else if (n < (1024LL*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024);
    snprintf(buf, size, "%.2fG", d);
  } else if (n < (1024LL*1024*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024*1024);
    snprintf(buf, size, "%.2fT", d);
  } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024*1024*1024);
    snprintf(buf, size, "%.2fP", d);
  } else {
    snprintf(buf, size, "%" PRIu64 "B", n);
  }
}

std::vector<std::string> TokenizeRedisProtocol(const std::string &value) {
  std::vector<std::string> tokens;

  if (value.empty()) {
    return tokens;
  }

  enum ParserState { stateArrayLen, stateBulkLen, stateBulkData };
  uint64_t array_len = 0, bulk_len = 0;
  int state = stateArrayLen;
  const char *start = value.data(), *end = start + value.size(), *p;
  while (start != end) {
    switch (state) {
      case stateArrayLen:
        if (start[0] != '*') {
          return tokens;
        }
        p = strchr(start, '\r');
        if (!p || (p == end) || p[1] != '\n') {
          tokens.clear();
          return tokens;
        }
        array_len = std::stoull(std::string(start+1, p));
        start = p + 2;
        state = stateBulkLen;
        break;

      case stateBulkLen:
        if (start[0] != '$') {
          return tokens;
        }
        p = strchr(start, '\r');
        if (!p || (p == end) || p[1] != '\n') {
          tokens.clear();
          return tokens;
        }
        bulk_len = std::stoull(std::string(start+1, p));
        start = p + 2;
        state = stateBulkData;
        break;

      case stateBulkData:
        if (bulk_len+2 > static_cast<uint64_t>(end-start)) {
          tokens.clear();
          return tokens;
        }
        tokens.emplace_back(std::string(start, start+bulk_len));
        start += bulk_len + 2;
        state = stateBulkLen;
        break;
    }
  }
  if (array_len != tokens.size()) {
    tokens.clear();
  }
  return tokens;
}

bool IsPortInUse(int port) {
  int fd;
  Status s = SockConnect("0.0.0.0", static_cast<uint32_t>(port), &fd);
  if (fd > 0) close(fd);
  return s.IsOK();
}

void ThreadSetName(const char *name) {
#ifdef __APPLE__
  pthread_setname_np(name);
#else
  pthread_setname_np(pthread_self(), name);
#endif
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception */
int aeWait(int fd, int mask, uint64_t timeout) {
  struct pollfd pfd;
  int retmask = 0, retval;

  memset(&pfd, 0, sizeof(pfd));
  pfd.fd = fd;
  if (mask & AE_READABLE) pfd.events |= POLLIN;
  if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

  if ((retval = poll(&pfd, 1, timeout)) == 1) {
    if (pfd.revents & POLLIN) retmask |= AE_READABLE;
    if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
    if (pfd.revents & POLLERR) retmask |= AE_ERROR;
    if (pfd.revents & POLLHUP) retmask |= AE_HUP;
    return retmask;
  } else {
    return retval;
  }
}

uint64_t GetTimeStampMS(void) {
  auto tp = std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::system_clock::now());
  auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(tp.time_since_epoch());
  return ts.count();
}

uint64_t GetTimeStampUS(void) {
  auto tp = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now());
  auto ts = std::chrono::duration_cast<std::chrono::microseconds>(tp.time_since_epoch());
  return ts.count();
}

}  // namespace Util
