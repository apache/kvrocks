#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <event2/util.h>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <poll.h>
#include <errno.h>
#include <pthread.h>

#include <string>
#include <algorithm>

#include "util.h"
#include "status.h"

#ifndef POLLIN
# define POLLIN      0x0001    /* There is data to read */
# define POLLPRI     0x0002    /* There is urgent data to read */
# define POLLOUT     0x0004    /* Writing now will not block */
# define POLLERR     0x0008    /* Error condition */
# define POLLHUP     0x0010    /* Hung up */
# define POLLNVAL    0x0020    /* Invalid request: fd not open */
#endif

namespace Util {
sockaddr_in NewSockaddrInet(const std::string &host, uint32_t port) {
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = inet_addr(host.c_str());
  sin.sin_port = htons(port);
  return sin;
}

Status SockConnect(std::string host, uint32_t port, int *fd) {
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = inet_addr(host.c_str());
  sin.sin_port = htons(port);
  *fd = socket(AF_INET, SOCK_STREAM, 0);
  auto rv = connect(*fd, reinterpret_cast<sockaddr *>(&sin), sizeof(sin));
  if (rv < 0) {
    return Status(Status::NotOK, strerror(errno));
  }
  setsockopt(*fd, SOL_SOCKET, SO_KEEPALIVE, nullptr, 0);
  setsockopt(*fd, IPPROTO_TCP, TCP_NODELAY, nullptr, 0);
  return Status::OK();
}

int SockSend(int fd, const std::string &data) {
  auto rv = send(fd, data.c_str(), data.length(), 0);
  if (rv < 0) {
    LOG(ERROR) << "[Socket] Failed to send: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    return -1;
  }
  return 0;
}

int GetPeerAddr(int fd, std::string *addr, uint32_t *port) {
  sockaddr_storage sa{};
  socklen_t sa_len = sizeof(sa);
  if (getpeername(fd, reinterpret_cast<sockaddr *>(&sa), &sa_len) < 0) {
    return -1;
  }
  if (sa.ss_family == AF_INET) {
    auto sa4 = reinterpret_cast<sockaddr_in *>(&sa);
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, reinterpret_cast<void *>(&sa4->sin_addr), buf, INET_ADDRSTRLEN);
    addr->clear();
    addr->append(buf);
    *port = ntohs(sa4->sin_port);
    return 0;
  }
  return -2;  // only support AF_INET currently
}

std::string ToLower(std::string in) {
  std::transform(in.begin(), in.end(), in.begin(),
                 [](char c) -> char { return static_cast<char>(std::tolower(c)); });
  return in;
}

void Trim(const std::string &in, const std::string &chars, std::string *out) {
  out->clear();
  if (in.empty()) return;
  out->assign(in);
  out->erase(0, out->find_first_not_of(chars));
  out->erase(out->find_last_not_of(chars)+1);
}

void Split(std::string in, std::string delim, std::vector<std::string> *out) {
  if (in.empty() || !out) return;
  out->clear();

  std::string::size_type pos = 0;
  std::string elem, trimed_elem;
  do {
    pos = in.find_first_of(delim);
    elem = in.substr(0, pos);
    Trim(elem, delim, &trimed_elem);
    if (!trimed_elem.empty()) out->push_back(trimed_elem);
    in = in.substr(pos+1);
  } while (pos != std::string::npos);
}

void BytesToHuman(char *s, int size, uint64_t n) {
  double d;

  if (n < 1024) {
    snprintf(s, size, "%lluB", n);
    return;
  } else if (n < (1024*1024)) {
    d = static_cast<double>(n)/(1024);
    snprintf(s, size, "%.2fK", d);
  } else if (n < (1024LL*1024*1024)) {
    d = static_cast<double>(n)/(1024*1024);
    snprintf(s, size, "%.2fM", d);
  } else if (n < (1024LL*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024);
    snprintf(s, size, "%.2fG", d);
  } else if (n < (1024LL*1024*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024*1024);
    snprintf(s, size, "%.2fT", d);
  } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
    d = static_cast<double>(n)/(1024LL*1024*1024*1024*1024);
    snprintf(s, size, "%.2fP", d);
  } else {
    snprintf(s, size, "%lluB", n);
  }
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
}  // namespace Util
