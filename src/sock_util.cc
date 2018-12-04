#include <unistd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <event2/util.h>
#include <glog/logging.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <poll.h>
#include <string>

#include "sock_util.h"

sockaddr_in new_sockaddr_inet(std::string host, uint32_t port) {
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = inet_addr(host.c_str());
  sin.sin_port = htons(port);
  return sin;
}

int sock_check_liveness(int fd) {
  struct pollfd rfd[1];
  rfd[0].fd = fd;
  rfd[0].events = POLL_IN|POLL_PRI|POLL_ERR|POLL_HUP;
  rfd[0].revents = 0;

  if (poll(rfd, 1, 0) > 0) {
    char buf[1];
    ssize_t n = recv(fd, buf, sizeof(buf), MSG_PEEK);
    if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
      return 0;
    }
  }
  return 1;
}

int sock_connect(std::string host, uint32_t port, int* fd) {
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_addr.s_addr = inet_addr(host.c_str());
  sin.sin_port = htons(port);
  *fd = socket(AF_INET, SOCK_STREAM, 0);
  auto rv = connect(*fd, (sockaddr*)&sin, sizeof(sin));
  if (rv < 0) {
    LOG(ERROR) << "[Socket] Failed to connect: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    return rv;
  }
  setsockopt(*fd, SOL_SOCKET, SO_KEEPALIVE, nullptr, 0);
  setsockopt(*fd, IPPROTO_TCP, TCP_NODELAY, nullptr, 0);
  return 0;
}

int sock_send(int fd, const std::string &data) {
  auto rv = send(fd, data.c_str(), data.length(), 0);
  if (rv < 0) {
    LOG(ERROR) << "[Socket] Failed to send: "
               << evutil_socket_error_to_string(EVUTIL_SOCKET_ERROR());
    return -1;
  }
  return 0;
}
