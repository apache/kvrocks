#pragma once

#include <string>
#include <arpa/inet.h>

sockaddr_in new_sockaddr_inet(const std::string &host, uint32_t port);
int sock_check_liveness(int fd);
int sock_connect(std::string host, uint32_t port, int* fd);
int sock_send(int fd, const std::string &data);
int get_peer_addr(int fd, std::string *addr, uint32_t *port);
