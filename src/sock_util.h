#pragma once

#include <string>
#include <arpa/inet.h>

sockaddr_in NewSockaddrInet(const std::string &host, uint32_t port);
int SockConnect(std::string host, uint32_t port, int *fd);
int SockSend(int fd, const std::string &data);
int GetPeerAddr(int fd, std::string *addr, uint32_t *port);
