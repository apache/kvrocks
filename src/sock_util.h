#pragma once

#include <arpa/inet.h>

#include <cctype>
#include <string>
#include <vector>

namespace Util {
sockaddr_in NewSockaddrInet(const std::string &host, uint32_t port);
int SockConnect(std::string host, uint32_t port, int *fd);
int SockSend(int fd, const std::string &data);
int GetPeerAddr(int fd, std::string *addr, uint32_t *port);

std::string ToLower(std::string in);
void BytesToHuman(char *s, unsigned long long n);
std::string& Trim(std::string &in, std::string chars);
void Split(std::string in, std::string delim, std::vector<std::string> *out);
}
