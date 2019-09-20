#pragma once

#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <arpa/inet.h>

#include <cctype>
#include <string>
#include <vector>

#include "status.h"

namespace Util {
// sock util
sockaddr_in NewSockaddrInet(const std::string &host, uint32_t port);
Status SockConnect(std::string host, uint32_t port, int *fd, uint64_t conn_timeout = 0, uint64_t timeout = 0);
Status SockSend(int fd, const std::string &data);
int GetPeerAddr(int fd, std::string *addr, uint32_t *port);
bool IsPortInUse(int port);

// string util
Status StringToNum(const std::string &str, int64_t *n, int64_t min = INT64_MIN, int64_t max = INT64_MAX);
std::string ToLower(std::string in);
void BytesToHuman(char *buf, size_t size, uint64_t n);
void Trim(const std::string &in, const std::string &chars, std::string *out);
void Split(std::string in, std::string delim, std::vector<std::string> *out);
int StringMatch(const std::string &pattern, const std::string &in, int nocase);
int StringMatchLen(const char *p, int plen, const char *s, int slen, int nocase);

void ThreadSetName(const char *name);
int aeWait(int fd, int mask, uint64_t milliseconds);
}  // namespace Util
