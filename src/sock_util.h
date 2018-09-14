#pragma once

#include <string>

int sock_connect(std::string host, uint32_t port, int* fd);
int sock_send(int fd, const std::string &data);
