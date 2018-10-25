#pragma once
#include <string>
#include <vector>
#include <event2/buffer.h>

#define CRLF "\r\n"

namespace Redis {

void Reply(evbuffer *output, const std::string &data);
std::string SimpleString(std::string data);
std::string Error(std::string err);
std::string Integer(int64_t data);
std::string BulkString(std::string data);
std::string NilString();
std::string Array(std::vector<std::string> list);

std::string ParseSimpleString(evbuffer *input);

} // namespace Redis
