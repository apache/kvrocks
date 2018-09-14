#include "redis_reply.h"

namespace Redis {

void Reply(evbuffer *output, const std::string &data) {
  evbuffer_add(output, data.c_str(), data.length());
}

std::string SimpleString(std::string data) { return "+" + data + CRLF; }

std::string Error(std::string err) { return "-" + err + CRLF; }

std::string Integer(int64_t data) { return ":" + std::to_string(data) + CRLF; }

std::string BulkString(std::string data) {
  return "$" + std::to_string(data.length()) + CRLF + data + CRLF;
}

std::string Array(std::vector<std::string> list) {
  auto buf = "*" + std::to_string(list.size()) + CRLF;
  for (const auto &s : list) {
    buf += s;
  }
  return buf;
}

} // namespace Redis
