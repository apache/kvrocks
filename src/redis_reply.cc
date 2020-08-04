#include "redis_reply.h"
#include <numeric>

namespace Redis {

void Reply(evbuffer *output, const std::string &data) {
  evbuffer_add(output, data.c_str(), data.length());
}

std::string SimpleString(const std::string &data) { return "+" + data + CRLF; }

std::string Error(const std::string &err) { return "-" + err + CRLF; }

std::string Integer(int64_t data) { return ":" + std::to_string(data) + CRLF; }

std::string BulkString(const std::string &data) {
  return "$" + std::to_string(data.length()) + CRLF + data + CRLF;
}

std::string NilString() {
  return "$-1\r\n";
}

std::string MultiLen(int64_t len) {
  return "*"+std::to_string(len)+"\r\n";
}

std::string MultiBulkString(std::vector<std::string> values, bool output_nil_for_empty_string) {
  for (size_t i = 0; i < values.size(); i++) {
    if (values[i].empty() && output_nil_for_empty_string) {
      values[i] = NilString();
    }  else {
      values[i] = BulkString(values[i]);
    }
  }
  return Array(values);
}


std::string MultiBulkString(std::vector<std::string> values, const std::vector<rocksdb::Status> &statuses) {
  for (size_t i = 0; i < values.size(); i++) {
    if (i < statuses.size() && statuses[i].IsNotFound()) {
      values[i] = NilString();
    } else {
      values[i] = BulkString(values[i]);
    }
  }
  return Array(values);
}
std::string Array(std::vector<std::string> list) {
  return std::accumulate(list.begin(), list.end(), "*" + std::to_string(list.size()) + CRLF);
}

}  // namespace Redis
