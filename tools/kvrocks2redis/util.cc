#include "util.h"

#include "../../src/redis_reply.h"

std::string Rocksdb2Redis::Command2RESP(const std::vector<std::string> &cmd_args) {
  std::string output;
  output.append("*" + std::to_string(cmd_args.size()) + CRLF);
  for (const auto &arg : cmd_args) {
    output.append("$" + std::to_string(arg.size()) + CRLF);
    output.append(arg + CRLF);
  }
  return output;
}
