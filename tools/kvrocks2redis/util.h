#pragma once

#include <string>
#include <vector>

class Rocksdb2Redis {
 public:
  static std::string Command2RESP(const std::vector<std::string> &cmd_args);
};
