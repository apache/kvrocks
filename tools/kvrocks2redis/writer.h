#pragma once

#include <string>
#include <map>
#include <fstream>
#include <vector>

#include "../../src/status.h"

#include "config.h"

class Writer {
 public:
  explicit Writer(Kvrocks2redis::Config *config) : config_(config) {}
  ~Writer();
  virtual Status Write(const std::string &ns, const std::vector<std::string> &aofs);
  virtual Status FlushAll(const std::string &ns);
  Status OpenAofFile(const std::string &ns, bool truncate);
  Status GetAofFileStream(const std::string &ns, bool truncate = false);
  std::string GetAofFilePath(const std::string &ns);

 protected:
  Kvrocks2redis::Config *config_ = nullptr;
  std::map<std::string, std::ofstream> aof_file_streams_;
};
