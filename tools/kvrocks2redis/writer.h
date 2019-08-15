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
  virtual void Stop() {};
  Status OpenAofFile(const std::string &ns, bool truncate);
  Status GetAofFd(const std::string &ns, bool truncate = false);
  std::string GetAofFilePath(const std::string &ns);

 protected:
  Kvrocks2redis::Config *config_ = nullptr;
  std::map<std::string, int> aof_fds_;
};
