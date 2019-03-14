#pragma once

#include <glog/logging.h>

#include <string>
#include <vector>

#include "writer.h"

class RedisWriter : public Writer {
 public:
  explicit RedisWriter(Kvrocks2redis::Config *config) : Writer(config) {}
  Status Write(const std::string &ns, const std::vector<std::string> &aofs) override;
  Status FlushAll(const std::string &ns) override;
};
