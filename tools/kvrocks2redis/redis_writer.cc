#include "redis_writer.h"

Status RedisWriter::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  return Writer::Write(ns, aofs);
}

Status RedisWriter::FlushAll(const std::string &ns) {
  return Writer::FlushAll(ns);
}
