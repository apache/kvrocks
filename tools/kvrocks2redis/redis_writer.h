#pragma once

#include <glog/logging.h>
#include <string>
#include <vector>
#include <thread>

#include "writer.h"

class RedisWriter : public Writer {
 public:
  explicit RedisWriter(Kvrocks2redis::Config *config);
  ~RedisWriter();
  Status Write(const std::string &ns, const std::vector<std::string> &aofs) override;
  Status FlushAll(const std::string &ns) override;

  void Stop() override;

 private:
  std::thread t_;
  bool stop_flag_ = false;
  std::map<std::string, int> next_offset_fds_;
  std::map<std::string, std::istream::off_type> next_offsets_;
  std::map<std::string, int> redis_fds_;

  void sync();
  Status getRedisConn(const std::string &ns,
                      const std::string &host,
                      uint32_t port,
                      const std::string &auth,
                      int db_index);
  Status authRedis(const std::string &ns, const std::string &auth);
  Status selectDB(const std::string &ns, int db_number);

  Status updateNextOffset(const std::string &ns, std::istream::off_type offset);
  Status readNextOffsetFromFile(const std::string &ns, std::istream::off_type *offset);
  Status writeNextOffsetToFile(const std::string &ns, std::istream::off_type offset);
  std::string getNextOffsetFilePath(const std::string &ns);
};
