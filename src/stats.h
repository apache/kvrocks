#ifndef KVROCKS_STATS_H
#define KVROCKS_STATS_H

#include <map>
#include <string>
#include <unistd.h>

class Stats {
 public:
  long GetMemoryRSS();
  void GetRocksdbStats(std::map<std::string, std::string> *stats);
};

#endif //KVROCKS_STATS_H