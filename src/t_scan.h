//
// Created by hulk on 2018/10/24.
//

#ifndef KVROCKS_T_SCAN_H
#define KVROCKS_T_SCAN_H

#include "t_metadata.h"
class RedisScan : public RedisDB {
public:
  explicit RedisScan(Storage *db_wrapper):RedisDB(db_wrapper){}
  rocksdb::Status Get(RedisType type, Slice key, std::string *start_key);
  rocksdb::Status Set(RedisType type, Slice key, std::string *start_key);
};


#endif //KVROCKS_T_SCAN_H
