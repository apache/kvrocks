/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "batch_extractor.h"

#include <rocksdb/write_batch.h>
#include <glog/logging.h>

#include "redis_bitmap.h"
#include "redis_slot.h"
#include "redis_reply.h"

void WriteBatchExtractor::LogData(const rocksdb::Slice &blob) {
  log_data_.Decode(blob);
}

rocksdb::Status WriteBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key,
                                           const Slice &value) {
  if (column_family_id == kColumnFamilyIDZSetScore) {
    return rocksdb::Status::OK();
  }

  std::string ns, user_key, sub_key;
  std::vector<std::string> command_args;
  if (column_family_id == kColumnFamilyIDMetadata) {
    ExtractNamespaceKey(key, &ns, &user_key, is_slotid_encoded_);
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    Metadata metadata(kRedisNone);
    metadata.Decode(value.ToString());
    if (metadata.Type() == kRedisString) {
      command_args = {"SET", user_key, value.ToString().substr(5, value.size() - 5)};
      resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
      if (metadata.expire > 0) {
        command_args = {"EXPIREAT", user_key, std::to_string(metadata.expire)};
        resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
      }
    } else if (metadata.expire > 0) {
      auto args = log_data_.GetArguments();
      if (args->size() > 0) {
        RedisCommand cmd = static_cast<RedisCommand >(std::stoi((*args)[0]));
        if (cmd == kRedisCmdExpire) {
          command_args = {"EXPIREAT", user_key, std::to_string(metadata.expire)};
          resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
        }
      }
    }

    return rocksdb::Status::OK();
  }

  if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, is_slotid_encoded_);
    user_key = ikey.GetKey().ToString();
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();
    switch (log_data_.GetRedisType()) {
      case kRedisHash:command_args = {"HSET", user_key, sub_key, value.ToString()};
        break;
      case kRedisList: {
        auto args = log_data_.GetArguments();
        if (args->size() < 1) {
          LOG(ERROR) << "Fail to parse write_batch in putcf type list : args error ,should at least contain cmd";
          return rocksdb::Status::OK();
        }
        RedisCommand cmd = static_cast<RedisCommand >(std::stoi((*args)[0]));
        switch (cmd) {
          case kRedisCmdLSet:
            if (args->size() < 2) {
              LOG(ERROR) << "Fail to parse write_batch in putcf cmd lset : args error ,should contain lset index";
              return rocksdb::Status::OK();
            }
            command_args = {"LSET", user_key, (*args)[1], value.ToString()};
            break;
          case kRedisCmdLInsert:
            if (first_seen_) {
              if (args->size() < 4) {
                LOG(ERROR)
                   << "Fail to parse write_batch in putcf cmd linsert : args error, should contain before pivot value";
                return rocksdb::Status::OK();
              }
              command_args = {"LINSERT", user_key, (*args)[1] == "1" ? "before" : "after", (*args)[2], (*args)[3]};
              first_seen_ = false;
            }
            break;
          case kRedisCmdLRem:
            // lrem will be parsed in deletecf, so ignore this putcf
            break;
          default:command_args = {cmd == kRedisCmdLPush ? "LPUSH" : "RPUSH", user_key, value.ToString()};
        }
        break;
      }
      case kRedisSet:command_args = {"SADD", user_key, sub_key};
        break;
      case kRedisZSet: {
        double score = DecodeDouble(value.data());
        command_args = {"ZADD", user_key, std::to_string(score), sub_key};
        break;
      }
      case kRedisBitmap: {
        auto args = log_data_.GetArguments();
        if (args->size() < 1) {
          LOG(ERROR) << "Fail to parse write_batch in putcf type bitmap : args error ,should at least contain cmd";
          return rocksdb::Status::OK();
        }
        RedisCommand cmd = static_cast<RedisCommand >(std::stoi((*args)[0]));
        switch (cmd) {
          case kRedisCmdSetBit: {
            if (args->size() < 2) {
              LOG(ERROR) << "Fail to parse write_batch in putcf cmd setbit : args error ,should contain setbit offset";
              return rocksdb::Status::OK();
            }
            bool bit_value = Redis::Bitmap::GetBitFromValueAndOffset(value.ToString(), std::stoi((*args)[1]));
            command_args = {"SETBIT", user_key, (*args)[1], bit_value ? "1" : "0"};
            break;
          }
          case kRedisCmdBitOp:
            if (first_seen_) {
              if (args->size() < 4) {
                LOG(ERROR)
                   << "Fail to parse write_batch in putcf cmd bitop : args error, should at least contain srckey";
                return rocksdb::Status::OK();
              }
              command_args = {"BITOP", (*args)[1], user_key};
              command_args.insert(command_args.end(), args->begin() + 2, args->end());
              first_seen_ = false;
            }
            break;
          default:
            LOG(ERROR) << "Fail to parse write_batch in putcf type bitmap : cmd error";
            return rocksdb::Status::OK();
        }
        break;
      }
      case kRedisSortedint: {
        if (!to_redis_) {
          command_args = {"SIADD", user_key, std::to_string(DecodeFixed64(sub_key.data()))};
        }
        break;
      }
      default: break;
    }
  }

  if (!command_args.empty()) {
    resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteCF(uint32_t column_family_id, const Slice &key) {
  if (column_family_id == kColumnFamilyIDZSetScore) {
    return rocksdb::Status::OK();
  }

  std::string ns, user_key, sub_key;
  std::vector<std::string> command_args;
  if (column_family_id == kColumnFamilyIDMetadata) {
    ExtractNamespaceKey(key, &ns, &user_key, is_slotid_encoded_);
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    command_args = {"DEL", user_key};
  } else if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, is_slotid_encoded_);
    user_key = ikey.GetKey().ToString();
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();
    switch (log_data_.GetRedisType()) {
      case kRedisHash: command_args = {"HDEL", user_key, sub_key};
        break;
      case kRedisSet: command_args = {"SREM", user_key, sub_key};
        break;
      case kRedisZSet: command_args = {"ZREM", user_key, sub_key};
        break;
      case kRedisList: {
        auto args = log_data_.GetArguments();
        if (args->size() < 1) {
          LOG(ERROR) << "Fail to parse write_batch in DeleteCF type list : args error ,should contain cmd";
          return rocksdb::Status::OK();
        }
        RedisCommand cmd = static_cast<RedisCommand >(std::stoi((*args)[0]));
        switch (cmd) {
          case kRedisCmdLTrim:
            if (first_seen_) {
              if (args->size() < 3) {
                LOG(ERROR) << "Fail to parse write_batch in DeleteCF cmd ltrim : args error ,should contain start,stop";
                return rocksdb::Status::OK();
              }
              command_args = {"LTRIM", user_key, (*args)[1], (*args)[2]};
              first_seen_ = false;
            }
            break;
          case kRedisCmdLRem:
            if (first_seen_) {
              if (args->size() < 3) {
                LOG(ERROR) << "Fail to parse write_batch in DeleteCF cmd lrem : args error ,should contain count,value";
                return rocksdb::Status::OK();
              }
              command_args = {"LREM", user_key, (*args)[1], (*args)[2]};
              first_seen_ = false;
            }
            break;
          default:command_args = {cmd == kRedisCmdLPop ? "LPOP" : "RPOP", user_key};
        }
        break;
      }
      case kRedisSortedint: {
        if (!to_redis_) {
          command_args = {"SIREM", user_key, std::to_string(DecodeFixed64(sub_key.data()))};
        }
        break;
      }
      default: break;
    }
  }

  if (!command_args.empty()) {
    resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteRangeCF(uint32_t column_family_id,
                                                   const Slice& begin_key, const Slice& end_key) {
  // Do nothing about DeleteRange operations
  return rocksdb::Status::OK();
}
