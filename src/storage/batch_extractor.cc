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

#include <glog/logging.h>
#include <rocksdb/write_batch.h>

#include "cluster/redis_slot.h"
#include "parse_util.h"
#include "server/redis_reply.h"
#include "server/server.h"
#include "types/redis_bitmap.h"

void WriteBatchExtractor::LogData(const rocksdb::Slice &blob) {
  // Currently, we only have two kinds of log data
  if (ServerLogData::IsServerLogData(blob.data())) {
    ServerLogData server_log;
    if (server_log.Decode(blob).IsOK()) {
      // We don't handle server log currently
    }
  } else {
    // Redis type log data
    if (auto s = log_data_.Decode(blob); !s.IsOK()) {
      LOG(WARNING) << "Failed to decode Redis type log: " << s.Msg();
    }
  }
}

rocksdb::Status WriteBatchExtractor::PutCF(uint32_t column_family_id, const Slice &key, const Slice &value) {
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
      command_args = {"SET", user_key, value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0]))};
      resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
      if (metadata.expire > 0) {
        command_args = {"PEXPIREAT", user_key, std::to_string(metadata.expire)};
        resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
      }
    } else if (metadata.expire > 0) {
      auto args = log_data_.GetArguments();
      if (args->size() > 0) {
        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(parse_result.Msg());
        }
        auto cmd = static_cast<RedisCommand>(*parse_result);
        if (cmd == kRedisCmdExpire) {
          command_args = {"PEXPIREAT", user_key, std::to_string(metadata.expire)};
          resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
        }
      }
    }

    if (metadata.Type() == kRedisStream) {
      auto args = log_data_.GetArguments();
      bool isSetID = args && args->size() > 0 && (*args)[0] == "XSETID";
      if (!isSetID) {
        return rocksdb::Status::OK();
      }
      StreamMetadata stream_metadata;
      auto s = stream_metadata.Decode(value.ToString());
      if (!s.ok()) return s;
      command_args = {"XSETID",
                      user_key,
                      stream_metadata.last_entry_id.ToString(),
                      "ENTRIESADDED",
                      std::to_string(stream_metadata.entries_added),
                      "MAXDELETEDID",
                      stream_metadata.max_deleted_entry_id.ToString()};
      resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
    }
    return rocksdb::Status::OK();
  } else if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, is_slotid_encoded_);
    user_key = ikey.GetKey().ToString();
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    sub_key = ikey.GetSubKey().ToString();
    ns = ikey.GetNamespace().ToString();
    switch (log_data_.GetRedisType()) {
      case kRedisHash:
        command_args = {"HSET", user_key, sub_key, value.ToString()};
        break;
      case kRedisList: {
        auto args = log_data_.GetArguments();
        if (args->size() < 1) {
          LOG(ERROR) << "Fail to parse write_batch in putcf type list : args error ,should at least contain cmd";
          return rocksdb::Status::OK();
        }
        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(parse_result.Msg());
        }
        auto cmd = static_cast<RedisCommand>(*parse_result);
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
          default:
            command_args = {cmd == kRedisCmdLPush ? "LPUSH" : "RPUSH", user_key, value.ToString()};
        }
        break;
      }
      case kRedisSet:
        command_args = {"SADD", user_key, sub_key};
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
        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(parse_result.Msg());
        }
        auto cmd = static_cast<RedisCommand>(*parse_result);
        switch (cmd) {
          case kRedisCmdSetBit: {
            if (args->size() < 2) {
              LOG(ERROR) << "Fail to parse write_batch in putcf cmd setbit : args error ,should contain setbit offset";
              return rocksdb::Status::OK();
            }
            auto parse_result = ParseInt<int>((*args)[1], 10);
            if (!parse_result) {
              return rocksdb::Status::InvalidArgument(parse_result.Msg());
            }
            bool bit_value = Redis::Bitmap::GetBitFromValueAndOffset(value.ToString(), *parse_result);
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
      default:
        break;
    }
  } else if (column_family_id == kColumnFamilyIDStream) {
    auto s = ExtractStreamAddCommand(is_slotid_encoded_, key, value, &command_args);
    if (!s.IsOK()) {
      LOG(ERROR) << "Fail to parse write_batch for the stream type: " << s.Msg();
      return rocksdb::Status::OK();
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
      case kRedisHash:
        command_args = {"HDEL", user_key, sub_key};
        break;
      case kRedisSet:
        command_args = {"SREM", user_key, sub_key};
        break;
      case kRedisZSet:
        command_args = {"ZREM", user_key, sub_key};
        break;
      case kRedisList: {
        auto args = log_data_.GetArguments();
        if (args->size() < 1) {
          LOG(ERROR) << "Fail to parse write_batch in DeleteCF type list : args error ,should contain cmd";
          return rocksdb::Status::OK();
        }
        auto parse_result = ParseInt<int>((*args)[0], 10);
        if (!parse_result) {
          return rocksdb::Status::InvalidArgument(parse_result.Msg());
        }
        auto cmd = static_cast<RedisCommand>(*parse_result);
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
          default:
            command_args = {cmd == kRedisCmdLPop ? "LPOP" : "RPOP", user_key};
        }
        break;
      }
      case kRedisSortedint: {
        if (!to_redis_) {
          command_args = {"SIREM", user_key, std::to_string(DecodeFixed64(sub_key.data()))};
        }
        break;
      }
      default:
        break;
    }
  } else if (column_family_id == kColumnFamilyIDStream) {
    InternalKey ikey(key, is_slotid_encoded_);
    Slice encoded_id = ikey.GetSubKey();
    Redis::StreamEntryID entry_id;
    GetFixed64(&encoded_id, &entry_id.ms);
    GetFixed64(&encoded_id, &entry_id.seq);
    command_args = {"XDEL", ikey.GetKey().ToString(), entry_id.ToString()};
  }

  if (!command_args.empty()) {
    resp_commands_[ns].emplace_back(Redis::Command2RESP(command_args));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteRangeCF(uint32_t column_family_id, const Slice &begin_key,
                                                   const Slice &end_key) {
  // Do nothing about DeleteRange operations
  return rocksdb::Status::OK();
}

Status WriteBatchExtractor::ExtractStreamAddCommand(bool is_slotid_encoded, const Slice &subkey, const Slice &value,
                                                    std::vector<std::string> *command_args) {
  InternalKey ikey(subkey, is_slotid_encoded);
  std::string user_key = ikey.GetKey().ToString();
  *command_args = {"XADD", user_key};
  std::vector<std::string> values;
  auto s = Redis::DecodeRawStreamEntryValue(value.ToString(), &values);
  if (!s.IsOK()) {
    return s.Prefixed("failed to decode stream values");
  }
  Slice encoded_id = ikey.GetSubKey();
  Redis::StreamEntryID entry_id;
  GetFixed64(&encoded_id, &entry_id.ms);
  GetFixed64(&encoded_id, &entry_id.seq);

  command_args->emplace_back(entry_id.ToString());
  command_args->insert(command_args->end(), values.begin(), values.end());
  return Status::OK();
}
