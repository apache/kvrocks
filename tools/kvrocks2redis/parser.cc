#include "parser.h"

#include <memory>

#include <glog/logging.h>
#include <rocksdb/write_batch.h>

#include "../../src/redis_bitmap.h"
#include "parser.h"
#include "util.h"

Status Parser::ParseFullDB() {
  rocksdb::DB *db_ = storage_->GetDB();
  if (!lastest_snapshot_) lastest_snapshot_ = new LatestSnapShot(db_);
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_ = storage_->GetCFHandle("metadata");

  rocksdb::ReadOptions read_options;
  read_options.snapshot = lastest_snapshot_->GetSnapShot();
  read_options.fill_cache = false;
  std::unique_ptr<rocksdb::Iterator> iter(db_->NewIterator(read_options, metadata_cf_handle_));
  Status s;

  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Metadata metadata(kRedisNone);
    metadata.Decode(iter->value().ToString());
    if (metadata.Expired()) {  // ignore the expired key
      continue;
    }
    if (metadata.Type() == kRedisString) {
      s = parseSimpleKV(iter->key(), iter->value(), metadata.expire);
    } else {
      s = parseComplexKV(iter->key(), metadata);
    }
    if (!s.IsOK()) return s;
  }
  return Status::OK();
}

Status Parser::parseSimpleKV(const Slice &ns_key, const Slice &value, int expire) {
  std::string op, ns, user_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, cluster_enabled_);
  std::string output;
  output = Rocksdb2Redis::Command2RESP(
      {"SET", user_key, value.ToString().substr(5, value.size() - 5)});
  Status s = writer_->Write(ns, {output});
  if (!s.IsOK()) return s;

  if (expire > 0) {
    output = Rocksdb2Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(expire)});
    s = writer_->Write(ns, {output});
  }
  return s;
}

Status Parser::parseComplexKV(const Slice &ns_key, const Metadata &metadata) {
  RedisType type = metadata.Type();
  if (type < kRedisHash || type > kRedisSortedint) {
    return Status(Status::NotOK, "unknown metadata type: " + std::to_string(type));
  }

  std::string ns, prefix_key, user_key, sub_key, value, output, next_version_prefix_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, cluster_enabled_);
  InternalKey(ns_key, "", metadata.version, cluster_enabled_).Encode(&prefix_key);
  InternalKey(ns_key, "", metadata.version + 1, cluster_enabled_).Encode(&next_version_prefix_key);

  rocksdb::DB *db_ = storage_->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = lastest_snapshot_->GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  read_options.fill_cache = false;

  auto iter = db_->NewIterator(read_options);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }
    Status s;
    InternalKey ikey(iter->key(), cluster_enabled_);
    sub_key = ikey.GetSubKey().ToString();
    value = iter->value().ToString();
    switch (type) {
      case kRedisHash:
        output = Rocksdb2Redis::Command2RESP({"HSET", user_key, sub_key, value});
        break;
      case kRedisSet:
        output = Rocksdb2Redis::Command2RESP({"SADD", user_key, sub_key});
        break;
      case kRedisList:
        output = Rocksdb2Redis::Command2RESP({"RPUSH", user_key, value});
        break;
      case kRedisZSet: {
        double score = DecodeDouble(value.data());
        output = Rocksdb2Redis::Command2RESP({"ZADD", user_key, std::to_string(score), sub_key});
        break;
      }
      case kRedisBitmap: {
        int index = std::stoi(sub_key);
        s = Parser::parseBitmapSegment(ns, user_key, index, value);
        break;
      }
      case kRedisSortedint: {
        std::string val = std::to_string(DecodeFixed64(ikey.GetSubKey().data()));
        output = Rocksdb2Redis::Command2RESP({"ZADD", user_key, val, val});
        break;
      }
      default:break;  // should never get here
    }
    if (type != kRedisBitmap) {
      s = writer_->Write(ns, {output});
    }
    if (!s.IsOK()) return s;
  }

  if (metadata.expire > 0) {
    output = Rocksdb2Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(metadata.expire)});
    Status s = writer_->Write(ns, {output});
    if (!s.IsOK()) return s;
  }

  delete iter;
  return Status::OK();
}

Status Parser::parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap) {
  Status s;
  for (size_t i = 0; i < bitmap.size(); i++) {
    if (bitmap[i] == 0) continue;  // ignore zero byte
    for (int j = 0; j < 8; j++) {
      if (!(bitmap[i] & (1 << j))) continue;  // ignore zero bit
      s = writer_->Write(ns.ToString(), {Rocksdb2Redis::Command2RESP(
          {"SETBIT", user_key.ToString(), std::to_string(index * 8 + i * 8 + j), "1"})
      });
      if (!s.IsOK()) return s;
    }
  }
  return Status::OK();
}

rocksdb::Status Parser::ParseWriteBatch(const std::string &batch_string) {
  rocksdb::WriteBatch write_batch(batch_string);
  WriteBatchExtractor write_batch_extractor(cluster_enabled_);
  rocksdb::Status status;

  status = write_batch.Iterate(&write_batch_extractor);
  if (!status.ok()) return status;
  auto aof_strings = write_batch_extractor.GetAofStrings();
  for (const auto &iter : *aof_strings) {
    auto s = writer_->Write(iter.first, iter.second);
    if (!s.IsOK()) {
      LOG(ERROR) << "[kvrocks2redis] Failed to parse WriteBatch, encounter error: " << s.Msg();
    }
  }
  return rocksdb::Status::OK();
}

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
    ExtractNamespaceKey(key, &ns, &user_key, cluster_enabled_);
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    Metadata metadata(kRedisNone);
    metadata.Decode(value.ToString());
    if (metadata.Type() == kRedisString) {
      command_args = {"SET", user_key, value.ToString().substr(5, value.size() - 5)};
      aof_strings_[ns].emplace_back(Rocksdb2Redis::Command2RESP(command_args));
      if (metadata.expire > 0) {
        command_args = {"EXPIREAT", user_key, std::to_string(metadata.expire)};
        aof_strings_[ns].emplace_back(Rocksdb2Redis::Command2RESP(command_args));
      }
    } else if (metadata.expire > 0) {
      auto args = log_data_.GetArguments();
      if (args->size() > 0) {
        RedisCommand cmd = static_cast<RedisCommand >(std::stoi((*args)[0]));
        if (cmd == kRedisCmdExpire) {
          command_args = {"EXPIREAT", user_key, std::to_string(metadata.expire)};
          aof_strings_[ns].emplace_back(Rocksdb2Redis::Command2RESP(command_args));
        }
      }
    }

    return rocksdb::Status::OK();
  }

  if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, cluster_enabled_);
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
            if (firstSeen_) {
              if (args->size() < 4) {
                LOG(ERROR)
                    << "Fail to parse write_batch in putcf cmd linsert : args error ,should contain before pivot value ";
                return rocksdb::Status::OK();
              }
              command_args = {"LINSERT", user_key, (*args)[1] == "1" ? "before" : "after", (*args)[2], (*args)[3]};
              firstSeen_ = false;
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
          LOG(ERROR) << "Fail to parse write_batch in putcf cmd setbit : args error ,should contain setbit offset";
          return rocksdb::Status::OK();
        }
        bool bit_value = Redis::Bitmap::GetBitFromValueAndOffset(value.ToString(), std::stoi((*args)[0]));
        command_args = {"SETBIT", user_key, (*args)[0], bit_value ? "1" : "0"};
        break;
      }
      case kRedisSortedint: {
        std::string val = std::to_string(DecodeFixed64(sub_key.data()));
        if (!to_redis_) {
          command_args = {"SIADD", user_key, val};
        } else {
          command_args = {"ZADD", user_key, val, val};
        }
        break;
      }
      default: break;
    }
  }

  if (!command_args.empty()) {
    aof_strings_[ns].emplace_back(Rocksdb2Redis::Command2RESP(command_args));
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
    ExtractNamespaceKey(key, &ns, &user_key, cluster_enabled_);
    if (slot_ >= 0) {
      if (static_cast<uint16_t>(slot_) != GetSlotNumFromKey(user_key)) return rocksdb::Status::OK();
    }
    command_args = {"DEL", user_key};
  } else if (column_family_id == kColumnFamilyIDDefault) {
    InternalKey ikey(key, cluster_enabled_);
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
            if (firstSeen_) {
              if (args->size() < 3) {
                LOG(ERROR) << "Fail to parse write_batch in DeleteCF cmd ltrim : args error ,should contain start,stop";
                return rocksdb::Status::OK();
              }
              command_args = {"LTRIM", user_key, (*args)[1], (*args)[2]};
              firstSeen_ = false;
            }
            break;
          case kRedisCmdLRem:
            if (firstSeen_) {
              if (args->size() < 3) {
                LOG(ERROR) << "Fail to parse write_batch in DeleteCF cmd lrem : args error ,should contain count,value";
                return rocksdb::Status::OK();
              }
              command_args = {"LREM", user_key, (*args)[1], (*args)[2]};
              firstSeen_ = false;
            }
            break;
          default:command_args = {cmd == kRedisCmdLPop ? "LPOP" : "RPOP", user_key};
        }
        break;
      }
      case kRedisSortedint: {
        std::string sub_key_str = std::to_string(DecodeFixed64(sub_key.data()));
        std::string cmd_str = to_redis_ ? "ZREM" : "SIREM";
        command_args = {cmd_str, user_key, sub_key_str};
        break;
      }
      default: break;
    }
  }

  if (!command_args.empty()) {
    aof_strings_[ns].emplace_back(Rocksdb2Redis::Command2RESP(command_args));
  }
  return rocksdb::Status::OK();
}

rocksdb::Status WriteBatchExtractor::DeleteRangeCF(uint32_t column_family_id,
                                                   const Slice& begin_key, const Slice& end_key) {
  if (slot_ >= 0) return rocksdb::Status::OK();
  std::string beginns, endns, begin_userkey, end_userkey;
  ExtractNamespaceKey(begin_key, &beginns, &begin_userkey, cluster_enabled_);
  ExtractNamespaceKey(end_key, &endns, &end_userkey, cluster_enabled_);
  if (beginns == endns) {
    aof_strings_[beginns].emplace_back(Rocksdb2Redis::Command2RESP({"FLUSHDB"}));
  } else {
    aof_strings_["flushall"].emplace_back(Rocksdb2Redis::Command2RESP({"FLUSHALL"}));
  }
  return rocksdb::Status::OK();
}
