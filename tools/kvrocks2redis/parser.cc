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

#include "parser.h"

#include <glog/logging.h>
#include <rocksdb/write_batch.h>

#include <memory>

#include "../../src/redis_slot.h"
#include "../../src/redis_reply.h"
#include "db_util.h"

Status Parser::ParseFullDB() {
  rocksdb::DB *db_ = storage_->GetDB();
  if (!lastest_snapshot_) lastest_snapshot_ = std::unique_ptr<LatestSnapShot>(new LatestSnapShot(db_));
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
  ExtractNamespaceKey(ns_key, &ns, &user_key, is_slotid_encoded_);
  std::string output;
  output = Redis::Command2RESP(
      {"SET", user_key, value.ToString().substr(5, value.size() - 5)});
  Status s = writer_->Write(ns, {output});
  if (!s.IsOK()) return s;

  if (expire > 0) {
    output = Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(expire)});
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
  ExtractNamespaceKey(ns_key, &ns, &user_key, is_slotid_encoded_);
  InternalKey(ns_key, "", metadata.version, is_slotid_encoded_).Encode(&prefix_key);
  InternalKey(ns_key, "", metadata.version + 1, is_slotid_encoded_).Encode(&next_version_prefix_key);

  rocksdb::DB *db_ = storage_->GetDB();
  rocksdb::ReadOptions read_options;
  read_options.snapshot = lastest_snapshot_->GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  read_options.fill_cache = false;

  auto iter = DBUtil::UniqueIterator(db_, read_options);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }
    Status s;
    InternalKey ikey(iter->key(), is_slotid_encoded_);
    sub_key = ikey.GetSubKey().ToString();
    value = iter->value().ToString();
    switch (type) {
      case kRedisHash:
        output = Redis::Command2RESP({"HSET", user_key, sub_key, value});
        break;
      case kRedisSet:
        output = Redis::Command2RESP({"SADD", user_key, sub_key});
        break;
      case kRedisList:
        output = Redis::Command2RESP({"RPUSH", user_key, value});
        break;
      case kRedisZSet: {
        double score = DecodeDouble(value.data());
        output = Redis::Command2RESP({"ZADD", user_key, std::to_string(score), sub_key});
        break;
      }
      case kRedisBitmap: {
        int index = std::stoi(sub_key);
        s = Parser::parseBitmapSegment(ns, user_key, index, value);
        break;
      }
      case kRedisSortedint: {
        std::string val = std::to_string(DecodeFixed64(ikey.GetSubKey().data()));
        output = Redis::Command2RESP({"ZADD", user_key, val, val});
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
    output = Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(metadata.expire)});
    Status s = writer_->Write(ns, {output});
    if (!s.IsOK()) return s;
  }

  return Status::OK();
}

Status Parser::parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap) {
  Status s;
  for (size_t i = 0; i < bitmap.size(); i++) {
    if (bitmap[i] == 0) continue;  // ignore zero byte
    for (int j = 0; j < 8; j++) {
      if (!(bitmap[i] & (1 << j))) continue;  // ignore zero bit
      s = writer_->Write(ns.ToString(), {Redis::Command2RESP(
          {"SETBIT", user_key.ToString(), std::to_string(index * 8 + i * 8 + j), "1"})
      });
      if (!s.IsOK()) return s;
    }
  }
  return Status::OK();
}

rocksdb::Status Parser::ParseWriteBatch(const std::string &batch_string) {
  rocksdb::WriteBatch write_batch(batch_string);
  WriteBatchExtractor write_batch_extractor(is_slotid_encoded_, -1, true);
  rocksdb::Status status;

  status = write_batch.Iterate(&write_batch_extractor);
  if (!status.ok()) return status;
  auto resp_commands = write_batch_extractor.GetRESPCommands();
  for (const auto &iter : *resp_commands) {
    auto s = writer_->Write(iter.first, iter.second);
    if (!s.IsOK()) {
      LOG(ERROR) << "[kvrocks2redis] Failed to parse WriteBatch, encounter error: " << s.Msg();
    }
  }
  return rocksdb::Status::OK();
}
