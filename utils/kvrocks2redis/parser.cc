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

#include "cluster/redis_slot.h"
#include "db_util.h"
#include "server/redis_reply.h"
#include "storage/redis_metadata.h"
#include "types/redis_string.h"

Status Parser::ParseFullDB() {
  rocksdb::DB *db_ = storage_->GetDB();
  if (!latest_snapshot_) latest_snapshot_ = std::make_unique<LatestSnapShot>(db_);
  rocksdb::ColumnFamilyHandle *metadata_cf_handle_ = storage_->GetCFHandle(Engine::kMetadataColumnFamilyName);

  rocksdb::ReadOptions read_options;
  read_options.snapshot = latest_snapshot_->GetSnapShot();
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

Status Parser::parseSimpleKV(const Slice &ns_key, const Slice &value, uint64_t expire) {
  std::string ns, user_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, slot_id_encoded_);

  auto command =
      Redis::Command2RESP({"SET", user_key, value.ToString().substr(Metadata::GetOffsetAfterExpire(value[0]))});
  Status s = writer_->Write(ns, {command});
  if (!s.IsOK()) return s;

  if (expire > 0) {
    command = Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(expire / 1000)});
    s = writer_->Write(ns, {command});
  }

  return s;
}

Status Parser::parseComplexKV(const Slice &ns_key, const Metadata &metadata) {
  RedisType type = metadata.Type();
  if (type < kRedisHash || type > kRedisSortedint) {
    return {Status::NotOK, "unknown metadata type: " + std::to_string(type)};
  }

  std::string ns, user_key;
  ExtractNamespaceKey(ns_key, &ns, &user_key, slot_id_encoded_);
  std::string prefix_key;
  InternalKey(ns_key, "", metadata.version, slot_id_encoded_).Encode(&prefix_key);
  std::string next_version_prefix_key;
  InternalKey(ns_key, "", metadata.version + 1, slot_id_encoded_).Encode(&next_version_prefix_key);

  rocksdb::ReadOptions read_options;
  read_options.snapshot = latest_snapshot_->GetSnapShot();
  rocksdb::Slice upper_bound(next_version_prefix_key);
  read_options.iterate_upper_bound = &upper_bound;
  storage_->SetReadOptions(read_options);

  std::string output;
  auto iter = DBUtil::UniqueIterator(storage_, read_options);
  for (iter->Seek(prefix_key); iter->Valid(); iter->Next()) {
    if (!iter->key().starts_with(prefix_key)) {
      break;
    }

    InternalKey ikey(iter->key(), slot_id_encoded_);
    std::string sub_key = ikey.GetSubKey().ToString();
    std::string value = iter->value().ToString();
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
        output = Redis::Command2RESP({"ZADD", user_key, Util::Float2String(score), sub_key});
        break;
      }
      case kRedisBitmap: {
        int index = std::stoi(sub_key);
        auto s = Parser::parseBitmapSegment(ns, user_key, index, value);
        if (!s.IsOK()) return s.Prefixed("failed to parse bitmap segment");
        break;
      }
      case kRedisSortedint: {
        std::string val = std::to_string(DecodeFixed64(ikey.GetSubKey().data()));
        output = Redis::Command2RESP({"ZADD", user_key, val, val});
        break;
      }
      default:
        break;  // should never get here
    }

    if (type != kRedisBitmap) {
      auto s = writer_->Write(ns, {output});
      if (!s.IsOK()) return s.Prefixed(fmt::format("failed to write the '{}' command to AOF", output));
    }
  }

  if (metadata.expire > 0) {
    output = Redis::Command2RESP({"EXPIREAT", user_key, std::to_string(metadata.expire / 1000)});
    Status s = writer_->Write(ns, {output});
    if (!s.IsOK()) return s.Prefixed("failed to write the EXPIREAT command to AOF");
  }

  return Status::OK();
}

Status Parser::parseBitmapSegment(const Slice &ns, const Slice &user_key, int index, const Slice &bitmap) {
  Status s;
  for (size_t i = 0; i < bitmap.size(); i++) {
    if (bitmap[i] == 0) continue;  // ignore zero byte

    for (int j = 0; j < 8; j++) {
      if (!(bitmap[i] & (1 << j))) continue;  // ignore zero bit

      s = writer_->Write(
          ns.ToString(),
          {Redis::Command2RESP({"SETBIT", user_key.ToString(), std::to_string(index * 8 + i * 8 + j), "1"})});
      if (!s.IsOK()) return s.Prefixed("failed to write SETBIT command to AOF");
    }
  }
  return Status::OK();
}

Status Parser::ParseWriteBatch(const std::string &batch_string) {
  rocksdb::WriteBatch write_batch(batch_string);
  WriteBatchExtractor write_batch_extractor(slot_id_encoded_, -1, true);

  auto db_status = write_batch.Iterate(&write_batch_extractor);
  if (!db_status.ok())
    return {Status::NotOK, fmt::format("failed to iterate over the write batch: {}", db_status.ToString())};

  auto resp_commands = write_batch_extractor.GetRESPCommands();
  for (const auto &iter : *resp_commands) {
    auto s = writer_->Write(iter.first, iter.second);
    if (!s.IsOK()) {
      LOG(ERROR) << "[kvrocks2redis] Failed to write to AOF from the write batch. Error: " << s.Msg();
    }
  }

  return Status::OK();
}
