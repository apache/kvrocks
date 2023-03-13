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

#pragma once

#include <rocksdb/table_properties.h>

#include <memory>
#include <string>
#include <utility>

class CompactOnExpiredCollector : public rocksdb::TablePropertiesCollector {
 public:
  explicit CompactOnExpiredCollector(std::string cf_name, float trigger_threshold)
      : cf_name_(std::move(cf_name)), trigger_threshold_(trigger_threshold) {}
  const char *Name() const override { return "compact_on_expired_collector"; }
  bool NeedCompact() const override;
  rocksdb::Status AddUserKey(const rocksdb::Slice &key, const rocksdb::Slice &value, rocksdb::EntryType,
                             rocksdb::SequenceNumber, uint64_t) override;
  rocksdb::Status Finish(rocksdb::UserCollectedProperties *properties) override;
  rocksdb::UserCollectedProperties GetReadableProperties() const override;

 private:
  std::string cf_name_;
  float trigger_threshold_;
  int64_t total_keys_ = 0;
  int64_t deleted_keys_ = 0;
  std::string start_key_;
  std::string stop_key_;
};

class CompactOnExpiredTableCollectorFactory : public rocksdb::TablePropertiesCollectorFactory {
 public:
  explicit CompactOnExpiredTableCollectorFactory(std::string cf_name, float trigger_threshold)
      : cf_name_(std::move(cf_name)), trigger_threshold_(trigger_threshold) {}
  ~CompactOnExpiredTableCollectorFactory() override = default;
  rocksdb::TablePropertiesCollector *CreateTablePropertiesCollector(
      rocksdb::TablePropertiesCollectorFactory::Context context) override;
  const char *Name() const override { return "CompactOnExpiredCollector"; }

 private:
  friend std::shared_ptr<CompactOnExpiredTableCollectorFactory> NewCompactOnExpiredTableCollectorFactory(
      const std::string &cf_name, float trigger_threshold);
  std::string cf_name_;
  float trigger_threshold_ = 0.3;
};

std::shared_ptr<CompactOnExpiredTableCollectorFactory> NewCompactOnExpiredTableCollectorFactory(
    const std::string &cf_name, float trigger_threshold);
