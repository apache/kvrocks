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
#ifndef KVROCKS_TEST_BASE_H
#define KVROCKS_TEST_BASE_H

#include <gtest/gtest.h>

#include <filesystem>

#include "storage/redis_db.h"
#include "types/redis_hash.h"

class TestBase : public testing::Test {  // NOLINT
 protected:
  explicit TestBase() : config_(new Config()) {
    config_->db_dir = "testdb";
    config_->backup_dir = "testdb/backup";
    config_->RocksDB.compression = rocksdb::CompressionType::kNoCompression;
    config_->RocksDB.write_buffer_size = 1;
    config_->RocksDB.block_size = 100;
    storage_ = new Engine::Storage(config_);
    Status s = storage_->Open();
    if (!s.IsOK()) {
      std::cout << "Failed to open the storage, encounter error: " << s.Msg() << std::endl;
      assert(s.IsOK());
    }
  }
  ~TestBase() override {
    auto db_dir = config_->db_dir;
    delete storage_;
    delete config_;

    std::error_code ec;
    std::filesystem::remove_all(db_dir, ec);
    if (ec) {
      std::cout << "Encounter filesystem error: " << ec << std::endl;
    }
  }

  Engine::Storage *storage_;
  Config *config_ = nullptr;
  std::string key_;
  std::vector<Slice> fields_;
  std::vector<Slice> values_;
};
#endif  // KVROCKS_TEST_BASE_H
