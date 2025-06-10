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

#include <config/config.h>
#include <gtest/gtest.h>
#include <status.h>
#include <storage/storage.h>

#include <filesystem>

TEST(Storage, CreateBackup) {
  std::error_code ec;

  Config config;
  config.db_dir = "test_backup_dir";
  config.slot_id_encoded = false;

  std::filesystem::remove_all(config.db_dir, ec);
  ASSERT_TRUE(!ec);

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  ASSERT_TRUE(s.IsOK());

  auto ctx = engine::Context(storage.get());

  constexpr int cnt = 10;
  for (int i = 0; i < cnt; i++) {
    rocksdb::WriteBatch batch;
    batch.Put("k", "v");
    ASSERT_TRUE(storage->Write(ctx, rocksdb::WriteOptions(), &batch).ok());
  }
  uint64_t sequence_number = 0;
  s = storage->CreateBackup(&sequence_number);
  ASSERT_TRUE(s.IsOK());
  ASSERT_EQ(cnt, sequence_number);
  // check if backup success without caring about the sequence number
  s = storage->CreateBackup();
  ASSERT_TRUE(s.IsOK());

  std::filesystem::remove_all(config.db_dir, ec);
  ASSERT_TRUE(!ec);
}

TEST(Storage, ReadOnlyTransactions) {
  std::error_code ec;

  Config config;
  config.db_dir = "test_backup_dir";
  config.slot_id_encoded = false;

  std::filesystem::remove_all(config.db_dir, ec);
  ASSERT_TRUE(!ec);

  // Populate a DB with some test data so opening the read-only snapshot succeeds
  {
    auto storage = std::make_unique<engine::Storage>(&config);
    auto s = storage->Open();
    ASSERT_TRUE(s.IsOK());

    auto ctx = engine::Context(storage.get());
    rocksdb::WriteBatch batch;
    batch.Put("k", "v");
    ASSERT_TRUE(storage->Write(ctx, rocksdb::WriteOptions(), &batch).ok());
  }

  // Now load that DB in in read-only mode and try to write to it
  {
    auto storage = std::make_unique<engine::Storage>(&config);
    auto s = storage->Open(DBOpenMode::kDBOpenModeForReadOnly);
    std::cout << s.Msg() << std::endl;
    ASSERT_TRUE(s.IsOK());

    auto ctx = engine::Context(storage.get());

    // An empty write batch should not cause any error, even if the storage is opened in
    // read-only mode
    rocksdb::WriteBatch readonly_batch;
    ASSERT_TRUE(storage->Write(ctx, rocksdb::WriteOptions(), &readonly_batch).ok());

    rocksdb::WriteBatch read_write_batch;
    read_write_batch.Put("k", "v");
    ASSERT_FALSE(storage->Write(ctx, rocksdb::WriteOptions(), &read_write_batch).ok());
  }

  std::filesystem::remove_all(config.db_dir, ec);
  ASSERT_TRUE(!ec);
}
