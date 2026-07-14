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
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

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

TEST(Storage, RocksDBDictionaryCompressionOptions) {
  const char *path = "test_storage_options.conf";
  unlink(path);

  std::ofstream output_file(path, std::ios::out);
  output_file << "rocksdb.compression_max_dict_bytes 16384\n";
  output_file << "rocksdb.compression_zstd_max_train_bytes 262144\n";
  output_file.close();

  Config config;
  ASSERT_TRUE(config.Load(CLIOptions(path)).IsOK());
  config.db_dir = "test_storage_options_dir";

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  ASSERT_TRUE(s.IsOK());

  const auto options = storage->GetDB()->GetOptions();
  EXPECT_EQ(options.compression_opts.max_dict_bytes, 16384U);
  EXPECT_EQ(options.compression_opts.zstd_max_train_bytes, 262144U);

  unlink(path);
}

TEST(Storage, ReplDataManagerRejectsUnsafeFilenames) {
  Config config;
  config.db_dir = "test_repl_file_validation_dir/db";
  config.checkpoint_dir = "test_repl_file_validation_dir/checkpoint";
  config.slot_id_encoded = false;

  std::error_code ec;
  std::filesystem::remove_all("test_repl_file_validation_dir", ec);
  ASSERT_FALSE(ec);

  engine::Storage storage(&config);
  auto base_dir = std::string("test_repl_file_validation_dir/sync_checkpoint");

  auto valid_file = engine::Storage::ReplDataManager::NewTmpFile(&storage, base_dir, "meta/1");
  ASSERT_TRUE(valid_file);
  ASSERT_TRUE(valid_file->Close().ok());
  ASSERT_TRUE(engine::Storage::ReplDataManager::SwapTmpFile(&storage, base_dir, "meta/1").IsOK());
  EXPECT_TRUE(std::filesystem::exists("test_repl_file_validation_dir/sync_checkpoint/meta/1"));

  const std::vector<std::string> unsafe_files = {
      "../escape", "a/../../escape", "/tmp/escape", "a//b",      "a/",
      ".",         "a/..",           "a\\b",        "C:/escape", std::string("bad\0name", 8),
  };
  for (const auto &file : unsafe_files) {
    EXPECT_FALSE(engine::Storage::ReplDataManager::ValidateReplFileName(file).IsOK()) << file;
    EXPECT_FALSE(engine::Storage::ReplDataManager::NewTmpFile(&storage, base_dir, file)) << file;
    EXPECT_FALSE(engine::Storage::ReplDataManager::SwapTmpFile(&storage, base_dir, file).IsOK()) << file;
    EXPECT_FALSE(engine::Storage::ReplDataManager::FileExists(&storage, base_dir, file, 0)) << file;
    uint64_t file_size = 0;
    EXPECT_LT(engine::Storage::ReplDataManager::OpenDataFile(&storage, file, &file_size), 0) << file;
  }

  EXPECT_FALSE(std::filesystem::exists("test_repl_file_validation_dir/escape.tmp"));
  EXPECT_FALSE(std::filesystem::exists("test_repl_file_validation_dir/escape"));

  std::filesystem::remove_all("test_repl_file_validation_dir", ec);
  ASSERT_FALSE(ec);
}

TEST(Storage, GetFullReplDataInfoRejectsEmptyCheckpoint) {
  std::error_code ec;

  const std::string test_dir = "test_empty_checkpoint";
  Config config;
  config.db_dir = test_dir + "/db";
  config.checkpoint_dir = test_dir + "/checkpoint";
  config.slot_id_encoded = false;

  std::filesystem::remove_all(test_dir, ec);
  ASSERT_FALSE(ec);
  std::filesystem::create_directory(test_dir, ec);
  ASSERT_FALSE(ec);

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  ASSERT_TRUE(s.IsOK()) << s.Msg();

  auto ctx = engine::Context(storage.get());
  rocksdb::WriteBatch batch;
  batch.Put("k", "v");
  ASSERT_TRUE(storage->Write(ctx, rocksdb::WriteOptions(), &batch).ok());

  std::string files;
  s = engine::Storage::ReplDataManager::GetFullReplDataInfo(storage.get(), &files);
  ASSERT_TRUE(s.IsOK()) << s.Msg();

  std::filesystem::remove_all(config.checkpoint_dir, ec);
  ASSERT_FALSE(ec);
  std::filesystem::create_directory(config.checkpoint_dir, ec);
  ASSERT_FALSE(ec);

  files.clear();
  s = engine::Storage::ReplDataManager::GetFullReplDataInfo(storage.get(), &files);
  EXPECT_FALSE(s.IsOK());
  EXPECT_TRUE(files.empty());

  std::filesystem::remove_all(test_dir, ec);
  ASSERT_FALSE(ec);
}

TEST(Storage, TryPurgeCheckpoint) {
  std::error_code ec;

  const std::string test_dir = "test_purge_checkpoint";
  Config config;
  config.db_dir = test_dir + "/db";
  config.checkpoint_dir = test_dir + "/checkpoint";
  config.slot_id_encoded = false;

  std::filesystem::remove_all(test_dir, ec);
  ASSERT_FALSE(ec);
  std::filesystem::create_directory(test_dir, ec);
  ASSERT_FALSE(ec);

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  ASSERT_TRUE(s.IsOK()) << s.Msg();

  auto ctx = engine::Context(storage.get());
  rocksdb::WriteBatch batch;
  batch.Put("k", "v");
  ASSERT_TRUE(storage->Write(ctx, rocksdb::WriteOptions(), &batch).ok());

  std::string files;
  s = engine::Storage::ReplDataManager::GetFullReplDataInfo(storage.get(), &files);
  ASSERT_TRUE(s.IsOK()) << s.Msg();

  storage->SetCheckpointAccessTimeSecs(storage->GetCheckpointAccessTimeSecs() - 60);

  s = storage->TryPurgeCheckpoint(/*fetch_file_threads=*/1);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  EXPECT_TRUE(storage->ExistCheckpoint());

  s = storage->TryPurgeCheckpoint(/*fetch_file_threads=*/0);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  EXPECT_FALSE(storage->ExistCheckpoint());
  EXPECT_FALSE(std::filesystem::exists(config.checkpoint_dir + ".trash"));
  EXPECT_EQ(storage->GetCheckpointCreateTimeSecs(), 0);
  EXPECT_EQ(storage->GetCheckpointAccessTimeSecs(), 0);

  files.clear();
  s = engine::Storage::ReplDataManager::GetFullReplDataInfo(storage.get(), &files);
  ASSERT_TRUE(s.IsOK()) << s.Msg();
  EXPECT_TRUE(storage->ExistCheckpoint());

  std::filesystem::remove_all(test_dir, ec);
  ASSERT_FALSE(ec);
}