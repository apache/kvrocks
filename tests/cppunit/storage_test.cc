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
#include <storage/batch_decoder.h>
#include <storage/storage.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "test_base.h"

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

class WalGetTest : public TestBase {};

TEST_F(WalGetTest, ReturnsContainingBatch) {
  rocksdb::WriteBatch batch;
  batch.PutLogData("test-log");
  ASSERT_TRUE(batch.Put("first", "value1").ok());
  ASSERT_TRUE(batch.Put("second", "value2").ok());
  auto start = storage_->LatestSeqNumber() + 1;
  ASSERT_TRUE(storage_->Write(*ctx_, rocksdb::WriteOptions(), &batch).ok());
  std::vector<std::string> first, middle;
  ASSERT_TRUE(storage_->WalGet(start, true, &first).IsOK());
  ASSERT_TRUE(storage_->WalGet(start + 1, true, &middle).IsOK());
  EXPECT_EQ(first, middle);
  ASSERT_GE(first.size(), 5);
  EXPECT_EQ(first[0], "start_seq=" + std::to_string(start));
  EXPECT_EQ(first[1], "end_seq=" + std::to_string(start + 1));
  EXPECT_NE(first[2].find("test-log"), std::string::npos);
  EXPECT_NE(first[3].find("raw_value=value1"), std::string::npos);
  EXPECT_NE(first[4].find("raw_value=value2"), std::string::npos);
  EXPECT_FALSE(storage_->WalGet(0, true, &middle).IsOK());
  EXPECT_FALSE(storage_->WalGet(start + 2, true, &middle).IsOK());
}

TEST_F(WalGetTest, DetailsRespectKeyEncoding) {
  for (bool slots : {false, true}) {
    rocksdb::WriteBatch batch;
    auto key = ComposeNamespaceKey("ns", "key", slots);
    Metadata metadata(kRedisString, false);
    std::string value;
    metadata.Encode(&value);
    value.append("a\0b", 3);
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), key, value).ok());
    InternalKey internal(key, "field", 42, slots);
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), internal.Encode(), "data").ok());
    ASSERT_TRUE(batch.Delete(storage_->GetCFHandle(ColumnFamilyID::Metadata), key).ok());
    ASSERT_TRUE(batch.DeleteRange(storage_->GetCFHandle(ColumnFamilyID::Metadata), key, key + "z").ok());
    engine::WriteBatchDecoder decoder(true, slots);
    ASSERT_TRUE(batch.Iterate(&decoder).ok());
    const auto &entries = decoder.Get();
    ASSERT_EQ(entries.size(), 4);
    EXPECT_NE(entries[0].find("ns=ns"), std::string::npos);
    EXPECT_NE(entries[0].find("user_key=key"), std::string::npos);
    EXPECT_NE(entries[0].find(std::string("user_value=a\0b", 14)), std::string::npos);
    EXPECT_NE(entries[1].find("sub_key=field"), std::string::npos);
    EXPECT_NE(entries[1].find("version=42"), std::string::npos);
    EXPECT_NE(entries[2].find("type=DELETE"), std::string::npos);
    EXPECT_NE(entries[3].find("type=DELETERANGE"), std::string::npos);
    EXPECT_EQ(entries[0].find(",slot=") != std::string::npos, slots);
    engine::WriteBatchDecoder summary(false, slots);
    ASSERT_TRUE(batch.Iterate(&summary).ok());
    EXPECT_EQ(summary.Get()[0].find("user_value="), std::string::npos);
  }
}

TEST_F(WalGetTest, MalformedDataRemainsInspectable) {
  rocksdb::WriteBatch batch;
  batch.PutLogData("");
  ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Metadata), "", "").ok());
  ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), "x", "").ok());
  engine::WriteBatchDecoder decoder(true, true);
  ASSERT_TRUE(batch.Iterate(&decoder).ok());
  ASSERT_EQ(decoder.Get().size(), 3);
  EXPECT_NE(decoder.Get()[1].find("decode_error="), std::string::npos);
  EXPECT_NE(decoder.Get()[2].find("key_decode_error=1"), std::string::npos);
}

TEST_F(WalGetTest, BinaryKeyFieldsAreVisible) {
  for (bool slots : {false, true}) {
    const std::string ns("n\0", 2), user_key("k\0,\\", 4), subkey("\0\xff", 2);
    auto ns_key = ComposeNamespaceKey(ns, user_key, slots);
    auto key = InternalKey(ns_key, subkey, 0x0102030405060708ULL, slots).Encode();
    engine::WriteBatchDecoder decoder(true, slots);
    ASSERT_TRUE(decoder.DeleteCF(static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey), key).ok());
    const auto &entry = decoder.Get()[0];
    EXPECT_NE(entry.find(",ns_size=2,"), std::string::npos);
    EXPECT_NE(entry.find(",ns=n\\x00,ns_hex=6E00,"), std::string::npos);
    EXPECT_NE(entry.find(",user_key_size=4,"), std::string::npos);
    EXPECT_NE(entry.find(",user_key=k\\x00\\x2C\\x5C,user_key_hex=6B002C5C,"), std::string::npos);
    EXPECT_NE(entry.find(",version=72623859790382856,"), std::string::npos);
    EXPECT_NE(entry.find(",sub_key_size=2,"), std::string::npos);
    EXPECT_NE(entry.find(",sub_key=\\x00\\xFF,sub_key_hex=00FF"), std::string::npos);
    EXPECT_EQ(entry.find('\0'), std::string::npos);
    EXPECT_EQ(entry.find(",slot=") != std::string::npos, slots);
    if (slots) EXPECT_NE(entry.find(",slot=" + std::to_string(ExtractSlotId(ns_key)) + ","), std::string::npos);

    ASSERT_TRUE(decoder.DeleteRangeCF(static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey), key, key).ok());
    EXPECT_NE(decoder.Get()[1].find(",end_user_key_hex=6B002C5C,"), std::string::npos);
    EXPECT_NE(decoder.Get()[1].find(",end_version=72623859790382856,"), std::string::npos);

    engine::WriteBatchDecoder metadata(true, slots);
    ASSERT_TRUE(metadata.DeleteCF(static_cast<uint32_t>(ColumnFamilyID::Metadata), ns_key).ok());
    EXPECT_NE(metadata.Get()[0].find(",user_key_hex=6B002C5C"), std::string::npos);
    EXPECT_EQ(metadata.Get()[0].find(",version="), std::string::npos);

    // Every truncated header must remain safe and identify the incomplete field.
    for (size_t size = 0; size < key.size() - subkey.size(); ++size) {
      engine::WriteBatchDecoder truncated(true, slots);
      ASSERT_TRUE(truncated.DeleteCF(static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey), key.substr(0, size)).ok());
      EXPECT_NE(truncated.Get()[0].find("key_decode_error=1"), std::string::npos) << size;
      EXPECT_NE(truncated.Get()[0].find("key_error_field="), std::string::npos) << size;
    }
  }
}

TEST_F(WalGetTest, ZsetScoreKeyFields) {
  std::string subkey;
  PutDouble(&subkey, -1.25);
  subkey.append("m\0", 2);
  auto key = InternalKey(ComposeNamespaceKey("ns", "zset", false), subkey, 42, false).Encode();
  engine::WriteBatchDecoder decoder(true, false);
  ASSERT_TRUE(decoder.DeleteCF(static_cast<uint32_t>(ColumnFamilyID::SecondarySubkey), key).ok());
  EXPECT_NE(decoder.Get()[0].find(",score=-1.250000,"), std::string::npos);
  EXPECT_NE(decoder.Get()[0].find(",member_size=2,"), std::string::npos);
  EXPECT_NE(decoder.Get()[0].find(",member=m\\x00,member_hex=6D00"), std::string::npos);
}

TEST_F(WalGetTest, PropagateRecords) {
  auto cf = static_cast<uint32_t>(ColumnFamilyID::Propagate);
  const std::vector<std::pair<std::string, std::string>> records = {{"replication_id_", "repl-id"},
                                                                    {"lua_f_abc", "return 1"},
                                                                    {"lua_lib_code_lib", "#!lua name=lib\n"},
                                                                    {"lua_func_lib_func", "lib"},
                                                                    {"script", "*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n"},
                                                                    {"future_key", std::string("x\0", 2)}};
  const std::vector<std::string> kinds = {"replication_id",   "lua_script", "library_code",
                                          "function_library", "command",    "unknown"};
  engine::WriteBatchDecoder decoder(true, true);
  rocksdb::WriteBatch batch;
  for (const auto &[key, value] : records) {
    ASSERT_TRUE(batch.Put(storage_->GetCFHandle(ColumnFamilyID::Propagate), key, value).ok());
  }
  ASSERT_TRUE(batch.Iterate(&decoder).ok());
  const auto &entries = decoder.Get();
  ASSERT_EQ(entries.size(), records.size());
  for (size_t i = 0; i < records.size(); ++i) {
    EXPECT_NE(entries[i].find(",propagate_type=" + kinds[i]), std::string::npos);
    EXPECT_EQ(entries[i].find("key_decode_error"), std::string::npos);
    EXPECT_EQ(entries[i].find(",slot="), std::string::npos);
    engine::WriteBatchDecoder deletes(true, false);
    ASSERT_TRUE(deletes.DeleteCF(cf, records[i].first).ok());
    ASSERT_TRUE(deletes.SingleDeleteCF(cf, records[i].first).ok());
    ASSERT_TRUE(deletes.MergeCF(cf, records[i].first, "operand").ok());
    ASSERT_TRUE(deletes.DeleteRangeCF(cf, records[i].first, records[i].first).ok());
    for (const auto &entry : deletes.Get()) {
      EXPECT_NE(entry.find(",propagate_type=" + kinds[i]), std::string::npos);
    }
    EXPECT_NE(deletes.Get().back().find(",end_propagate_type=" + kinds[i]), std::string::npos);
  }
  EXPECT_NE(entries[0].find(",replication_id=repl-id,"), std::string::npos);
  EXPECT_NE(entries[1].find(",sha=abc,"), std::string::npos);
  EXPECT_NE(entries[1].find(",source=return 1,"), std::string::npos);
  EXPECT_NE(entries[2].find(",library_name=lib,"), std::string::npos);
  EXPECT_NE(entries[2].find(",source=#!lua name=lib\\x0A,"), std::string::npos);
  EXPECT_NE(entries[3].find(",function_name=func,"), std::string::npos);
  EXPECT_NE(entries[3].find(",library_name=lib,"), std::string::npos);
  EXPECT_NE(entries[4].find(",command_argc=2,"), std::string::npos);
  EXPECT_NE(entries[4].find(",command_arg_1=FLUSH,"), std::string::npos);
  EXPECT_NE(entries[5].find(",raw_value_hex=7800"), std::string::npos);
  engine::WriteBatchDecoder summary(false, true);
  ASSERT_TRUE(batch.Iterate(&summary).ok());
  EXPECT_EQ(summary.Get()[0].find("propagate_type="), std::string::npos);
}

TEST_F(WalGetTest, PropagateCommandBinaryAndMalformedValues) {
  auto cf = static_cast<uint32_t>(ColumnFamilyID::Propagate);
  engine::WriteBatchDecoder decoder(true, false);
  const std::string binary("a\0\r\n", 4);
  const std::string resp = "*2\r\n$4\r\n" + binary + "\r\n$0\r\n\r\n";
  ASSERT_TRUE(decoder.PutCF(cf, "script", resp).ok());
  EXPECT_NE(decoder.Get()[0].find(",command_arg_0=a\\x00\\x0D\\x0A,"), std::string::npos);
  EXPECT_NE(decoder.Get()[0].find(",command_arg_1=,command_arg_1_hex="), std::string::npos);
  const std::vector<std::string> malformed = {"",
                                              "*0\r\n",
                                              "*-1\r\n",
                                              "*1\r\n$-1\r\n",
                                              "*1\r\n$2\r\nx\r\n",
                                              "*1\r\n$1\r\nxXX",
                                              "*18446744073709551616\r\n",
                                              "*1\r\n$18446744073709551616\r\n",
                                              "*1\r\n+OK\r\n",
                                              resp + "trailing"};
  for (const auto &value : malformed) {
    engine::WriteBatchDecoder invalid(true, false);
    ASSERT_TRUE(invalid.PutCF(cf, "script", value).ok());
    EXPECT_NE(invalid.Get()[0].find("propagate_value_decode_error="), std::string::npos);
    EXPECT_NE(invalid.Get()[0].find("raw_value_hex="), std::string::npos);
  }
}
