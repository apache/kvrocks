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

#include <__filesystem/operations.h>
#include <config/config.h>
#include <gtest/gtest.h>
#include <status.h>
#include <storage/storage.h>

TEST(Storage, CreateBackup) {
  std::error_code ec;

  Config config;
  config.db_dir = "test_backup_dir";
  config.slot_id_encoded = false;

  std::filesystem::remove_all(config.db_dir, ec);
  assert(!ec);

  auto storage = std::make_unique<engine::Storage>(&config);
  auto s = storage->Open();
  assert(s.IsOK());

  constexpr int cnt = 10;
  for (int i = 0; i < cnt; i++) {
    rocksdb::WriteBatch batch;
    batch.Put("k", "v");
    assert(storage->Write(rocksdb::WriteOptions(), &batch).ok());
  }
  uint64_t sequence_number = 0;
  s = storage->CreateBackup(&sequence_number);
  assert(s.IsOK());
  assert(cnt == sequence_number);
  // check if backup success without caring about the sequence number
  s = storage->CreateBackup();
  assert(s.IsOK());

  std::filesystem::remove_all(config.db_dir, ec);
  assert(!ec);
}
