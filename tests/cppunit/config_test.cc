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

#include "config.h"
#include "server.h"
#include <map>
#include <vector>
#include <fstream>
#include <iostream>
#include <gtest/gtest.h>
#include <config_util.h>

TEST(Config, GetAndSet) {
  const char *path = "test.conf";
  Config config;

  config.Load(path);
  // Config.Set need accessing commands, so we should init and populate
  // the command table here.
  Redis::InitCommandsTable();
  Redis::PopulateCommands();
  std::map<std::string, std::string> mutable_cases = {
      {"timeout" , "1000"},
      {"maxclients" , "2000"},
      {"max-backup-to-keep" , "1"},
      {"max-backup-keep-hours" , "4000"},
      {"requirepass" , "mytest_requirepass"},
      {"masterauth" , "mytest_masterauth"},
      {"compact-cron" , "1 2 3 4 5"},
      {"bgsave-cron" , "5 4 3 2 1"},
      {"max-io-mb" , "5000"},
      {"max-db-size" , "6000"},
      {"max-replication-mb" , "7000"},
      {"slave-serve-stale-data" , "no"},
      {"slave-read-only" , "no"},
      {"slave-priority" , "101"},
      {"slowlog-log-slower-than" , "1234"},
      {"slowlog-max-len" , "123"},
      {"profiling-sample-ratio" , "50"},
      {"profiling-sample-record-max-len" , "1"},
      {"profiling-sample-record-threshold-ms" , "50"},
      {"profiling-sample-commands" , "get,set"},

      {"rocksdb.compression" , "no"},
      {"rocksdb.max_open_files" , "1234"},
      {"rocksdb.write_buffer_size" , "1234"},
      {"rocksdb.max_write_buffer_number" , "1"},
      {"rocksdb.target_file_size_base", "100"},
      {"rocksdb.max_background_compactions" , "2"},
      {"rocksdb.max_sub_compactions" , "3"},
      {"rocksdb.delayed_write_rate" , "1234"},
      {"rocksdb.stats_dump_period_sec" , "600"},
      {"rocksdb.compaction_readahead_size" , "1024"},
      {"rocksdb.level0_slowdown_writes_trigger" , "50"},
      {"rocksdb.level0_stop_writes_trigger", "100"},
      {"rocksdb.enable_blob_files", "no"},
      {"rocksdb.min_blob_size", "4096"},
      {"rocksdb.blob_file_size", "268435456"},
      {"rocksdb.enable_blob_garbage_collection", "yes"},
      {"rocksdb.blob_garbage_collection_age_cutoff", "25"},
      {"rocksdb.max_bytes_for_level_base", "268435456"},
      {"rocksdb.max_bytes_for_level_multiplier", "10"},
      {"rocksdb.level_compaction_dynamic_level_bytes", "yes"},
  };
  std::vector<std::string> values;
  for (const auto &iter : mutable_cases) {
    auto s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_TRUE(s.IsOK());
    config.Get(iter.first, &values);
    ASSERT_TRUE(s.IsOK());
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], iter.first);
    EXPECT_EQ(values[1], iter.second);
  }
  ASSERT_TRUE(config.Rewrite().IsOK());
  config.Load(path);
  for (const auto &iter : mutable_cases) {
    auto s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_TRUE(s.IsOK());
    config.Get(iter.first, &values);
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values[0], iter.first);
    EXPECT_EQ(values[1], iter.second);
  }
  unlink(path);

  std::map<std::string, std::string> immutable_cases = {
      {"daemonize", "yes"},
      {"bind", "0.0.0.0"},
      {"repl-bind", "0.0.0.0"},
      {"workers", "8"},
      {"repl-workers", "8"},
      {"tcp-backlog", "500"},
      {"slaveof", "no one"},
      {"db-name", "test_dbname"},
      {"dir", "test_dir"},
      {"backup-dir", "test_dir/backup"},
      {"pidfile", "test.pid"},
      {"supervised", "no"},
      {"rocksdb.block_size", "1234"},
      {"rocksdb.max_background_flushes", "16"},
      {"rocksdb.wal_ttl_seconds", "10000"},
      {"rocksdb.wal_size_limit_mb", "16"},
      {"rocksdb.enable_pipelined_write", "no"},
      {"rocksdb.cache_index_and_filter_blocks", "no"},
      {"rocksdb.metadata_block_cache_size", "100"},
      {"rocksdb.subkey_block_cache_size", "100"},
      {"rocksdb.row_cache_size", "100"},
  };
  for (const auto &iter : immutable_cases) {
    auto s = config.Set(nullptr, iter.first, iter.second);
    ASSERT_FALSE(s.IsOK());
  }
}

TEST(Config, Rewrite) {
  const char *path = "test.conf";
  unlink(path);

  std::ostringstream string_stream;
  string_stream << "rename-command KEYS KEYS_NEW" << "\n";
  string_stream << "rename-command GET GET_NEW" << "\n";
  string_stream << "rename-command SET SET_NEW" << "\n";
  std::ofstream output_file(path, std::ios::out);
  output_file.write(string_stream.str().c_str(), string_stream.str().size());
  output_file.close();

  Config config;
  Redis::PopulateCommands();
  ASSERT_TRUE(config.Load(path).IsOK());
  ASSERT_TRUE(config.Rewrite().IsOK());
  // Need to re-populate the command table since it has renamed by the previous
  Redis::PopulateCommands();
  ASSERT_TRUE(config.Load(path).IsOK());
  unlink(path);
}

TEST(Namespace, Add) {
  const char *path = "test.conf";
  unlink(path);

  Config config;
  config.Load(path) ;
  config.slot_id_encoded = false;
  EXPECT_TRUE(!config.AddNamespace("ns", "t0").IsOK());
  config.requirepass = "foobared";

  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    auto s = config.AddNamespace(namespaces[i], tokens[i]);
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "the token has already exists");
  }
  auto s = config.AddNamespace("n1", "t0");
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), "the namespace has already exists");

  s = config.AddNamespace(kDefaultNamespace, "mytoken");
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), "forbidden to add the default namespace");
  unlink(path);
}

TEST(Namespace, Set) {
  const char *path = "test.conf";
  unlink(path);

  Config config;
  config.Load(path);
  config.slot_id_encoded = false;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  std::vector<std::string> new_tokens = {"nt1", "nt2'", "nt3", "nt4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    auto s = config.SetNamespace(namespaces[i], tokens[i]);
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "the namespace was not found");
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.SetNamespace(namespaces[i], new_tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, new_tokens[i]);
  }
  unlink(path);
}

TEST(Namespace, Delete) {
  const char *path = "test.conf";
  unlink(path);

  Config config;
  config.Load(path);
  config.slot_id_encoded = false;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for (const auto &ns : namespaces) {
    config.DelNamespace(ns);
    std::string token;
    config.GetNamespace(ns, &token);
    EXPECT_TRUE(token.empty());
  }
  unlink(path);
}

TEST(Namespace, RewriteNamespaces) {
  const char *path = "test.conf";
  unlink(path);
  Config config;
  config.Load(path);
  config.requirepass = "test";
  config.backup_dir = "test";
  config.slot_id_encoded = false;
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(size_t i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  EXPECT_TRUE(config.AddNamespace("to-be-deleted-ns", "to-be-deleted-token").IsOK());
  EXPECT_TRUE(config.DelNamespace("to-be-deleted-ns").IsOK());

  Config new_config;
  auto s = new_config.Load(path) ;
  for(size_t i = 0; i < namespaces.size(); i++) {
    std::string token;
    new_config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }

  std::string token;
  EXPECT_FALSE(new_config.GetNamespace("to-be-deleted-ns", &token).IsOK());
  unlink(path);
}

TEST(Config, ParseConfigLine) {
  ASSERT_EQ(*ParseConfigLine(""), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("# hello"), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("       #x y z "), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("key value  "), (ConfigKV{"key", "value"}));
  ASSERT_EQ(*ParseConfigLine("key value#x"), (ConfigKV{"key", "value"}));
  ASSERT_EQ(*ParseConfigLine("key"), (ConfigKV{"key", ""}));
  ASSERT_EQ(*ParseConfigLine("    key    value1   value2   "), (ConfigKV{"key", "value1   value2"}));
  ASSERT_EQ(*ParseConfigLine(" #"), ConfigKV{});
  ASSERT_EQ(*ParseConfigLine("  key val ue #h e l l o"), (ConfigKV{"key", "val ue"}));
  ASSERT_EQ(*ParseConfigLine("key 'val ue'"), (ConfigKV{"key", "val ue"}));
  ASSERT_EQ(*ParseConfigLine(R"(key ' value\'\'v a l ')"), (ConfigKV{"key", " value''v a l "}));
  ASSERT_EQ(*ParseConfigLine(R"( key "val # hi" # hello!)"), (ConfigKV{"key", "val # hi"}));
  ASSERT_EQ(*ParseConfigLine(R"(key "\n \r \t ")"), (ConfigKV{"key", "\n \r \t "}));
  ASSERT_EQ(*ParseConfigLine("key ''"), (ConfigKV{"key", ""}));
  ASSERT_FALSE(ParseConfigLine("key \"hello "));
  ASSERT_FALSE(ParseConfigLine("key \'\\"));
  ASSERT_FALSE(ParseConfigLine("key \"hello'"));
  ASSERT_FALSE(ParseConfigLine("key \""));
  ASSERT_FALSE(ParseConfigLine("key '' ''"));
  ASSERT_FALSE(ParseConfigLine("key '' x"));
}

TEST(Config, DumpConfigLine) {
  ASSERT_EQ(DumpConfigLine({"key", "value"}), "key value");
  ASSERT_EQ(DumpConfigLine({"key", " v a l "}), R"(key " v a l ")");
  ASSERT_EQ(DumpConfigLine({"a", "'b"}), "a \"\\'b\"");
  ASSERT_EQ(DumpConfigLine({"a", "x#y"}), "a \"x#y\"");
  ASSERT_EQ(DumpConfigLine({"a", "x y"}), "a \"x y\"");
  ASSERT_EQ(DumpConfigLine({"a", "xy"}), "a xy");
  ASSERT_EQ(DumpConfigLine({"a", "x\n"}), "a \"x\\n\"");
}
