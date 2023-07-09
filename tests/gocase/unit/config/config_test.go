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
 */

package config

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestRenameCommand(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rename-command KEYS": "KEYSNEW",
		"rename-command GET":  "GETNEW",
		"rename-command SET":  "SETNEW",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	require.ErrorContains(t, rdb.Keys(ctx, "*").Err(), "unknown command")
	require.ErrorContains(t, rdb.Get(ctx, "key").Err(), "unknown command")
	require.ErrorContains(t, rdb.Set(ctx, "key", "1", 0).Err(), "unknown command")
	require.Equal(t, []interface{}{}, rdb.Do(ctx, "KEYSNEW", "*").Val())
	require.NoError(t, rdb.Do(ctx, "SETNEW", "key", "1").Err())
	require.Equal(t, "1", rdb.Do(ctx, "GETNEW", "key").Val())
	val := []string{}
	for _, v := range rdb.Do(ctx, "config", "get", "rename-command").Val().([]interface{}) {
		val = append(val, v.(string))
	}
	sort.Strings(val)
	require.EqualValues(t, []string{"GET GETNEW", "KEYS KEYSNEW", "SET SETNEW", "rename-command", "rename-command", "rename-command"}, val)
}

func TestSetConfigBackupDir(t *testing.T) {
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	originBackupDir := filepath.Join(configs["dir"], "backup")

	r := rdb.Do(ctx, "CONFIG", "GET", "backup-dir")
	rList := r.Val().([]interface{})
	require.EqualValues(t, rList[0], "backup-dir")
	require.EqualValues(t, rList[1], originBackupDir)

	hasCompactionFiles := func(dir string) bool {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			return false
		}
		files, err := os.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}
		return len(files) != 0
	}

	require.False(t, hasCompactionFiles(originBackupDir))

	require.NoError(t, rdb.Do(ctx, "bgsave").Err())
	time.Sleep(2000 * time.Millisecond)

	require.True(t, hasCompactionFiles(originBackupDir))

	newBackupDir := filepath.Join(configs["dir"], "backup2")

	require.False(t, hasCompactionFiles(newBackupDir))

	require.NoError(t, rdb.Do(ctx, "CONFIG", "SET", "backup-dir", newBackupDir).Err())

	r = rdb.Do(ctx, "CONFIG", "GET", "backup-dir")
	rList = r.Val().([]interface{})
	require.EqualValues(t, rList[0], "backup-dir")
	require.EqualValues(t, rList[1], newBackupDir)

	require.NoError(t, rdb.Do(ctx, "bgsave").Err())
	time.Sleep(2000 * time.Millisecond)

	require.True(t, hasCompactionFiles(newBackupDir))
	require.True(t, hasCompactionFiles(originBackupDir))
}

func TestSetConfigCompression(t *testing.T) {
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	require.NoError(t, rdb.Do(ctx, "SET", "foo", "bar").Err())

	require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	r := rdb.Do(ctx, "CONFIG", "GET", "rocksdb.compression")
	rList := r.Val().([]interface{})
	require.EqualValues(t, rList[1], "no")

	require.NoError(t, rdb.Do(ctx, "CONFIG", "SET", "rocksdb.compression", "lz4").Err())
	require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	r = rdb.Do(ctx, "CONFIG", "GET", "rocksdb.compression")
	rList = r.Val().([]interface{})
	require.EqualValues(t, rList[1], "lz4")

	require.NoError(t, rdb.Do(ctx, "CONFIG", "SET", "rocksdb.compression", "zstd").Err())
	require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	r = rdb.Do(ctx, "CONFIG", "GET", "rocksdb.compression")
	rList = r.Val().([]interface{})
	require.EqualValues(t, rList[1], "zstd")
}
