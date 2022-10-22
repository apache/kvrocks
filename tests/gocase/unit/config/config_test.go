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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestRenameCommand(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rename-command": "KEYS KEYSNEW",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	err := rdb.Keys(ctx, "*").Err()
	require.ErrorContains(t, err, "unknown command")
	r := rdb.Do(ctx, "KEYSNEW", "*")
	require.Equal(t, []interface{}{}, r.Val())
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
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}
		return len(files) != 0
	}

	require.False(t, hasCompactionFiles(originBackupDir))

	r = rdb.Do(ctx, "bgsave")
	time.Sleep(2000 * time.Millisecond)

	require.True(t, hasCompactionFiles(originBackupDir))

	newBackupDir := filepath.Join(configs["dir"], "backup2")

	require.False(t, hasCompactionFiles(newBackupDir))

	r = rdb.Do(ctx, "CONFIG", "SET", "backup-dir", newBackupDir)

	r = rdb.Do(ctx, "CONFIG", "GET", "backup-dir")
	rList = r.Val().([]interface{})
	require.EqualValues(t, rList[0], "backup-dir")
	require.EqualValues(t, rList[1], newBackupDir)

	r = rdb.Do(ctx, "bgsave")
	time.Sleep(2000 * time.Millisecond)

	require.True(t, hasCompactionFiles(newBackupDir))
	require.True(t, hasCompactionFiles(originBackupDir))
}
