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

package flushmemtable

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestFlushMemTableSync(t *testing.T) {
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()

	t.Run("flushmemtable synchronous", func(t *testing.T) {
		_, err := rdb.Do(ctx, "SET", "A", "B").Result()
		require.NoError(t, err)
		_, err = rdb.Do(ctx, "FLUSHMEMTABLE").Result()
		require.NoError(t, err)

		require.Condition(t, func() bool {
			dbDir := filepath.Join(configs["dir"], "db")
			matchedFiles, err := fs.Glob(os.DirFS(dbDir), "*.sst")
			require.NoError(t, err)
			return len(matchedFiles) > 0
		})
	})
}

func TestFlushMemTableAsync(t *testing.T) {
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()

	t.Run("flushmemtable asynchronous", func(t *testing.T) {
		_, err := rdb.Do(ctx, "SET", "A", "B").Result()
		require.NoError(t, err)
		_, err = rdb.Do(ctx, "FLUSHMEMTABLE", "ASYNC").Result()
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			dbDir := filepath.Join(configs["dir"], "db")
			matchedFiles, err := fs.Glob(os.DirFS(dbDir), "*.sst")
			require.NoError(t, err)
			return len(matchedFiles) > 0
		}, 5*time.Second, 100*time.Millisecond)
	})
}

func TestFlushMemTableInvalid(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()

	t.Run("invalid arguments", func(t *testing.T) {
		_, err := rdb.Do(ctx, "FLUSHMEMTABLE", "ASYNC", "ASYNC").Result()
		require.Contains(t, err.Error(), "wrong number of arguments")

		_, err = rdb.Do(ctx, "FLUSHMEMTABLE", "ASYN").Result()
		require.Contains(t, err.Error(), "parameter must be 'ASYNC'")
	})
}
