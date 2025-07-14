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

package kprofile

import (
	"context"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestKProfile(t *testing.T) {
	os.Setenv("MALLOC_CONF", "prof:true")
	defer func() {
		require.NoError(t, os.Unsetenv("MALLOC_CONF"))
	}()

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()
	info, err := rdb.InfoMap(ctx, "memory").Result()
	require.NoError(t, err)
	memoryAllocator, ok := info["Memory"]["mem_allocator"]
	require.True(t, ok, "mem_allocator should be present in memory info")
	isJemallocAllocator := strings.ToLower(memoryAllocator) == "jemalloc"

	t.Run("enable/disable memory profiling", func(t *testing.T) {
		for _, op := range []string{"DISABLE", "ENABLE"} {
			_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", op).Result()
			if !isJemallocAllocator {
				require.Contains(t, err.Error(), "memory profiling is not supported")
				return
			}
			require.NoError(t, err)
			status, err := rdb.Do(ctx, "KPROFILE", "MEMORY", "STATUS").Result()
			require.NoError(t, err)

			if op == "DISABLE" {
				require.EqualValues(t, "disabled", status)
			} else {
				require.EqualValues(t, "enabled", status)
			}
		}
	})

	t.Run("dump memory profiling file", func(t *testing.T) {
		_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", "DUMP", "/tmp/").Result()
		if !isJemallocAllocator {
			require.Contains(t, err.Error(), "memory profiling is not supported")
			return
		}
		require.NoError(t, err)

		_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", "DISABLE").Result()
		require.NoError(t, err)

		_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", "DUMP", "/tmp/").Result()
		require.Contains(t, err.Error(), "jemalloc profiling is not active, please enable it first")

		_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", "ENABLE").Result()
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			matchedFiles, err := fs.Glob(os.DirFS("/tmp"), "jeprof.*")
			require.NoError(t, err)
			return len(matchedFiles) > 0
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("wrong arguments", func(t *testing.T) {
		_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", "DUMP").Result()
		require.Contains(t, err.Error(), "wrong number of arguments")

		_, err = rdb.Do(ctx, "KPROFILE", "MEMORY", "ENABLE", "/tmp").Result()
		require.Contains(t, err.Error(), "wrong number of arguments")
	})
}
