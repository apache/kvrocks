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

package flushblockcache

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func getBlockCacheSize(rdb *redis.Client) (int64, error) {
	value := util.FindInfoEntry(rdb, "block_cache_usage", "rocksdb")
	return strconv.ParseInt(value, 10, 64)
}

func TestFlushBlockCache(t *testing.T) {
	configs := map[string]string{}
	srv := util.StartServer(t, configs)
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()

	t.Run("flushblockcache", func(t *testing.T) {
		_, err := rdb.Do(ctx, "SET", "A", "KVROCKS").Result()
		require.NoError(t, err)
		_, err = rdb.Do(ctx, "FLUSHMEMTABLE").Result()
		require.NoError(t, err)
		_, err = rdb.Do(ctx, "GET", "A").Result()
		require.NoError(t, err)
		initCacheSize, err := getBlockCacheSize(rdb)
		require.NoError(t, err)
		_, err = rdb.Do(ctx, "FLUSHBLOCKCACHE").Result()
		require.NoError(t, err)
		cacheSize, err := getBlockCacheSize(rdb)
		require.NoError(t, err)
		require.Less(t, cacheSize, initCacheSize)
		require.Equal(t, "KVROCKS", rdb.Do(ctx, "GET", "A").Val())
	})
}

func TestFlushBlockCacheInvalid(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()

	t.Run("invalid arguments", func(t *testing.T) {
		_, err := rdb.Do(ctx, "FLUSHBLOCKCACHE", "ARG").Result()
		require.Contains(t, err.Error(), "wrong number of arguments")
	})
}
