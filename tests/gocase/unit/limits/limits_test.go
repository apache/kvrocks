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

package limits

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNetworkLimits(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"maxclients": "10",
	})
	defer srv.Close()

	t.Run("check if maxclients works refusing connections", func(t *testing.T) {
		var clean []func()
		defer func() {
			for _, f := range clean {
				f()
			}
		}()

		for i := 0; i < 50; i++ {
			c := srv.NewTCPClient()
			clean = append(clean, func() { require.NoError(t, c.Close()) })
			require.NoError(t, c.WriteArgs("PING"))
			r, err := c.ReadLine()
			require.NoError(t, err)
			if strings.Contains(r, "ERR") {
				require.Regexp(t, ".*ERR max.*reached.*", r)
				require.Contains(t, []int{9, 10}, i)
				return
			}
			require.Equal(t, "+PONG", r)
		}

		require.Fail(t, "maxclients doesn't work refusing connections")
	})
}

func TestWriteBatchLimit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("check if rocksdb.write_options.write_batch_max_bytes works", func(t *testing.T) {
		ctx := context.Background()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()

		memberScores := []redis.Z{{Member: "kvrocks1", Score: 1}, {Member: "kvrocks2", Score: 2}, {Member: "kvrocks3", Score: 3}}
		key := "test_zset_key"

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.ZAdd(ctx, key, memberScores...).Err())

		// set write_batch_max_bytes to 10 bytes
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.ConfigSet(ctx, "rocksdb.write_options.write_batch_max_bytes", "10").Err())
		require.EqualError(t, rdb.ZAdd(ctx, key, memberScores...).Err(), "ERR Operation aborted: Memory limit reached")
		require.NoError(t, rdb.ConfigSet(ctx, "rocksdb.write_options.write_batch_max_bytes", "0").Err())

		// set write_batch_max_bytes to 1GB
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.ConfigSet(ctx, "rocksdb.write_options.write_batch_max_bytes", "1073741824").Err())
		require.NoError(t, rdb.ZAdd(ctx, key, memberScores...).Err())
		require.NoError(t, rdb.ConfigSet(ctx, "rocksdb.write_options.write_batch_max_bytes", "0").Err())

		// reset write_batch_max_bytes
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.ConfigSet(ctx, "rocksdb.write_options.write_batch_max_bytes", "0").Err())
		require.NoError(t, rdb.ZAdd(ctx, key, memberScores...).Err())
	})
}

func getEvictedClients(rdb *redis.Client, ctx context.Context) (int, error) {
	info, err := rdb.Info(ctx, "stats").Result()
	if err != nil {
		return 0, err
	}

	lines := strings.Split(info, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "evicted_clients:") {
			parts := strings.Split(line, ":")
			if len(parts) == 2 {
				return strconv.Atoi(strings.TrimSpace(parts[1]))
			}
		}
	}
	return 0, fmt.Errorf("evicted_clients not found")
}

func TestMaxMemoryClientsLimits(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("check if maxmemory-clients works well", func(t *testing.T) {
		var clean []func()
		defer func() {
			for _, f := range clean {
				f()
			}
		}()

		ctx := context.Background()
		rdb := srv.NewClient()
		defer rdb.Close()

		elem := strings.Repeat("a", 10240)
		for i := 0; i < 1024; i++ {
			require.NoError(t, rdb.RPush(ctx, "test_max_memory_clients", elem).Err())
		}

		require.NoError(t, rdb.ConfigSet(ctx, "maxmemory-clients", "10M").Err())
		require.NoError(t, rdb.LRange(ctx, "test_max_memory_clients", 0, -1).Err())
		time.Sleep(25 * time.Second)

		require.NoError(t, rdb.ConfigSet(ctx, "maxmemory-clients", "0").Err())
		r, err := getEvictedClients(rdb, ctx)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}
