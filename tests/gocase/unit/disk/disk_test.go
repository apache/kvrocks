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

package disk

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDisk(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rocksdb.compression":       "no",
		"rocksdb.write_buffer_size": "1",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Disk usage String", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "stringkey", "aaaaaaaaaaaaaaaaaaaaaaaa", 0).Err())
		val, err := rdb.Do(ctx, "Disk", "usage", "stringkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, 1)
	})

	t.Run("Disk usage Set", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			require.NoError(t, rdb.SAdd(ctx, "setkey", i).Err())
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "setkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, 1)
	})

	t.Run("Disk usage List", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			require.NoError(t, rdb.LPush(ctx, "listkey", i).Err())
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "listkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, 1)
	})

	t.Run("Disk usage Zset", func(t *testing.T) {
		for i := 0; i < 1000; i++ {
			require.NoError(t, rdb.ZAdd(ctx, "zsetkey", redis.Z{Score: float64(i), Member: "x" + strconv.Itoa(i)}).Err())
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "zsetkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, 1)
	})

	t.Run("Disk usage Bitmap", func(t *testing.T) {
		for i := 0; i < 1024*8*1000; i += 1024 * 8 {
			require.NoError(t, rdb.SetBit(ctx, "bitmapkey", int64(i), 1).Err())
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "bitmapkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, 1)
	})

	t.Run("Disk usage Sortedint", func(t *testing.T) {
		for i := 0; i < 100000; i++ {
			require.NoError(t, rdb.Do(ctx, "siadd", "sortedintkey", i).Err())
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "sortedintkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, 1)
	})

	t.Run("Disk usage with typo ", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "Disk", "usa", "sortedintkey").Err(), "Unknown operation")
	})

	t.Run("Disk usage nonexistent key ", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "Disk", "usage", "nonexistentkey").Err(), "Not found")
	})

}
