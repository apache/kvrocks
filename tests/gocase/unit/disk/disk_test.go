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
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDisk(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rocksdb.compression":       "no",
		"rocksdb.write_buffer_size": "1",
		"rocksdb.block_size":        "100",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	estimationFactor := 0.1
	t.Run("Disk usage String", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "stringkey", strings.Repeat("A", 1024), 0).Err())
		val, err := rdb.Do(ctx, "Disk", "usage", "stringkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(estimationFactor*1024))
		require.LessOrEqual(t, val, int(1024/estimationFactor))
	})

	t.Run("Disk usage Hash", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 10000; i++ {
			require.NoError(t, rdb.HSet(ctx, "hashkey", "key"+strconv.Itoa(i), "value"+strconv.Itoa(i)).Err())
			approximateSize += len("hashkey") + len("key"+strconv.Itoa(i)) + len("value"+strconv.Itoa(i)) + 8
		}
		require.NoError(t, rdb.Set(ctx, "hashkey", strings.Repeat("A", 1024), 0).Err())
		val, err := rdb.Do(ctx, "Disk", "usage", "hashkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage Set", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 10000; i++ {
			require.NoError(t, rdb.SAdd(ctx, "setkey", i).Err())
			approximateSize += len(strconv.Itoa(i)) + len("setkey") + 8
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "setkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage List", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 10000; i++ {
			require.NoError(t, rdb.LPush(ctx, "listkey", i).Err())
			approximateSize += len("listkey") + 8 + 8 + len(strconv.Itoa(i))
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "listkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage Zset", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 10000; i++ {
			require.NoError(t, rdb.ZAdd(ctx, "zsetkey", redis.Z{Score: float64(i), Member: "x" + strconv.Itoa(i)}).Err())
			approximateSize += (len("zsetkey") + 8 + len("x"+strconv.Itoa(i))) * 2
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "zsetkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage Bitmap", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 1024*8*100000; i += 1024 * 8 {
			require.NoError(t, rdb.SetBit(ctx, "bitmapkey", int64(i), 1).Err())
			approximateSize += len("bitmapkey") + 8 + len(strconv.Itoa(i/1024*8))
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "bitmapkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage Sortedint", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 100000; i++ {
			require.NoError(t, rdb.Do(ctx, "siadd", "sortedintkey", i).Err())
			approximateSize += len("sortedintkey") + 8 + 8
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "sortedintkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage Stream", func(t *testing.T) {
		approximateSize := 0
		for i := 0; i < 100000; i++ {
			require.NoError(t, rdb.Do(ctx, "xadd", "streamkey", "*", "key"+strconv.Itoa(i), "value"+strconv.Itoa(i)).Err())
			approximateSize += len("streamkey") + 8 + 8 + len("value"+strconv.Itoa(i)) + len("value"+strconv.Itoa(i))
		}
		val, err := rdb.Do(ctx, "Disk", "usage", "streamkey").Int()
		require.NoError(t, err)
		require.GreaterOrEqual(t, val, int(float64(approximateSize)*estimationFactor))
		require.LessOrEqual(t, val, int(float64(approximateSize)/estimationFactor))
	})

	t.Run("Disk usage with typo ", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "Disk", "usa", "sortedintkey").Err(), "Unknown operation")
	})

	t.Run("Disk usage nonexistent key ", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "Disk", "usage", "nonexistentkey").Err(), "Not found")
	})

	t.Run("Memory usage existing key - check that Kvrocks support it", func(t *testing.T) {
		key := "arbitrary-key"
		require.NoError(t, rdb.Del(ctx, key).Err())

		require.NoError(t, rdb.Set(ctx, key, "some-arbitrary-value-with-non-zero-length",
			time.Duration(0)).Err())

		size, err := rdb.MemoryUsage(ctx, key).Result()
		require.NoError(t, err)
		require.Greater(t, size, int64(0))

		_, err = rdb.MemoryUsage(ctx, "nonexistentkey").Result()
		require.ErrorIs(t, err, redis.Nil)
	})
}
