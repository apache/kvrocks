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

package stream

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestXPending(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XPENDING bug reproduction", func(t *testing.T) {
		key := "smtp"
		group := "send"
		consumer := "test"

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     "1764415413212-0",
			Values: []string{"type", "bug_repro"},
		}).Err())

		streams, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{key, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)
		require.Len(t, streams, 1)
		require.Len(t, streams[0].Messages, 1)
		require.Equal(t, "1764415413212-0", streams[0].Messages[0].ID)

		cmd := rdb.Do(ctx, "XCLAIM", key, group, consumer, "0", "1764415413212-0", "IDLE", "33444572", "FORCE")
		require.NoError(t, cmd.Err())

		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key,
			Group:  group,
			Start:  "1764415413212-0",
			End:    "1764415413212-0",
			Count:  1,
		}).Result()
		require.NoError(t, err)

		require.Len(t, pending, 1)
		if len(pending) > 0 {
			require.Equal(t, "1764415413212-0", pending[0].ID)
		}
	})

	t.Run("XPENDING Summary Form", func(t *testing.T) {
		key := "xpending_summary"
		group := "group1"
		consumer := "consumer1"

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())

		// Add 3 messages
		id1, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}}).Result()
		id2, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}}).Result()
		id3, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}}).Result()

		// Read 2 messages
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{key, ">"},
			Count:    2,
		})

		// Check summary
		res, err := rdb.XPending(ctx, key, group).Result()
		require.NoError(t, err)
		require.Equal(t, int64(2), res.Count)
		require.Equal(t, id1, res.Lower)
		require.Equal(t, id2, res.Higher)
		require.Len(t, res.Consumers, 1)
		require.Equal(t, int64(2), res.Consumers[consumer])

		// Read 3rd message with another consumer
		consumer2 := "consumer2"
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer2,
			Streams:  []string{key, ">"},
			Count:    1,
		})

		res, err = rdb.XPending(ctx, key, group).Result()
		require.NoError(t, err)
		require.Equal(t, int64(3), res.Count)
		require.Equal(t, id1, res.Lower)
		require.Equal(t, id3, res.Higher)
		require.Len(t, res.Consumers, 2)
		require.Equal(t, int64(2), res.Consumers[consumer])
		require.Equal(t, int64(1), res.Consumers[consumer2])
	})

	t.Run("XPENDING Extended Form", func(t *testing.T) {
		key := "xpending_extended"
		group := "group1"
		consumer := "consumer1"

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())
		id1, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}}).Result()
		id2, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}}).Result()

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{key, ">"},
			Count:    2,
		})

		// Range all
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key,
			Group:  group,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 2)
		require.Equal(t, id1, pending[0].ID)
		require.Equal(t, id2, pending[1].ID)

		// Range with count limit
		pending, err = rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key,
			Group:  group,
			Start:  "-",
			End:    "+",
			Count:  1,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, id1, pending[0].ID)
	})

	t.Run("XPENDING Filter by Consumer", func(t *testing.T) {
		key := "xpending_consumer"
		group := "group1"
		c1 := "c1"
		c2 := "c2"

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}})
		rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}})

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: c1, Streams: []string{key, ">"}, Count: 1})
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: c2, Streams: []string{key, ">"}, Count: 1})

		// Filter c1
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   key,
			Group:    group,
			Consumer: c1,
			Start:    "-",
			End:      "+",
			Count:    10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, c1, pending[0].Consumer)

		// Filter c2
		pending, err = rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream:   key,
			Group:    group,
			Consumer: c2,
			Start:    "-",
			End:      "+",
			Count:    10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, c2, pending[0].Consumer)
	})

	t.Run("XPENDING Filter by Idle Time", func(t *testing.T) {
		key := "xpending_idle"
		group := "group1"
		consumer := "c1"

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())
		id, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: []interface{}{"k", "v"}}).Result()

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{key, ">"}, Count: 1})

		// Wait a bit
		time.Sleep(50 * time.Millisecond)

		// Filter with small idle time (should match)
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key,
			Group:  group,
			Idle:   10 * time.Millisecond,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, id, pending[0].ID)

		// Filter with large idle time (should not match)
		pending, err = rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: key,
			Group:  group,
			Idle:   10 * time.Second,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 0)
	})

	t.Run("XPENDING Exclusive Ranges", func(t *testing.T) {
		key := "xpending_exclusive"
		group := "group1"
		consumer := "c1"

		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())
		id1, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: "1-0", Values: []interface{}{"k", "v"}}).Result()
		id2, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: "2-0", Values: []interface{}{"k", "v"}}).Result()
		id3, _ := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: "3-0", Values: []interface{}{"k", "v"}}).Result()

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: consumer, Streams: []string{key, ">"}, Count: 3})

		// Helper to get IDs using Do for raw arguments
		getIDs := func(start, end string) []string {
			val, err := rdb.Do(ctx, "XPENDING", key, group, start, end, 10).Result()
			require.NoError(t, err)

			// Parse result
			resSlice, ok := val.([]interface{})
			require.True(t, ok)
			ids := make([]string, 0)
			for _, item := range resSlice {
				itemSlice := item.([]interface{})
				ids = append(ids, itemSlice[0].(string))
			}
			return ids
		}

		// (1-0 +
		ids := getIDs("("+id1, "+")
		require.Equal(t, []string{id2, id3}, ids)

		// - (3-0
		ids = getIDs("-", "("+id3)
		require.Equal(t, []string{id1, id2}, ids)

		// (1-0 (3-0
		ids = getIDs("("+id1, "("+id3)
		require.Equal(t, []string{id2}, ids)
	})
}
