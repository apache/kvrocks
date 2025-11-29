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

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)


func TestXAckDel(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rocksdb.compression": "no",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XACKDEL basic functionality (KEEPREF default)", func(t *testing.T) {
		key := "stream_keepref"
		group := "group1"
		require.NoError(t, rdb.Del(ctx, key).Err())

		id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)
		_, err = rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)

		require.NoError(t, rdb.XGroupCreate(ctx, key, group, "0").Err())
		
		// Read to put in PEL
		_, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: "c1",
			Streams:  []string{key, ">"},
			Count:    2,
		}).Result()
		require.NoError(t, err)

		// XACKDEL with default (KEEPREF)
		// Should return 1 acknowledged, 1 deleted
		res, err := rdb.Do(ctx, "XACKDEL", key, group, "IDS", 1, id1).Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(1), resSlice[0]) // acked and deleted

		// Verify entry is gone from stream
		len, err := rdb.XLen(ctx, key).Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), len)

		// Verify PEL still has it (KEEPREF)
		pending, err := rdb.XPending(ctx, key, group).Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), pending.Count) // id1 is acked so it's removed from PEL of group1? 
		// Wait, XACK removes from PEL. 
		// KEEPREF means "preserves existing references to these entries in all consumer groups' PEL".
		// But XACK *removes* from the *current* group's PEL.
		// So for the *current* group, it is acknowledged (removed from PEL).
		// KEEPREF applies to *other* groups or if we didn't ack?
		// "Acknowledges the entries in the specified consumer group and deletes the entries from the stream, but preserves existing references to these entries in all consumer groups' PEL"
		// If I ack in group1, it is removed from group1's PEL.
		// So KEEPREF means it is NOT removed from group2's PEL.
	})

	t.Run("XACKDEL KEEPREF with multiple groups", func(t *testing.T) {
		key := "stream_keepref_multi"
		group1 := "g1"
		group2 := "g2"
		require.NoError(t, rdb.Del(ctx, key).Err())

		id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)

		require.NoError(t, rdb.XGroupCreate(ctx, key, group1, "0").Err())
		require.NoError(t, rdb.XGroupCreate(ctx, key, group2, "0").Err())

		// Read in both groups
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group1, Consumer: "c1", Streams: []string{key, ">"}})
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group2, Consumer: "c2", Streams: []string{key, ">"}})

		// Verify both have PEL
		p1, _ := rdb.XPending(ctx, key, group1).Result()
		require.Equal(t, int64(1), p1.Count)
		p2, _ := rdb.XPending(ctx, key, group2).Result()
		require.Equal(t, int64(1), p2.Count)

		// XACKDEL in group1 with KEEPREF
		res, err := rdb.Do(ctx, "XACKDEL", key, group1, "KEEPREF", "IDS", 1, id1).Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(1), resSlice[0]) // acked and deleted

		// Group1 PEL should be empty (acked)
		p1, _ = rdb.XPending(ctx, key, group1).Result()
		require.Equal(t, int64(0), p1.Count)

		// Group2 PEL should still have it (KEEPREF)
		p2, _ = rdb.XPending(ctx, key, group2).Result()
		require.Equal(t, int64(1), p2.Count)
		
		// Stream should be empty
		len, _ := rdb.XLen(ctx, key).Result()
		require.Equal(t, int64(0), len)
	})

	t.Run("XACKDEL DELREF", func(t *testing.T) {
		key := "stream_delref"
		group1 := "g1"
		group2 := "g2"
		require.NoError(t, rdb.Del(ctx, key).Err())

		id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)

		require.NoError(t, rdb.XGroupCreate(ctx, key, group1, "0").Err())
		require.NoError(t, rdb.XGroupCreate(ctx, key, group2, "0").Err())

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group1, Consumer: "c1", Streams: []string{key, ">"}})
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group2, Consumer: "c2", Streams: []string{key, ">"}})

		// XACKDEL in group1 with DELREF
		res, err := rdb.Do(ctx, "XACKDEL", key, group1, "DELREF", "IDS", 1, id1).Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(1), resSlice[0]) // acked and deleted

		// Group1 PEL should be empty
		p1, _ := rdb.XPending(ctx, key, group1).Result()
		require.Equal(t, int64(0), p1.Count)

		// Group2 PEL should ALSO be empty (DELREF)
		p2, _ := rdb.XPending(ctx, key, group2).Result()
		require.Equal(t, int64(0), p2.Count)
	})

	t.Run("XACKDEL ACKED", func(t *testing.T) {
		key := "stream_acked"
		group1 := "g1"
		group2 := "g2"
		require.NoError(t, rdb.Del(ctx, key).Err())

		id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)

		require.NoError(t, rdb.XGroupCreate(ctx, key, group1, "0").Err())
		require.NoError(t, rdb.XGroupCreate(ctx, key, group2, "0").Err())

		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group1, Consumer: "c1", Streams: []string{key, ">"}})
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group2, Consumer: "c2", Streams: []string{key, ">"}})

		// XACKDEL in group1 with ACKED
		// Group2 has NOT acked yet. So it should NOT delete.
		res, err := rdb.Do(ctx, "XACKDEL", key, group1, "ACKED", "IDS", 1, id1).Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(2), resSlice[0]) // acked but NOT deleted (dangling)

		// Stream still has element
		len, _ := rdb.XLen(ctx, key).Result()
		require.Equal(t, int64(1), len)

		// Now ack in group2 (standard XACK)
		rdb.XAck(ctx, key, group2, id1)

		// Try XACKDEL in group1 again (already acked in group1, so ack count 0)
		// But wait, if it's not in PEL of group1, XACKDEL might not process it?
		// "XACKDEL ... acknowledges the specified entry IDs ... and simultaneously attempts to delete"
		// If it's not in PEL, XACK returns 0.
		// Does XACKDEL continue to delete if ack returns 0?
		// My implementation:
		// It iterates entry_ids.
		// Checks PEL. If found, ack++.
		// Then checks strategy.
		// If ACKED: checks if all groups acked.
		// If so, delete.
		// So yes, even if already acked in current group, it should proceed to check other groups and delete.
		
		// However, my implementation of `AckDelEntries` iterates `entry_ids`.
		// Inside the loop:
		// 1. Check PEL of current group. If found, delete from PEL, ack++.
		// 2. Check strategy.
		//    If ACKED: check all groups.
		//    If all acked -> should_delete = true.
		// 3. If should_delete -> delete from stream.
		
		// So yes, it should work.
		
		res, err = rdb.Do(ctx, "XACKDEL", key, group1, "ACKED", "IDS", 1, id1).Result()
		require.NoError(t, err)
		resSlice = res.([]interface{})
		require.Equal(t, int64(1), resSlice[0]) // deleted (already acked in group1, but deleted now)

		len, _ = rdb.XLen(ctx, key).Result()
		require.Equal(t, int64(0), len)
	})

	t.Run("XACKDEL with multiple IDs", func(t *testing.T) {
		key := "stream_multi_ids"
		group := "g1"
		require.NoError(t, rdb.Del(ctx, key).Err())

		// Add 5 entries
		var ids []string
		for i := 0; i < 5; i++ {
			id, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": i}}).Result()
			require.NoError(t, err)
			ids = append(ids, id)
		}

		require.NoError(t, rdb.XGroupCreate(ctx, key, group, "0").Err())
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c1", Streams: []string{key, ">"}, Count: 5})

		// XACKDEL first 3
		res, err := rdb.Do(ctx, "XACKDEL", key, group, "IDS", 3, ids[0], ids[1], ids[2]).Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(1), resSlice[0])
		require.Equal(t, int64(1), resSlice[1])
		require.Equal(t, int64(1), resSlice[2])

		len, _ := rdb.XLen(ctx, key).Result()
		require.Equal(t, int64(2), len)

		pending, _ := rdb.XPending(ctx, key, group).Result()
		require.Equal(t, int64(2), pending.Count)
	})

	t.Run("XACKDEL with non-existent IDs", func(t *testing.T) {
		key := "stream_nonexist"
		group := "g1"
		require.NoError(t, rdb.Del(ctx, key).Err())

		id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)

		require.NoError(t, rdb.XGroupCreate(ctx, key, group, "0").Err())
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c1", Streams: []string{key, ">"}})

		// Try to XACKDEL with mix of existing and non-existing IDs
		res, err := rdb.Do(ctx, "XACKDEL", key, group, "IDS", 2, id1, "99999-0").Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(1), resSlice[0]) // id1 acked/deleted
		require.Equal(t, int64(-1), resSlice[1]) // 99999-0 not found
	})

	t.Run("XACKDEL on empty stream", func(t *testing.T) {
		key := "stream_empty"
		group := "g1"
		require.NoError(t, rdb.Del(ctx, key).Err())

		// Create stream with group but no entries
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, key, group, "0").Err())

		// XACKDEL should succeed but do nothing
		res, err := rdb.Do(ctx, "XACKDEL", key, group, "IDS", 1, "1-0").Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(-1), resSlice[0])
	})

	t.Run("XACKDEL ACKED with no consumer groups", func(t *testing.T) {
		key := "stream_no_groups"
		require.NoError(t, rdb.Del(ctx, key).Err())

		id1, err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: map[string]interface{}{"f": "v"}}).Result()
		require.NoError(t, err)

		group := "g1"
		require.NoError(t, rdb.XGroupCreate(ctx, key, group, "0").Err())
		rdb.XReadGroup(ctx, &redis.XReadGroupArgs{Group: group, Consumer: "c1", Streams: []string{key, ">"}})

		// With only one group and ACKED strategy, it should delete since no other groups exist
		res, err := rdb.Do(ctx, "XACKDEL", key, group, "ACKED", "IDS", 1, id1).Result()
		require.NoError(t, err)
		resSlice := res.([]interface{})
		require.Equal(t, int64(1), resSlice[0])

		len, _ := rdb.XLen(ctx, key).Result()
		require.Equal(t, int64(0), len)
	})
}
