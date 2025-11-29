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

// TestXAckDelDocExample tests the exact scenario from xackdel.md
func TestXAckDelDocExample(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"rocksdb.compression": "no",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// Clean up
	require.NoError(t, rdb.Del(ctx, "mystream").Err())

	// XADD mystream * field1 value1
	id1, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]interface{}{"field1": "value1"},
	}).Result()
	require.NoError(t, err)
	t.Logf("ID1: %s", id1)

	// XADD mystream * field2 value2
	id2, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "mystream",
		Values: map[string]interface{}{"field2": "value2"},
	}).Result()
	require.NoError(t, err)
	t.Logf("ID2: %s", id2)

	// XGROUP CREATE mystream mygroup 0
	require.NoError(t, rdb.XGroupCreate(ctx, "mystream", "mygroup", "0").Err())

	// XREADGROUP GROUP mygroup consumer1 COUNT 2 STREAMS mystream >
	entries, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "mygroup",
		Consumer: "consumer1",
		Streams:  []string{"mystream", ">"},
		Count:    2,
	}).Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Len(t, entries[0].Messages, 2)
	t.Logf("Read %d entries", len(entries[0].Messages))

	// XPENDING mystream mygroup
	pending, err := rdb.XPending(ctx, "mystream", "mygroup").Result()
	require.NoError(t, err)
	t.Logf("Pending before XACKDEL: %d", pending.Count)
	require.Equal(t, int64(2), pending.Count)

	// XACKDEL mystream mygroup KEEPREF IDS 2 id1 id2
	res, err := rdb.Do(ctx, "XACKDEL", "mystream", "mygroup", "KEEPREF", "IDS", 2, id1, id2).Result()
	require.NoError(t, err)
	
	resSlice, ok := res.([]interface{})
	require.True(t, ok, "Expected array response")
	require.Len(t, resSlice, 2)
	
	acknowledged := resSlice[0].(int64)
	deleted := resSlice[1].(int64)
	
	t.Logf("XACKDEL returned: acknowledged=%d, deleted=%d", acknowledged, deleted)
	
	// According to the doc, this should be 1 and 1
	// But logically it should be 2 and 2
	// Let's see what we actually get
	t.Logf("Doc expects: acknowledged=1, deleted=1")
	t.Logf("Logic expects: acknowledged=2, deleted=2")
	t.Logf("Actual result: acknowledged=%d, deleted=%d", acknowledged, deleted)

	// XPENDING mystream mygroup - should be 0
	pendingAfter, err := rdb.XPending(ctx, "mystream", "mygroup").Result()
	require.NoError(t, err)
	t.Logf("Pending after XACKDEL: %d", pendingAfter.Count)
	require.Equal(t, int64(0), pendingAfter.Count)

	// XRANGE mystream - + should be empty
	rangeRes, err := rdb.XRange(ctx, "mystream", "-", "+").Result()
	require.NoError(t, err)
	t.Logf("Stream length after XACKDEL: %d", len(rangeRes))
	require.Equal(t, 0, len(rangeRes))
}
