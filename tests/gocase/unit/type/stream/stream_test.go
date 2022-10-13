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
	"fmt"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestStream(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XADD wrong number of args", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "XADD", "mystream").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "XADD", "mystream", "*").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "XADD", "mystream", "*", "field").Err(), "wrong number of arguments")
	})

	t.Run("XADD can add entries into a stream that XRANGE can fetch", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err())

		require.EqualValues(t, 2, rdb.XLen(ctx, "mystream").Val())

		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, map[string]interface{}{"item": "1", "value": "a"}, items[0].Values)
		require.EqualValues(t, map[string]interface{}{"item": "2", "value": "b"}, items[1].Values)
	})

	t.Run("XADD stores entry value with respect to case sensitivity", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "myStream", Values: []string{"iTeM", "1", "vAluE", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "myStream", Values: []string{"ItEm", "2", "VaLUe", "B"}}).Err())

		require.EqualValues(t, 2, rdb.XLen(ctx, "myStream").Val())

		items := rdb.XRange(ctx, "myStream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, map[string]interface{}{"iTeM": "1", "vAluE": "a"}, items[0].Values)
		require.EqualValues(t, map[string]interface{}{"ItEm": "2", "VaLUe": "B"}, items[1].Values)
	})

	t.Run("XADD IDs are incremental", func(t *testing.T) {
		x1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val()
		x2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Val()
		x3 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Val()
		require.Less(t, x1, x2)
		require.Less(t, x2, x3)
	})

	t.Run("XADD IDs are incremental when ms is the same as well", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "2", "value", "b"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "3", "value", "c"}}).Err())
		v := rdb.Do(ctx, "EXEC").Val().([]interface{})
		require.Len(t, v, 3)
		require.Less(t, v[0], v[1])
		require.Less(t, v[1], v[2])
	})

	t.Run("XADD IDs correctly report an error when overflowing", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "18446744073709551615-18446744073709551615", Values: []string{"a", "b"}}).Err())
		require.ErrorContains(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "*", Values: []string{"c", "d"}}).Err(), "ERR")
	})

	t.Run("XADD auto-generated sequence is incremented for last ID", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		x1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
		x2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-*", Values: []string{"item", "2", "value", "b"}}).Val()
		require.Equal(t, "123-457", x2)
		require.Less(t, x1, x2)
	})

	t.Run("XADD auto-generated sequence is zero for future timestamp ID", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		x1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Val()
		x2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "789-*", Values: []string{"item", "2", "value", "b"}}).Val()
		require.Equal(t, "789-0", x2)
		require.Less(t, x1, x2)
	})

	t.Run("XADD auto-generated sequence can't be smaller than last ID", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "123-456", Values: []string{"item", "1", "value", "a"}}).Err())
		require.ErrorContains(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "42-*", Values: []string{"item", "2", "value", "b"}}).Err(), "ERR")
	})

	t.Run("XADD auto-generated sequence can't overflow", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-18446744073709551615", Values: []string{"a", "b"}}).Err())
		require.ErrorContains(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-*", Values: []string{"c", "d"}}).Err(), "ERR")
	})

	t.Run("XADD 0-* should succeed", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		x := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "0-*", Values: []string{"a", "b"}}).Val()
		require.Equal(t, "0-1", x)
	})
}

func TestStreamOffset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XADD advances the entries-added counter and sets the recorded-first-entry-id", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "x",
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 1, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "x",
			ID:     "2-0",
			Values: []string{"data", "a"},
		}).Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 2, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)
	})

	t.Run("XDEL/TRIM are reflected by recorded first entry", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 0; i < 5; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i+1),
				Values: []string{"data", "a"},
			}).Err())
		}

		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 5, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "2-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "1-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "3-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XTrimMaxLen(ctx, "x", 2).Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "4-0", r.RecordedFirstEntryID)
	})

	t.Run("Maximum XDEL ID behaves correctly", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i+1),
				Values: []string{"data", fmt.Sprintf("%c", 'a'+i)},
			}).Err())
		}

		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "0-0", r.MaxDeletedEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "2-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "2-0", r.MaxDeletedEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "1-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "2-0", r.MaxDeletedEntryID)
	})
}
