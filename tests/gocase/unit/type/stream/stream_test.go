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
	"math/rand"
	"strconv"
	"strings"
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

	t.Run("XADD with MAXLEN option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"xitem", strconv.Itoa(i)}}).Err())
			} else {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 5, Values: []string{"yitem", strconv.Itoa(i)}}).Err())
			}
		}
		require.EqualValues(t, 5, rdb.XLen(ctx, "mystream").Val())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		expected := 995
		for _, item := range items {
			require.Subset(t, map[string]interface{}{"xitem": strconv.Itoa(expected), "yitem": strconv.Itoa(expected)}, item.Values)
			expected++
		}
	})

	t.Run("XADD with MAXLEN option and the '=' argument", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.Do(ctx, "XADD", "mystream", "MAXLEN", "=", "5", "*", "xitem", "i").Err())
			} else {
				require.NoError(t, rdb.Do(ctx, "XADD", "mystream", "MAXLEN", "=", "5", "*", "yitem", "i").Err())
			}
		}
		require.EqualValues(t, 5, rdb.XLen(ctx, "mystream").Val())
	})

	t.Run("XADD with NOMKSTREAM option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.Empty(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", NoMkStream: true, Values: []string{"item", "1", "value", "a"}}).Val())
		require.Zero(t, rdb.Exists(ctx, "mystream").Val())
		require.NotEmpty(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: []string{"item", "1", "value", "a"}}).Val())
		require.NotEmpty(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", NoMkStream: true, Values: []string{"item", "2", "value", "b"}}).Val())
		require.EqualValues(t, 2, rdb.XLen(ctx, "mystream").Val())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, map[string]interface{}{"item": "1", "value": "a"}, items[0].Values)
		require.EqualValues(t, map[string]interface{}{"item": "2", "value": "b"}, items[1].Values)
	})

	t.Run("XADD with MINID option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		buildXAddArgs := func(id int, tag string) *redis.XAddArgs {
			c := id - 5
			if c < 0 {
				c = 1000
			}
			return &redis.XAddArgs{Stream: "mystream", MinID: strconv.Itoa(c), ID: strconv.Itoa(id), Values: []string{"xitem", strconv.Itoa(id)}}
		}
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.XAdd(ctx, buildXAddArgs(i+1, "xitem")).Err())
			} else {
				require.NoError(t, rdb.XAdd(ctx, buildXAddArgs(i+1, "yitem")).Err())
			}
		}
		require.EqualValues(t, 6, rdb.XLen(ctx, "mystream").Val())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		expected := 995
		for _, item := range items {
			require.Subset(t, map[string]interface{}{"xitem": strconv.Itoa(expected), "yitem": strconv.Itoa(expected)}, item.Values)
			expected++
		}
	})

	t.Run("XTRIM with MINID option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "2-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "3-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "4-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "5-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XTrimMinID(ctx, "mystream", "3-0").Err())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 3)
		require.EqualValues(t, "3-0", items[0].ID)
		require.EqualValues(t, "4-0", items[1].ID)
		require.EqualValues(t, "5-0", items[2].ID)
	})

	t.Run("XTRIM with MINID option, big delta from master record", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1641544570597-0", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "1641544570597-1", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XTrimMinID(ctx, "mystream", "1641544570597-0").Err())
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, "1641544570597-0", items[0].ID)
		require.EqualValues(t, "1641544570597-1", items[1].ID)
	})

	t.Run("XADD mass insertion and XLEN", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		insertIntoStreamKey(t, rdb, "mystream")
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		require.Len(t, items, 10000)
		for i := 0; i < 10000; i++ {
			require.Subset(t, items[i].Values, map[string]interface{}{"item": strconv.Itoa(i)})
		}
	})

	t.Run("XADD with ID 0-0", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "otherstream").Err())
		require.Error(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "otherstream",
			ID:     "0-0",
			Values: []string{"k", "v"},
		}).Err())
		require.Zero(t, rdb.Exists(ctx, "otherstream").Val())
	})

	t.Run("XRANGE COUNT works as expected", func(t *testing.T) {
		require.Len(t, rdb.XRangeN(ctx, "mystream", "-", "+", 10).Val(), 10)
	})

	t.Run("XREVRANGE COUNT works as expected", func(t *testing.T) {
		require.Len(t, rdb.XRevRangeN(ctx, "mystream", "+", "-", 10).Val(), 10)
	})

	t.Run("XRANGE can be used to iterate the whole stream", func(t *testing.T) {
		lastID, c := "-", 0
		for {
			items := rdb.XRangeN(ctx, "mystream", lastID, "+", 100).Val()
			if len(items) == 0 {
				break
			}
			for _, item := range items {
				require.Subset(t, item.Values, map[string]interface{}{"item": strconv.Itoa(c)})
				c++
			}
			lastID = streamNextID(t, items[len(items)-1].ID)
		}
		require.Equal(t, 10000, c)
	})

	t.Run("XREVRANGE returns the reverse of XRANGE", func(t *testing.T) {
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		revItems := rdb.XRevRange(ctx, "mystream", "+", "-").Val()
		util.ReverseSlice(revItems)
		require.EqualValues(t, items, revItems)
	})

	t.Run("XRANGE exclusive ranges", func(t *testing.T) {
		ids := []string{"0-1", "0-18446744073709551615", "1-0", "42-0", "42-42", "18446744073709551615-18446744073709551614", "18446744073709551615-18446744073709551615"}
		total := len(ids)
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		// DEL returns "QUEUED" here, so we use Do to avoid ParseInt.
		require.NoError(t, rdb.Do(ctx, "DEL", "vipstream").Err())
		for _, id := range ids {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "vipstream",
				ID:     id,
				Values: []string{"foo", "bar"},
			}).Err())
		}
		require.NoError(t, rdb.Do(ctx, "EXEC").Err())
		require.Len(t, rdb.XRange(ctx, "vipstream", "-", "+").Val(), total)
		require.Len(t, rdb.XRange(ctx, "vipstream", "("+ids[0], "+").Val(), total-1)
		require.Len(t, rdb.XRange(ctx, "vipstream", "-", "("+ids[total-1]).Val(), total-1)
		require.Len(t, rdb.XRange(ctx, "vipstream", "(0-1", "(1-0").Val(), 1)
		require.Len(t, rdb.XRange(ctx, "vipstream", "(1-0", "(42-42").Val(), 1)
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "(-", "+").Err(), "ERR")
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "-", "(+").Err(), "ERR")
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "(18446744073709551615-18446744073709551615", "+").Err(), "ERR")
		require.ErrorContains(t, rdb.XRange(ctx, "vipstream", "-", "(0-0").Err(), "ERR")
	})

	t.Run("XREAD with non empty stream", func(t *testing.T) {
		r := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"mystream", "0-0"},
			Count:   1,
		}).Val()
		require.Len(t, r, 1)
		require.Equal(t, "mystream", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"item": "0"})
	})

	t.Run("Non blocking XREAD with empty streams", func(t *testing.T) {
		// go-redis blocks underneath; fallback to Do
		require.Empty(t, rdb.Do(ctx, "XREAD", "STREAMS", "s1", "s2", "0-0", "0-0").Val())
	})

	t.Run("XREAD with non empty second stream", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		insertIntoStreamKey(t, rdb, "mystream")
		r := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{"nostream", "mystream", "0-0", "0-0"},
			Count:   1,
		}).Val()
		require.Len(t, r, 1)
		require.Equal(t, "mystream", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"item": "0"})
	})
}

// streamNextID returns the ID immediately greater than the specified one.
//
// Note that this function does not care to handle 'seq' overflow since it's a 64 bit value.
func streamNextID(t *testing.T, id string) string {
	parts := strings.Split(id, "-")
	require.Len(t, parts, 2)
	ms, seq := parts[0], parts[1]
	seqN, err := strconv.Atoi(seq)
	require.NoError(t, err)
	return fmt.Sprintf("%s-%d", ms, seqN+1)
}

func insertIntoStreamKey(t *testing.T, rdb *redis.Client, key string) {
	ctx := context.Background()
	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	for i := 0; i < 10000; i++ {
		// From time to time insert a field with a different set
		// of fields in order to stress the stream compression code.
		if rand.Float64() < 0.9 {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{"item": i},
			}).Err())
		} else {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{"item": i, "otherfield": "foo"},
			}).Err())
		}
	}
	require.NoError(t, rdb.Do(ctx, "EXEC").Err())
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
