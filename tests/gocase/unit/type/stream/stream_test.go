//go:build !ignore_when_tsan

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
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestStreamWithRESP2(t *testing.T) {
	streamTests(t, "no")
}

func TestStreamWithRESP3(t *testing.T) {
	streamTests(t, "yes")
}

var streamTests = func(t *testing.T, enabledRESP3 string) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": enabledRESP3,
	})
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
		require.Len(t, items, 1000)
		for i := 0; i < 1000; i++ {
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
		require.Equal(t, 1000, c)
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

	t.Run("Blocking XREAD waiting new data", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"old", "abcd1234"}}).Err())
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s1", "s2", "s3", "$", "$", "$"}, Block: 20 * time.Second}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt > 0
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"new", "abcd1234"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "s2", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"new": "abcd1234"})
	})

	t.Run("Blocking XREAD waiting old data", func(t *testing.T) {
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s1", "s2", "s3", "$", "0-0", "$"}, Block: 20 * time.Second}).Val()
		}()
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"foo", "abcd1234"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "s2", r[0].Stream)
		require.GreaterOrEqual(t, len(r[0].Messages), 2)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"old": "abcd1234"})
	})

	t.Run("Blocking XREAD will not reply with an empty array", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "s1").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s1", ID: "666", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s1", ID: "667", Values: []string{"f2", "v2"}}).Err())
		require.NoError(t, rdb.XDel(ctx, "s1", "667").Err())
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("XREAD", "BLOCK", "10", "STREAMS", "s1", "666"))
		time.Sleep(20 * time.Millisecond)
		c.MustRead(t, "$-1") // before the fix, client didn't even block, but was served synchronously with {s1 {}}
	})

	t.Run("Blocking XREAD for stream that ran dry (redis issue #5299)", func(t *testing.T) {
		// add an entry then delete it, now stream's last_id is 666.
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "666", Values: []string{"key", "value"}}).Err())
		require.NoError(t, rdb.XDel(ctx, "mystream", "666").Err())
		// pass an ID smaller than stream's last_id, released on timeout
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.Empty(t, c.XRead(ctx, &redis.XReadArgs{Streams: []string{"mystream", "665"}, Block: 10 * time.Millisecond}).Val())
		// throw an error if the ID equal or smaller than the last_id
		util.ErrorRegexp(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "665", Values: []string{"key", "value"}}).Err(), "ERR.*equal.*smaller.*")
		util.ErrorRegexp(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "666", Values: []string{"key", "value"}}).Err(), "ERR.*equal.*smaller.*")
		// entered blocking state and then release because of the new entry
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"mystream", "665"}}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", ID: "667", Values: []string{"key", "value"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "mystream", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Equal(t, "667-0", r[0].Messages[0].ID)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"key": "value"})
	})

	t.Run("XREAD with same stream name multiple times should work", func(t *testing.T) {
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"old", "abcd1234"}}).Err())
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"s2", "s2", "s2", "$", "$", "$"}, Block: 20 * time.Second}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "s2", Values: []string{"new", "abcd1234"}}).Err())
		r := <-ch
		require.Len(t, r, 3)
		require.Equal(t, "s2", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Subset(t, r[0].Messages[0].Values, map[string]interface{}{"new": "abcd1234"})
	})

	t.Run("XDEL basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "somestream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value0"}}).Err())
		id := rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value1"}}).Val()
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: []string{"foo", "value2"}}).Err())
		require.NoError(t, rdb.XDel(ctx, "somestream", id).Err())
		require.EqualValues(t, 2, rdb.XLen(ctx, "somestream").Val())
		items := rdb.XRange(ctx, "somestream", "-", "+").Val()
		require.Len(t, items, 2)
		require.Subset(t, items[0].Values, map[string]interface{}{"foo": "value0"})
		require.Subset(t, items[1].Values, map[string]interface{}{"foo": "value2"})
	})

	// Here the idea is to check the consistency of the stream data structure as we remove all the elements down to zero elements.
	t.Run("XDEL fuzz test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "somestream").Err())
		var ids []string
		// add enough elements to have a few radix tree nodes inside the stream
		cnt := 0
		for {
			ids = append(ids, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "somestream", Values: map[string]interface{}{"item": cnt}}).Val())
			cnt++
			if rdb.XInfoStream(ctx, "somestream").Val().Length > 500 {
				break
			}
		}
		// Now remove all the elements till we reach an empty stream and after every deletion,
		// check that the stream is sane enough to report the right number of elements with XRANGE:
		// this will also force accessing the whole data structure to check sanity.
		require.EqualValues(t, cnt, rdb.XLen(ctx, "somestream").Val())
		// We want to remove elements in random order to really test the implementation in a better way.
		rand.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		for _, id := range ids {
			require.EqualValues(t, 1, rdb.XDel(ctx, "somestream", id).Val())
			cnt--
			require.EqualValues(t, cnt, rdb.XLen(ctx, "somestream").Val())
			// The test would be too slow calling XRANGE for every iteration. Do it every 100 removal.
			if cnt%100 == 0 {
				require.Len(t, rdb.XRange(ctx, "somestream", "-", "+").Val(), cnt)
			}
		}
	})

	t.Run("XRANGE fuzzing", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		insertIntoStreamKey(t, rdb, "mystream")
		items := rdb.XRange(ctx, "mystream", "-", "+").Val()
		lowID, highID := items[0].ID, items[len(items)-1].ID
		for i := 0; i < 100; i++ {
			start, end := streamRandomID(lowID, highID), streamRandomID(lowID, highID)
			realRange := rdb.XRange(ctx, "mystream", start, end).Val()
			fakeRange := streamSimulateXRANGE(items, start, end)
			require.EqualValues(t, fakeRange, realRange, fmt.Sprintf("start=%s, end=%s", start, end))
		}
	})

	t.Run("XREVRANGE regression test for (redis issue #5006)", func(t *testing.T) {
		// add non compressed entries
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891230", Values: []string{"key1", "value1"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891240", Values: []string{"key2", "value2"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream", ID: "1234567891250", Values: []string{"key3", "value3"}}).Err())
		// add SAMEFIELD compressed entries
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891230", Values: []string{"key1", "value1"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891240", Values: []string{"key1", "value2"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "teststream2", ID: "1234567891250", Values: []string{"key1", "value3"}}).Err())
		items := rdb.XRevRange(ctx, "teststream", "1234567891245", "-").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, redis.XMessage{ID: "1234567891240-0", Values: map[string]interface{}{"key2": "value2"}}, items[0])
		require.EqualValues(t, redis.XMessage{ID: "1234567891230-0", Values: map[string]interface{}{"key1": "value1"}}, items[1])
		items = rdb.XRevRange(ctx, "teststream2", "1234567891245", "-").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, redis.XMessage{ID: "1234567891240-0", Values: map[string]interface{}{"key1": "value2"}}, items[0])
		require.EqualValues(t, redis.XMessage{ID: "1234567891230-0", Values: map[string]interface{}{"key1": "value1"}}, items[1])
	})

	t.Run("XREAD streamID edge (no-blocking)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-1", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-18446744073709551615", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-1", Values: []string{"f", "v"}}).Err())
		r := rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{"x", "1-18446744073709551615"}}).Val()
		require.Len(t, r, 1)
		require.Equal(t, "x", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Equal(t, "2-1", r[0].Messages[0].ID)
		require.Equal(t, map[string]interface{}{"f": "v"}, r[0].Messages[0].Values)
	})

	t.Run("XREAD streamID edge (blocking)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XRead(ctx, &redis.XReadArgs{Streams: []string{"x", "1-18446744073709551615"}}).Val()
		}()
		require.Eventually(t, func() bool {
			cnt, _ := strconv.Atoi(util.FindInfoEntry(rdb, "blocked_clients"))
			return cnt == 1
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-1", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "1-18446744073709551615", Values: []string{"f", "v"}}).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2-1", Values: []string{"f", "v"}}).Err())
		r := <-ch
		require.Len(t, r, 1)
		require.Equal(t, "x", r[0].Stream)
		require.Len(t, r[0].Messages, 1)
		require.Equal(t, "2-1", r[0].Messages[0].ID)
		require.Equal(t, map[string]interface{}{"f": "v"}, r[0].Messages[0].Values)
	})

	t.Run("XADD streamID edge", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", ID: "2577343934890-18446744073709551615", Values: []string{"f", "v"}}).Err()) // we need the timestamp to be in the future
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "x", Values: []string{"f2", "v2"}}).Err())
		items := rdb.XRange(ctx, "x", "-", "+").Val()
		require.Len(t, items, 2)
		require.EqualValues(t, redis.XMessage{ID: "2577343934890-18446744073709551615", Values: map[string]interface{}{"f": "v"}}, items[0])
		require.EqualValues(t, redis.XMessage{ID: "2577343934891-0", Values: map[string]interface{}{"f2": "v2"}}, items[1])
	})

	t.Run("XTRIM with MAXLEN option basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 1000; i++ {
			if rand.Float64() < 0.9 {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"xitem": i}}).Err())
			} else {
				require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"yitem": i}}).Err())
			}
		}
		require.NoError(t, rdb.XTrimMaxLen(ctx, "mystream", 666).Err())
		require.EqualValues(t, 666, rdb.XLen(ctx, "mystream").Val())
		require.NoError(t, rdb.XTrimMaxLen(ctx, "mystream", 555).Err())
		require.EqualValues(t, 555, rdb.XLen(ctx, "mystream").Val())
	})

	t.Run("XADD with LIMIT consecutive calls", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mystream").Err())
		for i := 0; i < 100; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", Values: map[string]interface{}{"xitem": "v"}}).Err())
		}
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 55, Values: map[string]interface{}{"xitem": "v"}}).Err())
		require.EqualValues(t, 55, rdb.XLen(ctx, "mystream").Val())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "mystream", MaxLen: 55, Values: map[string]interface{}{"xitem": "v"}}).Err())
		require.EqualValues(t, 55, rdb.XLen(ctx, "mystream").Val())
	})

	t.Run("XLEN with optional parameters specifying the entry ID to start counting from and direction", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 5; i <= 15; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i),
				Values: []string{"data", fmt.Sprintf("value-%d", i)},
			}).Err())
		}

		r := rdb.Do(ctx, "XLEN", "x", "non-id")
		require.ErrorContains(t, r.Err(), "Invalid stream ID")

		r = rdb.Do(ctx, "XLEN", "x", "15-0")
		val, err := r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "15-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "15-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 10, val)

		r = rdb.Do(ctx, "XLEN", "x", "50-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "50-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "50-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 11, val)

		r = rdb.Do(ctx, "XLEN", "x", "5-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 10, val)

		r = rdb.Do(ctx, "XLEN", "x", "5-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 10, val)

		r = rdb.Do(ctx, "XLEN", "x", "5-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "3-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 11, val)

		r = rdb.Do(ctx, "XLEN", "x", "3-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 11, val)

		r = rdb.Do(ctx, "XLEN", "x", "3-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 0, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 3, val)

		require.NoError(t, rdb.XDel(ctx, "x", "8-0").Err())

		r = rdb.Do(ctx, "XLEN", "x", "8-0")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "+")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 7, val)

		r = rdb.Do(ctx, "XLEN", "x", "8-0", "-")
		val, err = r.Int()
		require.NoError(t, err)
		require.Equal(t, 3, val)
	})
}

// streamSimulateXRANGE simulates Redis XRANGE implementation in Golang.
func streamSimulateXRANGE(items []redis.XMessage, start, end string) []redis.XMessage {
	result := make([]redis.XMessage, 0)
	for _, item := range items {
		if streamCompareID(item.ID, start) >= 0 && streamCompareID(item.ID, end) <= 0 {
			result = append(result, item)
		}
	}
	return result
}

func streamCompareID(a, b string) int {
	aParts, bParts := strings.Split(a, "-"), strings.Split(b, "-")
	aMs, _ := strconv.Atoi(aParts[0])
	aSeq, _ := strconv.Atoi(aParts[1])
	bMs, _ := strconv.Atoi(bParts[0])
	bSeq, _ := strconv.Atoi(bParts[1])
	if aMs > bMs {
		return 1
	}
	if aMs < bMs {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	if aSeq < bSeq {
		return -1
	}
	return 0
}

// streamRandomID generates a random stream entry ID with the ms part between min and max and
// a low sequence number (0 - 999 range), in order to stress test XRANGE against streamSimulateXRANGE.
func streamRandomID(minID, maxID string) string {
	minParts, maxParts := strings.Split(minID, "-"), strings.Split(maxID, "-")
	minMs, _ := strconv.Atoi(minParts[0])
	maxMs, _ := strconv.Atoi(maxParts[0])
	delta := int64(maxMs - minMs + 1)
	ms, seq := int64(minMs)+util.RandomInt(delta), util.RandomInt(1000)
	return fmt.Sprintf("%d-%d", ms, seq)
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
	for i := 0; i < 1000; i++ {
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

	t.Run("XADD with custom sequence number and timestamp set by the server", func(t *testing.T) {
		streamName := "test-stream-1"
		require.NoError(t, rdb.Del(ctx, streamName).Err())

		now := time.Now().UTC().UnixMilli()
		providedSeqNum := 123456789
		r, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     fmt.Sprintf("*-%d", providedSeqNum),
			Values: []string{"data", fmt.Sprintf("value-%d", providedSeqNum)},
		}).Result()

		require.NoError(t, err)

		ts, seqNum := parseStreamEntryID(r)
		require.GreaterOrEqual(t, ts, now)
		require.Less(t, ts, now+5_000)
		require.EqualValues(t, providedSeqNum, seqNum)
	})

	t.Run("XGROUP CREATE with different kinds of commands and XGROUP DESTROY", func(t *testing.T) {
		streamName := "test-stream-a"
		groupName := "test-group-a"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		// No such stream (No such key)
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIESREAD", "10").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIESREAD").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "MKSTREAM", "ENTRIESREAD").Err())
		require.NoError(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "MKSTREAM").Err())
		require.NoError(t, rdb.XInfoStream(ctx, streamName).Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$").Err())
		// Invalid syntax
		groupName = "test-group-b"
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREAT", streamName, groupName, "$").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIEREAD", "10").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, groupName, "$", "ENTRIESREAD", "-10").Err())
		require.Error(t, rdb.Do(ctx, "XGROUP", "CREATE", streamName, "1test-group-c", "$").Err())

		require.NoError(t, rdb.Del(ctx, "myStream").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{Stream: "myStream", Values: []string{"iTeM", "1", "vAluE", "a"}}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, "myStream", "myGroup", "$").Err())
		result, err := rdb.XGroupDestroy(ctx, "myStream", "myGroup").Result()
		require.NoError(t, err)
		require.Equal(t, int64(1), result)
		result, err = rdb.XGroupDestroy(ctx, "myStream", "myGroup").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), result)
	})

	t.Run("XGROUP CREATECONSUMER with different kinds of commands", func(t *testing.T) {
		streamName := "test-stream"
		groupName := "test-group"
		consumerName := "test-consumer"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		//No such stream
		require.Error(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		//no such group
		require.Error(t, rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "$").Err())

		r := rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Val()
		require.Equal(t, int64(1), r)
		r = rdb.XGroupCreateConsumer(ctx, streamName, groupName, consumerName).Val()
		require.Equal(t, int64(0), r)
	})

	t.Run("XGROUP SETID with different kinds of commands", func(t *testing.T) {
		streamName := "test-stream"
		groupName := "test-group"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		//No such stream
		require.Error(t, rdb.XGroupSetID(ctx, streamName, groupName, "$").Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		//No such group
		require.Error(t, rdb.XGroupSetID(ctx, streamName, groupName, "$").Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "$").Err())

		require.NoError(t, rdb.XGroupSetID(ctx, streamName, groupName, "0-0").Err())
		require.Error(t, rdb.Do(ctx, "xgroup", "setid", streamName, groupName, "$", "entries", "100").Err())
		require.Error(t, rdb.Do(ctx, "xgroup", "setid", streamName, groupName, "$", "entriesread", "-100").Err())
		require.NoError(t, rdb.Do(ctx, "xgroup", "setid", streamName, groupName, "$", "entriesread", "100").Err())
	})

	t.Run("XINFO GROUPS and XINFO CONSUMERS", func(t *testing.T) {
		streamName := "test-stream"
		group1 := "t1"
		group2 := "t2"
		consumer1 := "c1"
		consumer2 := "c2"
		consumer3 := "c3"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, group1, "$").Err())
		r := rdb.XInfoGroups(ctx, streamName).Val()
		require.Equal(t, group1, r[0].Name)
		require.Equal(t, int64(0), r[0].Consumers)
		require.Equal(t, int64(0), r[0].Pending)
		require.Equal(t, "1-0", r[0].LastDeliveredID)
		require.Equal(t, int64(0), r[0].EntriesRead)
		require.Equal(t, int64(0), r[0].Lag)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: []string{"data1", "b"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, group2, "$").Err())
		r = rdb.XInfoGroups(ctx, streamName).Val()
		require.Equal(t, group2, r[1].Name)
		require.Equal(t, "2-0", r[1].LastDeliveredID)

		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group1, consumer1).Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group1, consumer2).Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group2, consumer3).Err())
		r = rdb.XInfoGroups(ctx, streamName).Val()
		require.Equal(t, int64(2), r[0].Consumers)
		require.Equal(t, int64(1), r[1].Consumers)

		r1 := rdb.XInfoConsumers(ctx, streamName, group1).Val()
		require.Equal(t, consumer1, r1[0].Name)
		require.Equal(t, consumer2, r1[1].Name)
		r1 = rdb.XInfoConsumers(ctx, streamName, group2).Val()
		require.Equal(t, consumer3, r1[0].Name)
	})

	t.Run("XREAD After XGroupCreate and XGroupCreateConsumer, for issue #2109", func(t *testing.T) {
		streamName := "test-stream"
		group := "group"
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "*",
			Values: []string{"data1", "b"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, group, "0").Err())
		require.NoError(t, rdb.XGroupCreateConsumer(ctx, streamName, group, "consumer").Err())
		require.NoError(t, rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamName, "0"},
		}).Err())
	})

	t.Run("XREADGROUP with different kinds of commands", func(t *testing.T) {
		streamName := "mystream"
		groupName := "mygroup"
		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "1-0",
			Values: []string{"field1", "data1"},
		}).Err())
		require.NoError(t, rdb.XGroupCreate(ctx, streamName, groupName, "0").Err())
		consumerName := "myconsumer"
		r, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}}},
		}}, r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "2-0",
			Values: []string{"field2", "data2"},
		}).Err())
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "2-0", Values: map[string]interface{}{"field2": "data2"}}},
		}}, r)

		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, "0"},
			Count:    2,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream: streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}},
				{ID: "2-0", Values: map[string]interface{}{"field2": "data2"}}},
		}}, r)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "3-0",
			Values: []string{"field3", "data3"},
		}).Err())
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, ">"},
			Count:    1,
			NoAck:    true,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "3-0", Values: map[string]interface{}{"field3": "data3"}}},
		}}, r)
		r, err = rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName,
			Streams:  []string{streamName, "0"},
			Count:    2,
			NoAck:    false,
		}).Result()
		require.NoError(t, err)
		require.Equal(t, []redis.XStream{{
			Stream: streamName,
			Messages: []redis.XMessage{{ID: "1-0", Values: map[string]interface{}{"field1": "data1"}},
				{ID: "2-0", Values: map[string]interface{}{"field2": "data2"}}},
		}}, r)

		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		ch := make(chan []redis.XStream)
		go func() {
			ch <- c.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamName, ">"},
				Count:    2,
				Block:    10 * time.Second,
				NoAck:    false,
			}).Val()
		}()
		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: streamName,
			ID:     "4-0",
			Values: []string{"field4", "data4"},
		}).Err())
		r = <-ch
		require.Equal(t, []redis.XStream{{
			Stream:   streamName,
			Messages: []redis.XMessage{{ID: "4-0", Values: map[string]interface{}{"field4": "data4"}}},
		}}, r)
	})
}

func parseStreamEntryID(id string) (ts int64, seqNum int64) {
	values := strings.Split(id, "-")

	ts, _ = strconv.ParseInt(values[0], 10, 64)
	seqNum, _ = strconv.ParseInt(values[1], 10, 64)

	return
}
