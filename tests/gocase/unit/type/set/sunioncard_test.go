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

package set

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestSUnionCard(t *testing.T) {
	configs, err := util.GenerateConfigsMatrix([]util.ConfigOptions{
		{Name: "txn-context-enabled", Options: []string{"yes", "no"}},
		{Name: "resp3-enabled", Options: []string{"yes", "no"}},
	})
	require.NoError(t, err)
	for _, config := range configs {
		sunionCardTests(t, config)
	}
}

func sunionCardTests(t *testing.T, config util.KvrocksServerConfigs) {
	srv := util.StartServer(t, config)
	defer srv.Close()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	ctx := context.Background()

	t.Run("exact union and limits", func(t *testing.T) {
		require.NoError(t, rdb.SAdd(ctx, "first", "a", "b", "c").Err())
		require.NoError(t, rdb.SAdd(ctx, "second", "c", "d", "e").Err())
		for _, keys := range [][]interface{}{
			{"first", "second"},
			{"missing", "first", "second", "first", "missing"},
		} {
			args := append([]interface{}{"SUNIONCARD", len(keys)}, keys...)
			got, err := rdb.Do(ctx, args...).Int64()
			require.NoError(t, err)
			require.EqualValues(t, 5, got)
			for _, limit := range []int64{0, 1, 3, 5, 6, 2147483648, 9223372036854775807} {
				got, err = rdb.Do(ctx, append(args, "LIMIT", limit)...).Int64()
				require.NoError(t, err)
				want := int64(5)
				if limit > 0 && limit < want {
					want = limit
				}
				require.Equal(t, want, got)
			}
		}
	})

	t.Run("empty missing and binary keys and members", func(t *testing.T) {
		for _, opts := range [][]interface{}{nil, {"APPROX"}, {"LIMIT", 1}, {"APPROX", "LIMIT", 1}} {
			got, err := rdb.Do(ctx, append([]interface{}{"SUNIONCARD", 2, "missing", "missing"}, opts...)...).Int64()
			require.NoError(t, err)
			require.Zero(t, got)
		}
		require.NoError(t, rdb.SAdd(ctx, "", "", "\x00\xff", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "binary\x00key", "\x00\xff", "2").Err())
		for _, opts := range [][]interface{}{nil, {"aPpRoX"}, {"LIMIT", 0, "APPROX"}} {
			got, err := rdb.Do(ctx, append([]interface{}{"SUNIONCARD", 3, "", "binary\x00key", ""}, opts...)...).Int64()
			require.NoError(t, err)
			require.EqualValues(t, 4, got)
		}
		require.NoError(t, rdb.SAdd(ctx, "expired", "stale").Err())
		require.NoError(t, rdb.Expire(ctx, "expired", -time.Second).Err())
		got, err := rdb.Do(ctx, "SUNIONCARD", 2, "expired", "").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 3, got)
	})

	t.Run("wrong type is checked before LIMIT", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "wrongtype", "value", 0).Err())
		require.NoError(t, rdb.SAdd(ctx, "valid", "a", "b").Err())
		for _, keys := range [][]interface{}{
			{"wrongtype", "valid"}, {"valid", "wrongtype"}, {"missing", "wrongtype"},
		} {
			for _, opts := range [][]interface{}{nil, {"APPROX"}, {"LIMIT", 1}, {"APPROX", "LIMIT", 1}} {
				args := append([]interface{}{"SUNIONCARD", len(keys)}, keys...)
				util.ErrorRegexp(t, rdb.Do(ctx, append(args, opts...)...).Err(), "WRONGTYPE")
			}
		}
	})

	t.Run("reads preceding script writes", func(t *testing.T) {
		for _, option := range []string{"", "APPROX"} {
			got, err := rdb.Eval(ctx, `
				redis.call('DEL', KEYS[1], KEYS[2])
				redis.call('SADD', KEYS[1], 'stale')
				redis.call('DEL', KEYS[1])
				redis.call('SADD', KEYS[1], 'a', 'b')
				redis.call('SADD', KEYS[2], 'b', 'c')
				local args = {'SUNIONCARD', 3, KEYS[1], KEYS[2], KEYS[1]}
				if ARGV[1] ~= '' then table.insert(args, ARGV[1]) end
				local before = redis.call(unpack(args))
				redis.call('SREM', KEYS[1], 'a')
				local after = redis.call(unpack(args))
				table.insert(args, 'LIMIT')
				table.insert(args, 1)
				return {before, after, redis.call(unpack(args))}
			`, []string{"script-first", "script-second"}, option).Slice()
			require.NoError(t, err)
			require.Equal(t, []interface{}{int64(3), int64(2), int64(1)}, got)
		}
	})

	t.Run("recreated keys remain isolated after restart", func(t *testing.T) {
		require.NoError(t, rdb.SAdd(ctx, "recreated", "stale1", "stale2").Err())
		require.NoError(t, rdb.Del(ctx, "recreated").Err())
		require.NoError(t, rdb.Set(ctx, "recreated", "old string", 0).Err())
		require.NoError(t, rdb.Del(ctx, "recreated").Err())
		require.NoError(t, rdb.SAdd(ctx, "recreated", "expired").Err())
		require.NoError(t, rdb.Expire(ctx, "recreated", -time.Second).Err())
		require.NoError(t, rdb.SAdd(ctx, "recreated", "live").Err())
		require.NoError(t, rdb.SAdd(ctx, "recreated\x00suffix", "neighbor").Err())
		beforeKeys, err := rdb.Keys(ctx, "*").Result()
		require.NoError(t, err)
		for _, restart := range []bool{false, true} {
			if restart {
				srv.Restart()
			}
			for _, opts := range [][]interface{}{nil, {"APPROX"}, {"LIMIT", 1}, {"APPROX", "LIMIT", 1}} {
				got, err := rdb.Do(ctx, append([]interface{}{"SUNIONCARD", 3, "recreated", "missing", "recreated"}, opts...)...).Int64()
				require.NoError(t, err)
				require.EqualValues(t, 1, got)
			}
			afterKeys, err := rdb.Keys(ctx, "*").Result()
			require.NoError(t, err)
			require.ElementsMatch(t, beforeKeys, afterKeys)
			require.Equal(t, []string{"live"}, rdb.SMembers(ctx, "recreated").Val())
		}
	})

	t.Run("argument validation", func(t *testing.T) {
		for _, tc := range []struct {
			args []interface{}
			err  string
		}{
			{nil, "wrong number"},
			{[]interface{}{1}, "wrong number"},
			{[]interface{}{0, "key"}, "positive"},
			{[]interface{}{-1, "key"}, "positive"},
			{[]interface{}{"x", "key"}, "integer"},
			{[]interface{}{"1.5", "key"}, "integer"},
			{[]interface{}{"9223372036854775808", "key"}, "integer"},
			{[]interface{}{"9223372036854775807", "key"}, "wrong number"},
			{[]interface{}{2, "key"}, "wrong number"},
			{[]interface{}{1, "key", "UNKNOWN"}, "syntax"},
			{[]interface{}{1, "key", "LIMIT"}, "syntax"},
			{[]interface{}{1, "key", "APPROX", "LIMIT"}, "syntax"},
			{[]interface{}{1, "key", "LIMIT", -1}, "LIMIT.*negative"},
			{[]interface{}{1, "key", "LIMIT", "x"}, "LIMIT.*negative"},
			{[]interface{}{1, "key", "LIMIT", "1.5"}, "LIMIT.*negative"},
			{[]interface{}{1, "key", "LIMIT", "9223372036854775808"}, "LIMIT.*negative"},
		} {
			util.ErrorRegexp(t, rdb.Do(ctx, append([]interface{}{"SUNIONCARD"}, tc.args...)...).Err(), tc.err)
		}
		// Redis permits repeated options, with the last LIMIT taking effect.
		require.NoError(t, rdb.SAdd(ctx, "options", "a", "b", "c").Err())
		got, err := rdb.Do(ctx, "SUNIONCARD", 1, "options", "approx", "lImIt", 1, "APPROX", "LIMIT", 2).Int64()
		require.NoError(t, err)
		require.EqualValues(t, 2, got)
	})

	t.Run("APPROX uses HLL and respects LIMIT", func(t *testing.T) {
		first, second, all := make([]interface{}, 0, 15000), make([]interface{}, 0, 15000), make([]interface{}, 0, 20000)
		for i := 0; i < 20000; i++ {
			member := strconv.Itoa(i)
			all = append(all, member)
			if i < 15000 {
				first = append(first, member)
			}
			if i >= 5000 {
				second = append(second, member)
			}
		}
		require.NoError(t, rdb.SAdd(ctx, "large1", first...).Err())
		require.NoError(t, rdb.SAdd(ctx, "large2", second...).Err())
		require.NoError(t, rdb.PFAdd(ctx, "union-hll", all...).Err())
		want, err := rdb.PFCount(ctx, "union-hll").Result()
		require.NoError(t, err)
		require.InDelta(t, 20000, want, 1000)
		require.NotEqualValues(t, 20000, want)
		for _, opts := range [][]interface{}{
			{"APPROX"}, {"APPROX", "LIMIT", 0}, {"LIMIT", 30000, "APPROX"},
		} {
			got, err := rdb.Do(ctx, append([]interface{}{"SUNIONCARD", 4, "large1", "missing", "large2", "large1"}, opts...)...).Int64()
			require.NoError(t, err)
			require.Equal(t, want, got)
		}
		for _, limit := range []int64{1, 1000, 10000} {
			got, err := rdb.Do(ctx, "SUNIONCARD", 2, "large1", "large2", "APPROX", "LIMIT", limit).Int64()
			require.NoError(t, err)
			require.Equal(t, limit, got)
		}
		got, err := rdb.Do(ctx, "SUNIONCARD", 2, "large1", "large2").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 20000, got)
		require.EqualValues(t, 15000, rdb.SCard(ctx, "large1").Val())
		require.EqualValues(t, 15000, rdb.SCard(ctx, "large2").Val())
	})
}
