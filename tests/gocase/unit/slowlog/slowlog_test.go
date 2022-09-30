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

package slowlog

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSlowlog(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SLOWLOG - check that it starts with an empty log", func(t *testing.T) {
		require.EqualValues(t, 0, rdb.Do(ctx, "slowlog", "len").Val())
	})

	t.Run("SLOWLOG - only logs commands taking more time than specified", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", 100000).Err())
		require.NoError(t, rdb.Ping(ctx).Err())
		require.EqualValues(t, 0, rdb.Do(ctx, "slowlog", "len").Val())
	})

	t.Run("SLOWLOG - max entries is correctly handled", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", 0).Err())
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-max-len", 10).Err())
		for i := 0; i < 100; i++ {
			require.NoError(t, rdb.Ping(ctx).Err())
		}
		require.EqualValues(t, 10, rdb.Do(ctx, "slowlog", "len").Val())
	})

	t.Run("SLOWLOG - GET optional argument to limit output len works", func(t *testing.T) {
		require.EqualValues(t, 5, reflect.ValueOf(rdb.Do(ctx, "slowlog", "get", 5).Val()).Len())
	})

	t.Run("SLOWLOG - RESET subcommand works", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", 100000).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.EqualValues(t, 0, rdb.Do(ctx, "slowlog", "len").Val())
	})

	t.Run("SLOWLOG - logged entry sanity check", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "client", "setname", "foobar").Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())
		cmd := rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e := reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, 4, reflect.ValueOf(e).Len())
		require.EqualValues(t, 105, reflect.ValueOf(e).Index(0).Interface().(int64))
		require.EqualValues(t, true, reflect.ValueOf(e).Index(2).Interface().(int64) > 100000)
		require.EqualValues(t, []interface{}{"debug", "sleep", "0.2"}, reflect.ValueOf(e).Index(3).Interface())
	})

	t.Run("SLOWLOG - Rewritten commands are logged as their original command", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", 0).Err())
		// Test rewriting client arguments
		require.NoError(t, rdb.SAdd(ctx, "set", "a", "b", "c", "d", "e").Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// SPOP is rewritten as DEL when all keys are removed
		require.NoError(t, rdb.SPopN(ctx, "set", 10).Err())
		cmd := rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e := reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, []interface{}{"spop", "set", "10"}, reflect.ValueOf(e).Index(3).Interface())

		// Test replacing client arguments
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// GEOADD is replicated as ZADD
		require.NoError(t, rdb.GeoAdd(ctx, "cool-cities", &redis.GeoLocation{Longitude: -122.33207, Latitude: 47.60621, Name: "Seattle"}).Err())
		cmd = rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e = reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, []interface{}{"geoadd", "cool-cities", "-122.33207", "47.60621", "Seattle"}, reflect.ValueOf(e).Index(3).Interface())

		// Test replacing a single command argument
		require.NoError(t, rdb.Set(ctx, "A", 5, 0).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// GETSET is replicated as SET
		util.ErrorRegexp(t, rdb.GetSet(ctx, "a", "5").Err(), "nil")
		cmd = rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e = reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, []interface{}{"getset", "a", "5"}, reflect.ValueOf(e).Index(3).Interface())

		// INCRBYFLOAT calls rewrite multiple times, so it's a special case
		require.NoError(t, rdb.Set(ctx, "A", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())

		// INCRBYFLOAT is replicated as SET
		require.NoError(t, rdb.IncrByFloat(ctx, "A", 1.0).Err())
		cmd = rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e = reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, []interface{}{"incrbyfloat", "A", "1"}, reflect.ValueOf(e).Index(3).Interface())
	})

	t.Run("SLOWLOG - commands with too many arguments are trimmed", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", 0).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.SAdd(ctx, "set", 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33).Err())
		cmd := rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e := reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, []interface{}{"sadd", "set", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31", "... (2 more arguments)"}, reflect.ValueOf(e).Index(3).Interface())
	})

	t.Run("SLOWLOG - too long arguments are trimmed", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", 0).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.SAdd(ctx, "set", "foo", strings.Repeat("A", 129)).Err())
		cmd := rdb.Do(ctx, "slowlog", "get")
		require.NoError(t, cmd.Err())
		e := reflect.ValueOf(cmd.Val()).Index(0).Interface()
		require.EqualValues(t, []interface{}{"sadd", "set", "foo", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA... (1 more bytes)"}, reflect.ValueOf(e).Index(3).Interface())
	})

	t.Run("SLOWLOG - can clean older entries", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "client", "setname", "lastentry_client").Err())
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-max-len", 1).Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())

		require.EqualValues(t, 1, rdb.Do(ctx, "slowlog", "len").Val())
		require.NoError(t, rdb.Do(ctx, "config", "set", "slowlog-log-slower-than", -1).Err())
		require.NoError(t, rdb.Do(ctx, "slowlog", "reset").Err())
		require.NoError(t, rdb.Do(ctx, "debug", "sleep", 0.2).Err())
		require.EqualValues(t, 0, rdb.Do(ctx, "slowlog", "len").Val())
	})

}
