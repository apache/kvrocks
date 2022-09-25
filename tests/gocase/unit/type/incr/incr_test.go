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

package incr

import (
	"context"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestIncr(t *testing.T) {
	srvKvrocks := util.StartServer(t, util.TypeKvrocks, map[string]string{})
	srvRedis := util.StartServer(t, util.TypeRedis, map[string]string{})
	defer srvKvrocks.Close()
	defer srvRedis.Close()
	ctx := context.Background()
	kdb := srvKvrocks.NewClient()
	rdb := srvRedis.NewClient()

	defer func() { require.NoError(t, kdb.Close()) }()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("INCR against non existing key", func(t *testing.T) {
		require.EqualValues(t, rdb.Incr(ctx, "novar").Val(), kdb.Incr(ctx, "novar").Val())
		require.EqualValues(t, rdb.Get(ctx, "novar").Val(), kdb.Get(ctx, "novar").Val())
	})

	t.Run("INCR against key created by incr itself", func(t *testing.T) {
		require.EqualValues(t, 2, kdb.Incr(ctx, "novar").Val())
	})

	t.Run("INCR against key originally set with SET", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 100, 0).Err())
		require.EqualValues(t, 101, kdb.Incr(ctx, "novar").Val())
	})

	t.Run("INCR over 32bit value", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.EqualValues(t, 17179869185, kdb.Incr(ctx, "novar").Val())
	})

	t.Run("INCRBY over 32bit value with over 32bit increment", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.EqualValues(t, 34359738368, kdb.IncrBy(ctx, "novar", 17179869184).Val())
	})

	t.Run("INCR fails against key with spaces (left)", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", "    11", 0).Err())
		util.ErrorRegexp(t, kdb.Incr(ctx, "novar").Err(), "ERR.*")
	})

	t.Run("INCR fails against key with spaces (right)", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", "11    ", 0).Err())
		util.ErrorRegexp(t, kdb.Incr(ctx, "novar").Err(), "ERR.*")
	})

	t.Run("INCR fails against key with spaces (both)", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", "   11    ", 0).Err())
		util.ErrorRegexp(t, kdb.Incr(ctx, "novar").Err(), "ERR.*")
	})

	t.Run("INCR fails against a key holding a list", func(t *testing.T) {
		require.NoError(t, kdb.RPush(ctx, "mylist", 1).Err())
		require.ErrorContains(t, kdb.Incr(ctx, "mylist").Err(), "WRONGTYPE")
		require.NoError(t, kdb.RPop(ctx, "mylist").Err())
	})

	t.Run("DECRBY over 32bit value with over 32bit increment, negative res", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.EqualValues(t, -1, kdb.DecrBy(ctx, "novar", 17179869185).Val())
	})

	t.Run("INCRBYFLOAT against non existing key", func(t *testing.T) {
		require.NoError(t, kdb.Del(ctx, "novar").Err())
		require.EqualValues(t, 1, kdb.IncrByFloat(ctx, "novar", 1.0).Val())
		r, err := kdb.Get(ctx, "novar").Float64()
		require.NoError(t, err)
		require.EqualValues(t, 1, r)
		require.EqualValues(t, 1.25, kdb.IncrByFloat(ctx, "novar", 0.25).Val())
		r, err = kdb.Get(ctx, "novar").Float64()
		require.NoError(t, err)
		require.EqualValues(t, 1.25, r)
	})

	t.Run("INCRBYFLOAT against key originally set with SET", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 1.5, 0).Err())
		require.EqualValues(t, 3, kdb.IncrByFloat(ctx, "novar", 1.5).Val())
	})

	t.Run("INCRBYFLOAT over 32bit value", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.EqualValues(t, 17179869185.5, kdb.IncrByFloat(ctx, "novar", 1.5).Val())
	})

	t.Run("INCRBYFLOAT over 32bit value with over 32bit increment", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.EqualValues(t, 34359738368, kdb.IncrByFloat(ctx, "novar", 17179869184).Val())
	})

	t.Run("INCRBYFLOAT fails against key with spaces (left)", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", "    11", 0).Err())
		util.ErrorRegexp(t, kdb.IncrByFloat(ctx, "novar", 1.0).Err(), "ERR.*valid.*")
	})

	t.Run("INCRBYFLOAT fails against key with spaces (right)", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", "11  ", 0).Err())
		util.ErrorRegexp(t, kdb.IncrByFloat(ctx, "novar", 1.0).Err(), "ERR.*valid.*")
	})

	t.Run("INCRBYFLOAT fails against key with spaces (both)", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "novar", "  11  ", 0).Err())
		util.ErrorRegexp(t, kdb.IncrByFloat(ctx, "novar", 1.0).Err(), "ERR.*valid.*")
	})

	t.Run("INCRBYFLOAT fails against a key holding a list", func(t *testing.T) {
		require.NoError(t, kdb.Del(ctx, "mylist").Err())
		require.NoError(t, kdb.RPush(ctx, "mylist", 1).Err())
		require.ErrorContains(t, kdb.IncrByFloat(ctx, "mylist", 1.0).Err(), "WRONGTYPE")
		require.NoError(t, kdb.Del(ctx, "mylist").Err())
	})

	t.Run("INCRBYFLOAT does not allow NaN or Infinity", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "foo", 0, 0).Err())
		util.ErrorRegexp(t, kdb.Do(ctx, "INCRBYFLOAT", "foo", "+inf").Err(), "ERR.*would produce.*")
	})

	t.Run("INCRBYFLOAT decrement", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "foo", 1, 0).Err())
		require.InDelta(t, -0.1, kdb.IncrByFloat(ctx, "foo", -1.1).Val(), util.DefaultDelta)
	})

	t.Run("string to double with null terminator", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "foo", 1, 0).Err())
		require.NoError(t, kdb.SetRange(ctx, "foo", 2, "2").Err())
		util.ErrorRegexp(t, kdb.IncrByFloat(ctx, "foo", 1.0).Err(), "ERR.*valid.*")
	})
}
