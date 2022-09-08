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
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("INCR against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.Incr(ctx, "novar").Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Int()
		require.NoError(t, err)
		require.Equal(t, 1, value)
	})

	t.Run("INCR against key created by incr itself", func(t *testing.T) {
		require.NoError(t, rdb.Incr(ctx, "novar").Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Int()
		require.NoError(t, err)
		require.Equal(t, 2, value)
	})

	t.Run("INCR against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 100, 0).Err())
		require.NoError(t, rdb.Incr(ctx, "novar").Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Int()
		require.NoError(t, err)
		require.Equal(t, 101, value)
	})

	t.Run("INCR over 32bit value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.NoError(t, rdb.Incr(ctx, "novar").Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Int()
		require.NoError(t, err)
		require.Equal(t, 17179869185, value)
	})

	t.Run("INCRBY over 32bit value with over 32bit increment", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.NoError(t, rdb.IncrBy(ctx, "novar", 17179869184).Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Int()
		require.NoError(t, err)
		require.Equal(t, 34359738368, value)
	})

	t.Run("INCR fails against key with spaces (left)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "    11", 0).Err())
		require.ErrorContains(t, rdb.Incr(ctx, "novar").Err(), "ERR Invalid argument: value is not an integer")
	})

	t.Run("INCR fails against key with spaces (right)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "11    ", 0).Err())
		require.ErrorContains(t, rdb.Incr(ctx, "novar").Err(), "ERR Invalid argument: value is not an integer")
	})

	t.Run("INCR fails against key with spaces (both)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "   11    ", 0).Err())
		require.ErrorContains(t, rdb.Incr(ctx, "novar").Err(), "ERR Invalid argument: value is not an integer")
	})

	t.Run("INCR fails against a key holding a list", func(t *testing.T) {
		require.NoError(t, rdb.RPush(ctx, "mylist", 1).Err())
		require.ErrorContains(t, rdb.Incr(ctx, "novar").Err(), "ERR Invalid argument: value is not an integer")
		require.NoError(t, rdb.RPop(ctx, "mylist").Err())
	})

	t.Run("DECRBY over 32bit value with over 32bit increment, negative res", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.NoError(t, rdb.DecrBy(ctx, "novar", 17179869185).Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Int()
		require.NoError(t, err)
		require.Equal(t, -1, value)
	})

	t.Run("INCRBYFLOAT against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "novar").Err())
		require.NoError(t, rdb.IncrByFloat(ctx, "novar", 1.0).Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Float64()
		require.NoError(t, err)
		require.Equal(t, 1.0, value)
	})

	t.Run("INCRBYFLOAT against key originally set with SET", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 1.5, 0).Err())
		require.NoError(t, rdb.IncrByFloat(ctx, "novar", 1.5).Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Float64()
		require.NoError(t, err)
		require.Equal(t, 3.0, value)
	})

	t.Run("INCRBYFLOAT over 32bit value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.NoError(t, rdb.IncrByFloat(ctx, "novar", 1.5).Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Float64()
		require.NoError(t, err)
		require.Equal(t, 17179869185.5, value)
	})

	t.Run("INCRBYFLOAT over 32bit value with over 32bit increment", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", 17179869184, 0).Err())
		require.NoError(t, rdb.IncrByFloat(ctx, "novar", 17179869184).Err())
		result := rdb.Get(ctx, "novar")
		value, err := result.Float64()
		require.NoError(t, err)
		require.Equal(t, 34359738368.0, value)
	})

	t.Run("INCRBYFLOAT fails against key with spaces (left)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "    11", 0).Err())
		require.ErrorContains(t, rdb.IncrByFloat(ctx, "novar", 1.0).Err(), "ERR Invalid argument: value is not an float")
	})

	t.Run("INCRBYFLOAT fails against key with spaces (right)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "11  ", 0).Err())
		require.ErrorContains(t, rdb.IncrByFloat(ctx, "novar", 1.0).Err(), "ERR Invalid argument: value is not an float")
	})

	t.Run("INCRBYFLOAT fails against key with spaces (both)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "  11  ", 0).Err())
		require.ErrorContains(t, rdb.IncrByFloat(ctx, "novar", 1.0).Err(), "ERR Invalid argument: value is not an float")
	})

	t.Run("INCRBYFLOAT fails against a key holding a list", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", 1).Err())
		require.ErrorContains(t, rdb.IncrByFloat(ctx, "mylist", 1.0).Err(), "ERR Invalid argument: WRONGTYPE Operation against a key holding the wrong kind of value")
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
	})

	t.Run("INCRBYFLOAT does not allow NaN or Infinity", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", 0, 0).Err())
		r := rdb.Do(ctx, "INCRBYFLOAT", "foo", "+inf")
		require.ErrorContains(t, r.Err(), "ERR Invalid argument: increment would produce NaN or Infinity")
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
	})

	// TODO: INCRBYFLOAT precision for human friendly
	t.Run("INCRBYFLOAT precision for human friendly", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", 1.1, 0).Err())
		require.NoError(t, rdb.IncrByFloat(ctx, "foo", -1.0).Err())
		result := rdb.Get(ctx, "foo")
		value, err := result.Float64()
		require.NoError(t, err)
		require.Equal(t, 0.1, value)
	})

	t.Run("string to double with null terminator", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", 1, 0).Err())
		require.NoError(t, rdb.SetRange(ctx, "foo", 2, "2").Err())
		require.ErrorContains(t, rdb.IncrByFloat(ctx, "foo", 1.0).Err(), "ERR Invalid argument: value is not an float")
	})
}
