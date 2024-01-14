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

package rename

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRename_String(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Rename string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 10*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "world").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a2", "world2", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a3", "world3", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a3").Val())
	})

	t.Run("RenameNX string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "world", rdb.Get(ctx, "a1").Val())

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.EqualValues(t, "hello", rdb.Get(ctx, "a").Val())
	})

}

func TestRename_JSON(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	SET_CMD := "JSON.SET"
	GET_CMD := "JSON.GET"
	JSON_STR_A := `{"x":1,"y":2}`
	JSON_STR_B := `{"x":1}`

	t.Run("Rename json", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a").Val())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a1", "$", JSON_STR_B).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a1", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a1").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "world").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a1").Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a").Val())

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a1", "$", JSON_STR_B).Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a2", "$", JSON_STR_B).Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a3", "$", JSON_STR_B).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a1").Val())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a2").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a3").Val())
	})

	t.Run("RenameNX json", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a1", "$", JSON_STR_B).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, JSON_STR_B, rdb.Do(ctx, GET_CMD, "a1").Val())

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		require.EqualValues(t, nil, rdb.Do(ctx, GET_CMD, "a").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, SET_CMD, "a", "$", JSON_STR_A).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		require.EqualValues(t, JSON_STR_A, rdb.Do(ctx, GET_CMD, "a").Val())
	})

}

func TestRename_List(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualListValues := func(t *testing.T, key string, value []string) {
		require.EqualValues(t, len(value), rdb.LLen(ctx, key).Val())
		for i := 0; i < len(value); i++ {
			require.EqualValues(t, value[i], rdb.LIndex(ctx, key, int64(i)).Val())
		}
	}

	t.Run("Rename string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, 3, rdb.LLen(ctx, "a1").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Set(ctx, "a1", "world", 0).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualListValues(t, "a1", []string{"3", "2", "1"})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "2").Err())
		require.NoError(t, rdb.LPush(ctx, "a2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a3", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualListValues(t, "a3", []string{"3", "2", "1"})
	})

	t.Run("RenameNX string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a", []string{"3", "2", "1"})
		EqualListValues(t, "a1", []string{"3"})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a1", []string{"3", "2", "1"})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.LPush(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualListValues(t, "a", []string{"3", "2", "1"})
	})

}

func TestRename_hash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualListValues := func(t *testing.T, key string, value map[string]string) {
		require.EqualValues(t, len(value), rdb.HLen(ctx, key).Val())
		for sub_key := range value {
			require.EqualValues(t, value[sub_key], rdb.HGet(ctx, key, sub_key).Val())
		}
	}

	t.Run("Rename hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.HSet(ctx, "a2", "a", "1").Err())
		require.NoError(t, rdb.HSet(ctx, "a3", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualListValues(t, "a3", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
	})

	t.Run("RenameNX hash", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.NoError(t, rdb.HSet(ctx, "a1", "a", "1").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
		})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualListValues(t, "a1", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.HSet(ctx, "a", "a", "1", "b", "2", "c", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualListValues(t, "a", map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		})
	})

}

func TestRename_set(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualSetValues := func(t *testing.T, key string, value []string) {
		require.EqualValues(t, len(value), rdb.SCard(ctx, key).Val())
		for index := range value {
			require.EqualValues(t, true, rdb.SIsMember(ctx, key, value[index]).Val())
		}
	}

	t.Run("Rename set", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "a", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "1").Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a2", "a2", "1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a3", "a3", "1").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualSetValues(t, "a3", []string{"1", "2", "3"})
	})

	t.Run("RenameNX set", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.NoError(t, rdb.SAdd(ctx, "a1", "1").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSetValues(t, "a", []string{"1", "2", "3"})
		EqualSetValues(t, "a1", []string{"1"})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSetValues(t, "a1", []string{"1", "2", "3"})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.SAdd(ctx, "a", "1", "2", "3").Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualSetValues(t, "a", []string{"1", "2", "3"})

	})

}

func TestRename_zset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualZSetValues := func(t *testing.T, key string, value map[string]int) {
		require.EqualValues(t, len(value), rdb.ZCard(ctx, key).Val())
		for sub_key := range value {
			score := value[sub_key]
			require.EqualValues(t, []string{sub_key}, rdb.ZRangeByScore(ctx, key,
				&redis.ZRangeBy{Max: strconv.Itoa(score), Min: strconv.Itoa(score)}).Val())
			require.EqualValues(t, float64(score), rdb.ZScore(ctx, key, sub_key).Val())
		}
	}

	Z_MEMBER := []redis.Z{{Member: "a", Score: 1}, {Member: "b", Score: 2}, {Member: "c", Score: 3}}
	Z_MEMBER_2 := []redis.Z{{Member: "a", Score: 2}}

	t.Run("Rename zset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", Z_MEMBER_2...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", Z_MEMBER_2...).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", 1, 2, 3).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", Z_MEMBER_2...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a2", Z_MEMBER_2...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a3", Z_MEMBER_2...).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualZSetValues(t, "a3", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})

	})

	t.Run("RenameNX zset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.NoError(t, rdb.ZAdd(ctx, "a1", Z_MEMBER_2...).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualZSetValues(t, "a", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		EqualZSetValues(t, "a1", map[string]int{
			"a": 2,
		})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualZSetValues(t, "a1", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.ZAdd(ctx, "a", Z_MEMBER...).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualZSetValues(t, "a", map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		})

	})

}

func TestRename_Bitmap(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualBitSetValues := func(t *testing.T, key string, value []int64) {
		for i := 0; i < len(value); i++ {
			require.EqualValues(t, int64(value[i]), rdb.Do(ctx, "BITPOS", key, 1, value[i]/8).Val())
		}
	}

	SetBits := func(t *testing.T, key string, value []int64) {
		for i := 0; i < len(value); i++ {
			require.NoError(t, rdb.Do(ctx, "SETBIT", key, value[i], 1).Err())
		}
	}
	BITSET_A := []int64{16, 1024 * 8 * 2, 1024 * 8 * 12}
	BITSET_B := []int64{1}

	t.Run("Rename Bitmap", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", BITSET_A)
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualBitSetValues(t, "a1", BITSET_A)
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// newkey has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", BITSET_A)
		SetBits(t, "a1", BITSET_B)
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualBitSetValues(t, "a1", BITSET_A)
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// newkey has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", BITSET_A)
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualBitSetValues(t, "a1", BITSET_A)
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		SetBits(t, "a", BITSET_A)
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualBitSetValues(t, "a", BITSET_A)

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		SetBits(t, "a", BITSET_A)
		SetBits(t, "a1", BITSET_B)
		SetBits(t, "a2", BITSET_B)
		SetBits(t, "a3", BITSET_B)
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualBitSetValues(t, "a3", BITSET_A)
	})

	t.Run("RenameNX Bitmap", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", BITSET_A)
		SetBits(t, "a1", BITSET_B)
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualBitSetValues(t, "a", BITSET_A)
		EqualBitSetValues(t, "a1", BITSET_B)

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", BITSET_A)
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualBitSetValues(t, "a1", BITSET_A)
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		SetBits(t, "a", BITSET_A)
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualBitSetValues(t, "a", BITSET_A)

	})

}

func TestRename_SInt(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	EqualSIntValues := func(t *testing.T, key string, value []int) {
		require.EqualValues(t, len(value), rdb.Do(ctx, "SICARD", key).Val())
		for i := 0; i < len(value); i++ {
			require.EqualValues(t, []interface{}{int64(1)}, rdb.Do(ctx, "SIEXISTS", key, value[i]).Val())
		}
	}

	t.Run("Rename SInt", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// to-key has value with TTL
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a1", 99).Err())
		require.NoError(t, rdb.Expire(ctx, "a1", 1000*time.Second).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		util.BetweenValues(t, rdb.TTL(ctx, "a1").Val(), time.Second, 10*time.Second)

		// to-key has value that not same type
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.LPush(ctx, "a1", "a").Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		require.EqualValues(t, -1, rdb.TTL(ctx, "a1").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a").Err())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})

		// rename*3
		require.NoError(t, rdb.Del(ctx, "a", "a1", "a2", "a3").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a1", 85).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a2", 77, 0).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a3", 111, 222, 333).Err())
		require.NoError(t, rdb.Rename(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Rename(ctx, "a1", "a2").Err())
		require.NoError(t, rdb.Rename(ctx, "a2", "a3").Err())
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a1").Val())
		require.EqualValues(t, "", rdb.Get(ctx, "a2").Val())
		EqualSIntValues(t, "a3", []int{3, 4, 5, 123, 245})
	})

	t.Run("RenameNX SInt", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a1", 99).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSIntValues(t, "a", []int{3, 4, 5, 123, 245})
		EqualSIntValues(t, "a1", []int{99})

		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.EqualValues(t, true, rdb.RenameNX(ctx, "a", "a1").Val())
		EqualSIntValues(t, "a1", []int{3, 4, 5, 123, 245})
		require.EqualValues(t, "", rdb.Get(ctx, "a").Val())

		// key == newkey
		require.NoError(t, rdb.Del(ctx, "a", "a1").Err())
		require.NoError(t, rdb.Do(ctx, "SIADD", "a", 3, 4, 5, 123, 245).Err())
		require.EqualValues(t, false, rdb.RenameNX(ctx, "a", "a").Val())
		EqualSIntValues(t, "a", []int{3, 4, 5, 123, 245})

	})

}
