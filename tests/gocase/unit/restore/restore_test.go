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

package restore

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRestore_String(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := util.RandString(0, 10, util.Alpha)
	value := "\x00\x03bar\n\x00\xe6\xbeI`\xeef\xfd\x17"
	require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
	require.Equal(t, "bar", rdb.Get(ctx, key).Val())
	require.EqualValues(t, -1, rdb.TTL(ctx, key).Val())

	// Cannot restore to an existing key.
	newValue := "\x00\x03new\n\x000tA\x15\x9ch\x17|"
	require.EqualError(t, rdb.Restore(ctx, key, 0, newValue).Err(),
		"ERR target key name already exists.")

	// Restore exists key with the replacement flag
	require.NoError(t, rdb.RestoreReplace(ctx, key, 10*time.Second, newValue).Err())
	require.Equal(t, "new", rdb.Get(ctx, key).Val())
	require.Greater(t, rdb.TTL(ctx, key).Val(), 5*time.Second)
	require.LessOrEqual(t, rdb.TTL(ctx, key).Val(), 10*time.Second)
}

func TestRestore_Hash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("List pack encoding", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x10\x1f\x1f\x00\x00\x00\x06\x00\x82f1\x03\x82v1\x03\x82f2\x03\x82v2\x03\x82f3\x03\x82v3\x03\xff\x0b\x00L\xcd\xdfe(4xd"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, map[string]string{
			"f1": "v1",
			"f2": "v2",
			"f3": "v3",
		}, rdb.HGetAll(ctx, key).Val())
	})
}

func TestRestore_ZSet(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("List pack encoding", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x11\x1c\x1c\x00\x00\x00\x06\x00\x81a\x02\x01\x01\x81b\x02\x831.2\x04\x81c\x02\x831.5\x04\xff\x0b\x00\xc9{\xd4\xbe\x98\xba\x87\xab"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []redis.Z{
			{Member: "a", Score: 1},
			{Member: "b", Score: 1.2},
			{Member: "c", Score: 1.5},
		}, rdb.ZRangeWithScores(ctx, key, 0, -1).Val())
	})
}

func TestRestore_List(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	rand.Seed(time.Now().Unix())
	t.Run("List pack encoding", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x12\x01\x02\xc3%@z\az\x00\x00\x00\a\x00\xb5x\xe0+\x00\x026\xa2y\xe0\x18\x00\x02#\x8ez\xe0\x04\x00\t\x0f\x01\x01\x02\x01\x03\x01\x04\x01\xff\n\x00\x89\x14\xff>\xf8F\x0e="
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, 7, rdb.LLen(ctx, key).Val())
		require.EqualValues(t, []string{
			"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			"yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
			"zzzzzzzzzzzzzz",
			"1", "2", "3", "4",
		}, rdb.LRange(ctx, key, 0, -1).Val())
		require.EqualValues(t, -1, rdb.TTL(ctx, key).Val())
	})
}

func TestRestore_Set(t *testing.T) {

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// TODO: normal encoding
	t.Run("SET encoding with listpack", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x14\x15\x15\x00\x00\x00\x05\x00\x84abcd\x05\x01\x01\x02\x01\x03\x01\x04\x01\xff\x0b\x00\xc8h\xa3\xaf\x8b\x1f\xd4\xab"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"1", "2", "3", "4", "abcd"}, rdb.SMembers(ctx, key).Val())
	})
	t.Run("16bit INTSET encoding", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x0b\x10\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x02\x00\x03\x00\x04\x00\x0b\x00\xc4}\x10TeTI<"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"1", "2", "3", "4"}, rdb.SMembers(ctx, key).Val())
	})
	t.Run("32bit INTSET encoding", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x0b\x14\x04\x00\x00\x00\x03\x00\x00\x00\xd2\x04\x00\x005\x82\x00\x00@\xe2\x01\x00\x0b\x00h\xc8u\x0b/\x95\\X"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"1234", "123456", "33333"}, rdb.SMembers(ctx, key).Val())
	})
	t.Run("64bit INTSET encoding", func(t *testing.T) {
		key := util.RandString(0, 10, util.Alpha)
		value := "\x0b\xc3\x1b \x04\b\x00\x00\x00\x03 \x03\x04\xe3\x80^\xef\x0c \a\x00\xe4\xa0\a\x00\xe5`\a\x01\x00\x00\x0b\x00Sb\xaf\xbf\x1c\xb6J="
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"55555555555", "55555555556", "55555555557"}, rdb.SMembers(ctx, key).Val())

	})
}
