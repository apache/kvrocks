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

	key := util.RandString(32, 64, util.Alpha)
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

	t.Run("Hash object encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x04\x06\x02f2\xc0\x01\x02f3\xc1\xd2\x04\xc3\x12@F\n01234567890\xe00\t\x0189\xc0\x01\x02f4\xc2Na\xbc\x00\x02f6\xc0b\x02f5\x0e12345678901234\a\x00\xc5f\xe3\xf8\xa4w\a)"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, map[string]string{
			"0123456789012345678901234567890123456789012345678901234567890123456789": "1",
			"f2": "1",
			"f3": "1234",
			"f4": "12345678",
			"f5": "12345678901234",
			"f6": "98",
		}, rdb.HGetAll(ctx, key).Val())
	})

	t.Run("Zip list encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\r22\x00\x00\x00,\x00\x00\x00\b\x00\x00\x0bxxxxxxxxxxy\r\xf5\x02\x02f2\x04\xfe \x03\x02f3\x04\xc0,\x01\x04\x02f4\x04\xf0\x87\xd6\x12\xff\a\x00\xba\x1dc/U\xa5\x88\x94"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, map[string]string{
			"xxxxxxxxxxy": "4",
			"f2":          "32",
			"f3":          "300",
			"f4":          "1234567",
		}, rdb.HGetAll(ctx, key).Val())
	})

	t.Run("List pack encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
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

	t.Run("ZSet object encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x03\x03\x04yyyy\x12456789.20000000001\x15012345678901234567890\x05123.5\x04xxxx\x011\x06\x00\xb3\xa7b%\x96\x01\xe8\xdb"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []redis.Z{
			{Member: "xxxx", Score: 1.0},
			{Member: "012345678901234567890", Score: 123.5},
			{Member: "yyyy", Score: 456789.2},
		}, rdb.ZRangeWithScores(ctx, key, 0, -1).Val())
	})

	t.Run("List pack encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x11''\x00\x00\x00\x06\x00\x81a\x02\x831.2\x04\x81b\x02\x861234.5\a\x81c\x02\xf4\xd8Y`\xe0\x02\x00\x00\x00\t\xff\n\x00\xce\xfdp\xbdHN\xdbG"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []redis.Z{
			{Member: "a", Score: 1.2},
			{Member: "b", Score: 1234.5},
			{Member: "c", Score: 12354345432},
		}, rdb.ZRangeWithScores(ctx, key, 0, -1).Val())
	})

	t.Run("Zip list encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x0c22\x00\x00\x00,\x00\x00\x00\x06\x00\x00\x06zzzzzz\b\x031.5\x05\x04xxxx\x06\x061234.5\b\x05yyyyy\a\xf0@\xe2\x01\xff\x06\x00)\xe2\xb6\x8b\x9b9\xc1&"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []redis.Z{
			{Member: "zzzzzz", Score: 1.5},
			{Member: "xxxx", Score: 1234.5},
			{Member: "yyyyy", Score: 123456},
		}, rdb.ZRangeWithScores(ctx, key, 0, -1).Val())
	})
}

func TestRestore_List(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("List object encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x01\x05\x04hijk\x03efg\x02bc\x01a\x15012345678901234567890\x06\x00\xcb\xdc\xf9\x0ee|{g"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{
			"hijk", "efg", "bc", "a", "012345678901234567890",
		}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("Zip list encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\n##\x00\x00\x00\x1f\x00\x00\x00\b\x00\x00\xc090\x04\xc0\xd2\x04\x04\xfe{\x03\xfd\x02\xf2\x02\x01c\x03\x01b\x03\x01a\xff\x06\x00(H.j/\xf9\x04\x8f"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{
			"12345", "1234", "123", "12", "1", "c", "b", "a",
		}, rdb.LRange(ctx, key, 0, -1).Val())
	})

	t.Run("Quick list encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
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

	t.Run("Set object encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x02\x05\x02ab\xc1\xd2\x04\x01a\x15012345678901234567890\x03abc\x06\x00s\xf8_\x01\xf3\xf56\xd8"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{
			"012345678901234567890", "1234", "a", "ab", "abc",
		}, rdb.SMembers(ctx, key).Val())
	})

	t.Run("List pack encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x14\x15\x15\x00\x00\x00\x05\x00\x84abcd\x05\x01\x01\x02\x01\x03\x01\x04\x01\xff\x0b\x00\xc8h\xa3\xaf\x8b\x1f\xd4\xab"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"1", "2", "3", "4", "abcd"}, rdb.SMembers(ctx, key).Val())
	})

	t.Run("16bit INTSET encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x0b\x10\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x02\x00\x03\x00\x04\x00\x0b\x00\xc4}\x10TeTI<"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"1", "2", "3", "4"}, rdb.SMembers(ctx, key).Val())
	})

	t.Run("32bit INTSET encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x0b\x14\x04\x00\x00\x00\x03\x00\x00\x00\xd2\x04\x00\x005\x82\x00\x00@\xe2\x01\x00\x0b\x00h\xc8u\x0b/\x95\\X"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"1234", "123456", "33333"}, rdb.SMembers(ctx, key).Val())
	})

	t.Run("64bit INTSET encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x0b\xc3\x1b \x04\b\x00\x00\x00\x03 \x03\x04\xe3\x80^\xef\x0c \a\x00\xe4\xa0\a\x00\xe5`\a\x01\x00\x00\x0b\x00Sb\xaf\xbf\x1c\xb6J="
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []string{"55555555555", "55555555556", "55555555557"}, rdb.SMembers(ctx, key).Val())
	})
}

func TestRestoreWithTTL(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := util.RandString(32, 64, util.Alpha)
	value := "\x02\x05\x02ab\xc1\xd2\x04\x01a\x15012345678901234567890\x03abc\x06\x00s\xf8_\x01\xf3\xf56\xd8"
	require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
	require.EqualValues(t, -1, rdb.TTL(ctx, key).Val())
	require.EqualValues(t, []string{
		"012345678901234567890", "1234", "a", "ab", "abc",
	}, rdb.SMembers(ctx, key).Val())

	// Cannot restore to an existing key.
	newValue := "\x0b\x10\x02\x00\x00\x00\x04\x00\x00\x00\x01\x00\x02\x00\x03\x00\x04\x00\x0b\x00\xc4}\x10TeTI<"
	require.EqualError(t, rdb.Restore(ctx, key, 0, newValue).Err(),
		"ERR target key name already exists.")

	// Restore exists key with the replacement flag
	require.NoError(t, rdb.RestoreReplace(ctx, key, 10*time.Second, newValue).Err())
	require.EqualValues(t, []string{"1", "2", "3", "4"}, rdb.SMembers(ctx, key).Val())
	require.Greater(t, rdb.TTL(ctx, key).Val(), 5*time.Second)
	require.LessOrEqual(t, rdb.TTL(ctx, key).Val(), 10*time.Second)
}
