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

	t.Run("ZSet2 object encoding", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x05\x03\x01cffffff\n@\x01b\x9a\x99\x99\x99\x99\x99\x01@\x01a\x9a\x99\x99\x99\x99\x99\xf1?\x0b\x00\x15\xae\xd7&\xda\x10\xe1\x03"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, []redis.Z{
			{Member: "a", Score: 1.1},
			{Member: "b", Score: 2.2},
			{Member: "c", Score: 3.3},
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

	t.Run("List pack with 32bit string", func(t *testing.T) {
		key := util.RandString(32, 64, util.Alpha)
		value := "\x12\r\x02\xc3\x12A\xe7\a\xe7\x01\x00\x00\xf0\x00\x80\x01\xe0\xff\x01\xe0\xcc\x01\x01\x01\xff\x02\xc3A\xca\x80\x00\x00\x8b\x8f\a\x8f\x8b\x00\x00\x01\x00\xf0\x80 \x06\x03\x00\x00\xc0\x7f\xe0\xff\x03\xe1\r\a\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0c\x00O\xfb\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00@\x00\xe4\xff'\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xb7\x00\x01\xc0\x7f\xe0\xb7\xc1\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x95\x00\xed\xff\xbf\xe0\xff\x00\xe0_\x00\xe2\xffw\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xab\x00\xe4\xf3\xd3@\xfb@\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xbb\x00\xe3\xff\xdf\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0'\x00\xe4\xffO\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xd3\x00\xe8\xff\x13\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x9b\x00\xf9\xffc\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xe7\x00\xe6\xff\x17\xe0o\x00\xe1\xff\x7f\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xb7\x00\xe8\xff\xff\xe0\xff\x00\xe0\x1d\x00\x03\x02\x97\x85\xff\x02\xc3A\xb2\x80\x00\x00\x82\x8f\a\x8f\x82\x00\x00\x01\x00\xf0\x80 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\t\x00\x01\xc0\x7f\xe0\t\x13\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x13\x00N\x9f@\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xc0\x00\xed\xffs\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xeb\x00\xf3\xaf\x83@\xb7\xe03\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0+\x00\xe3\x17\x87\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe07\x00\xe5G\x87@O\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xa7\x00\xfb\xff\x83\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0g\x00\xe4\xff\x8f\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0a\x00\x03\x02\x85\x85\xff\x02\xc3A\xd7\x80\x00\x00\x8b\x8f\a\x8f\x8b\x00\x00\x01\x00\xf0\x80 \x06\xe0\xff\x00\xe0U\x00\x01\xc0\x7f\xe0U_\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe4\xff\x7f\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xc7\x00\xf0\xffG\xe0\xff\x00\xe0\xff\x00\xe0k\x00\xe3\xff\x8b\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0+\x00\xe6\xffc\xe0\x0b\x00\xe1\xff\x1b\xe0\xff\x00\xe0W\x00\xe2\xffo\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0W\x00\xe6\xff\x8f\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe03\x00\xe8{{\xe0{\x83\xe0\xf3\x00\xe1\xff\x7f\xe0?\x00\xe1\xffO\xe0\x0f\x00\xe1\x87\x1f\xe0\x87\x8f\xe0\x03\x00@\x9b@\x03@\x00\xc0\a\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x1f\x00\xe3\xffG\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0+\x00\xf4\xff\xd3\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xe7\x00\xfb\xff\xbf\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xfb\x00\xf5\xff\xa3\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0M\x00\x03\x02\x97\x85\xff\x02\xc3A\xab\x80\x00\x00\x87\x0f\a\x0f\x87\x00\x00\x01\x00\xf0\x00 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0=\x00\x01\xc0\x7f\xe0=G\xe0\xff\x00\xe0\xe7\x00\xe2\xff?\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x87\x00\xe6\xff\xbf\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0g\x00\xef\xff\xe7\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xc7\x00\xc5\xf7@\a@\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xa1\x00\x03\x02\x8e\x85\xff\x02\xc3A\xae\x80\x00\x00\x8b\x8f\a\x8f\x8b\x00\x00\x01\x00\xf0\x80 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xdd\x00\x01\xc0\x7f\xe0\xdd\xe7\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe03\x00\xe8\xff[\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0E\x00\x03\x02\x97\x85\xff\x02\xc3A\xc0\x80\x00\x00\x87\x0f\a\x0f\x87\x00\x00\x01\x00\xf0\x00 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0-\x00\x01\xc0\x7f\xe0=\x03\xc0\x00@O\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe03\x00\xef\xff\xb7\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x13\x00FK@\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x13\x00\xfe\xff\a\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0'\x00\xed\xff\x97\xe0\xb7\x00\xc1\xc7@\a@\x03@\x00\xc0\a\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x9b\x00\xe6\xff\xdb\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0A\x00\x03\x02\x8e\x85\xff\x02\xc3A\xae\x80\x00\x00\x8b\x8f\a\x8f\x8b\x00\x00\x01\x00\xf0\x80 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0=\x00\x01\xc0\x7f\xe0\x01\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xf5\x00\x03\x02\x97\x85\xff\x02\xc3A\xf4\x80\x00\x00\x8b\x8f\a\x8f\x8b\x00\x00\x01\x00\xf0\x80 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x91\x00\x01\xc0\x7f\xe0\x91\x9b\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x03\x00\xed\xff\a\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00@\x00O{@\x03@\x00@\a@\x03\xe0\x1b\x00\xe0\x1f'\xe0W\x00@\x87@\x03\xe0\x17\x00@#@\x03\xe0\x03\x00@\x0f@\x03\xe0/\x00@;@\x03\xe0\x17\x00@#\xe0#\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0W\x00\xe9#\xd3\xe0\x17+\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xb7\x00\xd6\x87\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xcf\x00L7\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x0b\x00\xf3\xff\xaf\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\x8f\x00\xe7\xff\xcf\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0+\x00\xe6\xffc\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xb7\x00\xe4\xff\xdf\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xad\x00\x03\x02\x97\x85\xff\x02\xc3A\xb2\x80\x00\x00\x87\x0f\a\x0f\x87\x00\x00\x01\x00\xf0\x00 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0=\x00\x01\xc0\x7f\xe0=G\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0[\x00\xcd\x0b@\a\xe0'\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xc7\x00\x01\xc0\x7f\xe0\xc7\xd1\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xe9\x00\x01\xc0\x7f\xe0\xe9\xf3\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0Q\x00\x03\x02\x8e\x85\xff\x02\xc3A\xc9\x80\x00\x00\x8b\x8f\a\x8f\x8b\x00\x00\x01\x00\xf0\x80 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xed\x00\x01\xc0\x7f\xe0\xc9\xf7@\xd3@\x03\xe0\xff\x00\xe0\xff\x00\xe0K\x00Bg@\x03\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0'\x00\xee\xff\xa3\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0'\x00S\xc7\xe0\x0f\x03@\x00@\x1b@\x00\xc0\a\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xf7\x00\x01\xc0\x7f\xe1\xf7\x01\xe0\xa7\x00\x03\x02\x97\x85\xff\x02\xc3A\xa7\x80\x00\x00\x87\x0f\a\x0f\x87\x00\x00\x01\x00\xf0\x00 \x06\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xe9\x00\x01\xc0\x7f\xe0\xe9\xf3\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0\xff\x00\xe0C\x00Hw\xe0\xff\x03\xe1K\a\xe0\xff\x00\xe0\xff\x00\xe2Kc\xe0\xffS\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\xff\a\xe1\x19\a\x03\x02\x8e\x85\xff\x02\xc3\x14B\xc3\x06\xc3\x02\x00\x00^\x01\x80\xe0\xff\x01\xe0\xff\x01\xe0\xa1\x01\x01\x01\xff\x0b\x00$\x1f\xc8\xe2\xfa\x15>\xdb"
		require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
		require.EqualValues(t, 601, rdb.LLen(ctx, key).Val())
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

	// TTL is 0, key is created without any expire.
	require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
	require.EqualValues(t, -1, rdb.TTL(ctx, key).Val())
	require.EqualValues(t, []string{
		"012345678901234567890", "1234", "a", "ab", "abc",
	}, rdb.SMembers(ctx, key).Val())

	// TTL is 0 with ABSTTL, key is created without any expire.
	require.NoError(t, rdb.Do(ctx, "RESTORE", key, 0, value, "REPLACE", "ABSTTL").Err())
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

func TestRestoreWithExpiredTTL(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := util.RandString(32, 64, util.Alpha)
	value := "\x00\x03bar\n\x00\xe6\xbeI`\xeef\xfd\x17"

	// Wrong TTL value
	require.EqualError(t, rdb.Do(ctx, "RESTORE", key, -1, value).Err(), "ERR out of numeric range")
	require.NoError(t, rdb.Do(ctx, "RESTORE", key, 0, value).Err())
	require.Equal(t, "bar", rdb.Get(ctx, key).Val())
	// Expired TTL with ABSTTL, will not actually restore the key.
	require.NoError(t, rdb.Do(ctx, "RESTORE", key, 1111, value, "REPLACE", "ABSTTL").Err())
	require.EqualError(t, rdb.Get(ctx, key).Err(), redis.Nil.Error())
}
