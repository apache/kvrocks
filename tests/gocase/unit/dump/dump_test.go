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

package dump

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestDump_String(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Del(ctx, "dump_test_key1", "dump_test_key2").Err())
	require.NoError(t, rdb.Set(ctx, "dump_test_key1", "hello,world!", 0).Err())
	require.NoError(t, rdb.Set(ctx, "dump_test_key2", "654321", 0).Err())
	require.Equal(t, "\x00\x0chello,world!\x0c\x00N~\xe6\xc8\xd38h\x17", rdb.Dump(ctx, "dump_test_key1").Val())
	require.Equal(t, "\x00\xc2\xf1\xfb\t\x00\x0c\x00gSN\xfd\xf2y\xa2\x9d", rdb.Dump(ctx, "dump_test_key2").Val())
}

func TestDump_Hash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Del(ctx, "dump_test_key1").Err())
	require.NoError(t, rdb.HMSet(ctx, "dump_test_key1", "name", "redis tutorial", "description", "redis basic commands for caching", "likes", 20, "visitors", 23000).Err())
	require.Equal(t, "\x04\x04\x0bdescription redis basic commands for caching\x05likes\xc0\x14\x04name\x0eredis tutorial\bvisitors\xc1\xd8Y\x0c\x008\x96\xa68b\xebuQ", rdb.Dump(ctx, "dump_test_key1").Val())
}

func TestDump_ZSet(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	zMember := []redis.Z{{Member: "kvrocks1", Score: 1}, {Member: "kvrocks2", Score: 2}, {Member: "kvrocks3", Score: 3}}
	require.NoError(t, rdb.Del(ctx, "dump_test_key1").Err())
	require.NoError(t, rdb.ZAdd(ctx, "dump_test_key1", zMember...).Err())
	require.Equal(t, "\x05\x03\bkvrocks3\x00\x00\x00\x00\x00\x00\b@\bkvrocks2\x00\x00\x00\x00\x00\x00\x00@\bkvrocks1\x00\x00\x00\x00\x00\x00\xf0?\x0c\x00L_7\xd3\xd4\xc9\xf4\xe4", rdb.Dump(ctx, "dump_test_key1").Val())
}

func TestDump_List(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Del(ctx, "dump_test_key1").Err())
	require.NoError(t, rdb.RPush(ctx, "dump_test_key1", "kvrocks1").Err())
	require.NoError(t, rdb.RPush(ctx, "dump_test_key1", "kvrocks2").Err())
	require.NoError(t, rdb.RPush(ctx, "dump_test_key1", "kvrocks3").Err())
	require.Equal(t, "\x12\x03\x01\bkvrocks1\x01\bkvrocks2\x01\bkvrocks3\x0c\x00\xa8\xf9S\x986\x98\xaf\xcd", rdb.Dump(ctx, "dump_test_key1").Val())
}

func TestDump_Set(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Del(ctx, "dump_test_key1").Err())
	require.NoError(t, rdb.SAdd(ctx, "dump_test_key1", "kvrocks1").Err())
	require.NoError(t, rdb.SAdd(ctx, "dump_test_key1", "kvrocks2").Err())
	require.NoError(t, rdb.SAdd(ctx, "dump_test_key1", "kvrocks3").Err())
	require.Equal(t, "\x02\x03\bkvrocks1\bkvrocks2\bkvrocks3\x0c\x00\xfdP\xc9\x95sS\x87\x18", rdb.Dump(ctx, "dump_test_key1").Val())
}
