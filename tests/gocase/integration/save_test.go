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

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSave(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	kdb := srv.NewClient()
	defer func() { require.NoError(t, kdb.Close()) }()

	t.Run("Type string in rdb", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, kdb.Set(ctx, "123456", "0000123", 0).Err())
		require.NoError(t, kdb.Save(ctx).Err())
		srvRedis := util.StartRedisServer(t, map[string]string{
			"kvrocks-dir": srv.GetDir(),
		})
		defer srvRedis.Close()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
		require.Equal(t, "0000123", rdb.Get(ctx, "123456").Val())
	})

	t.Run("Type hash in rdb", func(t *testing.T) {
		require.NoError(t, kdb.HMSet(ctx, "hfoo", "k1", "f1", "k2", "-123").Err())
		require.NoError(t, kdb.Save(ctx).Err())
		srvRedis := util.StartRedisServer(t, map[string]string{
			"kvrocks-dir": srv.GetDir(),
		})
		defer srvRedis.Close()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		require.Equal(t, "f1", rdb.HGet(ctx, "hfoo", "k1").Val())
		require.Equal(t, "-123", rdb.HGet(ctx, "hfoo", "k2").Val())
	})

	t.Run("Type list in rdb", func(t *testing.T) {
		require.NoError(t, kdb.LPush(ctx, "lfoo", "abc", "100000").Err())
		require.NoError(t, kdb.Save(ctx).Err())
		srvRedis := util.StartRedisServer(t, map[string]string{
			"kvrocks-dir": srv.GetDir(),
		})
		defer srvRedis.Close()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		require.Equal(t, "100000", rdb.LPop(ctx, "lfoo").Val())
		require.Equal(t, "abc", rdb.LPop(ctx, "lfoo").Val())
	})

	t.Run("Type set in rdb", func(t *testing.T) {
		require.NoError(t, kdb.SAdd(ctx, "sfoo", "-1000", "abcdefg", "100000").Err())
		require.NoError(t, kdb.Save(ctx).Err())
		srvRedis := util.StartRedisServer(t, map[string]string{
			"kvrocks-dir": srv.GetDir(),
		})
		defer srvRedis.Close()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		require.Equal(t, true, rdb.SIsMember(ctx, "sfoo", "-1000").Val())
		require.Equal(t, true, rdb.SIsMember(ctx, "sfoo", "abcdefg").Val())
		require.Equal(t, true, rdb.SIsMember(ctx, "sfoo", "100000").Val())
	})

	t.Run("Type zset in rdb", func(t *testing.T) {
		require.NoError(t, kdb.ZAdd(ctx, "zfoo", redis.Z{Score: 10, Member: "x"}).Err())
		require.NoError(t, kdb.ZAdd(ctx, "zfoo", redis.Z{Score: 20, Member: "y"}).Err())
		require.NoError(t, kdb.ZAdd(ctx, "zfoo", redis.Z{Score: 1.5, Member: "z"}).Err())
		require.NoError(t, kdb.Save(ctx).Err())
		srvRedis := util.StartRedisServer(t, map[string]string{
			"kvrocks-dir": srv.GetDir(),
		})
		defer srvRedis.Close()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		require.Equal(t, float64(10), rdb.ZScore(ctx, "zfoo", "x").Val())
		require.Equal(t, float64(20), rdb.ZScore(ctx, "zfoo", "y").Val())
		require.Equal(t, float64(1.5), rdb.ZScore(ctx, "zfoo", "z").Val())
	})

	t.Run("test expiration in rdb", func(t *testing.T) {
		require.NoError(t, kdb.Set(ctx, "exkey1", "val", time.Duration(1)*time.Second).Err())
		require.NoError(t, kdb.Set(ctx, "exkey2", "val", time.Duration(50)*time.Second).Err())
		require.NoError(t, kdb.Save(ctx).Err())
		srvRedis := util.StartRedisServer(t, map[string]string{
			"kvrocks-dir": srv.GetDir(),
		})
		time.Sleep(time.Duration(2) * time.Second)
		defer srvRedis.Close()
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		require.Equal(t, time.Duration(-2), rdb.TTL(ctx, "exkey1").Val())
		require.Less(t, rdb.TTL(ctx, "exkey2").Val(), time.Duration(50)*time.Second)
		require.Greater(t, rdb.TTL(ctx, "exkey2").Val(), time.Duration(1)*time.Second)
	})
}
