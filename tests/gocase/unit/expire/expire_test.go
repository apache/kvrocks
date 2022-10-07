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

package expire

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestExpire(t *testing.T) {
	svr := util.StartServer(t, map[string]string{})
	defer svr.Close()

	ctx := context.Background()
	rdb := svr.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("EXPIRE - set timeouts multiple times", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "foobar", 0).Err())
		require.Equal(t, true, rdb.Expire(ctx, "x", 5*time.Second).Val())
		ttl := rdb.TTL(ctx, "x").Val()
		require.LessOrEqual(t, ttl, 5*time.Second)
		require.GreaterOrEqual(t, ttl, 4*time.Second)
		require.Equal(t, true, rdb.Expire(ctx, "x", 10*time.Second).Val())
		require.Equal(t, 10*time.Second, rdb.TTL(ctx, "x").Val())
		require.NoError(t, rdb.Expire(ctx, "x", 2*time.Second).Err())
	})

	t.Run("EXPIRE - It should be still possible to read 'x'", func(t *testing.T) {
		require.Equal(t, "foobar", rdb.Get(ctx, "x").Val())
	})

	t.Run("EXPIRE - write on expire should work", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.LPush(ctx, "x", "foo").Err())
		require.NoError(t, rdb.Expire(ctx, "x", 1000*time.Second).Err())
		require.NoError(t, rdb.LPush(ctx, "x", "bar").Err())
		require.Equal(t, []string{"bar", "foo"}, rdb.LRange(ctx, "x", 0, -1).Val())
	})

	t.Run("EXPIREAT - Check for EXPIRE alike behavior", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", "foo", 0).Err())
		require.NoError(t, rdb.ExpireAt(ctx, "x", time.Now().Add(15*time.Second)).Err())
		ttl := rdb.TTL(ctx, "x").Val()
		require.GreaterOrEqual(t, ttl, 13*time.Second)
		require.LessOrEqual(t, ttl, 16*time.Second)
	})

	t.Run("SETEX - Set + Expire combo operation. Check for TTL", func(t *testing.T) {
		require.NoError(t, rdb.SetEx(ctx, "x", "test", 12*time.Second).Err())
		ttl := rdb.TTL(ctx, "x").Val()
		require.GreaterOrEqual(t, ttl, 10*time.Second)
		require.LessOrEqual(t, ttl, 12*time.Second)
	})

	t.Run("SETEX - Check value", func(t *testing.T) {
		require.Equal(t, "test", rdb.Get(ctx, "x").Val())
	})

	t.Run("SETEX - Overwrite old key", func(t *testing.T) {
		require.NoError(t, rdb.SetEx(ctx, "y", "foo", 1*time.Second).Err())
		require.Equal(t, "foo", rdb.Get(ctx, "y").Val())
	})

	t.Run("SETEX - Wrong time parameter", func(t *testing.T) {
		pattern := ".*invalid expire*."
		util.ErrorRegexp(t, rdb.SetEx(ctx, "z", "foo", -10).Err(), pattern)
	})

	t.Run("PERSIST can undo an EXPIRE", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "foo", 0).Err())
		require.NoError(t, rdb.Expire(ctx, "x", 12*time.Second).Err())
		ttl := rdb.TTL(ctx, "x").Val()
		require.GreaterOrEqual(t, ttl, 10*time.Second)
		require.LessOrEqual(t, ttl, 12*time.Second)
		require.Equal(t, true, rdb.Persist(ctx, "x").Val())
		require.Equal(t, time.Duration(-1), rdb.TTL(ctx, "x").Val())
		require.Equal(t, "foo", rdb.Get(ctx, "x").Val())
	})

	t.Run("PERSIST returns 0 against non existing or non volatile keys", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "foo", 0).Err())
		require.Equal(t, false, rdb.Persist(ctx, "foo").Val())
		require.Equal(t, false, rdb.Persist(ctx, "nokeyatall").Val())
	})

	t.Run("EXPIRE pricision is now the millisecond", func(t *testing.T) {
		a, b := "", ""
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Del(ctx, "x").Err())
			require.NoError(t, rdb.SetEx(ctx, "x", "somevalue", 1*time.Second).Err())
			time.Sleep(900 * time.Millisecond)
			a = rdb.Get(ctx, "x").Val()
			time.Sleep(1100 * time.Millisecond)
			b = rdb.Get(ctx, "x").Val()
			if a == "somevalue" && b == "" {
				break
			}
		}
		require.Equal(t, "somevalue", a)
		require.Equal(t, "", b)
	})

	t.Run("PEXPIRE/PSETEX/PEXPIREAT can set sub-second expires", func(t *testing.T) {
		a, b, c, d, e, f := "", "", "", "", "", ""
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Del(ctx, "x", "y", "z").Err())
			require.NoError(t, rdb.Set(ctx, "x", "somevalue", 100*time.Millisecond).Err())
			time.Sleep(80 * time.Millisecond)
			a = rdb.Get(ctx, "x").Val()
			time.Sleep(2100 * time.Millisecond)
			b = rdb.Get(ctx, "x").Val()

			require.NoError(t, rdb.Set(ctx, "x", "somevalue", 0).Err())
			require.NoError(t, rdb.PExpire(ctx, "x", 100*time.Millisecond).Err())
			time.Sleep(80 * time.Millisecond)
			c = rdb.Get(ctx, "x").Val()
			time.Sleep(2100 * time.Millisecond)
			d = rdb.Get(ctx, "x").Val()

			require.NoError(t, rdb.Set(ctx, "x", "somevalue", 0).Err())
			require.NoError(t, rdb.PExpireAt(ctx, "x", time.UnixMilli((time.Now().Unix()*1000+100))).Err())
			time.Sleep(80 * time.Millisecond)
			e = rdb.Get(ctx, "x").Val()
			time.Sleep(2100 * time.Millisecond)
			f = rdb.Get(ctx, "x").Val()

			if a == "somevalue" && b == "" && c == "somevalue" && d == "" && e == "somevalue" && f == "" {
				break
			}
		}
		require.Equal(t, "somevalue", a)
		require.Equal(t, "", b)
	})

	t.Run("TTL returns tiem to live in seconds", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.SetEx(ctx, "x", "somevalue", 10*time.Second).Err())
		ttl := rdb.TTL(ctx, "x").Val()
		require.Greater(t, ttl, 8*time.Second)
		require.LessOrEqual(t, ttl, 10*time.Second)
	})

	t.Run("PTTL returns time to live in milliseconds", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.SetEx(ctx, "x", "somevalue", 1*time.Second).Err())
		ttl := rdb.PTTL(ctx, "x").Val()
		require.Greater(t, ttl, 900*time.Millisecond)
		require.LessOrEqual(t, ttl, 1000*time.Millisecond)
	})

	t.Run("TTL / PTTL return -1 if key has no expire", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", "hello", 0).Err())
		require.Equal(t, time.Duration(-1), rdb.TTL(ctx, "x").Val())
		require.Equal(t, time.Duration(-1), rdb.PTTL(ctx, "x").Val())
	})

	t.Run("TTL / PTTL return -2 if key does not exit", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.Equal(t, time.Duration(-2), rdb.TTL(ctx, "x").Val())
		require.Equal(t, time.Duration(-2), rdb.PTTL(ctx, "x").Val())
	})

	t.Run("Redis should actively expire keys incrementally", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Do(ctx, "PSETEX", "key1", 500, "a").Err())
		require.NoError(t, rdb.Do(ctx, "PSETEX", "key2", 500, "a").Err())
		require.NoError(t, rdb.Do(ctx, "PSETEX", "key3", 500, "a").Err())
		//require.NoError(t, rdb.Set(ctx, "key1", "a", 500*time.Millisecond).Err())
		//require.NoError(t, rdb.Set(ctx, "key2", "a", 500*time.Millisecond).Err())
		//require.NoError(t, rdb.Set(ctx, "key3", "a", 500*time.Millisecond).Err())
		require.NoError(t, rdb.Do(ctx, "DBSIZE", "scan").Err())
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, int64(3), rdb.DBSize(ctx).Val())
		time.Sleep(2000 * time.Millisecond)
		require.NoError(t, rdb.Do(ctx, "DBSIZE", "scan").Err())
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, int64(0), rdb.DBSize(ctx).Val())
	})

	t.Run("5 keys in, 5 keys out", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "a", "c", 0).Err())
		require.NoError(t, rdb.Expire(ctx, "a", 5*time.Second).Err())
		require.NoError(t, rdb.Set(ctx, "t", "c", 0).Err())
		require.NoError(t, rdb.Set(ctx, "e", "c", 0).Err())
		require.NoError(t, rdb.Set(ctx, "s", "c", 0).Err())
		require.NoError(t, rdb.Set(ctx, "foo", "b", 0).Err())
		res := rdb.Keys(ctx, "*").Val()
		sort.Strings(res)
		require.Equal(t, []string{"a", "e", "foo", "s", "t"}, res)
	})

	t.Run("EXPIRE with empty string as TTL should report an error", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		pattern := ".*not an integer*."
		util.ErrorRegexp(t, rdb.Do(ctx, "expire", "foo", "").Err(), pattern)
	})

}
