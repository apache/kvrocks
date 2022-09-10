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

package unit

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestKeyspace(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("DEL against a single item", func(t *testing.T) {
		key := "x"
		value := "foo"
		assert.Nil(t, rdb.Set(ctx, key, value, 0).Err())
		assert.Equal(t, value, rdb.Get(ctx, key).Val())
		assert.Equal(t, int64(1), rdb.Del(ctx, key).Val())
		assert.Equal(t, "", rdb.Get(ctx, key).Val())
	})

	t.Run("Vararg DEL", func(t *testing.T) {
		assert.Nil(t, rdb.Set(ctx, "foo1", "a", 0).Err())
		assert.Nil(t, rdb.Set(ctx, "foo2", "b", 0).Err())
		assert.Nil(t, rdb.Set(ctx, "foo3", "c", 0).Err())
		assert.Equal(t, int64(3), rdb.Del(ctx, "foo1", "foo2", "foo3").Val())
		assert.Equal(t, []interface{}{nil, nil, nil}, rdb.MGet(ctx, "foo1", "foo2", "foo3").Val())
	})

	t.Run("KEYS with pattern", func(t *testing.T) {
		for _, key := range []string{"key_x", "key_y", "key_z", "foo_a", "foo_b", "foo_c"} {
			assert.Nil(t, rdb.Set(ctx, key, "hello", 0).Err())
		}
		keys := rdb.Keys(ctx, "foo*").Val()
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		assert.Equal(t, []string{"foo_a", "foo_b", "foo_c"}, keys)
	})

	t.Run("KEYS to get all keys", func(t *testing.T) {
		keys := rdb.Keys(ctx, "*").Val()
		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})
		assert.Equal(t, []string{"foo_a", "foo_b", "foo_c", "key_x", "key_y", "key_z"}, keys)
	})

	t.Run("DEL all keys", func(t *testing.T) {
		vals := rdb.Keys(ctx, "*").Val()
		assert.Equal(t, int64(len(vals)), rdb.Del(ctx, vals...).Val())
		assert.Nil(t, rdb.Do(ctx, "dbsize", "scan").Err())
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, int64(0), rdb.Do(ctx, "dbsize").Val())
	})

	t.Run("EXISTS", func(t *testing.T) {
		newKey := "newkey"
		assert.Nil(t, rdb.Set(ctx, newKey, "test", 0).Err())
		assert.Equal(t, int64(1), rdb.Exists(ctx, newKey).Val())
		assert.Equal(t, int64(1), rdb.Del(ctx, newKey).Val())
		assert.Equal(t, int64(0), rdb.Exists(ctx, newKey).Val())
	})

	t.Run("Zero length value in key. SET/GET/EXISTS", func(t *testing.T) {
		emptyKey := "emptykey"
		assert.Nil(t, rdb.Set(ctx, emptyKey, nil, 0).Err())
		assert.Equal(t, int64(1), rdb.Exists(ctx, emptyKey).Val())
		assert.Equal(t, int64(1), rdb.Del(ctx, emptyKey).Val())
		assert.Equal(t, int64(0), rdb.Exists(ctx, emptyKey).Val())
	})

	t.Run("Commands pipelining", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("SET k1 xyzk\r\nGET k1\r\nPING\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		assert.Equal(t, "+OK", r)
		r, err = c.ReadLine()
		require.NoError(t, err)
		assert.Equal(t, "$4", r)
		r, err = c.ReadLine()
		require.NoError(t, err)
		assert.Equal(t, "xyzk", r)
		r, err = c.ReadLine()
		require.NoError(t, err)
		assert.Equal(t, "+PONG", r)
	})

	t.Run("Non existing command", func(t *testing.T) {
		assert.Contains(t, rdb.Do(ctx, "foobaredcommand").Err().Error(), "ERR")
	})

	t.Run("RANDOMKEY", func(t *testing.T) {
		rdb.FlushDB(ctx)
		assert.Nil(t, rdb.Set(ctx, "foo", "x", 0).Err())
		assert.Nil(t, rdb.Set(ctx, "bar", "y", 0).Err())

		fooSeen := false
		barSeen := false
		for i := 0; i < 1000; i++ {
			if rdb.RandomKey(ctx).Val() == "foo" {
				fooSeen = true
			} else if rdb.RandomKey(ctx).Val() == "bar" {
				barSeen = true
			}
		}
		assert.True(t, fooSeen || barSeen)
	})

	t.Run("RANDOMKEY against empty DB", func(t *testing.T) {
		rdb.FlushDB(ctx)
		assert.Equal(t, "", rdb.RandomKey(ctx).Val())
	})

	t.Run("RANDOMKEY regression 1", func(t *testing.T) {
		rdb.FlushDB(ctx)
		assert.Nil(t, rdb.Set(ctx, "x", 10, 0).Err())
		assert.Equal(t, int64(1), rdb.Del(ctx, "x").Val())
		assert.Equal(t, "", rdb.RandomKey(ctx).Val())
	})

	t.Run("KEYS * two times with long key, Github issue #1208", func(t *testing.T) {
		rdb.FlushDB(ctx)
		assert.Nil(t, rdb.Set(ctx, "dlskeriewrioeuwqoirueioqwrueoqwrueqw", "teset", 0).Err())
		assert.Equal(t, []string{"dlskeriewrioeuwqoirueioqwrueoqwrueqw"}, rdb.Keys(ctx, "*").Val())
	})

	t.Run("KEYS with multi namespace", func(t *testing.T) {
		rdb.FlushDB(ctx)
		rdb.ConfigSet(ctx, "requirepass", "foobared")
		rdb.Do(ctx, "namespace", "add", "test_ns1", "test_ns_token1")
		rdb.Do(ctx, "namespace", "add", "test_ns2", "test_ns_token2")

		ns1Keys := []string{"foo_a", "foo_b", "foo_c", "key_l"}
		ns2Keys := []string{"foo_d", "foo_e", "foo_f", "key_m"}
		ns1PrefixKeys := []string{"foo_a", "foo_b", "foo_c"}
		ns2PrefixKeys := []string{"foo_d", "foo_e", "foo_f"}

		tokenKeys := map[string][]string{
			"test_ns_token1": ns1Keys,
			"test_ns_token2": ns2Keys,
		}
		tokenPrefixKeys := map[string][]string{
			"test_ns_token1": ns1PrefixKeys,
			"test_ns_token2": ns2PrefixKeys,
		}

		for token, keys := range tokenKeys {
			rdb.Do(ctx, "auth", token)
			for _, key := range keys {
				assert.Nil(t, rdb.Set(ctx, key, "hello", 0).Err())
			}
		}
		for token, keys := range tokenKeys {
			rdb.Do(ctx, "auth", token)
			gotKeys := rdb.Keys(ctx, "*").Val()
			sort.Slice(gotKeys, func(i, j int) bool {
				return gotKeys[i] < gotKeys[j]
			})
			assert.Equal(t, keys, gotKeys)
		}

		for token, prefixKeys := range tokenPrefixKeys {
			rdb.Do(ctx, "auth", token)
			gotKeys := rdb.Keys(ctx, "foo*").Val()
			sort.Slice(gotKeys, func(i, j int) bool {
				return gotKeys[i] < gotKeys[j]
			})
			assert.Equal(t, prefixKeys, gotKeys)
		}
	})
}
