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

package migrate

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMigrate(t *testing.T) {
	srv1 := util.StartServer(t, map[string]string{})
	srv2 := util.StartServer(t, map[string]string{})
	srv3 := util.StartServer(t, map[string]string{
		"requirepass": "pwd",
	})

	defer srv1.Close()
	defer srv2.Close()
	defer srv3.Close()

	ctx := context.Background()
	rdb1 := srv1.NewClient()
	rdb2 := srv2.NewClient()
	rdb3 := srv3.NewClientWithOption(&redis.Options{
		Password: "pwd",
	})
	defer func() { require.NoError(t, rdb1.Close()) }()
	defer func() { require.NoError(t, rdb2.Close()) }()
	defer func() { require.NoError(t, rdb3.Close()) }()

	timeout := 50 * time.Millisecond

	t.Run("test basic migration", func(t *testing.T) {
		keyValues := map[string]string{
			"test_string_key0": "test value 0",
			"test_string_key1": "test value 1",
			"test_string_key2": "test value 2",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
			require.NoError(t, rdb1.Migrate(ctx, srv2.Host(), fmt.Sprint(srv2.Port()), key, 0, timeout).Err())
			require.Equal(t, value, rdb2.Get(ctx, key).Val())
			require.Equal(t, redis.Nil, rdb1.Get(ctx, key).Err())
		}
	})

	t.Run("test key already existed", func(t *testing.T) {
		keyValues := map[string]string{
			"test_string_key3": "test value 3",
			"test_string_key4": "test value 4",
			"test_string_key5": "test value 5",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
			require.NoError(t, rdb2.Set(ctx, key, value, 0).Err())
		}
		for key := range keyValues {
			require.NotNil(t, rdb1.Migrate(ctx, srv2.Host(), fmt.Sprint(srv2.Port()), key, 0, timeout).Err())
		}
	})

	t.Run("test full migrate command", func(t *testing.T) {
		keyValues := map[string]string{
			"test full migrate command1": "test full migrate command value1",
			"test full migrate command2": "test full migrate command value2",
			"test full migrate command3": "test full migrate command value3",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
		}
		keys := make([]interface{}, 0, len(keyValues))
		for key := range keyValues {
			keys = append(keys, key)
		}
		args := []interface{}{"MIGRATE", srv2.Host(), fmt.Sprint(srv2.Port()), "", 0, timeout, "COPY", "REPLACE", "KEYS"}
		args = append(args, keys...)
		require.NoError(t, rdb1.Do(ctx, args...).Err())
		for key, value := range keyValues {
			require.Equal(t, value, rdb2.Get(ctx, key).Val())
			require.Equal(t, value, rdb1.Get(ctx, key).Val())
		}
	})

	t.Run("test full migrate command without copy", func(t *testing.T) {
		keyValues := map[string]string{
			"test full migrate command4": "test full migrate command value4",
			"test full migrate command5": "test full migrate command value5",
			"test full migrate command6": "test full migrate command value6",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
		}
		keys := make([]interface{}, 0, len(keyValues))
		for key := range keyValues {
			keys = append(keys, key)
		}
		args := []interface{}{"MIGRATE", srv2.Host(), fmt.Sprint(srv2.Port()), "", 0, timeout, "REPLACE", "KEYS"}
		args = append(args, keys...)
		require.NoError(t, rdb1.Do(ctx, args...).Err())
		for key, value := range keyValues {
			require.Equal(t, value, rdb2.Get(ctx, key).Val())
			require.Equal(t, redis.Nil, rdb1.Get(ctx, key).Err())
		}
	})

	t.Run("test migrate with error password", func(t *testing.T) {
		keyValues := map[string]string{
			"test migrate with error password1": "test migrate with error password1",
			"test migrate with error password2": "test migrate with error password2",
			"test migrate with error password3": "test migrate with error password3",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
		}
		keys := make([]interface{}, 0, len(keyValues))
		for key := range keyValues {
			keys = append(keys, key)
		}
		args := []interface{}{"MIGRATE", srv3.Host(), fmt.Sprint(srv3.Port()), "", 0, timeout, "REPLACE", "AUTH", "wrong passwd", "KEYS"}
		args = append(args, keys...)
		require.NotNil(t, rdb1.Do(ctx, args...).Err())
		for key, value := range keyValues {
			require.Equal(t, value, rdb1.Get(ctx, key).Val())
		}
	})

	t.Run("test migrate with auth", func(t *testing.T) {
		keyValues := map[string]string{
			"test migrate with auth1": "test migrate with auth value1",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
		}
		keys := make([]interface{}, 0, len(keyValues))
		for key := range keyValues {
			keys = append(keys, key)
		}
		args := []interface{}{"MIGRATE", srv3.Host(), fmt.Sprint(srv3.Port()), "", 0, timeout, "REPLACE", "AUTH", "pwd", "KEYS"}
		args = append(args, keys...)
		require.Nil(t, rdb1.Do(ctx, args...).Err())
		for key, value := range keyValues {
			require.Equal(t, value, rdb3.Get(ctx, key).Val())
			require.Equal(t, redis.Nil, rdb1.Get(ctx, key).Err())
		}
	})

	t.Run("test migrate with invalid key command", func(t *testing.T) {
		keyValues := map[string]string{
			"test migrate with invalid key command": "test migrate with invalid key command value",
		}
		for key, value := range keyValues {
			require.NoError(t, rdb1.Set(ctx, key, value, 0).Err())
		}
		keys := make([]interface{}, 0, len(keyValues))
		for key := range keyValues {
			keys = append(keys, key)
		}
		args := []interface{}{"MIGRATE", srv2.Host(), fmt.Sprint(srv2.Port()), "key", 0, timeout, "REPLACE", "KEYS"}
		args = append(args, keys...)
		require.NotNil(t, rdb1.Do(ctx, args...).Err())
		for key, value := range keyValues {
			require.Equal(t, redis.Nil, rdb2.Get(ctx, key).Err())
			require.Equal(t, value, rdb1.Get(ctx, key).Val())
		}
	})

	t.Run("test migrate with duplicate keys in target server", func(t *testing.T) {
		require.NoError(t, rdb1.Set(ctx, "key1", "server1_value1", 0).Err())
		require.NoError(t, rdb1.Set(ctx, "key2", "server1_value2", 0).Err())
		require.NoError(t, rdb2.Set(ctx, "key1", "server2_value2", 0).Err())

		args := []interface{}{"MIGRATE", srv2.Host(), fmt.Sprint(srv2.Port()), "", 0, timeout, "KEYS", "key2", "key1"}
		require.NotNil(t, rdb1.Do(ctx, args...).Err())
		require.Equal(t, "server1_value1", rdb1.Get(ctx, "key1").Val())
		require.Equal(t, "server1_value2", rdb1.Get(ctx, "key2").Val())
		require.Equal(t, "server2_value2", rdb2.Get(ctx, "key1").Val())
	})

	t.Run("test partial migration", func(t *testing.T) {
		require.NoError(t, rdb1.Set(ctx, "p1", "q1", 0).Err())
		args := []interface{}{"MIGRATE", srv2.Host(), fmt.Sprint(srv2.Port()), "", 0, timeout, "KEYS", "p1", "p2", "p3"}
		require.Nil(t, rdb1.Do(ctx, args...).Err())

		require.Equal(t, "q1", rdb2.Get(ctx, "p1").Val())
	})
}
