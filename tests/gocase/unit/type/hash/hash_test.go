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

package hash

import (
	"context"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestHash(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	kvArray := []string{"a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "key1", "value1", "key2", "value2", "key3", "value3", "key10", "value10", "z", "z", "x", "x"}
	t.Run("HRange normal situation ", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
		require.EqualValues(t, []interface{}{"key1", "value1", "key10", "value10"}, rdb.Do(ctx, "HRange", "hashkey", "key1", "key2", "limit", 100).Val())
		require.EqualValues(t, []interface{}{"key1", "value1", "key10", "value10", "key2", "value2"}, rdb.Do(ctx, "HRange", "hashkey", "key1", "key3", "limit", 100).Val())
	})

	t.Run("HRange stop <= start", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HRange", "hashkey", "key2", "key1", "limit", 100).Val())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HRange", "hashkey", "key1", "key1", "limit", 100).Val())
	})

	t.Run("HRange limit", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
		require.EqualValues(t, []interface{}{"a", "a", "b", "b"}, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit", 2).Val())
		require.EqualValues(t, []interface{}{"a", "a", "b", "b", "c", "c", "d", "d", "e", "e", "key1", "value1", "key10", "value10", "key2", "value2", "key3", "value3", "x", "x", "z", "z"}, rdb.Do(ctx, "HRange", "hashkey", "a", "zzz", "limit", 10000).Val())
	})

	t.Run("HRange limit is negative", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit", -100).Val())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit", 0).Val())
	})

	t.Run("HRange nonexistent key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit", 10000).Val())
		require.EqualValues(t, []interface{}{}, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit", 10000).Val())
	})

	t.Run("HRange limit typo", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
		require.ErrorContains(t, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limitzz", 10000).Err(), "ERR syntax")
	})

	t.Run("HRange wrong number of arguments", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "hashkey").Err())
		require.NoError(t, rdb.HMSet(ctx, "hashkey", kvArray).Err())
		require.ErrorContains(t, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit", 10000, "a").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HRange", "hashkey", "a", "z", "limit").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HRange", "hashkey", "a").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HRange", "hashkey").Err(), "wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "HRange").Err(), "wrong number of arguments")
	})

}
