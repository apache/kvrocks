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
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func runWithFieldExpirationHash(t *testing.T, fn func(t *testing.T, rdb *redis.Client, ctx context.Context)) {
	t.Helper()

	srv := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode": "field-expiration",
		"resp3-enabled":      "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	fn(t, rdb, ctx)
}

func requireHashMetadata(t *testing.T, meta util.KMetadataResponse, size, persist int64) {
	t.Helper()

	require.Equal(t, "hash", meta.Type)
	require.Equal(t, "field-expiration", meta.Mode)
	require.Equal(t, size, meta.Size)
	require.Equal(t, persist, meta.Persist)
	require.LessOrEqual(t, meta.Persist, meta.Size)
	if meta.Size == meta.Persist {
		require.Equal(t, int64(0), meta.Lower)
		require.Equal(t, int64(0), meta.Upper)
	} else {
		require.Greater(t, meta.Lower, int64(0))
		require.GreaterOrEqual(t, meta.Upper, meta.Lower)
	}
}

func waitHashFieldExpired(t *testing.T, rdb *redis.Client, ctx context.Context, key, field string) {
	t.Helper()

	require.Eventually(t, func() bool {
		err := rdb.HGet(ctx, key, field).Err()
		return errors.Is(err, redis.Nil)
	}, 3*time.Second, 50*time.Millisecond)
}

func requireIntArray(t *testing.T, got interface{}, want []int64) {
	t.Helper()

	items, ok := got.([]interface{})
	require.Truef(t, ok, "expected []interface{}, got %T", got)
	require.Len(t, items, len(want))
	for i, item := range items {
		require.Equal(t, want[i], item)
	}
}

func TestHashFieldExpirationMetadataLifecycle(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-lifecycle"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 60, "FIELDS", 1, "a").Val(), []int64{1})
		m1 := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, m1, 2, 1)
		require.Equal(t, m1.Lower, m1.Upper)

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 120, "FIELDS", 1, "b").Val(), []int64{1})
		m2 := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, m2, 2, 0)
		require.Equal(t, m1.Lower, m2.Lower)
		require.Greater(t, m2.Upper, m1.Upper)

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 30, "LT", "FIELDS", 1, "b").Val(), []int64{1})
		m3 := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, m3, 2, 0)
		require.Less(t, m3.Lower, m2.Lower)
		require.Equal(t, m2.Upper, m3.Upper)

		requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, "b").Val(), []int64{1})
		m4 := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, m4, 2, 1)
		require.Equal(t, m3.Lower, m4.Lower)
		require.Equal(t, m3.Upper, m4.Upper)

		requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, "a").Val(), []int64{1})
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
	})
}

func TestHashFieldExpirationFiltersReadsWithoutMutatingMetadata(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-read-filter"
		require.Equal(t, int64(3), rdb.HSet(ctx, key, "a", "1", "b", "2", "c", "3").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "a").Val(), []int64{1})
		before := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, before, 3, 2)
		waitHashFieldExpired(t, rdb, ctx, key, "a")

		require.ErrorIs(t, rdb.HGet(ctx, key, "a").Err(), redis.Nil)
		require.False(t, rdb.HExists(ctx, key, "a").Val())
		require.Equal(t, int64(0), rdb.HStrLen(ctx, key, "a").Val())
		require.Equal(t, []interface{}{nil, "2"}, rdb.HMGet(ctx, key, "a", "b").Val())

		all := rdb.HGetAll(ctx, key).Val()
		require.NotContains(t, all, "a")
		keys := rdb.HKeys(ctx, key).Val()
		require.NotContains(t, keys, "a")
		values := rdb.HVals(ctx, key).Val()
		require.ElementsMatch(t, []string{"2", "3"}, values)
		scanned, _, err := rdb.HScan(ctx, key, 0, "", 10).Result()
		require.NoError(t, err)
		require.NotContains(t, scanned, "a")
		scanned, cursor, err := rdb.HScan(ctx, key, 0, "", 1).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"b", "2"}, scanned)
		require.NotZero(t, cursor)
		rangeByLex := rdb.Do(ctx, "hrangebylex", key, "[a", "[z").Val()
		require.NotContains(t, rangeByLex, "a")
		randField := rdb.HRandField(ctx, key, 10).Val()
		require.NotContains(t, randField, "a")
		require.Equal(t, int64(3), rdb.HLen(ctx, key).Val())

		after := util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, before, after)
	})
}

func TestHashFieldExpirationWriteCleanupMetadata(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		makeExpired := func(t *testing.T, key, value string) {
			t.Helper()
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", value, "b", "2").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "a").Val(), []int64{1})
			waitHashFieldExpired(t, rdb, ctx, key, "a")
		}

		t.Run("hdel", func(t *testing.T) {
			key := "hfe-cleanup-hdel"
			makeExpired(t, key, "1")
			require.Equal(t, int64(0), rdb.HDel(ctx, key, "a").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		})

		t.Run("hpersist", func(t *testing.T) {
			key := "hfe-cleanup-hpersist"
			makeExpired(t, key, "1")
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, "a").Val(), []int64{-2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		})

		t.Run("hexpire", func(t *testing.T) {
			key := "hfe-cleanup-hexpire"
			makeExpired(t, key, "1")
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 60, "FIELDS", 1, "a").Val(), []int64{-2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		})

		t.Run("hset", func(t *testing.T) {
			key := "hfe-cleanup-hset"
			makeExpired(t, key, "1")
			require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "new").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
			require.Equal(t, "new", rdb.HGet(ctx, key, "a").Val())
		})

		t.Run("hsetnx", func(t *testing.T) {
			key := "hfe-cleanup-hsetnx"
			makeExpired(t, key, "1")
			require.Equal(t, true, rdb.HSetNX(ctx, key, "a", "new").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
		})

		t.Run("hincrby", func(t *testing.T) {
			key := "hfe-cleanup-hincrby"
			makeExpired(t, key, "bad")
			require.Equal(t, int64(2), rdb.HIncrBy(ctx, key, "a", 2).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
		})

		t.Run("hincrbyfloat", func(t *testing.T) {
			key := "hfe-cleanup-hincrbyfloat"
			makeExpired(t, key, "bad")
			require.Equal(t, 1.5, rdb.HIncrByFloat(ctx, key, "a", 1.5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
		})
	})
}

func TestHashFieldExpirationOptionsAndDuplicates(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-options"
		require.Equal(t, int64(3), rdb.HSet(ctx, key, "a", "1", "b", "2", "c", "3").Val())

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 10, "NX", "FIELDS", 2, "a", "a").Val(), []int64{1, 0})
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 2)
		requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 2, "a", "a").Val(), []int64{1, -1})
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 3)

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "GT", "FIELDS", 1, "b").Val(), []int64{0})
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 3)
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "LT", "FIELDS", 1, "b").Val(), []int64{2})
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "FIELDS", 2, "c", "c").Val(), []int64{2, -2})
		require.Equal(t, int64(1), rdb.HLen(ctx, key).Val())
	})
}

func TestHashFieldExpirationHLenMetadataSize(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-hlen"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "a").Val(), []int64{1})
		waitHashFieldExpired(t, rdb, ctx, key, "a")
		require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
		require.Equal(t, map[string]string{"b": "2"}, rdb.HGetAll(ctx, key).Val())
	})
}

func TestHashFieldExpirationLegacyRejectsFieldTTLCommands(t *testing.T) {
	srv := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode": "legacy",
		"resp3-enabled":      "yes",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "hfe-legacy"
	require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
	require.Error(t, rdb.Do(ctx, "hexpire", key, 10, "FIELDS", 1, "a").Err())
	require.Error(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, "a").Err())
	require.Equal(t, "1", rdb.HGet(ctx, key, "a").Val())
}

func TestHashFieldExpirationParseErrors(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-parse"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
		for _, args := range [][]interface{}{
			{"hexpire", key, 10, "FIELDS", 0},
			{"hexpire", key, 10, "FIELDS", 2, "a"},
			{"hexpire", key, 10, "NX", "XX", "FIELDS", 1, "a"},
			{"hexpire", key, 10, "FIELDS", 1, "a", "NX"},
			{"hexpire", key, "not-int", "FIELDS", 1, "a"},
			{"hpersist", key, "FIELDS", 0},
			{"hpersist", key, "FIELDS", 2, "a"},
		} {
			require.Error(t, rdb.Do(ctx, args...).Err(), args)
		}
	})
}

func TestHashFieldExpirationReadCommandSet(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-read-command-set"
		require.Equal(t, int64(4), rdb.HSet(ctx, key, "a", "1", "b", "2", "c", "3", "d", "4").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 2, "a", "c").Val(), []int64{1, 1})
		waitHashFieldExpired(t, rdb, ctx, key, "a")
		waitHashFieldExpired(t, rdb, ctx, key, "c")

		keys := rdb.HKeys(ctx, key).Val()
		sort.Strings(keys)
		require.Equal(t, []string{"b", "d"}, keys)
		require.ElementsMatch(t, []string{"2", "4"}, rdb.HVals(ctx, key).Val())
		require.Equal(t, []interface{}{"b", "2", "d", "4"}, rdb.Do(ctx, "hrangebylex", key, "[a", "[z").Val())
		require.Equal(t, []interface{}{"d", "4"}, rdb.Do(ctx, "hrangebylex", key, "[a", "[z", "LIMIT", 1, 1).Val())
	})
}

func TestHashFieldExpirationRandFieldAllExpired(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-rand-all-expired"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 2, "a", "b").Val(), []int64{1, 1})
		waitHashFieldExpired(t, rdb, ctx, key, "a")
		waitHashFieldExpired(t, rdb, ctx, key, "b")

		require.Nil(t, rdb.Do(ctx, "hrandfield", key).Val())
		require.Equal(t, []interface{}{}, rdb.Do(ctx, "hrandfield", key, 10).Val())
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 0)
	})
}
