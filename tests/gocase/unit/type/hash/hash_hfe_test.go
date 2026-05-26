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

const (
	hfePersistentField = "a-persistent"
	hfeLiveField       = "b-live"
	hfeExpiredField    = "c-expired"
	hfeMissingField    = "d-missing"
	hfeKeeperField     = "z-keeper"
	hfeLiveTTLSeconds  = 300
)

func runWithFieldExpirationHash(t *testing.T, fn func(t *testing.T, rdb *redis.Client, ctx context.Context)) {
	t.Helper()

	runWithFieldExpirationHashConfigs(t, nil, fn)
}

func runWithFieldExpirationHashConfigs(t *testing.T, configs util.KvrocksServerConfigs, fn func(t *testing.T, rdb *redis.Client, ctx context.Context)) {
	t.Helper()

	serverConfigs := util.KvrocksServerConfigs{
		"hash-encoding-mode": "field-expiration",
		"resp3-enabled":      "yes",
	}
	for k, v := range configs {
		serverConfigs[k] = v
	}
	srv := util.StartServer(t, serverConfigs)
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

func requireHLenCommandInfoFlags(t *testing.T, rdb *redis.Client, ctx context.Context, want []interface{}) {
	t.Helper()

	info, err := rdb.Do(ctx, "command", "info", "hlen").Slice()
	require.NoError(t, err)
	require.Len(t, info, 1)
	hlenInfo := info[0].([]interface{})
	require.Len(t, hlenInfo, 6)
	require.Equal(t, "hlen", hlenInfo[0])
	require.Equal(t, want, hlenInfo[2])
}

func waitHashFieldExpired(t *testing.T, rdb *redis.Client, ctx context.Context, key, field string) {
	t.Helper()

	require.Eventually(t, func() bool {
		err := rdb.HGet(ctx, key, field).Err()
		return errors.Is(err, redis.Nil)
	}, 5*time.Second, 50*time.Millisecond)
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

func createHashFieldStates(t *testing.T, rdb *redis.Client, ctx context.Context, key string) {
	t.Helper()

	require.Equal(t, int64(4), rdb.HSet(ctx, key,
		hfePersistentField, "10",
		hfeLiveField, "20",
		hfeExpiredField, "30",
		hfeKeeperField, "40").Val())
	requireIntArray(t, rdb.Do(ctx, "hexpire", key, hfeLiveTTLSeconds, "FIELDS", 1, hfeLiveField).Val(), []int64{1})
	requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, hfeExpiredField).Val(), []int64{1})
	waitHashFieldExpired(t, rdb, ctx, key, hfeExpiredField)
	requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
}

func scanPairsToMap(t *testing.T, pairs []string) map[string]string {
	t.Helper()

	require.Equal(t, 0, len(pairs)%2)
	result := make(map[string]string, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		result[pairs[i]] = pairs[i+1]
	}
	return result
}

func requireHashValues(t *testing.T, rdb *redis.Client, ctx context.Context, key string, want map[string]string) {
	t.Helper()

	for field, value := range want {
		require.Equal(t, value, rdb.HGet(ctx, key, field).Val(), field)
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
		rangeByLex := rdb.Do(ctx, "hrangebylex", key, "[a", "[zz", "LIMIT", 0, 10).Val()
		require.NotContains(t, rangeByLex, "a")
		randField := rdb.HRandField(ctx, key, 10).Val()
		require.NotContains(t, randField, "a")
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

func TestHashFieldExpirationReadCommandsAcrossFieldStates(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-read-state-matrix"
		createHashFieldStates(t, rdb, ctx, key)
		before := util.GetKMetadata(t, rdb, ctx, key)

		require.Equal(t, "10", rdb.HGet(ctx, key, hfePersistentField).Val())
		require.Equal(t, "20", rdb.HGet(ctx, key, hfeLiveField).Val())
		require.ErrorIs(t, rdb.HGet(ctx, key, hfeExpiredField).Err(), redis.Nil)
		require.ErrorIs(t, rdb.HGet(ctx, key, hfeMissingField).Err(), redis.Nil)

		require.True(t, rdb.HExists(ctx, key, hfePersistentField).Val())
		require.True(t, rdb.HExists(ctx, key, hfeLiveField).Val())
		require.False(t, rdb.HExists(ctx, key, hfeExpiredField).Val())
		require.False(t, rdb.HExists(ctx, key, hfeMissingField).Val())

		require.Equal(t, int64(2), rdb.HStrLen(ctx, key, hfePersistentField).Val())
		require.Equal(t, int64(2), rdb.HStrLen(ctx, key, hfeLiveField).Val())
		require.Equal(t, int64(0), rdb.HStrLen(ctx, key, hfeExpiredField).Val())
		require.Equal(t, int64(0), rdb.HStrLen(ctx, key, hfeMissingField).Val())

		require.Equal(t, []interface{}{"10", "20", nil, nil},
			rdb.HMGet(ctx, key, hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val())

		require.Equal(t, map[string]string{
			hfePersistentField: "10",
			hfeLiveField:       "20",
			hfeKeeperField:     "40",
		}, rdb.HGetAll(ctx, key).Val())
		require.ElementsMatch(t, []string{hfePersistentField, hfeLiveField, hfeKeeperField}, rdb.HKeys(ctx, key).Val())
		require.ElementsMatch(t, []string{"10", "20", "40"}, rdb.HVals(ctx, key).Val())

		scanned, cursor, err := rdb.HScan(ctx, key, 0, "", 100).Result()
		require.NoError(t, err)
		require.Zero(t, cursor)
		require.Equal(t, map[string]string{
			hfePersistentField: "10",
			hfeLiveField:       "20",
			hfeKeeperField:     "40",
		}, scanPairsToMap(t, scanned))

		scannedKeys, cursor, err := rdb.HScanNoValues(ctx, key, 0, "", 100).Result()
		require.NoError(t, err)
		require.Zero(t, cursor)
		require.ElementsMatch(t, []string{hfePersistentField, hfeLiveField, hfeKeeperField}, scannedKeys)

		rangeByLex := rdb.Do(ctx, "hrangebylex", key, "[a", "[zz", "LIMIT", 0, 10).Val()
		require.Equal(t, []interface{}{
			hfePersistentField, "10",
			hfeLiveField, "20",
			hfeKeeperField, "40",
		}, rangeByLex)
		require.Equal(t, []interface{}{hfeLiveField, "20"},
			rdb.Do(ctx, "hrangebylex", key, "[a", "[zz", "LIMIT", 1, 1).Val())
		require.Equal(t, []interface{}{
			hfeKeeperField, "40",
			hfeLiveField, "20",
			hfePersistentField, "10",
		}, rdb.Do(ctx, "hrangebylex", key, "[zz", "[a", "REV", "LIMIT", 0, 10).Val())

		randFields := rdb.HRandField(ctx, key, 20).Val()
		require.ElementsMatch(t, []string{hfePersistentField, hfeLiveField, hfeKeeperField}, randFields)
		randFields = rdb.HRandField(ctx, key, -20).Val()
		require.NotContains(t, randFields, hfeExpiredField)
		require.NotContains(t, randFields, hfeMissingField)
		for _, field := range randFields {
			require.Contains(t, []string{hfePersistentField, hfeLiveField, hfeKeeperField}, field)
		}
		randWithValues := rdb.HRandFieldWithValues(ctx, key, 20).Val()
		gotRandValues := map[string]string{}
		for _, kv := range randWithValues {
			gotRandValues[kv.Key] = kv.Value
		}
		require.Equal(t, map[string]string{
			hfePersistentField: "10",
			hfeLiveField:       "20",
			hfeKeeperField:     "40",
		}, gotRandValues)

		after := util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, before, after)
	})
}

func TestHashFieldExpirationWriteCommandsAcrossFieldStates(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("hdel mixed fields", func(t *testing.T) {
			key := "hfe-write-hdel-mixed"
			createHashFieldStates(t, rdb, ctx, key)
			require.Equal(t, int64(2),
				rdb.HDel(ctx, key, hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField, hfePersistentField).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
			require.Equal(t, map[string]string{hfeKeeperField: "40"}, rdb.HGetAll(ctx, key).Val())
		})

		t.Run("hset clears ttl and treats expired as new", func(t *testing.T) {
			key := "hfe-write-hset-mixed"
			createHashFieldStates(t, rdb, ctx, key)
			require.Equal(t, int64(2), rdb.HSet(ctx, key,
				hfePersistentField, "11",
				hfeLiveField, "21",
				hfeExpiredField, "31",
				hfeMissingField, "41").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
			requireHashValues(t, rdb, ctx, key, map[string]string{
				hfePersistentField: "11",
				hfeLiveField:       "21",
				hfeExpiredField:    "31",
				hfeMissingField:    "41",
				hfeKeeperField:     "40",
			})
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{-1, -1, -1, -1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
		})

		t.Run("hmset clears ttl and returns ok", func(t *testing.T) {
			key := "hfe-write-hmset-mixed"
			createHashFieldStates(t, rdb, ctx, key)
			require.True(t, rdb.HMSet(ctx, key,
				hfePersistentField, "11",
				hfeLiveField, "21",
				hfeExpiredField, "31",
				hfeMissingField, "41").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
			requireHashValues(t, rdb, ctx, key, map[string]string{
				hfePersistentField: "11",
				hfeLiveField:       "21",
				hfeExpiredField:    "31",
				hfeMissingField:    "41",
				hfeKeeperField:     "40",
			})
		})

		t.Run("hsetnx writes only missing and expired fields", func(t *testing.T) {
			key := "hfe-write-hsetnx-mixed"
			createHashFieldStates(t, rdb, ctx, key)
			require.Equal(t, int64(2), rdb.Do(ctx, "hsetnx", key,
				hfePersistentField, "11",
				hfeLiveField, "21",
				hfeExpiredField, "31",
				hfeMissingField, "41").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 4)
			require.Equal(t, "10", rdb.HGet(ctx, key, hfePersistentField).Val())
			require.Equal(t, "20", rdb.HGet(ctx, key, hfeLiveField).Val())
			require.Equal(t, "31", rdb.HGet(ctx, key, hfeExpiredField).Val())
			require.Equal(t, "41", rdb.HGet(ctx, key, hfeMissingField).Val())
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, hfeLiveField).Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
		})

		t.Run("hincrby keeps live ttl and ignores expired value", func(t *testing.T) {
			key := "hfe-write-hincrby-mixed"
			createHashFieldStates(t, rdb, ctx, key)
			require.Equal(t, int64(15), rdb.HIncrBy(ctx, key, hfePersistentField, 5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
			require.Equal(t, int64(25), rdb.HIncrBy(ctx, key, hfeLiveField, 5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
			require.Equal(t, int64(5), rdb.HIncrBy(ctx, key, hfeExpiredField, 5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 3)
			require.Equal(t, int64(5), rdb.HIncrBy(ctx, key, hfeMissingField, 5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 4)
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, hfeLiveField).Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
		})

		t.Run("hincrbyfloat keeps live ttl and ignores expired value", func(t *testing.T) {
			key := "hfe-write-hincrbyfloat-mixed"
			createHashFieldStates(t, rdb, ctx, key)
			require.Equal(t, 10.5, rdb.HIncrByFloat(ctx, key, hfePersistentField, 0.5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
			require.Equal(t, 20.5, rdb.HIncrByFloat(ctx, key, hfeLiveField, 0.5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
			require.Equal(t, 0.5, rdb.HIncrByFloat(ctx, key, hfeExpiredField, 0.5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 3)
			require.Equal(t, 0.5, rdb.HIncrByFloat(ctx, key, hfeMissingField, 0.5).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 4)
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, hfeLiveField).Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
		})
	})
}

func TestHashFieldExpirationSetExpireAcrossFieldStates(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-hsetexpire-mixed"
		createHashFieldStates(t, rdb, ctx, key)

		require.Equal(t, "OK", rdb.Do(ctx, "hsetexpire", key, 60,
			hfePersistentField, "11",
			hfeLiveField, "21",
			hfeExpiredField, "31",
			hfeMissingField, "41").Val())
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
		requireHashValues(t, rdb, ctx, key, map[string]string{
			hfePersistentField: "11",
			hfeLiveField:       "21",
			hfeExpiredField:    "31",
			hfeMissingField:    "41",
			hfeKeeperField:     "40",
		})
		require.Greater(t, rdb.TTL(ctx, key).Val(), time.Duration(0))
		requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 4,
			hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{-1, -1, -1, -1})
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 5)
	})
}

func TestHashFieldExpirationExpireAndPersistAcrossFieldStates(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("hexpire mixed field states", func(t *testing.T) {
			key := "hfe-hexpire-mixed"
			createHashFieldStates(t, rdb, ctx, key)

			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 600, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{1, 1, -2, -2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 1)
			require.Equal(t, map[string]string{
				hfePersistentField: "10",
				hfeLiveField:       "20",
				hfeKeeperField:     "40",
			}, rdb.HGetAll(ctx, key).Val())
		})

		t.Run("hexpire nx only persistent", func(t *testing.T) {
			key := "hfe-hexpire-nx"
			createHashFieldStates(t, rdb, ctx, key)

			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 600, "NX", "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{1, 0, -2, -2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 1)
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 3,
				hfePersistentField, hfeLiveField, hfeKeeperField).Val(), []int64{1, 1, -1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 3)
		})

		t.Run("hexpire xx only live ttl", func(t *testing.T) {
			key := "hfe-hexpire-xx"
			createHashFieldStates(t, rdb, ctx, key)

			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 600, "XX", "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{0, 1, -2, -2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 2)
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, hfeLiveField).Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 3)
		})

		t.Run("hexpire gt and lt compare against current ttl", func(t *testing.T) {
			key := "hfe-hexpire-gt-lt"
			createHashFieldStates(t, rdb, ctx, key)

			requireIntArray(t, rdb.Do(ctx, "hexpire", key, hfeLiveTTLSeconds-60, "GT", "FIELDS", 1, hfeLiveField).Val(),
				[]int64{0})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, hfeLiveTTLSeconds+600, "GT", "FIELDS", 1, hfeLiveField).Val(),
				[]int64{1})
			afterGT := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, afterGT, 4, 2)

			requireIntArray(t, rdb.Do(ctx, "hexpire", key, hfeLiveTTLSeconds+1200, "LT", "FIELDS", 1, hfeLiveField).Val(),
				[]int64{0})
			require.Equal(t, afterGT, util.GetKMetadata(t, rdb, ctx, key))
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, hfeLiveTTLSeconds, "LT", "FIELDS", 1, hfeLiveField).Val(),
				[]int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 4, 2)
		})

		t.Run("hexpire immediate mixed field states", func(t *testing.T) {
			key := "hfe-hexpire-immediate"
			createHashFieldStates(t, rdb, ctx, key)

			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{2, 2, -2, -2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
			require.Equal(t, map[string]string{hfeKeeperField: "40"}, rdb.HGetAll(ctx, key).Val())
		})

		t.Run("hpersist mixed field states", func(t *testing.T) {
			key := "hfe-hpersist-mixed"
			createHashFieldStates(t, rdb, ctx, key)

			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(), []int64{-1, 1, -2, -2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 3)
			require.Equal(t, map[string]string{
				hfePersistentField: "10",
				hfeLiveField:       "20",
				hfeKeeperField:     "40",
			}, rdb.HGetAll(ctx, key).Val())
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

func TestHashFieldExpirationHLenFastPathAndRepair(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-hlen-repair"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "a").Val(), []int64{1})
		before := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, before, 2, 1)

		require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
		require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))

		waitHashFieldExpired(t, rdb, ctx, key, "a")
		require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
		require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
		require.Equal(t, int64(1), rdb.HLen(ctx, key).Val())
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		require.Equal(t, map[string]string{"b": "2"}, rdb.HGetAll(ctx, key).Val())

		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "b").Val(), []int64{1})
		waitHashFieldExpired(t, rdb, ctx, key, "b")
		require.Equal(t, int64(0), rdb.Do(ctx, "hlen", key, "REPAIR").Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
		require.Error(t, rdb.Do(ctx, "kmetadata", key).Err())
	})
}

func TestHashFieldExpirationHLenApproximateConfig(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"hash-length-mode": "approximate",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-hlen-approx-config"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "a").Val(), []int64{1})
		before := util.GetKMetadata(t, rdb, ctx, key)
		waitHashFieldExpired(t, rdb, ctx, key, "a")

		require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
		require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
		require.Equal(t, int64(1), rdb.Do(ctx, "hlen", key, "REPAIR").Val())
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
	})
}

func TestHashFieldExpirationHLenProposalFastPathTimeline(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-hlen-proposal-timeline"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "field1", "value1", "field2", "value2").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "field1").Val(), []int64{1})
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 4, "FIELDS", 1, "field2").Val(), []int64{1})

		initial := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, initial, 2, 0)
		require.Less(t, initial.Lower, initial.Upper)
		require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
		require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
		require.Equal(t, initial, util.GetKMetadata(t, rdb, ctx, key))

		waitHashFieldExpired(t, rdb, ctx, key, "field1")
		require.Equal(t, "value2", rdb.HGet(ctx, key, "field2").Val())
		require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
		require.Equal(t, initial, util.GetKMetadata(t, rdb, ctx, key))

		require.Equal(t, int64(1), rdb.HLen(ctx, key).Val())
		afterRepair := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, afterRepair, 1, 0)
		require.Equal(t, initial.Upper, afterRepair.Lower)
		require.Equal(t, initial.Upper, afterRepair.Upper)
		require.Equal(t, int64(1), rdb.Do(ctx, "hlen", key, "APPROX").Val())

		require.Equal(t, int64(1), rdb.HLen(ctx, key).Val())
		require.Equal(t, afterRepair, util.GetKMetadata(t, rdb, ctx, key))

		waitHashFieldExpired(t, rdb, ctx, key, "field2")
		require.Equal(t, int64(1), rdb.Do(ctx, "hlen", key, "APPROX").Val())
		require.Equal(t, afterRepair, util.GetKMetadata(t, rdb, ctx, key))
		require.Equal(t, int64(0), rdb.HLen(ctx, key).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
		require.Error(t, rdb.Do(ctx, "kmetadata", key).Err())
	})
}

func TestHashFieldExpirationHLenMetadataEffectsByPath(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("no ttl candidates fast path does not mutate metadata", func(t *testing.T) {
			key := "hfe-hlen-effect-persistent"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
			before := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, before, 2, 2)

			require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
			require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
		})

		t.Run("future ttl lower bound fast path does not mutate metadata", func(t *testing.T) {
			key := "hfe-hlen-effect-future"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "ttl", "1", "persist", "2").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 300, "FIELDS", 1, "ttl").Val(), []int64{1})
			before := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, before, 2, 1)

			require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
			require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
		})

		t.Run("slow repair removes expired ttl candidates and rewrites metadata", func(t *testing.T) {
			key := "hfe-hlen-effect-repair"
			require.Equal(t, int64(3), rdb.HSet(ctx, key, "persist", "1", "expired", "2", "live", "3").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "expired").Val(), []int64{1})
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 300, "FIELDS", 1, "live").Val(), []int64{1})
			before := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, before, 3, 1)
			require.Less(t, before.Lower, before.Upper)
			waitHashFieldExpired(t, rdb, ctx, key, "expired")

			require.Equal(t, int64(3), rdb.Do(ctx, "hlen", key, "APPROX").Val())
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
			require.Equal(t, int64(2), rdb.HLen(ctx, key).Val())
			afterRepair := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, afterRepair, 2, 1)
			require.Equal(t, before.Upper, afterRepair.Lower)
			require.Equal(t, before.Upper, afterRepair.Upper)
			require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
			require.Equal(t, map[string]string{"persist": "1", "live": "3"}, rdb.HGetAll(ctx, key).Val())
		})

		t.Run("all ttl candidates expired fast delete removes metadata", func(t *testing.T) {
			key := "hfe-hlen-effect-fast-delete"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 2, "a", "b").Val(), []int64{1, 1})
			before := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, before, 2, 0)
			waitHashFieldExpired(t, rdb, ctx, key, "a")
			waitHashFieldExpired(t, rdb, ctx, key, "b")

			require.Equal(t, int64(2), rdb.Do(ctx, "hlen", key, "APPROX").Val())
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
			require.Equal(t, int64(0), rdb.HLen(ctx, key).Val())
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
			require.Error(t, rdb.Do(ctx, "kmetadata", key).Err())
		})
	})
}

func TestHashFieldExpirationHLenParseErrors(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hfe-hlen-parse"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())

		require.ErrorContains(t, rdb.Do(ctx, "hlen", key, "BAD").Err(), "syntax")
		require.ErrorContains(t, rdb.Do(ctx, "hlen", key, "APPROX", "REPAIR").Err(), "wrong number")
	})
}

func TestHashFieldExpirationHLenReadonlyAndRepairFlags(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		requireHLenCommandInfoFlags(t, rdb, ctx, []interface{}{"readonly"})

		key := "hfe-hlen-flags"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
		requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "a").Val(), []int64{1})
		waitHashFieldExpired(t, rdb, ctx, key, "a")

		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1])`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
		require.Equal(t, int64(2), rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'APPROX')`, 1, key).Val())
		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'REPAIR')`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
	})
}

func TestHashFieldExpirationHLenAccurateConfigDynamicFlagsInEvalRO(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"hash-length-mode": "accurate",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		requireHLenCommandInfoFlags(t, rdb, ctx, []interface{}{"readonly"})
		key := "hfe-hlen-dynamic-accurate"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())

		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1])`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
		require.Equal(t, int64(1),
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'APPROX')`, 1, key).Val())
		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'REPAIR')`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
	})
}

func TestHashFieldExpirationHLenApproximateConfigDynamicFlagsInEvalRO(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"hash-length-mode": "approximate",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		requireHLenCommandInfoFlags(t, rdb, ctx, []interface{}{"readonly"})
		key := "hfe-hlen-dynamic-approx"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())

		require.Equal(t, int64(1),
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1])`, 1, key).Val())
		require.Equal(t, int64(1),
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'APPROX')`, 1, key).Val())
		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'REPAIR')`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
	})
}

func TestHashFieldExpirationHLenLegacyAccurateConfigDynamicFlagsInEvalRO(t *testing.T) {
	srv := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode": "legacy",
		"hash-length-mode":   "accurate",
		"resp3-enabled":      "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	requireHLenCommandInfoFlags(t, rdb, ctx, []interface{}{"readonly"})
	key := "hfe-hlen-dynamic-legacy"
	require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
	require.Equal(t, int64(1), rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1])`, 1, key).Val())
	require.Equal(t, int64(1), rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'APPROX')`, 1, key).Val())
	require.ErrorContains(t,
		rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'REPAIR')`, 1, key).Err(),
		"Write commands are not allowed from read-only scripts")
}

func TestHashFieldExpirationHLenLegacyConfigDefaultStaysReadonly(t *testing.T) {
	srv := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode": "legacy",
		"hash-length-mode":   "accurate",
		"resp3-enabled":      "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "hfe-hlen-legacy-flags"
	require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
	require.Equal(t, int64(1), rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1])`, 1, key).Val())
	require.ErrorContains(t,
		rdb.Do(ctx, "eval_ro", `return redis.call('hlen', KEYS[1], 'REPAIR')`, 1, key).Err(),
		"Write commands are not allowed from read-only scripts")
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

		for _, test := range []struct {
			name        string
			args        []interface{}
			errContains string
		}{
			{
				name:        "hexpire missing fields clause",
				args:        []interface{}{"hexpire", key, 10},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hexpire missing fields clause after option",
				args:        []interface{}{"hexpire", key, 10, "NX"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hexpire missing numfields",
				args:        []interface{}{"hexpire", key, 10, "FIELDS"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hexpire numfields is zero",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", 0, "a"},
				errContains: "integer",
			},
			{
				name:        "hexpire numfields is negative",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", -1, "a"},
				errContains: "integer",
			},
			{
				name:        "hexpire numfields is not an integer",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", "not-int", "a"},
				errContains: "integer",
			},
			{
				name:        "hexpire numfields is out of range",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", "9223372036854775808", "a"},
				errContains: "integer",
			},
			{
				name:        "hexpire has too few fields",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", 2, "a"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hexpire has too many fields",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", 1, "a", "b"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hexpire option after fields",
				args:        []interface{}{"hexpire", key, 10, "FIELDS", 1, "a", "NX"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hexpire unknown option",
				args:        []interface{}{"hexpire", key, 10, "UNKNOWN", "FIELDS", 1, "a"},
				errContains: "syntax",
			},
			{
				name:        "hexpire duplicate option",
				args:        []interface{}{"hexpire", key, 10, "NX", "NX", "FIELDS", 1, "a"},
				errContains: "syntax",
			},
			{
				name:        "hexpire mutually exclusive options",
				args:        []interface{}{"hexpire", key, 10, "NX", "XX", "FIELDS", 1, "a"},
				errContains: "syntax",
			},
			{
				name:        "hexpire ttl is not an integer",
				args:        []interface{}{"hexpire", key, "not-int", "FIELDS", 1, "a"},
				errContains: "integer",
			},
			{
				name:        "hexpire ttl is negative",
				args:        []interface{}{"hexpire", key, -1, "FIELDS", 1, "a"},
				errContains: "invalid expire time",
			},
			{
				name:        "hexpire ttl has trailing characters",
				args:        []interface{}{"hexpire", key, "10ms", "FIELDS", 1, "a"},
				errContains: "integer",
			},
			{
				name:        "hexpire ttl is out of int64 range",
				args:        []interface{}{"hexpire", key, "9223372036854775808", "FIELDS", 1, "a"},
				errContains: "integer",
			},
			{
				name:        "hpersist missing fields clause",
				args:        []interface{}{"hpersist", key},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hpersist wrong fields keyword",
				args:        []interface{}{"hpersist", key, "FIELD", 1, "a"},
				errContains: "syntax",
			},
			{
				name:        "hpersist missing numfields",
				args:        []interface{}{"hpersist", key, "FIELDS"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hpersist numfields is zero",
				args:        []interface{}{"hpersist", key, "FIELDS", 0, "a"},
				errContains: "integer",
			},
			{
				name:        "hpersist numfields is negative",
				args:        []interface{}{"hpersist", key, "FIELDS", -1, "a"},
				errContains: "integer",
			},
			{
				name:        "hpersist numfields is not an integer",
				args:        []interface{}{"hpersist", key, "FIELDS", "not-int", "a"},
				errContains: "integer",
			},
			{
				name:        "hpersist numfields is out of range",
				args:        []interface{}{"hpersist", key, "FIELDS", "9223372036854775808", "a"},
				errContains: "integer",
			},
			{
				name:        "hpersist has too few fields",
				args:        []interface{}{"hpersist", key, "FIELDS", 2, "a"},
				errContains: "wrong number of arguments",
			},
			{
				name:        "hpersist has too many fields",
				args:        []interface{}{"hpersist", key, "FIELDS", 1, "a", "b"},
				errContains: "wrong number of arguments",
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				require.ErrorContains(t, rdb.Do(ctx, test.args...).Err(), test.errContains)
			})
		}
	})
}

func TestHashFieldExpirationInputCornerCases(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("hexpire zero ttl deletes immediately", func(t *testing.T) {
			key := "hfe-zero-ttl"
			require.Equal(t, int64(3), rdb.HSet(ctx, key, "a", "1", "b", "2", "keeper", "3").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "FIELDS", 3, "a", "b", "missing").Val(),
				[]int64{2, 2, -2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
			require.Equal(t, map[string]string{"keeper": "3"}, rdb.HGetAll(ctx, key).Val())
			require.ErrorIs(t, rdb.HGet(ctx, key, "a").Err(), redis.Nil)
			require.ErrorIs(t, rdb.HGet(ctx, key, "b").Err(), redis.Nil)
		})

		t.Run("hexpire and hpersist return missing for missing key", func(t *testing.T) {
			key := "hfe-missing-key"
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "FIELDS", 2, "a", "b").Val(), []int64{-2, -2})
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 2, "a", "b").Val(), []int64{-2, -2})
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
		})

		t.Run("hexpire ttl overflow leaves field and metadata unchanged", func(t *testing.T) {
			key := "hfe-ttl-overflow"
			require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
			before := util.GetKMetadata(t, rdb, ctx, key)
			require.ErrorContains(t, rdb.Do(ctx, "hexpire", key, "9223372036854775807", "FIELDS", 1, "a").Err(),
				"overflow")
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
			require.Equal(t, "1", rdb.HGet(ctx, key, "a").Val())
		})

		t.Run("hexpire and hpersist reject wrong type", func(t *testing.T) {
			key := "hfe-wrong-type"
			require.NoError(t, rdb.Set(ctx, key, "value", 0).Err())
			require.ErrorContains(t, rdb.Do(ctx, "hexpire", key, 10, "FIELDS", 1, "a").Err(), "WRONGTYPE")
			require.ErrorContains(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, "a").Err(), "WRONGTYPE")
			require.Equal(t, "value", rdb.Get(ctx, key).Val())
		})

		t.Run("keywords and command name are case insensitive", func(t *testing.T) {
			key := "hfe-case-insensitive"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
			requireIntArray(t, rdb.Do(ctx, "hExPiRe", key, 60, "nX", "fIeLdS", 1, "a").Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 1)
			requireIntArray(t, rdb.Do(ctx, "hPeRsIsT", key, "fIeLdS", 1, "a").Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
		})

		t.Run("empty field name is valid", func(t *testing.T) {
			key := "hfe-empty-field"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "", "empty", "normal", "value").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 60, "FIELDS", 1, "").Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 1)
			requireIntArray(t, rdb.Do(ctx, "hpersist", key, "FIELDS", 1, "").Val(), []int64{1})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 0, "FIELDS", 1, "").Val(), []int64{2})
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
			require.Equal(t, map[string]string{"normal": "value"}, rdb.HGetAll(ctx, key).Val())
		})
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
