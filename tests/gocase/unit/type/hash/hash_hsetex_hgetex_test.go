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
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func requireIntegerReply(t *testing.T, rdb *redis.Client, ctx context.Context, want int64, args ...interface{}) {
	t.Helper()

	got, err := rdb.Do(ctx, args...).Int64()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func requireOptionalStringArray(t *testing.T, got interface{}, want ...interface{}) {
	t.Helper()

	values, ok := got.([]interface{})
	require.Truef(t, ok, "expected []interface{}, got %T", got)
	require.Equal(t, want, values)
}

func requireMetadataHeaderUnchanged(t *testing.T, before, after util.KMetadataResponse) {
	t.Helper()

	require.Equal(t, before.Flags, after.Flags)
	require.Equal(t, before.Mode, after.Mode)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Expire, after.Expire)
}

func TestHashFieldExpirationHSetExStateMatrix(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"rocksdb.disable_auto_compactions": "yes",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("future deadline", func(t *testing.T) {
			key := "hsetex-state-future"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)
			expireAt := time.Now().Add(10 * time.Minute).UnixMilli()

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", expireAt, "FIELDS", 4,
				hfePersistentField, "p2", hfeLiveField, "l2", hfeExpiredField, "x2", hfeMissingField, "m2")

			after := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, after, 5, 1)
			require.Equal(t, before.Lower, after.Lower)
			require.Equal(t, expireAt, after.Upper)
			requireMetadataHeaderUnchanged(t, before, after)
			requireHashValues(t, rdb, ctx, key, map[string]string{
				hfePersistentField: "p2",
				hfeLiveField:       "l2",
				hfeExpiredField:    "x2",
				hfeMissingField:    "m2",
				hfeKeeperField:     "40",
			})
			requireIntArray(t, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(),
				[]int64{expireAt, expireAt, expireAt, expireAt})
		})

		t.Run("discard ttl", func(t *testing.T) {
			key := "hsetex-state-discard"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FIELDS", 4,
				hfePersistentField, "p2", hfeLiveField, "l2", hfeExpiredField, "x2", hfeMissingField, "m2")

			after := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, after, 5, 5)
			requireMetadataHeaderUnchanged(t, before, after)
			requireHashValues(t, rdb, ctx, key, map[string]string{
				hfePersistentField: "p2",
				hfeLiveField:       "l2",
				hfeExpiredField:    "x2",
				hfeMissingField:    "m2",
				hfeKeeperField:     "40",
			})
			requireIntArray(t, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Val(),
				[]int64{-1, -1, -1, -1})
		})

		t.Run("keep ttl", func(t *testing.T) {
			key := "hsetex-state-keep"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "KEEPTTL", "FIELDS", 4,
				hfePersistentField, "p2", hfeLiveField, "l2", hfeExpiredField, "x2", hfeMissingField, "m2")

			after := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, after, 5, 3)
			require.Equal(t, before.Lower, after.Lower)
			require.Equal(t, before.Upper, after.Upper)
			requireMetadataHeaderUnchanged(t, before, after)
			require.Equal(t, "p2", rdb.HGet(ctx, key, hfePersistentField).Val())
			require.Equal(t, "l2", rdb.HGet(ctx, key, hfeLiveField).Val())
			require.ErrorIs(t, rdb.HGet(ctx, key, hfeExpiredField).Err(), redis.Nil)
			require.Equal(t, "m2", rdb.HGet(ctx, key, hfeMissingField).Val())
		})

		t.Run("past deadline", func(t *testing.T) {
			key := "hsetex-state-past"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", 1, "FIELDS", 4,
				hfePersistentField, "p2", hfeLiveField, "l2", hfeExpiredField, "x2", hfeMissingField, "m2")

			after := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, after, 1, 1)
			requireMetadataHeaderUnchanged(t, before, after)
			require.Equal(t, map[string]string{hfeKeeperField: "40"}, rdb.HGetAll(ctx, key).Val())
		})
	})
}

func TestHashFieldExpirationHGetExStateMatrix(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"rocksdb.disable_auto_compactions": "yes",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		tests := []struct {
			name        string
			option      []interface{}
			wantSize    int64
			wantPersist int64
		}{
			{name: "no modifier", wantSize: 3, wantPersist: 2},
			{name: "persist", option: []interface{}{"PERSIST"}, wantSize: 3, wantPersist: 3},
			{name: "past deadline", option: []interface{}{"PXAT", int64(1)}, wantSize: 1, wantPersist: 1},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				key := "hgetex-state-" + test.name
				createHashFieldStates(t, rdb, ctx, key)
				before := util.GetKMetadata(t, rdb, ctx, key)
				args := []interface{}{"hgetex", key}
				args = append(args, test.option...)
				args = append(args, "FIELDS", 4, hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField)

				got, err := rdb.Do(ctx, args...).Result()
				require.NoError(t, err)
				requireOptionalStringArray(t, got, "10", "20", nil, nil)

				after := util.GetKMetadata(t, rdb, ctx, key)
				requireHashMetadata(t, after, test.wantSize, test.wantPersist)
				requireMetadataHeaderUnchanged(t, before, after)
				if test.name == "no modifier" {
					require.Equal(t, before.Lower, after.Lower)
					require.Equal(t, before.Upper, after.Upper)
				}
			})
		}

		t.Run("future deadline", func(t *testing.T) {
			key := "hgetex-state-future"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)
			expireAt := time.Now().Add(10 * time.Minute).UnixMilli()

			got, err := rdb.Do(ctx, "hgetex", key, "PXAT", expireAt, "FIELDS", 4,
				hfePersistentField, hfeLiveField, hfeExpiredField, hfeMissingField).Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "10", "20", nil, nil)

			after := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, after, 3, 1)
			require.Equal(t, before.Lower, after.Lower)
			require.Equal(t, expireAt, after.Upper)
			requireMetadataHeaderUnchanged(t, before, after)
		})

		t.Run("empty string differs from missing", func(t *testing.T) {
			key := "hgetex-empty"
			require.Equal(t, int64(1), rdb.HSet(ctx, key, "empty", "").Val())
			got, err := rdb.Do(ctx, "hgetex", key, "FIELDS", 2, "empty", "missing").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "", nil)
		})
	})
}

func TestHashFieldExpirationHSetExConditionsAndDuplicates(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"rocksdb.disable_auto_compactions": "yes",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("FXX is atomic", func(t *testing.T) {
			key := "hsetex-fxx"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)

			requireIntegerReply(t, rdb, ctx, 0, "hsetex", key, "FXX", "FIELDS", 2,
				hfePersistentField, "changed", hfeMissingField, "created")
			require.Equal(t, "10", rdb.HGet(ctx, key, hfePersistentField).Val())
			require.ErrorIs(t, rdb.HGet(ctx, key, hfeMissingField).Err(), redis.Nil)
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FXX", "FIELDS", 2,
				hfePersistentField, "p2", hfeLiveField, "l2")
			require.Equal(t, "p2", rdb.HGet(ctx, key, hfePersistentField).Val())
			require.Equal(t, "l2", rdb.HGet(ctx, key, hfeLiveField).Val())
		})

		t.Run("ordered expired cleanup", func(t *testing.T) {
			key := "hsetex-ordered-cleanup"
			createHashFieldStates(t, rdb, ctx, key)
			before := util.GetKMetadata(t, rdb, ctx, key)

			requireIntegerReply(t, rdb, ctx, 0, "hsetex", key, "FNX", "FIELDS", 2,
				hfeExpiredField, "x2", hfePersistentField, "p2")
			after := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, after, 3, 2)
			require.Equal(t, before.Lower, after.Lower)
			require.Equal(t, before.Upper, after.Upper)
			require.Equal(t, "10", rdb.HGet(ctx, key, hfePersistentField).Val())
			require.ErrorIs(t, rdb.HGet(ctx, key, hfeExpiredField).Err(), redis.Nil)

			key = "hsetex-no-late-cleanup"
			createHashFieldStates(t, rdb, ctx, key)
			before = util.GetKMetadata(t, rdb, ctx, key)
			requireIntegerReply(t, rdb, ctx, 0, "hsetex", key, "FNX", "FIELDS", 2,
				hfePersistentField, "p2", hfeExpiredField, "x2")
			require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
		})

		t.Run("FNX rebuilds expired as missing", func(t *testing.T) {
			key := "hsetex-fnx-expired"
			createHashFieldStates(t, rdb, ctx, key)
			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FNX", "KEEPTTL", "FIELDS", 2,
				hfeExpiredField, "x2", hfeMissingField, "m2")
			require.Equal(t, "x2", rdb.HGet(ctx, key, hfeExpiredField).Val())
			require.Equal(t, "m2", rdb.HGet(ctx, key, hfeMissingField).Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 5, 4)
		})

		t.Run("duplicates use one transition and last value", func(t *testing.T) {
			key := "hsetex-duplicates"
			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FNX", "FIELDS", 3,
				"field", "first", "field", "second", "field", "last")
			require.Equal(t, "last", rdb.HGet(ctx, key, "field").Val())
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)

			expireAt := time.Now().Add(10 * time.Minute).UnixMilli()
			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FXX", "PXAT", expireAt, "FIELDS", 2,
				"field", "next", "field", "final")
			require.Equal(t, "final", rdb.HGet(ctx, key, "field").Val())
			meta := util.GetKMetadata(t, rdb, ctx, key)
			requireHashMetadata(t, meta, 1, 0)
			require.Equal(t, expireAt, meta.Lower)
			require.Equal(t, expireAt, meta.Upper)
		})
	})
}

func TestHashFieldExpirationHGetExDuplicates(t *testing.T) {
	runWithFieldExpirationHashConfigs(t, util.KvrocksServerConfigs{
		"rocksdb.disable_auto_compactions": "yes",
	}, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("past deadline returns value then null", func(t *testing.T) {
			key := "hgetex-duplicate-delete"
			require.Equal(t, int64(1), rdb.HSet(ctx, key, "field", "value").Val())
			got, err := rdb.Do(ctx, "hgetex", key, "PXAT", 1, "FIELDS", 2, "field", "field").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "value", nil)
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
		})

		t.Run("persist changes counter once", func(t *testing.T) {
			key := "hgetex-duplicate-persist"
			require.Equal(t, int64(1), rdb.HSet(ctx, key, "field", "value").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 300, "FIELDS", 1, "field").Val(), []int64{1})
			got, err := rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 2, "field", "field").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "value", "value")
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		})

		t.Run("expired physical cleans once", func(t *testing.T) {
			key := "hgetex-duplicate-expired"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "field", "value", "keeper", "value").Val())
			requireIntArray(t, rdb.Do(ctx, "hexpire", key, 1, "FIELDS", 1, "field").Val(), []int64{1})
			waitHashFieldExpired(t, rdb, ctx, key, "field")
			got, err := rdb.Do(ctx, "hgetex", key, "FIELDS", 2, "field", "field").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, nil, nil)
			requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		})
	})
}

func TestHashFieldExpirationHSetExHGetExParserCompatibility(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hsetex-hgetex-parser"
		require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())

		tests := []struct {
			name        string
			args        []interface{}
			errContains string
			exactError  string
		}{
			{name: "hsetex repeated fields", args: []interface{}{"hsetex", key, "FIELDS", 1, "a", "x", "FIELDS", 1, "b", "y"}, errContains: "FIELDS keyword specified multiple times"},
			{name: "hgetex repeated fields", args: []interface{}{"hgetex", key, "FIELDS", 1, "a", "FIELDS", 1, "b"}, errContains: "FIELDS keyword specified multiple times"},
			{name: "hsetex zero fields", args: []interface{}{"hsetex", key, "FIELDS", 0, "a", "x"}, errContains: "invalid number of fields"},
			{name: "hgetex negative fields", args: []interface{}{"hgetex", key, "FIELDS", -1, "a"}, errContains: "invalid number of fields"},
			{name: "hsetex invalid fields integer", args: []interface{}{"hsetex", key, "FIELDS", "invalid", "a", "x"}, errContains: "invalid number of fields"},
			{name: "hgetex fields integer overflow", args: []interface{}{"hgetex", key, "FIELDS", "9223372036854775808", "a"}, errContains: "invalid number of fields"},
			{name: "hsetex short block", args: []interface{}{"hsetex", key, "FIELDS", 2, "a", "x"}, errContains: "wrong number of arguments"},
			{name: "hgetex short block", args: []interface{}{"hgetex", key, "FIELDS", 2, "a"}, errContains: "wrong number of arguments"},
			{name: "hsetex int max short block", args: []interface{}{"hsetex", key, "FIELDS", 2147483647, "a", "x"}, errContains: "wrong number of arguments"},
			{name: "hgetex int max short block", args: []interface{}{"hgetex", key, "FIELDS", 2147483647, "a"}, errContains: "wrong number of arguments"},
			{name: "hsetex fields above int max", args: []interface{}{"hsetex", key, "FIELDS", "2147483648", "a", "x"}, errContains: "invalid number of fields"},
			{name: "hgetex fields above int max", args: []interface{}{"hgetex", key, "FIELDS", "2147483648", "a"}, errContains: "invalid number of fields"},
			{name: "hsetex missing field count", args: []interface{}{"hsetex", key, "FNX", "EX", 10, "FIELDS"}, exactError: "ERR wrong number of arguments"},
			{name: "hgetex missing field count", args: []interface{}{"hgetex", key, "EX", 10, "FIELDS"}, exactError: "ERR wrong number of arguments"},
			{name: "hsetex duplicate fields before missing count", args: []interface{}{"hsetex", key, "FIELDS", 1, "a", "x", "FIELDS"}, errContains: "FIELDS keyword specified multiple times"},
			{name: "hgetex duplicate fields before missing count", args: []interface{}{"hgetex", key, "FIELDS", 1, "a", "FIELDS"}, errContains: "FIELDS keyword specified multiple times"},
			{name: "hsetex unknown trailing token", args: []interface{}{"hsetex", key, "FIELDS", 1, "a", "x", "unknown"}, errContains: "unknown argument: unknown"},
			{name: "hgetex unknown trailing token", args: []interface{}{"hgetex", key, "FIELDS", 1, "a", "unknown"}, errContains: "unknown argument: unknown"},
			{name: "hsetex condition conflict", args: []interface{}{"hsetex", key, "FXX", "FNX", "FIELDS", 1, "a", "x"}, errContains: "Only one of FXX or FNX"},
			{name: "hsetex repeated fxx", args: []interface{}{"hsetex", key, "FXX", "FXX", "FIELDS", 1, "a", "x"}, errContains: "Only one of FXX or FNX"},
			{name: "hsetex repeated fnx", args: []interface{}{"hsetex", key, "FNX", "FNX", "FIELDS", 1, "missing", "x"}, errContains: "Only one of FXX or FNX"},
			{name: "hsetex expiration conflict", args: []interface{}{"hsetex", key, "EX", 10, "KEEPTTL", "FIELDS", 1, "a", "x"}, errContains: "Only one of EX, PX, EXAT, PXAT or KEEPTTL"},
			{name: "hsetex repeated expiration", args: []interface{}{"hsetex", key, "EX", 10, "EX", 20, "FIELDS", 1, "a", "x"}, errContains: "Only one of EX, PX, EXAT, PXAT or KEEPTTL"},
			{name: "hsetex repeated keepttl", args: []interface{}{"hsetex", key, "KEEPTTL", "KEEPTTL", "FIELDS", 1, "a", "x"}, errContains: "Only one of EX, PX, EXAT, PXAT or KEEPTTL"},
			{name: "hgetex expiration conflict", args: []interface{}{"hgetex", key, "PERSIST", "PX", 10, "FIELDS", 1, "a"}, errContains: "Only one of EX, PX, EXAT, PXAT or PERSIST"},
			{name: "hgetex repeated expiration", args: []interface{}{"hgetex", key, "PX", 10, "PX", 20, "FIELDS", 1, "a"}, errContains: "Only one of EX, PX, EXAT, PXAT or PERSIST"},
			{name: "hgetex repeated persist", args: []interface{}{"hgetex", key, "PERSIST", "PERSIST", "FIELDS", 1, "a"}, errContains: "Only one of EX, PX, EXAT, PXAT or PERSIST"},
			{name: "hsetex missing fields", args: []interface{}{"hsetex", key, "EX", 10, "UNKNOWN", "x"}, errContains: "unknown argument: UNKNOWN"},
			{name: "hgetex missing fields", args: []interface{}{"hgetex", key, "EX", 10, "UNKNOWN"}, errContains: "unknown argument: UNKNOWN"},
			{name: "hsetex missing expire", args: []interface{}{"hsetex", key, "FIELDS", 1, "a", "x", "EX"}, errContains: "missing expire time"},
			{name: "hgetex missing expire", args: []interface{}{"hgetex", key, "FIELDS", 1, "a", "EX"}, errContains: "missing expire time"},
			{name: "hsetex conflict before missing expire", args: []interface{}{"hsetex", key, "EX", 10, "FIELDS", 1, "a", "x", "PX"}, errContains: "Only one of EX, PX, EXAT, PXAT or KEEPTTL"},
			{name: "hgetex conflict before missing expire", args: []interface{}{"hgetex", key, "PERSIST", "FIELDS", 1, "a", "PX"}, errContains: "Only one of EX, PX, EXAT, PXAT or PERSIST"},
			{name: "hsetex fields consumed as time", args: []interface{}{"hsetex", key, "EX", "FIELDS", 1, "a", "x"}, errContains: "value is not an integer or out of range"},
			{name: "hgetex fields consumed as time", args: []interface{}{"hgetex", key, "EX", "FIELDS", 1, "a"}, errContains: "value is not an integer or out of range"},
			{name: "hsetex negative expire", args: []interface{}{"hsetex", key, "EX", -1, "FIELDS", 1, "a", "x"}, errContains: "invalid expire time, must be >= 0"},
			{name: "hgetex negative expire", args: []interface{}{"hgetex", key, "PX", -1, "FIELDS", 1, "a"}, errContains: "invalid expire time, must be >= 0"},
			{name: "hsetex expire integer overflow", args: []interface{}{"hsetex", key, "PXAT", "9223372036854775808", "FIELDS", 1, "a", "x"}, errContains: "value is not an integer or out of range"},
			{name: "hsetex absolute expire too large", args: []interface{}{"hsetex", key, "PXAT", hfeMaxAbsTimeMs + 1, "FIELDS", 1, "a", "x"}, exactError: "ERR invalid expire time"},
			{name: "hgetex absolute expire too large", args: []interface{}{"hgetex", key, "PXAT", hfeMaxAbsTimeMs + 1, "FIELDS", 1, "a"}, exactError: "ERR invalid expire time"},
			{name: "hsetex option from hgetex", args: []interface{}{"hsetex", key, "PERSIST", "FIELDS", 1, "a", "x"}, errContains: "unknown argument: PERSIST"},
			{name: "hgetex option from hsetex", args: []interface{}{"hgetex", key, "KEEPTTL", "FIELDS", 1, "a"}, errContains: "unknown argument: KEEPTTL"},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				before := util.GetKMetadata(t, rdb, ctx, key)
				valuesBefore := rdb.HGetAll(ctx, key).Val()
				err := rdb.Do(ctx, test.args...).Err()
				if test.exactError != "" {
					require.EqualError(t, err, test.exactError)
				} else {
					require.ErrorContains(t, err, test.errContains)
				}
				require.Equal(t, valuesBefore, rdb.HGetAll(ctx, key).Val())
				require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
			})
		}

		t.Run("flexible ordering and option-like data", func(t *testing.T) {
			expireAt := time.Now().Add(10 * time.Minute).UnixMilli()
			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", expireAt, "FIELDS", 1, "a", "3")
			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FIELDS", 1, "a", "5", "FXX", "KEEPTTL")
			requireIntArray(t, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 1, "a").Val(), []int64{expireAt})

			requireIntegerReply(t, rdb, ctx, 0, "hsetex", key, "FIELDS", 1, "missing-after-fields", "x", "FXX")
			require.False(t, rdb.HExists(ctx, key, "missing-after-fields").Val())

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FIELDS", 2, "EX", "60", "FIELDS", "value")
			require.Equal(t, "60", rdb.HGet(ctx, key, "EX").Val())
			require.Equal(t, "value", rdb.HGet(ctx, key, "FIELDS").Val())

			got, err := rdb.Do(ctx, "hgetex", key, "FIELDS", 2, "EX", "FIELDS", "PERSIST").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "60", "value")

			expireAt = time.Now().Add(20 * time.Minute).UnixMilli()
			got, err = rdb.Do(ctx, "hgetex", key, "FIELDS", 1, "a", "PXAT", expireAt).Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "5")
			requireIntArray(t, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 1, "a").Val(), []int64{expireAt})

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FIELDS", 2,
				"ordinary", "FIELDS", "another", "EX")
			require.Equal(t, "FIELDS", rdb.HGet(ctx, key, "ordinary").Val())
			require.Equal(t, "EX", rdb.HGet(ctx, key, "another").Val())
		})

		t.Run("keywords are case insensitive", func(t *testing.T) {
			expireAt := time.Now().Add(30 * time.Minute).UnixMilli()
			requireIntegerReply(t, rdb, ctx, 1, "HsEtEx", key, "pXaT", expireAt, "fIeLdS", 1, "a", "case")
			requireIntegerReply(t, rdb, ctx, 1, "HsEtEx", key, "fXx", "fIeLdS", 1, "a", "4", "kEePtTl")
			requireIntArray(t, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 1, "a").Val(), []int64{expireAt})
			got, err := rdb.Do(ctx, "HgEtEx", key, "fIeLdS", 1, "a", "pErSiSt").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "4")
			requireIntArray(t, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 1, "a").Val(), []int64{-1})
		})
	})
}

func TestHashFieldExpirationHSetExHGetExParsingAndCommandFlags(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		require.EqualError(t, rdb.Do(ctx, "hsetex", "key").Err(),
			"ERR wrong number of arguments")
		require.EqualError(t, rdb.Do(ctx, "hgetex", "key", "FIELDS").Err(),
			"ERR wrong number of arguments")

		wrongTypeKey := "hsetex-hgetex-wrongtype"
		require.NoError(t, rdb.Set(ctx, wrongTypeKey, "value", 0).Err())

		hsetErr := rdb.Do(ctx, "hsetex", wrongTypeKey, "FIELDS", 0, "a", "x").Err()
		require.ErrorContains(t, hsetErr, "invalid number of fields")
		require.NotContains(t, hsetErr.Error(), "WRONGTYPE")

		hgetErr := rdb.Do(ctx, "hgetex", wrongTypeKey, "FIELDS", 0, "a").Err()
		require.ErrorContains(t, hgetErr, "invalid number of fields")
		require.NotContains(t, hgetErr.Error(), "WRONGTYPE")

		require.ErrorContains(t, rdb.Do(ctx, "hsetex", wrongTypeKey, "FIELDS", 1, "a", "x").Err(), "WRONGTYPE")
		require.ErrorContains(t, rdb.Do(ctx, "hgetex", wrongTypeKey, "FIELDS", 1, "a").Err(), "WRONGTYPE")

		t.Run("parse error aborts multi", func(t *testing.T) {
			key := "hgetex-parse-error-multi"
			require.NoError(t, rdb.Del(ctx, key).Err())
			require.NoError(t, rdb.Do(ctx, "MULTI").Err())
			require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", key, "value").Val())
			require.ErrorContains(t, rdb.Do(ctx, "HGETEX", wrongTypeKey, "FIELDS", 0, "a").Err(),
				"invalid number of fields")
			require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
			require.Zero(t, rdb.Exists(ctx, key).Val())
		})

		for _, test := range []struct {
			command string
			arity   int64
			flag    string
		}{
			{command: "hsetex", arity: -6, flag: "write"},
			{command: "hgetex", arity: -5, flag: "no-dbsize-check"},
		} {
			info, err := rdb.Do(ctx, "command", "info", test.command).Slice()
			require.NoError(t, err)
			require.Len(t, info, 1)
			commandInfo := info[0].([]interface{})
			require.Equal(t, test.arity, commandInfo[1])
			require.Contains(t, commandInfo[2].([]interface{}), test.flag)
		}

		key := "hsetex-hgetex-eval-ro"
		require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hsetex', KEYS[1], 'FIELDS', 1, 'a', '2')`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
		require.ErrorContains(t,
			rdb.Do(ctx, "eval_ro", `return redis.call('hgetex', KEYS[1], 'FIELDS', 1, 'a')`, 1, key).Err(),
			"Write commands are not allowed from read-only scripts")
	})
}

func TestHashFieldExpirationHSetExHGetExLegacyPolicy(t *testing.T) {
	srv := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode": "legacy",
		"resp3-enabled":      "yes",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "hsetex-hgetex-legacy"
	require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
	require.NoError(t, rdb.PExpire(ctx, key, time.Minute).Err())
	before := util.GetKMetadata(t, rdb, ctx, key)
	valuesBefore := rdb.HGetAll(ctx, key).Val()
	for _, args := range [][]interface{}{
		{"hsetex", key, "FIELDS", 1, "a", "x"},
		{"hsetex", key, "KEEPTTL", "FIELDS", 1, "a", "x"},
		{"hsetex", key, "PXAT", 1, "FIELDS", 1, "a", "x"},
		{"hsetex", key, "FXX", "FIELDS", 1, "missing", "x"},
		{"hsetex", key, "FNX", "FIELDS", 1, "a", "x"},
		{"hgetex", key, "FIELDS", 1, "a"},
		{"hgetex", key, "PERSIST", "FIELDS", 1, "a"},
		{"hgetex", key, "PXAT", 1, "FIELDS", 1, "a"},
		{"hgetex", key, "PX", 60000, "FIELDS", 1, "missing"},
	} {
		require.ErrorContains(t, rdb.Do(ctx, args...).Err(),
			"hash field expiration is not supported by legacy hash encoding")
		require.Equal(t, valuesBefore, rdb.HGetAll(ctx, key).Val())
		require.Equal(t, before, util.GetKMetadata(t, rdb, ctx, key))
	}

	missing := "hsetex-hgetex-legacy-missing"
	got, err := rdb.Do(ctx, "hgetex", missing, "PX", 1000, "FIELDS", 2, "a", "b").Result()
	require.NoError(t, err)
	requireOptionalStringArray(t, got, nil, nil)
	requireIntegerReply(t, rdb, ctx, 0, "hsetex", missing, "FXX", "FIELDS", 1, "a", "1")
	for _, args := range [][]interface{}{
		{"hsetex", missing, "FIELDS", 1, "a", "1"},
		{"hsetex", missing, "FNX", "PXAT", 1, "FIELDS", 1, "a", "1"},
		{"hsetex", missing, "KEEPTTL", "FIELDS", 1, "a", "1"},
	} {
		require.ErrorContains(t, rdb.Do(ctx, args...).Err(), "hash field expiration")
		require.Equal(t, int64(0), rdb.Exists(ctx, missing).Val())
	}
}

func TestHashFieldExpirationHSetExHGetExMetadataSequence(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hsetex-hgetex-metadata-sequence"
		t1 := time.Now().Add(10 * time.Minute).UnixMilli()
		t2 := t1 + int64((10*time.Minute)/time.Millisecond)

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", t1, "FIELDS", 3,
			"a", "1", "b", "2", "c", "3")
		meta := util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, meta, 3, 0)
		require.Equal(t, t1, meta.Lower)
		require.Equal(t, t1, meta.Upper)
		originalHeader := meta

		got, err := rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 2, "a", "missing").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "1", nil)
		meta = util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, meta, 3, 1)
		require.Equal(t, t1, meta.Lower)
		require.Equal(t, t1, meta.Upper)
		requireMetadataHeaderUnchanged(t, originalHeader, meta)

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "KEEPTTL", "FIELDS", 3,
			"a", "10", "b", "20", "d", "40")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, meta, 4, 2)
		require.Equal(t, t1, meta.Lower)
		require.Equal(t, t1, meta.Upper)

		got, err = rdb.Do(ctx, "hgetex", key, "PXAT", t2, "FIELDS", 3, "a", "c", "missing").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "10", "3", nil)
		meta = util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, meta, 4, 1)
		require.Equal(t, t1, meta.Lower)
		require.Equal(t, t2, meta.Upper)

		require.Equal(t, int64(1), rdb.HDel(ctx, key, "b").Val())
		meta = util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, meta, 3, 1)
		require.Equal(t, t1, meta.Lower)
		require.Equal(t, t2, meta.Upper)

		require.Equal(t, int64(10), rdb.HIncrBy(ctx, key, "c", 7).Val())
		require.Equal(t, meta, util.GetKMetadata(t, rdb, ctx, key))

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FIELDS", 1, "a", "100")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		requireHashMetadata(t, meta, 3, 2)
		require.Equal(t, t1, meta.Lower)
		require.Equal(t, t2, meta.Upper)

		got, err = rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 1, "c").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "10")
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 3, 3)

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", 1, "FIELDS", 2,
			"a", "0", "c", "0")
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
		require.Equal(t, map[string]string{"d": "40"}, rdb.HGetAll(ctx, key).Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "hlen", key, "REPAIR").Val())
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
	})
}

func TestHashFieldExpirationHSetExHGetExConservativeBounds(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hsetex-hgetex-bounds"
		now := time.Now().UnixMilli()
		t10 := now + 10*60*1000
		t20 := now + 20*60*1000
		t25 := now + 25*60*1000
		t30 := now + 30*60*1000

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", t20, "FIELDS", 2,
			"a", "1", "b", "2")
		meta := util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, t20, meta.Lower)
		require.Equal(t, t20, meta.Upper)

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", t30, "FIELDS", 1, "a", "3")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, t20, meta.Lower)
		require.Equal(t, t30, meta.Upper)

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", t25, "FIELDS", 1, "b", "4")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, t20, meta.Lower)
		require.Equal(t, t30, meta.Upper)

		got, err := rdb.Do(ctx, "hgetex", key, "PXAT", t10, "FIELDS", 1, "a").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "3")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, t10, meta.Lower)
		require.Equal(t, t30, meta.Upper)

		got, err = rdb.Do(ctx, "hgetex", key, "PXAT", t20, "FIELDS", 1, "a").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "3")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, t10, meta.Lower)
		require.Equal(t, t30, meta.Upper)

		got, err = rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 1, "a").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "3")
		meta = util.GetKMetadata(t, rdb, ctx, key)
		require.Equal(t, int64(1), meta.Persist)
		require.Equal(t, t10, meta.Lower)
		require.Equal(t, t30, meta.Upper)

		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "FIELDS", 1, "b", "5")
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 2, 2)
	})
}

func TestHashFieldExpirationHSetExHGetExKeyLifecycle(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		t.Run("preserve key ttl and version", func(t *testing.T) {
			key := "hsetex-hgetex-key-ttl"
			require.Equal(t, int64(2), rdb.HSet(ctx, key, "a", "1", "b", "2").Val())
			require.NoError(t, rdb.PExpire(ctx, key, time.Minute).Err())
			before := util.GetKMetadata(t, rdb, ctx, key)
			expireAt := time.Now().Add(10 * time.Minute).UnixMilli()

			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", expireAt, "FIELDS", 1, "a", "3")
			after := util.GetKMetadata(t, rdb, ctx, key)
			requireMetadataHeaderUnchanged(t, before, after)

			got, err := rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 1, "a").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "3")
			after = util.GetKMetadata(t, rdb, ctx, key)
			requireMetadataHeaderUnchanged(t, before, after)
		})

		t.Run("past deadline on missing key leaves no tombstone", func(t *testing.T) {
			key := "hsetex-missing-past"
			requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", 1, "FIELDS", 2,
				"a", "1", "b", "2")
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
			require.Error(t, rdb.Do(ctx, "kmetadata", key).Err())
		})

		t.Run("missing key replies", func(t *testing.T) {
			key := "hsetex-hgetex-missing"
			requireIntegerReply(t, rdb, ctx, 0, "hsetex", key, "FXX", "FIELDS", 1, "a", "1")
			got, err := rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 2, "a", "b").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, nil, nil)
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
		})

		t.Run("last physical field deletes metadata", func(t *testing.T) {
			key := "hgetex-last-field"
			require.Equal(t, int64(1), rdb.HSet(ctx, key, "a", "1").Val())
			got, err := rdb.Do(ctx, "hgetex", key, "PXAT", 1, "FIELDS", 1, "a").Result()
			require.NoError(t, err)
			requireOptionalStringArray(t, got, "1")
			require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
			require.Error(t, rdb.Do(ctx, "kmetadata", key).Err())
		})
	})
}

func TestHashFieldExpirationHSetExHGetExSingleTimeSnapshot(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		key := "hsetex-hgetex-one-clock"
		requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PX", 600000, "FIELDS", 3,
			"a", "1", "b", "2", "c", "3")
		expires := rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 3, "a", "b", "c").Val().([]interface{})
		require.Equal(t, expires[0], expires[1])
		require.Equal(t, expires[1], expires[2])

		got, err := rdb.Do(ctx, "hgetex", key, "PX", 700000, "FIELDS", 3, "a", "b", "c").Result()
		require.NoError(t, err)
		requireOptionalStringArray(t, got, "1", "2", "3")
		expires = rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 3, "a", "b", "c").Val().([]interface{})
		require.Equal(t, expires[0], expires[1])
		require.Equal(t, expires[1], expires[2])
	})
}

func TestHashFieldExpirationHSetExFNXConcurrency(t *testing.T) {
	runWithFieldExpirationHash(t, func(t *testing.T, rdb *redis.Client, ctx context.Context) {
		const clients = 16
		key := "hsetex-fnx-concurrent"
		start := make(chan struct{})
		results := make(chan int64, clients)
		errors := make(chan error, clients)
		var wg sync.WaitGroup
		for i := 0; i < clients; i++ {
			wg.Add(1)
			go func(value int) {
				defer wg.Done()
				<-start
				result, err := rdb.Do(ctx, "hsetex", key, "FNX", "FIELDS", 1, "field", value).Int64()
				if err != nil {
					errors <- err
					return
				}
				results <- result
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		close(errors)

		for err := range errors {
			require.NoError(t, err)
		}
		succeeded := 0
		for result := range results {
			if result == 1 {
				succeeded++
			} else {
				require.Equal(t, int64(0), result)
			}
		}
		require.Equal(t, 1, succeeded)
		requireHashMetadata(t, util.GetKMetadata(t, rdb, ctx, key), 1, 1)
	})
}

func TestHashFieldExpirationHSetExHGetExRestart(t *testing.T) {
	srv := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode": "field-expiration",
		"resp3-enabled":      "yes",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()

	key := "hsetex-hgetex-restart"
	expireAt := time.Now().Add(time.Hour).UnixMilli()
	requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "PXAT", expireAt, "FIELDS", 3,
		"a", "1", "b", "2", "c", "3")
	got, err := rdb.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 1, "a").Result()
	require.NoError(t, err)
	requireOptionalStringArray(t, got, "1")
	requireIntegerReply(t, rdb, ctx, 1, "hsetex", key, "KEEPTTL", "FIELDS", 2,
		"b", "20", "d", "40")

	valuesBefore := rdb.HGetAll(ctx, key).Val()
	expiresBefore := rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 4, "a", "b", "c", "d").Val()
	metadataBefore := util.GetKMetadata(t, rdb, ctx, key)
	require.NoError(t, rdb.Close())

	srv.Restart()
	rdb = srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	require.Equal(t, valuesBefore, rdb.HGetAll(ctx, key).Val())
	require.Equal(t, expiresBefore, rdb.Do(ctx, "hpexpiretime", key, "FIELDS", 4, "a", "b", "c", "d").Val())
	require.Equal(t, metadataBefore, util.GetKMetadata(t, rdb, ctx, key))
}
