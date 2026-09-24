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

package slotmigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSlotMigrateHashFieldExpiration(t *testing.T) {
	ctx := context.Background()
	source := util.StartServer(t, util.KvrocksServerConfigs{
		"cluster-enabled":                  "yes",
		"hash-encoding-mode":               "legacy",
		"rocksdb.disable_auto_compactions": "yes",
		"migrate-batch-size-kb":            "1",
	})
	defer source.Close()
	sourceClient := source.NewClient()
	defer func() { require.NoError(t, sourceClient.Close()) }()
	sourceID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, sourceClient.Do(ctx, "clusterx", "SETNODEID", sourceID).Err())

	destination := util.StartServer(t, util.KvrocksServerConfigs{
		"cluster-enabled":                  "yes",
		"hash-encoding-mode":               "legacy",
		"rocksdb.disable_auto_compactions": "yes",
	})
	defer destination.Close()
	destinationClient := destination.NewClient()
	defer func() { require.NoError(t, destinationClient.Close()) }()
	destinationID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, destinationClient.Do(ctx, "clusterx", "SETNODEID", destinationID).Err())
	nodes := fmt.Sprintf("%s %s %d master - 0-10000\n%s %s %d master - 10001-16383",
		sourceID, source.Host(), source.Port(), destinationID, destination.Host(), destination.Port())
	require.NoError(t, sourceClient.Do(ctx, "clusterx", "SETNODES", nodes, "1").Err())
	require.NoError(t, destinationClient.Do(ctx, "clusterx", "SETNODES", nodes, "1").Err())

	for slot, phase := range []string{"snapshot", "wal"} {
		t.Run(phase, func(t *testing.T) {
			key := fmt.Sprintf("hfe_{%s}", util.SlotTable[slot])
			legacyKey := fmt.Sprintf("legacy_{%s}", util.SlotTable[slot])
			binaryValue := strings.Repeat("\x00", 8) + strings.Repeat("value\xff", 512)
			require.NoError(t, sourceClient.ConfigSet(ctx, "hash-encoding-mode", "legacy").Err())
			require.NoError(t, sourceClient.HSet(ctx, legacyKey, "field", binaryValue).Err())
			require.NoError(t, sourceClient.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
			require.NoError(t, sourceClient.HSet(ctx, key, "persistent", binaryValue, "live", "10",
				"persist", "20", "overwrite", "old", "deleted", "gone", "expired", "gone").Err())
			expireAt := time.Now().Add(10 * time.Minute).UnixMilli()
			require.NoError(t, sourceClient.Do(ctx, "hpexpireat", key, expireAt, "FIELDS", 4,
				"live", "persist", "overwrite", "deleted").Err())
			expiredValue, err := sourceClient.HGet(ctx, key, "expired").Result()
			require.NoError(t, err)
			require.Equal(t, "gone", expiredValue)
			expiredResult, err := sourceClient.Do(ctx, "hpexpire", key, 1000, "FIELDS", 1, "expired").Result()
			require.NoError(t, err)
			require.Equal(t, []interface{}{int64(1)}, expiredResult)
			require.NoError(t, sourceClient.PExpireAt(ctx, key, time.Now().Add(20*time.Minute)).Err())
			// This field must already be expired on the source before migration starts.
			require.Eventually(t, func() bool {
				return errors.Is(sourceClient.HGet(ctx, key, "expired").Err(), redis.Nil)
			}, 5*time.Second, 10*time.Millisecond)

			values := map[string]string{
				"persistent": binaryValue, "live": "10", "persist": "20", "overwrite": "old", "deleted": "gone",
			}
			expires := []interface{}{int64(-1), expireAt, expireAt, expireAt, expireAt, int64(-2)}
			getDelModes := []string{"legacy", "field-expiration"}
			getDelMetadata := make(map[string]util.KMetadataResponse)
			getDelKeyExpireAt := time.Now().Add(20 * time.Minute).Truncate(time.Second).UnixMilli()
			for _, mode := range getDelModes {
				getDelKey := fmt.Sprintf("hgetdel-%s_{%s}", mode, util.SlotTable[slot])
				require.NoError(t, sourceClient.ConfigSet(ctx, "hash-encoding-mode", mode).Err())
				require.NoError(t, sourceClient.HSet(ctx, getDelKey, "removed", binaryValue, "live", "ttl", "keeper", "persistent").Err())
				require.NoError(t, sourceClient.HSet(ctx, getDelKey+"-all", "first", binaryValue, "last", "value").Err())
				if mode == "field-expiration" {
					require.NoError(t, sourceClient.Do(ctx, "hpexpireat", getDelKey, expireAt, "FIELDS", 2, "removed", "live").Err())
					require.NoError(t, sourceClient.Do(ctx, "hpexpireat", getDelKey+"-all", expireAt, "FIELDS", 1, "last").Err())
				}
				require.NoError(t, sourceClient.Do(ctx, "pexpireat", getDelKey, getDelKeyExpireAt).Err())
				require.ErrorContains(t, destinationClient.Do(ctx, "hgetdel", getDelKey, "FIELDS", 1, "keeper").Err(), "MOVED")
			}
			deleteFields := func() {
				for _, mode := range getDelModes {
					otherMode := "legacy"
					if mode == "legacy" {
						otherMode = "field-expiration"
					}
					require.NoError(t, sourceClient.ConfigSet(ctx, "hash-encoding-mode", otherMode).Err())
					getDelKey := fmt.Sprintf("hgetdel-%s_{%s}", mode, util.SlotTable[slot])
					got, err := sourceClient.Do(ctx, "hgetdel", getDelKey, "FIELDS", 3, "removed", "missing", "removed").Result()
					require.NoError(t, err)
					require.Equal(t, []interface{}{binaryValue, nil, nil}, got)
					got, err = sourceClient.Do(ctx, "hgetdel", getDelKey+"-all", "FIELDS", 4, "first", "first", "missing", "last").Result()
					require.NoError(t, err)
					require.Equal(t, []interface{}{binaryValue, nil, nil, "value"}, got)
					exists, err := sourceClient.Exists(ctx, getDelKey+"-all").Result()
					require.NoError(t, err)
					require.Zero(t, exists)
					metadata := util.GetKMetadata(t, sourceClient, ctx, getDelKey)
					require.Equal(t, mode, metadata.Mode)
					require.Equal(t, int64(2), metadata.Size)
					if mode == "field-expiration" {
						require.Equal(t, int64(1), metadata.Persist)
					}
					getDelMetadata[getDelKey] = metadata
				}
				require.NoError(t, sourceClient.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
			}
			var fieldExpireAt int64
			expiringKey, expiringField, expiringValue := key, "live", "11"
			if phase == "wal" {
				// Keep the snapshot transfer running while subsequent hash changes enter the WAL.
				require.NoError(t, sourceClient.ConfigSet(ctx, "migrate-batch-rate-limit-mb", "1").Err())
				fillerKey := fmt.Sprintf("filler_{%s}", util.SlotTable[slot])
				pipe := sourceClient.Pipeline()
				for i := 0; i < 1024; i++ {
					pipe.RPush(ctx, fillerKey, strings.Repeat("x", 4096))
				}
				_, err := pipe.Exec(ctx)
				require.NoError(t, err)
				require.NoError(t, sourceClient.Do(ctx, "clusterx", "migrate", slot, destinationID).Err())
				// Import starts only after the source has acquired its snapshot.
				waitForImportState(t, destinationClient, slot, "start")
				requireMigrateState(t, sourceClient, slot, SlotMigrationStateStarted)
				deleteFields()

				require.NoError(t, sourceClient.HIncrBy(ctx, key, "live", 5).Err())
				require.NoError(t, sourceClient.Do(ctx, "hpexpireat", key, expireAt+60000, "FIELDS", 1, "persistent").Err())
				require.NoError(t, sourceClient.Do(ctx, "hpersist", key, "FIELDS", 1, "persist").Err())
				require.NoError(t, sourceClient.HSet(ctx, key, "overwrite", "new").Err())
				require.NoError(t, sourceClient.HDel(ctx, key, "deleted").Err())
				require.NoError(t, sourceClient.HSet(ctx, legacyKey, "field", binaryValue+"updated").Err())
				require.NoError(t, sourceClient.Do(ctx, "hsetex", key+"_new", "PXAT", expireAt,
					"FIELDS", 2, "field", "created-during-migration", "short-lived", "temporary").Err())
				// The new key can only arrive through WAL; leave time for the rate-limited snapshot transfer.
				fieldExpireAt = time.Now().Add(8 * time.Second).UnixMilli()
				require.NoError(t, sourceClient.Do(ctx, "hpexpireat", key+"_new", fieldExpireAt, "FIELDS", 1, "short-lived").Err())
				expiringKey, expiringField, expiringValue = key+"_new", "short-lived", "temporary"
				values["live"] = "15"
				values["overwrite"] = "new"
				delete(values, "deleted")
				expires = []interface{}{expireAt + 60000, expireAt, int64(-1), int64(-1), int64(-2), int64(-2)}
			} else {
				deleteFields()
				fieldExpireAt = time.Now().Add(3 * time.Second).UnixMilli()
				require.NoError(t, sourceClient.Do(ctx, "hpexpireat", key, fieldExpireAt, "FIELDS", 1, "live").Err())
				expires[1] = fieldExpireAt
			}

			metadata := util.GetKMetadata(t, sourceClient, ctx, key)
			require.Equal(t, "field-expiration", metadata.Mode)
			legacyMetadata := util.GetKMetadata(t, sourceClient, ctx, legacyKey)
			require.Equal(t, "legacy", legacyMetadata.Mode)
			if phase == "snapshot" {
				require.Equal(t, int64(6), metadata.Size)
				require.Equal(t, int64(1), metadata.Persist)
				require.NoError(t, sourceClient.Do(ctx, "clusterx", "migrate", slot, destinationID).Err())
			}
			waitForMigrateStateInDuration(t, sourceClient, slot, SlotMigrationStateSuccess, time.Minute)
			waitForImportState(t, destinationClient, slot, SlotImportStateSuccess)

			got, err := destinationClient.HGetAll(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, values, got)
			fieldExpires, err := destinationClient.Do(ctx, "hpexpiretime", key, "FIELDS", 6,
				"persistent", "live", "persist", "overwrite", "deleted", "expired").Result()
			require.NoError(t, err)
			require.Equal(t, expires, fieldExpires)
			require.Equal(t, metadata, util.GetKMetadata(t, destinationClient, ctx, key))
			require.Equal(t, legacyMetadata, util.GetKMetadata(t, destinationClient, ctx, legacyKey))
			legacyValue, err := destinationClient.HGet(ctx, legacyKey, "field").Result()
			require.NoError(t, err)
			if phase == "wal" {
				require.Equal(t, binaryValue+"updated", legacyValue)
				newValue, err := destinationClient.HGetAll(ctx, key+"_new").Result()
				require.NoError(t, err)
				require.Equal(t, map[string]string{"field": "created-during-migration", "short-lived": "temporary"}, newValue)
				newExpiry, err := destinationClient.Do(ctx, "hpexpiretime", key+"_new", "FIELDS", 2, "field", "short-lived").Result()
				require.NoError(t, err)
				require.Equal(t, []interface{}{expireAt, fieldExpireAt}, newExpiry)
			} else {
				require.Equal(t, binaryValue, legacyValue)
			}

			// Writes must use the migrated mode even though the destination defaults to legacy encoding.
			wantLive := int64(11)
			if phase == "wal" {
				wantLive = 16
			}
			newLive, err := destinationClient.HIncrBy(ctx, key, "live", 1).Result()
			require.NoError(t, err)
			require.Equal(t, wantLive, newLive)
			values["live"] = fmt.Sprint(wantLive)
			liveValue, err := destinationClient.HGet(ctx, key, "live").Result()
			require.NoError(t, err)
			require.Equal(t, values["live"], liveValue)
			fieldExpires, err = destinationClient.Do(ctx, "hpexpiretime", key, "FIELDS", 1, "live").Result()
			require.NoError(t, err)
			require.Equal(t, []interface{}{expires[1]}, fieldExpires)

			expiringValueOnDestination, err := destinationClient.HGet(ctx, expiringKey, expiringField).Result()
			require.NoError(t, err)
			require.Equal(t, expiringValue, expiringValueOnDestination)
			fieldExpires, err = destinationClient.Do(ctx, "hpexpiretime", expiringKey, "FIELDS", 1, expiringField).Result()
			require.NoError(t, err)
			require.Equal(t, []interface{}{fieldExpireAt}, fieldExpires)
			require.Eventually(t, func() bool {
				return errors.Is(destinationClient.HGet(ctx, expiringKey, expiringField).Err(), redis.Nil)
			}, 10*time.Second, 10*time.Millisecond)
			require.GreaterOrEqual(t, time.Now().UnixMilli(), fieldExpireAt)
			if phase == "snapshot" {
				delete(values, "live")
			}
			got, err = destinationClient.HGetAll(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, values, got)
			if phase == "wal" {
				require.ErrorIs(t, destinationClient.HGet(ctx, key+"_new", "short-lived").Err(), redis.Nil)
				newValue, err := destinationClient.HGetAll(ctx, key+"_new").Result()
				require.NoError(t, err)
				require.Equal(t, map[string]string{"field": "created-during-migration"}, newValue)
			}
			t.Run("hgetdel", func(t *testing.T) {
				for _, mode := range getDelModes {
					getDelKey := fmt.Sprintf("hgetdel-%s_{%s}", mode, util.SlotTable[slot])
					require.ErrorContains(t, sourceClient.Do(ctx, "hgetdel", getDelKey, "FIELDS", 1, "keeper").Err(), "MOVED")
					got, err := destinationClient.HGetAll(ctx, getDelKey).Result()
					require.NoError(t, err)
					require.Equal(t, map[string]string{"live": "ttl", "keeper": "persistent"}, got)
					require.Equal(t, getDelMetadata[getDelKey], util.GetKMetadata(t, destinationClient, ctx, getDelKey))
					at, err := destinationClient.Do(ctx, "pexpiretime", getDelKey).Int64()
					require.NoError(t, err)
					require.Equal(t, getDelKeyExpireAt, at)
					if mode == "field-expiration" {
						expires, err := destinationClient.Do(ctx, "hpexpiretime", getDelKey, "FIELDS", 3, "removed", "live", "keeper").Result()
						require.NoError(t, err)
						require.Equal(t, []interface{}{int64(-2), expireAt, int64(-1)}, expires)
					}
					exists, err := destinationClient.Exists(ctx, getDelKey+"-all").Result()
					require.NoError(t, err)
					require.Zero(t, exists)
				}
			})
		})
	}
}
