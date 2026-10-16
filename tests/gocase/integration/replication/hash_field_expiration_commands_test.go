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

package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func requireHFEArray(t *testing.T, got interface{}, want ...interface{}) {
	t.Helper()

	values, ok := got.([]interface{})
	require.Truef(t, ok, "expected []interface{}, got %T", got)
	require.Equal(t, want, values)
}

func waitForHashFieldToExpire(t *testing.T, rdb *redis.Client, ctx context.Context, key, field string) {
	t.Helper()

	require.Eventually(t, func() bool {
		return errors.Is(rdb.HGet(ctx, key, field).Err(), redis.Nil)
	}, 5*time.Second, 50*time.Millisecond)
}

func TestHashFieldExpirationReplication(t *testing.T) {
	ctx := context.Background()
	master := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode":               "legacy",
		"rocksdb.disable_auto_compactions": "yes",
		"use-rsid-psync":                   "yes",
	})
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()

	replica := util.StartServer(t, util.KvrocksServerConfigs{
		"hash-encoding-mode":               "legacy",
		"rocksdb.disable_auto_compactions": "yes",
		"use-rsid-psync":                   "yes",
	})
	defer replica.Close()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	legacyKey := "legacy-hash"
	binaryValue := "\x00\x00\x00\x00\x00\x00\x00\x00value\xff"
	require.NoError(t, masterClient.HSet(ctx, legacyKey, "field", binaryValue).Err())
	require.NoError(t, masterClient.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
	key := "hfe-hash"
	require.NoError(t, masterClient.HSet(ctx, key, "persistent", binaryValue, "live", "10", "expired", "gone").Err())
	expireAt := time.Now().Add(10 * time.Minute).UnixMilli()
	require.NoError(t, masterClient.Do(ctx, "hpexpireat", key, expireAt, "FIELDS", 1, "live").Err())
	expiredValue, err := masterClient.HGet(ctx, key, "expired").Result()
	require.NoError(t, err)
	require.Equal(t, "gone", expiredValue)
	expiredResult, err := masterClient.Do(ctx, "hpexpire", key, 1000, "FIELDS", 1, "expired").Result()
	require.NoError(t, err)
	requireHFEArray(t, expiredResult, int64(1))
	require.NoError(t, masterClient.PExpireAt(ctx, key, time.Now().Add(20*time.Minute)).Err())
	// This field must already be expired on the source before the checkpoint is taken.
	waitForHashFieldToExpire(t, masterClient, ctx, key, "expired")

	metadata := util.GetKMetadata(t, masterClient, ctx, key)
	require.Equal(t, "field-expiration", metadata.Mode)
	require.Equal(t, int64(3), metadata.Size)
	require.Equal(t, int64(1), metadata.Persist)

	requireReplicatedHash := func(t *testing.T, values map[string]string, expires ...interface{}) {
		t.Helper()
		util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)
		got, err := replicaClient.HGetAll(ctx, key).Result()
		require.NoError(t, err)
		require.Equal(t, values, got)
		fieldExpires, err := replicaClient.Do(ctx, "hpexpiretime", key, "FIELDS", 3, "persistent", "live", "expired").Result()
		require.NoError(t, err)
		requireHFEArray(t, fieldExpires, expires...)
		require.Equal(t, util.GetKMetadata(t, masterClient, ctx, key), util.GetKMetadata(t, replicaClient, ctx, key))
	}

	expiringKey := "hfe-full-sync-expiry"
	require.NoError(t, masterClient.HSet(ctx, expiringKey, "field", "value", "keeper", "persistent").Err())
	fieldExpireAt := time.Now().Add(3 * time.Second).UnixMilli()
	require.NoError(t, masterClient.Do(ctx, "hpexpireat", expiringKey, fieldExpireAt, "FIELDS", 1, "field").Err())

	// Distinct replication histories force a checkpoint transfer even while the source WAL is retained.
	util.SlaveOf(t, replicaClient, master)
	util.WaitForSync(t, replicaClient)
	require.Equal(t, "1", util.FindInfoEntry(masterClient, "sync_full"))
	requireReplicatedHash(t, map[string]string{"persistent": binaryValue, "live": "10"}, int64(-1), expireAt, int64(-2))
	require.Equal(t, metadata, util.GetKMetadata(t, replicaClient, ctx, key))
	got, err := replicaClient.HGet(ctx, legacyKey, "field").Result()
	require.NoError(t, err)
	require.Equal(t, binaryValue, got)
	require.Equal(t, "legacy", util.GetKMetadata(t, replicaClient, ctx, legacyKey).Mode)

	got, err = replicaClient.HGet(ctx, expiringKey, "field").Result()
	require.NoError(t, err)
	require.Equal(t, "value", got)
	fieldExpires, err := replicaClient.Do(ctx, "hpexpiretime", expiringKey, "FIELDS", 1, "field").Result()
	require.NoError(t, err)
	requireHFEArray(t, fieldExpires, fieldExpireAt)
	waitForHashFieldToExpire(t, replicaClient, ctx, expiringKey, "field")
	require.GreaterOrEqual(t, time.Now().UnixMilli(), fieldExpireAt)
	fields, err := replicaClient.HGetAll(ctx, expiringKey).Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"keeper": "persistent"}, fields)

	// Subsequent updates must use WAL replication without another full sync.
	require.NoError(t, masterClient.HIncrBy(ctx, key, "live", 5).Err())
	requireReplicatedHash(t, map[string]string{"persistent": binaryValue, "live": "15"}, int64(-1), expireAt, int64(-2))
	require.Equal(t, metadata, util.GetKMetadata(t, replicaClient, ctx, key))

	require.NoError(t, masterClient.Do(ctx, "hpexpireat", key, expireAt, "FIELDS", 1, "persistent").Err())
	requireReplicatedHash(t, map[string]string{"persistent": binaryValue, "live": "15"}, expireAt, expireAt, int64(-2))
	require.NoError(t, masterClient.Do(ctx, "hpersist", key, "FIELDS", 1, "live").Err())
	requireReplicatedHash(t, map[string]string{"persistent": binaryValue, "live": "15"}, expireAt, int64(-1), int64(-2))
	require.NoError(t, masterClient.HSet(ctx, key, "persistent", "updated").Err())
	requireReplicatedHash(t, map[string]string{"persistent": "updated", "live": "15"}, int64(-1), int64(-1), int64(-2))

	require.NoError(t, masterClient.HSet(ctx, legacyKey, "field", binaryValue+"updated").Err())
	util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)
	got, err = replicaClient.HGet(ctx, legacyKey, "field").Result()
	require.NoError(t, err)
	require.Equal(t, binaryValue+"updated", got)
	require.Equal(t, "legacy", util.GetKMetadata(t, replicaClient, ctx, legacyKey).Mode)

	incrementalExpireAt := time.Now().Add(3 * time.Second).UnixMilli()
	require.NoError(t, masterClient.Do(ctx, "hpexpireat", key, incrementalExpireAt, "FIELDS", 1, "live").Err())
	requireReplicatedHash(t, map[string]string{"persistent": "updated", "live": "15"}, int64(-1), incrementalExpireAt, int64(-2))
	waitForHashFieldToExpire(t, replicaClient, ctx, key, "live")
	require.GreaterOrEqual(t, time.Now().UnixMilli(), incrementalExpireAt)
	requireReplicatedHash(t, map[string]string{"persistent": "updated"}, int64(-1), int64(-2), int64(-2))
	require.Equal(t, "1", util.FindInfoEntry(masterClient, "sync_full"))
}

func TestHashFieldExpirationHSetExHGetExReplication(t *testing.T) {
	configs := util.KvrocksServerConfigs{
		"hash-encoding-mode":               "field-expiration",
		"rocksdb.disable_auto_compactions": "yes",
		"resp3-enabled":                    "yes",
	}
	master := util.StartServer(t, configs)
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()

	replica := util.StartServer(t, configs)
	defer replica.Close()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	ctx := context.Background()
	util.SlaveOf(t, replicaClient, master)
	util.WaitForSync(t, replicaClient)

	key := "hsetex-hgetex-replication"
	result, err := masterClient.Do(ctx, "hsetex", key, "PX", 600000, "FIELDS", 3,
		"a", "1", "b", "2", "c", "3").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), result)
	util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)

	masterExpire := masterClient.Do(ctx, "hpexpiretime", key, "FIELDS", 3, "a", "b", "c").Val()
	replicaExpire := replicaClient.Do(ctx, "hpexpiretime", key, "FIELDS", 3, "a", "b", "c").Val()
	require.Equal(t, masterExpire, replicaExpire)
	expires := masterExpire.([]interface{})
	require.Equal(t, expires[0], expires[1])
	require.Equal(t, expires[1], expires[2])
	require.Equal(t, util.GetKMetadata(t, masterClient, ctx, key), util.GetKMetadata(t, replicaClient, ctx, key))

	got, err := masterClient.Do(ctx, "hgetex", key, "PERSIST", "FIELDS", 2, "a", "missing").Result()
	require.NoError(t, err)
	requireHFEArray(t, got, "1", nil)
	result, err = masterClient.Do(ctx, "hsetex", key, "KEEPTTL", "FIELDS", 2, "b", "20", "d", "40").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), result)
	expireAt := time.Now().Add(20 * time.Minute).UnixMilli()
	got, err = masterClient.Do(ctx, "hgetex", key, "PXAT", expireAt, "FIELDS", 2, "a", "c").Result()
	require.NoError(t, err)
	requireHFEArray(t, got, "1", "3")
	util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)

	require.Equal(t, masterClient.HGetAll(ctx, key).Val(), replicaClient.HGetAll(ctx, key).Val())
	require.Equal(t, masterClient.Do(ctx, "hpexpiretime", key, "FIELDS", 4, "a", "b", "c", "d").Val(),
		replicaClient.Do(ctx, "hpexpiretime", key, "FIELDS", 4, "a", "b", "c", "d").Val())
	require.Equal(t, util.GetKMetadata(t, masterClient, ctx, key), util.GetKMetadata(t, replicaClient, ctx, key))

	cleanupKey := "hsetex-condition-cleanup-replication"
	require.Equal(t, int64(2), masterClient.HSet(ctx, cleanupKey, "expired", "value", "keeper", "value").Val())
	require.Equal(t, []interface{}{int64(1)}, masterClient.Do(ctx, "hexpire", cleanupKey, 1, "FIELDS", 1, "expired").Val())
	util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)
	waitForHashFieldToExpire(t, masterClient, ctx, cleanupKey, "expired")

	result, err = masterClient.Do(ctx, "hsetex", cleanupKey, "FXX", "FIELDS", 1, "expired", "new").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(0), result)
	util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)
	require.Equal(t, map[string]string{"keeper": "value"}, replicaClient.HGetAll(ctx, cleanupKey).Val())
	require.Equal(t, util.GetKMetadata(t, masterClient, ctx, cleanupKey),
		util.GetKMetadata(t, replicaClient, ctx, cleanupKey))
}

func TestHashFieldExpirationHGetDelReplication(t *testing.T) {
	ctx := context.Background()
	configs := util.KvrocksServerConfigs{
		"hash-encoding-mode":               "legacy",
		"rocksdb.disable_auto_compactions": "yes",
		"use-rsid-psync":                   "yes",
	}
	master := util.StartServer(t, configs)
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()
	replica := util.StartServer(t, configs)
	defer replica.Close()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	modes := []string{"legacy", "field-expiration"}
	phases := []string{"full-sync", "incremental"}
	binaryValue := "\x00\x00\x00\x00\x00\x00\x00\x01value\xff"
	fieldExpireAt := time.Now().Add(10 * time.Minute).UnixMilli()
	keyExpireAt := time.Now().Add(20 * time.Minute).Truncate(time.Second).UnixMilli()
	for _, mode := range modes {
		require.NoError(t, masterClient.ConfigSet(ctx, "hash-encoding-mode", mode).Err())
		for _, phase := range phases {
			key := "hgetdel-" + mode + "-" + phase
			require.NoError(t, masterClient.HSet(ctx, key, "removed", binaryValue, "live", "ttl", "keeper", "persistent").Err())
			require.NoError(t, masterClient.HSet(ctx, key+"-all", "first", binaryValue, "last", "value").Err())
			if mode == "field-expiration" {
				require.NoError(t, masterClient.Do(ctx, "HPEXPIREAT", key, fieldExpireAt, "FIELDS", 2, "removed", "live").Err())
				require.NoError(t, masterClient.Do(ctx, "HPEXPIREAT", key+"-all", fieldExpireAt, "FIELDS", 1, "last").Err())
			}
			require.NoError(t, masterClient.Do(ctx, "PEXPIREAT", key, keyExpireAt).Err())
		}
	}

	deleteFields := func(t *testing.T, phase string) {
		t.Helper()
		for _, mode := range modes {
			otherMode := "legacy"
			if mode == "legacy" {
				otherMode = "field-expiration"
			}
			require.NoError(t, masterClient.ConfigSet(ctx, "hash-encoding-mode", otherMode).Err())
			key := "hgetdel-" + mode + "-" + phase
			got, err := masterClient.Do(ctx, "HGETDEL", key, "FIELDS", 3, "removed", "missing", "removed").Result()
			require.NoError(t, err)
			requireHFEArray(t, got, binaryValue, nil, nil)
			got, err = masterClient.Do(ctx, "HGETDEL", key+"-all", "FIELDS", 4, "first", "first", "missing", "last").Result()
			require.NoError(t, err)
			requireHFEArray(t, got, binaryValue, nil, nil, "value")
		}
	}
	checkReplicated := func(t *testing.T, phase string) {
		t.Helper()
		util.WaitForOffsetSync(t, masterClient, replicaClient, 5*time.Second)
		for _, mode := range modes {
			key := "hgetdel-" + mode + "-" + phase
			for _, client := range []*redis.Client{masterClient, replicaClient} {
				values, err := client.HGetAll(ctx, key).Result()
				require.NoError(t, err)
				require.Equal(t, map[string]string{"live": "ttl", "keeper": "persistent"}, values)
				exists, err := client.Exists(ctx, key+"-all").Result()
				require.NoError(t, err)
				require.Zero(t, exists)
				at, err := client.Do(ctx, "PEXPIRETIME", key).Int64()
				require.NoError(t, err)
				require.Equal(t, keyExpireAt, at)
				metadata := util.GetKMetadata(t, client, ctx, key)
				require.Equal(t, mode, metadata.Mode)
				require.Equal(t, int64(2), metadata.Size)
				if mode == "field-expiration" {
					require.Equal(t, int64(1), metadata.Persist)
					got, err := client.Do(ctx, "HPEXPIRETIME", key, "FIELDS", 3, "removed", "live", "keeper").Result()
					require.NoError(t, err)
					requireHFEArray(t, got, int64(-2), fieldExpireAt, int64(-1))
				}
			}
			require.Equal(t, util.GetKMetadata(t, masterClient, ctx, key), util.GetKMetadata(t, replicaClient, ctx, key))
		}
	}

	deleteFields(t, "full-sync")
	util.SlaveOf(t, replicaClient, master)
	util.WaitForSync(t, replicaClient)
	require.Equal(t, "1", util.FindInfoEntry(masterClient, "sync_full"))
	checkReplicated(t, "full-sync")
	for _, mode := range modes {
		values, err := replicaClient.HGetAll(ctx, "hgetdel-"+mode+"-incremental-all").Result()
		require.NoError(t, err)
		require.Equal(t, map[string]string{"first": binaryValue, "last": "value"}, values)
	}
	deleteFields(t, "incremental")
	checkReplicated(t, "incremental")
	for _, mode := range modes {
		key := "hgetdel-" + mode + "-incremental"
		require.ErrorContains(t, replicaClient.Do(ctx, "HGETDEL", key, "FIELDS", 1, "keeper").Err(), "READONLY")
	}
	checkReplicated(t, "full-sync")
	require.Equal(t, "1", util.FindInfoEntry(masterClient, "sync_full"))
}
