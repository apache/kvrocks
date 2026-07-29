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
