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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func compactMigrationDB(t *testing.T, ctx context.Context, client *redis.Client) {
	t.Helper()
	require.NoError(t, client.Do(ctx, "flushmemtable").Err())
	for range 2 {
		require.NoError(t, client.Do(ctx, "compact").Err())
		require.Eventually(t, func() bool {
			info, err := client.Info(ctx).Result()
			return err == nil && strings.Contains(info, "is_compacting:no\r\n")
		}, 5*time.Second, 10*time.Millisecond)
	}
}

func TestSlotMigrateRenewedExpirationWithCompaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)

	source := util.StartServer(t, util.KvrocksServerConfigs{
		"cluster-enabled":                  "yes",
		"rocksdb.disable_auto_compactions": "yes",
		"migrate-batch-size-kb":            "16",
		"migrate-batch-rate-limit-mb":      "1",
	})
	t.Cleanup(source.Close)
	src := source.NewClient()
	t.Cleanup(func() { require.NoError(t, src.Close()) })
	destination := util.StartServer(t, util.KvrocksServerConfigs{
		"cluster-enabled":                  "yes",
		"rocksdb.disable_auto_compactions": "yes",
	})
	t.Cleanup(destination.Close)
	dst := destination.NewClient()
	t.Cleanup(func() { require.NoError(t, dst.Close()) })

	const sourceID = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	const destinationID = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, src.Do(ctx, "clusterx", "setnodeid", sourceID).Err())
	require.NoError(t, dst.Do(ctx, "clusterx", "setnodeid", destinationID).Err())
	nodes := fmt.Sprintf("%s %s %d master - 0-10000\n%s %s %d master - 10001-16383",
		sourceID, source.Host(), source.Port(), destinationID, destination.Host(), destination.Port())
	require.NoError(t, src.Do(ctx, "clusterx", "setnodes", nodes, "1").Err())
	require.NoError(t, dst.Do(ctx, "clusterx", "setnodes", nodes, "1").Err())

	const slot = 0
	key := func(prefix string) string { return fmt.Sprintf("%s_{%s}", prefix, util.SlotTable[slot]) }
	// The first list delays reading the expiring metadata from the snapshot; the
	// last list keeps WAL replay pending while we compact the imported snapshot.
	for _, filler := range []string{key("a_filler"), key("z_filler")} {
		pipe := src.Pipeline()
		value := strings.Repeat("x", 4096)
		for range 2048 {
			pipe.RPush(ctx, filler, value)
		}
		_, err := pipe.Exec(ctx)
		require.NoError(t, err)
	}

	extended, persistent := key("m_extended"), key("m_persistent")
	recreated, untouched := key("m_recreated"), key("m_untouched")
	marker := key("n_marker")
	fields := map[string]string{"first": "one", "second": "two", "binary": "\x00value\xff"}
	for _, hash := range []string{extended, persistent, recreated, untouched} {
		require.NoError(t, src.HSet(ctx, hash, fields).Err())
	}
	require.NoError(t, src.Set(ctx, marker, "snapshot passed the hashes", 0).Err())
	now, err := src.Time(ctx).Result()
	require.NoError(t, err)
	expireAt := now.Add(2 * time.Second)
	for _, hash := range []string{extended, persistent, recreated, untouched} {
		require.True(t, src.PExpireAt(ctx, hash, expireAt).Val())
	}
	oldExpiry, err := src.Do(ctx, "pexpiretime", extended).Int64()
	require.NoError(t, err)

	require.NoError(t, src.Do(ctx, "clusterx", "migrate", slot, destinationID).Err())
	// Import starts only after the source has acquired its RocksDB snapshot.
	waitForImportState(t, dst, slot, "start")
	renewedExpireAt := expireAt.Add(time.Minute)
	require.True(t, src.PExpireAt(ctx, extended, renewedExpireAt).Val())
	renewedExpiry, err := src.Do(ctx, "pexpiretime", extended).Int64()
	require.NoError(t, err)
	require.True(t, src.Persist(ctx, persistent).Val())
	require.Equal(t, int64(1), src.Del(ctx, recreated).Val())
	require.NoError(t, src.HSet(ctx, recreated, "new", "new version").Err())

	require.Eventually(t, func() bool {
		now, err := src.Time(ctx).Result()
		return err == nil && now.UnixMilli() > oldExpiry
	}, 5*time.Second, 10*time.Millisecond)
	compactMigrationDB(t, ctx, src)
	requireMigrateState(t, src, slot, SlotMigrationStateStarted)

	importConn := dst.Conn()
	t.Cleanup(func() { require.NoError(t, importConn.Close()) })
	markerArrived := func() bool {
		if err := importConn.Do(ctx, "asking").Err(); err != nil {
			return false
		}
		exists, err := importConn.Exists(ctx, marker).Result()
		return err == nil && exists == 1
	}
	require.False(t, markerArrived(), "source compaction must run before the expiring snapshot keys are read")
	require.Eventually(t, markerArrived, 20*time.Second, 10*time.Millisecond)
	compactMigrationDB(t, ctx, dst)
	requireMigrateState(t, src, slot, SlotMigrationStateStarted)
	waitForImportState(t, dst, slot, "start")

	waitForMigrateStateInDuration(t, src, slot, SlotMigrationStateSuccess, 30*time.Second)
	waitForImportState(t, dst, slot, SlotImportStateSuccess)
	for _, hash := range []string{extended, persistent} {
		got, err := dst.HGetAll(ctx, hash).Result()
		require.NoError(t, err)
		require.Equal(t, fields, got)
	}
	expiry, err := dst.Do(ctx, "pexpiretime", extended).Int64()
	require.NoError(t, err)
	require.Equal(t, renewedExpiry, expiry)
	require.Equal(t, time.Duration(-1), dst.PTTL(ctx, persistent).Val())
	require.Equal(t, int64(0), dst.Exists(ctx, untouched).Val())
	require.Equal(t, map[string]string{"new": "new version"}, dst.HGetAll(ctx, recreated).Val())
}
