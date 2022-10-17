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

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestSlotMigrateFromSlave(t *testing.T) {
	ctx := context.Background()

	master := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { master.Close() }()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()
	masterID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODEID", masterID).Err())

	slave := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { slave.Close() }()
	slaveClient := slave.NewClient()
	defer func() { require.NoError(t, slaveClient.Close()) }()
	slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-100\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Slave cannot migrate slot", func(t *testing.T) {
		require.ErrorContains(t, slaveClient.Do(ctx, "clusterx", "migrate", "1", masterID).Err(), "Can't migrate slot")
	})

	t.Run("MIGRATE - Cannot migrate slot to a slave", func(t *testing.T) {
		require.ErrorContains(t, masterClient.Do(ctx, "clusterx", "migrate", "1", slaveID).Err(), "Can't migrate slot to a slave")
	})
}

func TestSlotMigrateServerKilled(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv1Alive := true
	defer func() {
		if srv1Alive {
			srv1.Close()
		}
	}()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Slot is out of range", func(t *testing.T) {
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "-1", id1).Err(), "Slot is out of range")
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "16384", id1).Err(), "Slot is out of range")
	})

	t.Run("MIGRATE - Cannot migrate slot to itself", func(t *testing.T) {
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", "1", id0).Err(), "Can't migrate slot to myself")
	})

	t.Run("MIGRATE - Fail to migrate slot if destination server is not running", func(t *testing.T) {
		srv1.Close()
		srv1Alive = false
		require.NoError(t, rdb0.Do(ctx, "clusterx", "migrate", "1", id1).Err())
		time.Sleep(50 * time.Millisecond)
		i := rdb0.ClusterInfo(ctx).Val()
		require.Contains(t, i, "migrating_slot: 1")
		require.Contains(t, i, "migrating_state: fail")
	})
}

func TestSlotMigrateDestServerKilledAgain(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv1Alive := true
	defer func() {
		if srv1Alive {
			srv1.Close()
		}
	}()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Migrate slot with empty string key or value", func(t *testing.T) {
		require.NoError(t, rdb0.Set(ctx, "", "slot0", 0).Err())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[0]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[0], "", 0).Err())
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "0", id1).Val())
		require.Eventually(t, func() bool {
			i := rdb0.ClusterInfo(ctx).Val()
			return strings.Contains(i, "migrating_slot: 0") && strings.Contains(i, "migrating_state: success")
		}, 5*time.Second, 100*time.Millisecond)
		require.Equal(t, "slot0", rdb1.Get(ctx, "").Val())
		require.Equal(t, "", rdb1.Get(ctx, util.SlotTable[0]).Val())
		require.NoError(t, rdb1.Del(ctx, util.SlotTable[0]).Err())
	})

	t.Run("MIGRATE - Migrate binary key-value", func(t *testing.T) {
		k1 := fmt.Sprintf("\x3a\x88{%s}\x3d\xaa", util.SlotTable[1])
		cnt := 257
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, k1, "\0000\0001").Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "1", id1).Val())
		k2 := fmt.Sprintf("\x49\x1f\x7f{%s}\xaf", util.SlotTable[1])
		require.NoError(t, rdb0.Set(ctx, k2, "\0000\0001", 0).Err())
		time.Sleep(time.Second)
		require.Eventually(t, func() bool {
			i := rdb1.ClusterInfo(ctx).Val()
			return strings.Contains(i, "importing_slot: 1") && strings.Contains(i, "import_state: success")
		}, 5*time.Second, 100*time.Millisecond)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, k1).Val())
		require.Equal(t, "\0000\0001", rdb1.LPop(ctx, k1).Val())
		require.Equal(t, "\0000\0001", rdb1.Get(ctx, k2).Val())
	})

	t.Run("MIGRATE - Migrate empty slot", func(t *testing.T) {
		require.NoError(t, rdb0.FlushDB(ctx).Err())
		require.NoError(t, rdb1.FlushDB(ctx).Err())
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "2", id1).Val())
		require.Eventually(t, func() bool {
			i := rdb0.ClusterInfo(ctx).Val()
			return strings.Contains(i, "migrating_slot: 2") && strings.Contains(i, "migrating_state: success")
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb1.Keys(ctx, "*").Err())
	})

	t.Run("MIGRATE - Fail to migrate slot because destination server is killed while migrating", func(t *testing.T) {
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[8], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "8", id1).Val())
		i := rdb0.ClusterInfo(ctx).Val()
		require.Contains(t, i, "migrating_slot: 8")
		require.Contains(t, i, "migrating_state: start")
		srv1.Close()
		srv1Alive = false
		time.Sleep(time.Second)
		i = rdb0.ClusterInfo(ctx).Val()
		require.Contains(t, i, "migrating_slot: 8")
		require.Contains(t, i, "migrating_state: fail")
	})
}

func TestSlotMigrateSourceServerFlushedOrKilled(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	srv0Alive := true
	defer func() {
		if srv0Alive {
			srv0.Close()
		}
	}()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()
	id0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODEID", id0).Err())

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Fail to migrate slot because source server is flushed", func(t *testing.T) {
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[11], i).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "32").Err())
		require.Equal(t, map[string]string{"migrate-speed": "32"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "11", id1).Val())
		require.Eventually(t, func() bool {
			i := rdb0.ClusterInfo(ctx).Val()
			return strings.Contains(i, "migrating_slot: 11") && strings.Contains(i, "migrating_state: start")
		}, 5*time.Second, 100*time.Millisecond)
		require.NoError(t, rdb0.FlushDB(ctx).Err())
		time.Sleep(time.Second)
		require.Eventually(t, func() bool {
			i := rdb0.ClusterInfo(ctx).Val()
			return strings.Contains(i, "migrating_slot: 11") && strings.Contains(i, "migrating_state: fail")
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("MIGRATE - Fail to migrate slot because source server is killed while migrating", func(t *testing.T) {
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[20], i).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "32").Err())
		require.Equal(t, map[string]string{"migrate-speed": "32"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", "20", id1).Val())
		require.Eventually(t, func() bool {
			return slices.Contains(rdb1.Keys(ctx, "*").Val(), util.SlotTable[20])
		}, 5*time.Second, 100*time.Millisecond)

		srv0.Close()
		srv0Alive = false
		time.Sleep(100 * time.Millisecond)
		require.NotContains(t, rdb1.Keys(ctx, "*").Val(), util.SlotTable[20])
	})
}
