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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

type SlotMigrationState string
type SlotImportState string

const (
	SlotMigrationStateStarted SlotMigrationState = "start"
	SlotMigrationStateSuccess SlotMigrationState = "success"
	SlotMigrationStateFailed  SlotMigrationState = "fail"

	SlotImportStateSuccess SlotImportState = "success"
	SlotImportStateFailed  SlotImportState = "error"
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
		require.ErrorContains(t, slaveClient.Do(ctx, "clusterx", "migrate", 1, masterID).Err(), "Can't migrate slot")
	})

	t.Run("MIGRATE - Cannot migrate slot to a slave", func(t *testing.T) {
		require.ErrorContains(t, masterClient.Do(ctx, "clusterx", "migrate", 1, slaveID).Err(), "Can't migrate slot to a slave")
	})
}

func TestSlotMigrateDestServerKilled(t *testing.T) {
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
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", -1, id1).Err(), "Slot is out of range")
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", 16384, id1).Err(), "Slot is out of range")
	})

	t.Run("MIGRATE - Cannot migrate slot to itself", func(t *testing.T) {
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", 1, id0).Err(), "Can't migrate slot to myself")
	})

	t.Run("MIGRATE - Fail to migrate slot if destination server is not running", func(t *testing.T) {
		slot := 1
		srv1.Close()
		srv1Alive = false
		require.NoError(t, rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Err())
		time.Sleep(50 * time.Millisecond)
		requireMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
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
		slot := 0
		require.NoError(t, rdb0.Set(ctx, "", "slot0", 0).Err())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slot], "", 0).Err())
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.Equal(t, "slot0", rdb1.Get(ctx, "").Val())
		require.Equal(t, "", rdb1.Get(ctx, util.SlotTable[slot]).Val())
		require.NoError(t, rdb1.Del(ctx, util.SlotTable[slot]).Err())
	})

	t.Run("MIGRATE - Migrate binary key-value", func(t *testing.T) {
		slot := 1
		k1 := fmt.Sprintf("\x3a\x88{%s}\x3d\xaa", util.SlotTable[slot])
		cnt := 257
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, k1, "\0000\0001").Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		k2 := fmt.Sprintf("\x49\x1f\x7f{%s}\xaf", util.SlotTable[slot])
		require.NoError(t, rdb0.Set(ctx, k2, "\0000\0001", 0).Err())
		time.Sleep(time.Second)
		waitForImportState(t, rdb1, slot, SlotImportStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, k1).Val())
		require.Equal(t, "\0000\0001", rdb1.LPop(ctx, k1).Val())
		require.Equal(t, "\0000\0001", rdb1.Get(ctx, k2).Val())
	})

	t.Run("MIGRATE - Migrate empty slot", func(t *testing.T) {
		slot := 2
		require.NoError(t, rdb0.FlushDB(ctx).Err())
		require.NoError(t, rdb1.FlushDB(ctx).Err())
		time.Sleep(500 * time.Millisecond)
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.NoError(t, rdb1.Keys(ctx, "*").Err())
	})

	t.Run("MIGRATE - Fail to migrate slot because destination server is killed while migrating", func(t *testing.T) {
		slot := 8
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		requireMigrateState(t, rdb0, slot, SlotMigrationStateStarted)
		srv1.Close()
		srv1Alive = false
		time.Sleep(time.Second)
		requireMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
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
		slot := 11
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "32").Err())
		require.Equal(t, map[string]string{"migrate-speed": "32"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateStarted)
		require.NoError(t, rdb0.FlushDB(ctx).Err())
		time.Sleep(time.Second)
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
	})

	t.Run("MIGRATE - Fail to migrate slot because source server is killed while migrating", func(t *testing.T) {
		slot := 20
		for i := 0; i < 20000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "32").Err())
		require.Equal(t, map[string]string{"migrate-speed": "32"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		require.Eventually(t, func() bool {
			return slices.Contains(rdb1.Keys(ctx, "*").Val(), util.SlotTable[slot])
		}, 5*time.Second, 100*time.Millisecond)

		srv0.Close()
		srv0Alive = false
		time.Sleep(100 * time.Millisecond)
		require.NotContains(t, rdb1.Keys(ctx, "*").Val(), util.SlotTable[slot])
	})
}

func TestSlotMigrateNewNodeAndAuth(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d master -", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Migrate slot to newly added node", func(t *testing.T) {
		slot := 21
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
		require.ErrorContains(t, rdb1.Set(ctx, util.SlotTable[slot], "foobar", 0).Err(), "MOVED")

		cnt := 100
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())

		k := fmt.Sprintf("{%s}_1", util.SlotTable[slot])
		require.ErrorContains(t, rdb0.Set(ctx, k, "slot21_value1", 0).Err(), "MOVED")
		require.Equal(t, "OK", rdb1.Set(ctx, k, "slot21_value1", 0).Val())
	})

	t.Run("MIGRATE - Auth before migrating slot", func(t *testing.T) {
		require.NoError(t, rdb1.ConfigSet(ctx, "requirepass", "password").Err())
		cnt := 100
		slot := 22
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}

		// migrating slot will fail if no auth
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[slot]).Err(), "MOVED")

		// migrating slot will fail if auth with wrong password
		require.NoError(t, rdb0.ConfigSet(ctx, "requirepass", "pass").Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateFailed)
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[slot]).Err(), "MOVED")

		// migrating slot will succeed if auth with right password
		require.NoError(t, rdb0.ConfigSet(ctx, "requirepass", "password").Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, 1, rdb1.Exists(ctx, util.SlotTable[slot]).Val())
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())
	})
}

func TestSlotMigrateThreeNodes(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
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

	srv2 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv2.Close() }()
	rdb2 := srv2.NewClient()
	defer func() { require.NoError(t, rdb2.Close()) }()
	id2 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx02"
	require.NoError(t, rdb2.Do(ctx, "clusterx", "SETNODEID", id2).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id0, srv0.Host(), srv0.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s\n", id1, srv1.Host(), srv1.Port(), id0)
	clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id2, srv2.Host(), srv2.Port())
	require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdb2.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("MIGRATE - Fail to migrate slot because source server is changed to slave during migrating", func(t *testing.T) {
		slot := 10
		for i := 0; i < 10000; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id2).Val())
		requireMigrateState(t, rdb0, slot, SlotMigrationStateStarted)

		// change source server to slave by set topology
		clusterNodes := fmt.Sprintf("%s %s %d master - 0-10000\n", id1, srv1.Host(), srv1.Port())
		clusterNodes += fmt.Sprintf("%s %s %d slave %s\n", id0, srv0.Host(), srv0.Port(), id1)
		clusterNodes += fmt.Sprintf("%s %s %d master - 10001-16383", id2, srv2.Host(), srv2.Port())
		require.NoError(t, rdb0.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, rdb2.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())

		// check destination importing status
		waitForImportState(t, rdb2, slot, SlotImportStateFailed)
	})
}

func TestSlotMigrateDataType(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv0.Close() }()
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

	t.Run("MIGRATE - Cannot migrate two slot at the same time", func(t *testing.T) {
		cnt := 20000
		slot := 0
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		otherSlot := 2
		require.ErrorContains(t, rdb0.Do(ctx, "clusterx", "migrate", otherSlot, id1).Err(), "There is already a migrating slot")
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())
	})

	t.Run("MIGRATE - Slot migrate all types of existing data", func(t *testing.T) {
		slot := 1
		keys := make(map[string]string, 0)
		for _, typ := range []string{"string", "expired_string", "list", "hash", "set", "zset", "bitmap", "sortint", "stream"} {
			keys[typ] = fmt.Sprintf("%s_{%s}", typ, util.SlotTable[slot])
			require.NoError(t, rdb0.Del(ctx, keys[typ]).Err())
		}
		// type string
		require.NoError(t, rdb0.Set(ctx, keys["string"], keys["string"], 0).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["string"], 10*time.Second).Err())
		// type expired string
		require.NoError(t, rdb0.Set(ctx, keys["expired_string"], keys["expired_string"], time.Second).Err())
		time.Sleep(3 * time.Second)
		require.Empty(t, rdb0.Get(ctx, keys["expired_string"]).Val())
		// type list
		require.NoError(t, rdb0.RPush(ctx, keys["list"], 0, 1, 2, 3, 4, 5).Err())
		require.NoError(t, rdb0.LPush(ctx, keys["list"], 9, 3, 7, 3, 5, 4).Err())
		require.NoError(t, rdb0.LSet(ctx, keys["list"], 5, 0).Err())
		require.NoError(t, rdb0.LInsert(ctx, keys["list"], "before", 9, 3).Err())
		require.NoError(t, rdb0.LTrim(ctx, keys["list"], 3, -3).Err())
		require.NoError(t, rdb0.RPop(ctx, keys["list"]).Err())
		require.NoError(t, rdb0.LPop(ctx, keys["list"]).Err())
		require.NoError(t, rdb0.LRem(ctx, keys["list"], 4, 3).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["list"], 10*time.Second).Err())
		// type hash
		require.NoError(t, rdb0.HMSet(ctx, keys["hash"], 0, 1, 2, 3, 4, 5, 6, 7).Err())
		require.NoError(t, rdb0.HDel(ctx, keys["hash"], "2").Err())
		require.NoError(t, rdb0.Expire(ctx, keys["hash"], 10*time.Second).Err())
		// type set
		require.NoError(t, rdb0.SAdd(ctx, keys["set"], 0, 1, 2, 3, 4, 5).Err())
		require.NoError(t, rdb0.SRem(ctx, keys["set"], 1, 3).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["set"], 10*time.Second).Err())
		// type zset
		require.NoError(t, rdb0.ZAdd(ctx, keys["zset"], []redis.Z{{0, 1}, {2, 3}, {4, 5}}...).Err())
		require.NoError(t, rdb0.ZRem(ctx, keys["zset"], 1, 3).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["zset"], 10*time.Second).Err())
		// type bitmap
		for i := 1; i < 20; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys["bitmap"], int64(i), 1).Err())
		}
		for i := 10000; i < 11000; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys["bitmap"], int64(i), 1).Err())
		}
		require.NoError(t, rdb0.Expire(ctx, keys["bitmap"], 10*time.Second).Err())
		// type sortint
		require.NoError(t, rdb0.Do(ctx, "SIADD", keys["sortint"], 2, 4, 1, 3).Err())
		require.NoError(t, rdb0.Do(ctx, "SIREM", keys["sortint"], 1).Err())
		require.NoError(t, rdb0.Expire(ctx, keys["sortint"], 10*time.Second).Err())
		// type stream
		for i := 1; i < 20; i++ {
			idxStr := strconv.FormatInt(int64(i), 10)
			require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
				Stream: keys["stream"],
				ID:     idxStr + "-0",
				Values: []string{"key" + idxStr, "value" + idxStr},
			}).Err())
		}
		require.NoError(t, rdb0.Expire(ctx, keys["stream"], 10*time.Second).Err())
		// check source data existence
		for _, typ := range []string{"string", "list", "hash", "set", "zset", "bitmap", "sortint", "stream"} {
			require.EqualValues(t, 1, rdb0.Exists(ctx, keys[typ]).Val())
		}
		// get source data
		lv := rdb0.LRange(ctx, keys["list"], 0, -1).Val()
		hv := rdb0.HGetAll(ctx, keys["hash"]).Val()
		sv := rdb0.SMembers(ctx, keys["set"]).Val()
		zv := rdb0.ZRangeWithScores(ctx, keys["zset"], 0, -1).Val()
		siv := rdb0.Do(ctx, "SIRANGE", keys["sortint"], 0, -1).Val()
		stV := rdb0.XRange(ctx, keys["stream"], "-", "+").Val()
		streamInfo := rdb0.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "19-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 19, streamInfo.EntriesAdded)
		require.EqualValues(t, "0-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 19, streamInfo.Length)

		// migrate slot 1, all keys above are belong to slot 1
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)

		// check destination data
		// type string
		require.Equal(t, keys["string"], rdb1.Get(ctx, keys["string"]).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["string"]).Val(), time.Second, 10*time.Second)
		require.Empty(t, rdb1.Get(ctx, keys["expired_string"]).Val())
		// type list
		require.EqualValues(t, lv, rdb1.LRange(ctx, keys["list"], 0, -1).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["list"]).Val(), time.Second, 10*time.Second)
		// type hash
		require.EqualValues(t, hv, rdb1.HGetAll(ctx, keys["hash"]).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["hash"]).Val(), time.Second, 10*time.Second)
		// type set
		require.EqualValues(t, sv, rdb1.SMembers(ctx, keys["set"]).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["set"]).Val(), time.Second, 10*time.Second)
		// type zset
		require.EqualValues(t, zv, rdb1.ZRangeWithScores(ctx, keys["zset"], 0, -1).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["zset"]).Val(), time.Second, 10*time.Second)
		// type bitmap
		for i := 1; i < 20; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys["bitmap"], int64(i)).Val())
		}
		for i := 10000; i < 11000; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys["bitmap"], int64(i)).Val())
		}
		for i := 0; i < 20; i += 2 {
			require.EqualValues(t, 0, rdb1.GetBit(ctx, keys["bitmap"], int64(i)).Val())
		}
		util.BetweenValues(t, rdb1.TTL(ctx, keys["bitmap"]).Val(), time.Second, 10*time.Second)
		// type sortint
		require.EqualValues(t, siv, rdb1.Do(ctx, "SIRANGE", keys["sortint"], 0, -1).Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["sortint"]).Val(), time.Second, 10*time.Second)
		// type stream
		require.EqualValues(t, stV, rdb1.XRange(ctx, keys["stream"], "-", "+").Val())
		util.BetweenValues(t, rdb1.TTL(ctx, keys["stream"]).Val(), time.Second, 10*time.Second)
		streamInfo = rdb1.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "19-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 19, streamInfo.EntriesAdded)
		require.EqualValues(t, "0-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 19, streamInfo.Length)
		// topology is changed on source server
		for _, typ := range []string{"string", "list", "hash", "set", "zset", "bitmap", "sortint", "stream"} {
			require.ErrorContains(t, rdb0.Exists(ctx, keys[typ]).Err(), "MOVED")
		}
	})

	t.Run("MIGRATE - increment sync stream from WAL", func(t *testing.T) {
		slot := 40
		keys := make(map[string]string, 0)
		for _, typ := range []string{"stream"} {
			keys[typ] = fmt.Sprintf("%s_{%s}", typ, util.SlotTable[slot])
			require.NoError(t, rdb0.Del(ctx, keys[typ]).Err())
		}
		for i := 1; i < 1000; i++ {
			idxStr := strconv.FormatInt(int64(i), 10)
			require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
				Stream: keys["stream"],
				ID:     idxStr + "-0",
				Values: []string{"key" + idxStr, "value" + idxStr},
			}).Err())
		}
		streamInfo := rdb0.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "999-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 999, streamInfo.EntriesAdded)
		require.EqualValues(t, "0-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 999, streamInfo.Length)

		// Slowdown the migration speed to prevent running before next increment commands
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "256").Err())
		defer func() {
			require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "4096").Err())
		}()
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		newStreamID := "1001"
		require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: keys["stream"],
			ID:     newStreamID + "-0",
			Values: []string{"key" + newStreamID, "value" + newStreamID},
		}).Err())
		require.NoError(t, rdb0.XDel(ctx, keys["stream"], "1-0").Err())
		require.NoError(t, rdb0.Do(ctx, "XSETID", keys["stream"], "1001-0", "MAXDELETEDID", "2-0").Err())
		waitForMigrateStateInDuration(t, rdb0, slot, SlotMigrationStateSuccess, time.Minute)

		streamInfo = rdb1.XInfoStream(ctx, keys["stream"]).Val()
		require.EqualValues(t, "1001-0", streamInfo.LastGeneratedID)
		require.EqualValues(t, 1000, streamInfo.EntriesAdded)
		require.EqualValues(t, "2-0", streamInfo.MaxDeletedEntryID)
		require.EqualValues(t, 999, streamInfo.Length)
	})

	t.Run("MIGRATE - Migrating empty stream", func(t *testing.T) {
		slot := 31
		key := fmt.Sprintf("stream_{%s}", util.SlotTable[slot])

		require.NoError(t, rdb0.Del(ctx, key).Err())

		require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     "5-0",
			Values: []string{"key5", "value5"},
		}).Err())
		require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
			Stream: key,
			ID:     "7-0",
			Values: []string{"key7", "value7"},
		}).Err())
		require.NoError(t, rdb0.XDel(ctx, key, "7-0").Err())
		require.NoError(t, rdb0.XDel(ctx, key, "5-0").Err())

		originInfo := rdb0.XInfoStream(ctx, key)
		originRes, err := originInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, "7-0", originRes.LastGeneratedID)
		require.EqualValues(t, 2, originRes.EntriesAdded)
		require.EqualValues(t, "7-0", originRes.MaxDeletedEntryID)
		require.EqualValues(t, 0, originRes.Length)

		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)

		require.ErrorContains(t, rdb0.Exists(ctx, key).Err(), "MOVED")

		migratedInfo := rdb1.XInfoStream(ctx, key)
		migratedRes, err := migratedInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, originRes.LastGeneratedID, migratedRes.LastGeneratedID)
		require.EqualValues(t, originRes.EntriesAdded, migratedRes.EntriesAdded)
		require.EqualValues(t, originRes.MaxDeletedEntryID, migratedRes.MaxDeletedEntryID)
		require.EqualValues(t, originRes.Length, migratedRes.Length)
	})

	t.Run("MIGRATE - Migrating stream with deleted entries", func(t *testing.T) {
		slot := 32
		key := fmt.Sprintf("stream_{%s}", util.SlotTable[slot])

		require.NoError(t, rdb0.Del(ctx, key).Err())

		for i := 1; i < 6; i++ {
			idxStr := strconv.FormatInt(int64(i), 10)
			require.NoError(t, rdb0.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				ID:     idxStr + "-0",
				Values: []string{"key" + idxStr, "value" + idxStr},
			}).Err())
		}
		require.NoError(t, rdb0.XDel(ctx, key, "1-0").Err())
		require.NoError(t, rdb0.XDel(ctx, key, "3-0").Err())

		originInfo := rdb0.XInfoStream(ctx, key)
		originRes, err := originInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, "5-0", originRes.LastGeneratedID)
		require.EqualValues(t, 5, originRes.EntriesAdded)
		require.EqualValues(t, "3-0", originRes.MaxDeletedEntryID)
		require.EqualValues(t, 3, originRes.Length)

		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)

		require.ErrorContains(t, rdb0.Exists(ctx, key).Err(), "MOVED")

		migratedInfo := rdb1.XInfoStream(ctx, key)
		migratedRes, err := migratedInfo.Result()
		require.NoError(t, err)
		require.EqualValues(t, originRes.LastGeneratedID, migratedRes.LastGeneratedID)
		require.EqualValues(t, originRes.EntriesAdded, migratedRes.EntriesAdded)
		require.EqualValues(t, originRes.MaxDeletedEntryID, migratedRes.MaxDeletedEntryID)
		require.EqualValues(t, originRes.Length, migratedRes.Length)
	})

	t.Run("MIGRATE - Accessing slot is forbidden on source server but not on destination server", func(t *testing.T) {
		slot := 3
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slot], 3, 0).Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.ErrorContains(t, rdb0.Set(ctx, util.SlotTable[slot], "source-value", 0).Err(), "MOVED")
		require.ErrorContains(t, rdb0.Del(ctx, util.SlotTable[slot]).Err(), "MOVED")
		require.ErrorContains(t, rdb0.Exists(ctx, util.SlotTable[slot]).Err(), "MOVED")
		require.NoError(t, rdb1.Set(ctx, util.SlotTable[slot], "destination-value", 0).Err())
	})

	t.Run("MIGRATE - Slot isn't forbidden writing when starting migrating", func(t *testing.T) {
		slot := 5
		cnt := 20000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		requireMigrateState(t, rdb0, slot, SlotMigrationStateStarted)
		// write during migrating
		require.EqualValues(t, cnt+1, rdb0.LPush(ctx, util.SlotTable[slot], cnt).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.Equal(t, strconv.Itoa(cnt), rdb1.LPop(ctx, util.SlotTable[slot]).Val())
	})

	t.Run("MIGRATE - Slot keys are not cleared after migration but cleared after setslot", func(t *testing.T) {
		slot := 6
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slot], "slot6", 0).Err())
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.Equal(t, "slot6", rdb1.Get(ctx, util.SlotTable[slot]).Val())
		require.Contains(t, rdb0.Keys(ctx, "*").Val(), util.SlotTable[slot])
		require.NoError(t, rdb0.Do(ctx, "clusterx", "setslot", slot, "node", id1, "2").Err())
		require.NotContains(t, rdb0.Keys(ctx, "*").Val(), util.SlotTable[slot])
	})

	t.Run("MIGRATE - Migrate incremental data via parsing and filtering data in WAL", func(t *testing.T) {
		migratingSlot := 15
		keys := []string{
			// key for slowing migrate-speed when migrating existing data
			util.SlotTable[migratingSlot],
			// the following keys belong to slot 15; keys of all the data types (string/hash/set/zset/list/sortint)
			"key:000042915392",
			"key:000043146202",
			"key:000044434182",
			"key:000045189446",
			"key:000047413016",
			"key:000049190069",
			"key:000049930003",
			"key:000049980785",
			"key:000056730838",
		}
		for _, key := range keys {
			require.NoError(t, rdb0.Del(ctx, key).Err())
		}
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "64").Err())
		require.Equal(t, map[string]string{"migrate-speed": "64"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())

		cnt := 2000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, keys[0], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", migratingSlot, id1).Val())

		// write key that doesn't belong to this slot
		nonMigratingSlot := 12
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[nonMigratingSlot]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[nonMigratingSlot], "non-migrating-value", 0).Err())

		// write increment operations include all kinds of types
		// 1. type string
		require.NoError(t, rdb0.SetEx(ctx, keys[1], 15, 10000*time.Second).Err())
		require.NoError(t, rdb0.IncrBy(ctx, keys[1], 2).Err())
		require.NoError(t, rdb0.DecrBy(ctx, keys[1], 1).Err())
		require.NoError(t, rdb0.Set(ctx, keys[2], "val", 0).Err())
		require.NoError(t, rdb0.Del(ctx, keys[2]).Err())
		require.NoError(t, rdb0.SetBit(ctx, keys[3], 10086, 1).Err())
		require.NoError(t, rdb0.Expire(ctx, keys[3], 10000*time.Second).Err())
		// verify expireat binlog could be parsed
		slotWithExpiringKey := nonMigratingSlot + 1
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slotWithExpiringKey]).Err())
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slotWithExpiringKey], "expiring-value", 0).Err())
		require.NoError(t, rdb0.ExpireAt(ctx, util.SlotTable[slotWithExpiringKey], time.Now().Add(100*time.Second)).Err())
		// verify del command
		slotWithDeletedKey := nonMigratingSlot + 2
		require.NoError(t, rdb0.Set(ctx, util.SlotTable[slotWithDeletedKey], "will-be-deleted", 0).Err())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slotWithDeletedKey]).Err())
		// 2. type hash
		require.NoError(t, rdb0.HMSet(ctx, keys[4], "f1", "1", "f2", "2").Err())
		require.NoError(t, rdb0.HDel(ctx, keys[4], "f1").Err())
		require.NoError(t, rdb0.HIncrBy(ctx, keys[4], "f2", 2).Err())
		require.NoError(t, rdb0.HIncrBy(ctx, keys[4], "f2", -1).Err())
		// 3. type set
		require.NoError(t, rdb0.SAdd(ctx, keys[5], 1, 2).Err())
		require.NoError(t, rdb0.SRem(ctx, keys[5], 1).Err())
		// 4. type zset
		require.NoError(t, rdb0.ZAdd(ctx, keys[6], []redis.Z{{2, "m1"}}...).Err())
		require.NoError(t, rdb0.ZIncrBy(ctx, keys[6], 2, "m1").Err())
		require.NoError(t, rdb0.ZIncrBy(ctx, keys[6], -1, "m1").Err())
		require.NoError(t, rdb0.ZAdd(ctx, keys[6], []redis.Z{{3, "m3"}}...).Err())
		require.NoError(t, rdb0.ZRem(ctx, keys[6], "m3").Err())
		require.NoError(t, rdb0.ZAdd(ctx, keys[6], []redis.Z{{1, ""}}...).Err())
		require.NoError(t, rdb0.ZRem(ctx, keys[6], "").Err())
		// 5. type list
		require.NoError(t, rdb0.LPush(ctx, keys[7], "item1").Err())
		require.NoError(t, rdb0.RPush(ctx, keys[7], "item2").Err())
		require.NoError(t, rdb0.LPush(ctx, keys[7], "item3").Err())
		require.NoError(t, rdb0.RPush(ctx, keys[7], "item4").Err())
		require.Equal(t, "item3", rdb0.LPop(ctx, keys[7]).Val())
		require.Equal(t, "item4", rdb0.RPop(ctx, keys[7]).Val())
		require.NoError(t, rdb0.LPush(ctx, keys[7], "item7").Err())
		require.NoError(t, rdb0.RPush(ctx, keys[7], "item8").Err())
		require.NoError(t, rdb0.LSet(ctx, keys[7], 0, "item5").Err())
		require.NoError(t, rdb0.LInsert(ctx, keys[7], "before", "item2", "item6").Err())
		require.NoError(t, rdb0.LRem(ctx, keys[7], 1, "item7").Err())
		require.NoError(t, rdb0.LTrim(ctx, keys[7], 1, -1).Err())
		// 6. type bitmap
		for i := 1; i < 20; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys[8], int64(i), 1).Err())
		}
		for i := 10000; i < 11000; i += 2 {
			require.NoError(t, rdb0.SetBit(ctx, keys[8], int64(i), 1).Err())
		}
		// 7. type sortint
		require.NoError(t, rdb0.Do(ctx, "SIADD", keys[9], 2, 4, 1, 3).Err())
		require.NoError(t, rdb0.Do(ctx, "SIREM", keys[9], 2).Err())
		// check data in source server
		require.EqualValues(t, cnt, rdb0.LLen(ctx, keys[0]).Val())
		strv := rdb0.Get(ctx, keys[1]).Val()
		strt := rdb0.TTL(ctx, keys[1]).Val()
		bv := rdb0.GetBit(ctx, keys[3], 10086).Val()
		bt := rdb0.TTL(ctx, keys[3]).Val()
		hv := rdb0.HGetAll(ctx, keys[4]).Val()
		sv := rdb0.SMembers(ctx, keys[5]).Val()
		zv := rdb0.ZRangeWithScores(ctx, keys[6], 0, -1).Val()
		lv := rdb0.LRange(ctx, keys[7], 0, -1).Val()
		siv := rdb0.Do(ctx, "SIRANGE", keys[9], 0, -1).Val()
		waitForMigrateStateInDuration(t, rdb0, migratingSlot, SlotMigrationStateSuccess, time.Minute)
		waitForImportState(t, rdb1, migratingSlot, SlotImportStateSuccess)
		// check if the data is consistent
		// 1. type string
		require.EqualValues(t, cnt, rdb1.LLen(ctx, keys[0]).Val())
		require.EqualValues(t, strv, rdb1.Get(ctx, keys[1]).Val())
		require.Less(t, rdb1.TTL(ctx, keys[1]).Val()-strt, 100*time.Second)
		require.Empty(t, rdb1.Get(ctx, keys[2]).Val())
		require.EqualValues(t, bv, rdb1.GetBit(ctx, keys[3], 10086).Val())
		require.Less(t, rdb1.TTL(ctx, keys[3]).Val()-bt, 100*time.Second)
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[slotWithExpiringKey]).Err(), "MOVED")
		// 2. type hash
		require.EqualValues(t, hv, rdb1.HGetAll(ctx, keys[4]).Val())
		require.EqualValues(t, "3", rdb1.HGet(ctx, keys[4], "f2").Val())
		// 3. type set
		require.EqualValues(t, sv, rdb1.SMembers(ctx, keys[5]).Val())
		// 4. type zset
		require.EqualValues(t, zv, rdb1.ZRangeWithScores(ctx, keys[6], 0, -1).Val())
		require.EqualValues(t, 3, rdb1.ZScore(ctx, keys[6], "m1").Val())
		// 5. type list
		require.EqualValues(t, lv, rdb1.LRange(ctx, keys[7], 0, -1).Val())
		// 6. type bitmap
		for i := 1; i < 20; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys[8], int64(i)).Val())
		}
		for i := 10000; i < 11000; i += 2 {
			require.EqualValues(t, 1, rdb1.GetBit(ctx, keys[8], int64(i)).Val())
		}
		for i := 0; i < 20; i += 2 {
			require.EqualValues(t, 0, rdb1.GetBit(ctx, keys[8], int64(i)).Val())
		}
		// 7. type sortint
		require.EqualValues(t, siv, rdb1.Do(ctx, "SIRANGE", keys[9], 0, -1).Val())

		// not migrate if the key doesn't belong to slot
		require.Equal(t, "non-migrating-value", rdb0.Get(ctx, util.SlotTable[nonMigratingSlot]).Val())
		require.ErrorContains(t, rdb1.Exists(ctx, util.SlotTable[nonMigratingSlot]).Err(), "MOVED")
		require.EqualValues(t, 0, rdb0.Exists(ctx, util.SlotTable[slotWithDeletedKey]).Val())
	})

	t.Run("MIGRATE - Slow migrate speed", func(t *testing.T) {
		slot := 16
		require.NoError(t, rdb0.ConfigSet(ctx, "migrate-speed", "16").Err())
		require.Equal(t, map[string]string{"migrate-speed": "16"}, rdb0.ConfigGet(ctx, "migrate-speed").Val())
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
		// more than pipeline size(16) and max items(16) in command
		cnt := 1000
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		// should not finish 1.5s
		time.Sleep(1500 * time.Millisecond)
		requireMigrateState(t, rdb0, slot, SlotMigrationStateStarted)
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
	})

	t.Run("MIGRATE - Data of migrated slot can't be written to source but can be written to destination", func(t *testing.T) {
		slot := 17
		require.NoError(t, rdb0.Del(ctx, util.SlotTable[slot]).Err())
		cnt := 100
		for i := 0; i < cnt; i++ {
			require.NoError(t, rdb0.LPush(ctx, util.SlotTable[slot], i).Err())
		}
		require.Equal(t, "OK", rdb0.Do(ctx, "clusterx", "migrate", slot, id1).Val())
		waitForMigrateState(t, rdb0, slot, SlotMigrationStateSuccess)
		require.EqualValues(t, cnt, rdb1.LLen(ctx, util.SlotTable[slot]).Val())
		// write the migrated slot to source server
		k := fmt.Sprintf("{%s}_1", util.SlotTable[slot])
		require.ErrorContains(t, rdb0.Set(ctx, k, "slot17_value1", 0).Err(), "MOVED")
		// write the migrated slot to destination server
		require.NoError(t, rdb1.Set(ctx, k, "slot17_value1", 0).Err())
	})
}

func waitForMigrateState(t testing.TB, client *redis.Client, slot int, state SlotMigrationState) {
	waitForMigrateStateInDuration(t, client, slot, state, 5*time.Second)
}

func waitForMigrateStateInDuration(t testing.TB, client *redis.Client, slot int, state SlotMigrationState, d time.Duration) {
	require.Eventually(t, func() bool {
		i := client.ClusterInfo(context.Background()).Val()
		return strings.Contains(i, fmt.Sprintf("migrating_slot: %d", slot)) &&
			strings.Contains(i, fmt.Sprintf("migrating_state: %s", state))
	}, d, 100*time.Millisecond)
}

func requireMigrateState(t testing.TB, client *redis.Client, slot int, state SlotMigrationState) {
	i := client.ClusterInfo(context.Background()).Val()
	require.Contains(t, i, fmt.Sprintf("migrating_slot: %d", slot))
	require.Contains(t, i, fmt.Sprintf("migrating_state: %s", state))
}

func waitForImportState(t testing.TB, client *redis.Client, n int, state SlotImportState) {
	require.Eventually(t, func() bool {
		i := client.ClusterInfo(context.Background()).Val()
		return strings.Contains(i, fmt.Sprintf("importing_slot: %d", n)) &&
			strings.Contains(i, fmt.Sprintf("import_state: %s", state))
	}, 10*time.Second, 100*time.Millisecond)
}
