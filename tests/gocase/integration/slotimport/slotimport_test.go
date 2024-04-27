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

package slotimport

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestImportSlaveServer(t *testing.T) {
	ctx := context.Background()

	srvA := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srvA.Close() }()
	rdbA := srvA.NewClient()
	defer func() { require.NoError(t, rdbA.Close()) }()
	srvAID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdbA.Do(ctx, "clusterx", "SETNODEID", srvAID).Err())

	srvB := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srvB.Close() }()
	rdbB := srvB.NewClient()
	defer func() { require.NoError(t, rdbB.Close()) }()
	srvBID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdbB.Do(ctx, "clusterx", "SETNODEID", srvBID).Err())

	clusterNodes := fmt.Sprintf("%s 127.0.0.1 %d master - 0-100", srvAID, srvA.Port())
	clusterNodes = fmt.Sprintf("%s\n%s 127.0.0.1 %d slave %s", clusterNodes, srvBID, srvB.Port(), srvAID)

	require.NoError(t, rdbA.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdbB.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("IMPORT - Can't import slot to slave", func(t *testing.T) {
		time.Sleep(100 * time.Millisecond)
		require.ErrorContains(t, rdbB.Do(ctx, "cluster", "import", 1, 0).Err(), "Slave can't import slot")
	})
}

func TestImportedServer(t *testing.T) {
	ctx := context.Background()

	srvA := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srvA.Close() }()
	rdbA := srvA.NewClient()
	defer func() { require.NoError(t, rdbA.Close()) }()
	srvAID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, rdbA.Do(ctx, "clusterx", "SETNODEID", srvAID).Err())

	srvB := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srvB.Close() }()
	rdbB := srvB.NewClient()
	defer func() { require.NoError(t, rdbB.Close()) }()
	srvBID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdbB.Do(ctx, "clusterx", "SETNODEID", srvBID).Err())

	clusterNodes := fmt.Sprintf("%s 127.0.0.1 %d master - 0-8191", srvAID, srvA.Port())
	clusterNodes = fmt.Sprintf("%s\n%s 127.0.0.1 %d master - 8192-16383", clusterNodes, srvBID, srvB.Port())

	require.NoError(t, rdbA.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, rdbB.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("IMPORT - error slot", func(t *testing.T) {
		require.ErrorContains(t, rdbA.Do(ctx, "cluster", "import", -1, 0).Err(), "Slot is out of range")
		require.ErrorContains(t, rdbA.Do(ctx, "cluster", "import", 16384, 0).Err(), "Slot is out of range")
	})

	t.Run("IMPORT - slot with error state", func(t *testing.T) {
		require.ErrorContains(t, rdbA.Do(ctx, "cluster", "import", 1, 4).Err(), "Invalid import state")
		require.ErrorContains(t, rdbA.Do(ctx, "cluster", "import", 1, -3).Err(), "Invalid import state")
	})

	t.Run("IMPORT - slot with wrong state", func(t *testing.T) {
		require.Contains(t, rdbA.Do(ctx, "cluster", "import", 1, 0).Err(),
			"Can't import slot which belongs to me")
	})

	t.Run("IMPORT - slot states in right order", func(t *testing.T) {
		slotNum := 1
		slotKey := util.SlotTable[slotNum]

		// import start
		require.NoError(t, rdbA.Set(ctx, slotKey, "slot1", 0).Err())
		require.Equal(t, "slot1", rdbA.Get(ctx, slotKey).Val())
		require.Equal(t, "OK", rdbB.Do(ctx, "cluster", "import", slotNum, 0).Val())
		clusterInfo := rdbB.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 1")
		require.Contains(t, clusterInfo, "import_state: start")
		clusterNodes := rdbB.ClusterNodes(ctx).Val()
		require.Contains(t, clusterNodes, fmt.Sprintf("[%d-<-%s]", slotNum, srvAID))

		require.NoError(t, rdbA.Do(ctx, "clusterx", "migrate", slotNum, srvBID).Err())
		require.Eventually(t, func() bool {
			clusterInfo := rdbA.ClusterInfo(context.Background()).Val()
			return strings.Contains(clusterInfo, fmt.Sprintf("migrating_slot: %d", slotNum)) &&
				strings.Contains(clusterInfo, fmt.Sprintf("migrating_state: %s", "success"))
		}, 5*time.Second, 100*time.Millisecond)

		// import success
		require.Equal(t, "OK", rdbB.Do(ctx, "cluster", "import", slotNum, 1).Val())
		clusterInfo = rdbB.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 1")
		require.Contains(t, clusterInfo, "import_state: success")

		// import finish and should not contain the import section
		clusterNodes = rdbB.ClusterNodes(ctx).Val()
		require.NotContains(t, clusterNodes, fmt.Sprintf("[%d-<-%s]", slotNum, srvAID))

		time.Sleep(50 * time.Millisecond)
		require.Equal(t, "slot1", rdbB.Get(ctx, slotKey).Val())
	})

	t.Run("IMPORT - slot state 'error'", func(t *testing.T) {
		slotNum := 10
		slotKey := util.SlotTable[slotNum]

		require.Equal(t, "OK", rdbB.Do(ctx, "cluster", "import", slotNum, 0).Val())
		require.NoError(t, rdbB.Set(ctx, slotKey, "slot10_again", 0).Err())
		require.Equal(t, "slot10_again", rdbB.Get(ctx, slotKey).Val())

		// import error
		require.Equal(t, "OK", rdbB.Do(ctx, "cluster", "import", slotNum, 2).Val())
		time.Sleep(50 * time.Millisecond)

		clusterInfo := rdbB.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 10")
		require.Contains(t, clusterInfo, "import_state: error")

		// get empty
		require.Zero(t, rdbB.Exists(ctx, slotKey).Val())
	})

	t.Run("IMPORT - connection broken", func(t *testing.T) {
		slotNum := 11
		slotKey := util.SlotTable[slotNum]
		require.Equal(t, "OK", rdbB.Do(ctx, "cluster", "import", slotNum, 0).Val())
		require.NoError(t, rdbB.Set(ctx, slotKey, "slot11", 0).Err())
		require.Equal(t, "slot11", rdbB.Get(ctx, slotKey).Val())

		// close connection, server will stop importing
		require.NoError(t, rdbB.Close())
		rdbB = srvB.NewClient()
		time.Sleep(50 * time.Millisecond)

		clusterInfo := rdbB.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 11")
		require.Contains(t, clusterInfo, "import_state: error")

		// get empty
		require.Zero(t, rdbB.Exists(ctx, slotKey).Val())
	})
}

func TestClusterMigrate(t *testing.T) {
	ctx := context.Background()

	mockID0 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	mockSrv0Host := "127.0.0.1"
	mockSrv0Port := 6666

	srv1 := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv1.Close() }()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()
	id1 := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODEID", id1).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-8191\n", mockID0, mockSrv0Host, mockSrv0Port)
	clusterNodes += fmt.Sprintf("%s %s %d master - 8192-16383\n", id1, srv1.Host(), srv1.Port())
	require.NoError(t, rdb1.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	slotNum := 1
	require.Equal(t, "OK", rdb1.Do(ctx, "cluster", "import", slotNum, 0).Val())

	// create a new client that is not importing
	cli := srv1.NewClient()
	slotKey := util.SlotTable[slotNum]

	t.Run("IMPORT - query a key in importing slot without asking", func(t *testing.T) {	
		util.ErrorRegexp(t, cli.Type(ctx, slotKey).Err(), fmt.Sprintf("MOVED %d.*%d.*", slotNum, mockSrv0Port))
	})

	t.Run("IMPORT - query a key in importing slot after asking", func(t *testing.T) {
		require.Equal(t, "OK", cli.Do(ctx, "asking").Val())
		require.NoError(t, cli.Type(ctx, slotKey).Err())
	})

	t.Run("IMPORT - asking flag will be reset after executing", func(t *testing.T) {
		require.Equal(t, "OK", cli.Do(ctx, "asking").Val())
		require.NoError(t, cli.Type(ctx, slotKey).Err())
		util.ErrorRegexp(t, cli.Type(ctx, slotKey).Err(), fmt.Sprintf("MOVED %d.*%d.*", slotNum, mockSrv0Port))
	})

}