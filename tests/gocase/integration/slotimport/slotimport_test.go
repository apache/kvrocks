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

	srv := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { srv.Close() }()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	srvID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	clusterNodes := fmt.Sprintf("%s 127.0.0.1 %d master - 0-16383", srvID, srv.Port())
	require.NoError(t, rdb.Do(ctx, "clusterx", "SETNODEID", srvID).Err())
	require.NoError(t, rdb.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	t.Run("IMPORT - error slot", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cluster", "import", -1, 0).Err(), "Slot is out of range")
		require.ErrorContains(t, rdb.Do(ctx, "cluster", "import", 16384, 0).Err(), "Slot is out of range")
	})

	t.Run("IMPORT - slot with error state", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cluster", "import", 1, 4).Err(), "Invalid import state")
		require.ErrorContains(t, rdb.Do(ctx, "cluster", "import", 1, -3).Err(), "Invalid import state")
	})

	t.Run("IMPORT - slot states in right order", func(t *testing.T) {
		slotNum := 1
		slotKey := util.SlotTable[slotNum]

		// import start
		require.Equal(t, "OK", rdb.Do(ctx, "cluster", "import", slotNum, 0).Val())
		require.NoError(t, rdb.Set(ctx, slotKey, "slot1", 0).Err())
		require.Equal(t, "slot1", rdb.Get(ctx, slotKey).Val())
		clusterInfo := rdb.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 1")
		require.Contains(t, clusterInfo, "import_state: start")

		clusterNodes := rdb.ClusterNodes(ctx).Val()
		require.Contains(t, clusterNodes, fmt.Sprintf("[%d-<-%s]", slotNum, srvID))
		// import success
		require.Equal(t, "OK", rdb.Do(ctx, "cluster", "import", slotNum, 1).Val())
		clusterInfo = rdb.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 1")
		require.Contains(t, clusterInfo, "import_state: success")

		// import finish and should not contain the import section
		clusterNodes = rdb.ClusterNodes(ctx).Val()
		require.NotContains(t, clusterNodes, fmt.Sprintf("[%d-<-%s]", slotNum, srvID))

		time.Sleep(50 * time.Millisecond)
		require.Equal(t, "slot1", rdb.Get(ctx, slotKey).Val())
	})

	t.Run("IMPORT - slot state 'error'", func(t *testing.T) {
		slotNum := 10
		slotKey := util.SlotTable[slotNum]

		require.Equal(t, "OK", rdb.Do(ctx, "cluster", "import", slotNum, 0).Val())
		require.NoError(t, rdb.Set(ctx, slotKey, "slot10_again", 0).Err())
		require.Equal(t, "slot10_again", rdb.Get(ctx, slotKey).Val())

		// import error
		require.Equal(t, "OK", rdb.Do(ctx, "cluster", "import", slotNum, 2).Val())
		time.Sleep(50 * time.Millisecond)

		clusterInfo := rdb.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 10")
		require.Contains(t, clusterInfo, "import_state: error")

		// get empty
		require.Zero(t, rdb.Exists(ctx, slotKey).Val())
	})

	t.Run("IMPORT - connection broken", func(t *testing.T) {
		slotNum := 11
		slotKey := util.SlotTable[slotNum]
		require.Equal(t, "OK", rdb.Do(ctx, "cluster", "import", slotNum, 0).Val())
		require.NoError(t, rdb.Set(ctx, slotKey, "slot11", 0).Err())
		require.Equal(t, "slot11", rdb.Get(ctx, slotKey).Val())

		// close connection, server will stop importing
		require.NoError(t, rdb.Close())
		rdb = srv.NewClient()
		time.Sleep(50 * time.Millisecond)

		clusterInfo := rdb.ClusterInfo(ctx).Val()
		require.Contains(t, clusterInfo, "importing_slot: 11")
		require.Contains(t, clusterInfo, "import_state: error")

		// get empty
		require.Zero(t, rdb.Exists(ctx, slotKey).Val())
	})
}
