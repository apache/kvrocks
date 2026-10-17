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

package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

const (
	testNodeID = "07c37dfeb235213a872192d90877d0cd55635b91"
	// A short lease so tests don't have to wait long for expiry.
	shortLeaseMs = 200
)

// initClusterMaster sets up a single-node cluster in master mode and returns a helper
// function that builds the clusterx SETNODES argument for that node.
func initClusterMaster(t *testing.T, srv *util.KvrocksServer) {
	t.Helper()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, rdb.Do(ctx, "clusterx", "SETNODEID", testNodeID).Err())
	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383", testNodeID, srv.Host(), srv.Port())
	require.NoError(t, rdb.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
}

// TestLeaseDisabledMode verifies that master_lease_mode=disabled (default) has no effect.
func TestLeaseDisabledMode(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"cluster-enabled":   "yes",
		"master-lease-mode": "disabled",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	initClusterMaster(t, srv)

	// Renew once with a very short lease.
	require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, shortLeaseMs, 1).Err())

	// Wait for the lease to expire.
	time.Sleep(time.Duration(shortLeaseMs+100) * time.Millisecond)

	// Writes should still succeed because mode is disabled.
	require.NoError(t, rdb.Set(ctx, "key-disabled", "value", 0).Err())
}

// TestLeaseBlockWriteMode verifies that writes are rejected after lease expiry in block-write mode.
func TestLeaseBlockWriteMode(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"cluster-enabled":   "yes",
		"master-lease-mode": "block-write",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	initClusterMaster(t, srv)

	t.Run("cold start: writes allowed before first HEARTBEAT", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "cold-key", "value", 0).Err())
	})

	t.Run("write succeeds when lease is valid", func(t *testing.T) {
		// Renew with a generous lease.
		require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 5000, 1).Err())
		require.NoError(t, rdb.Set(ctx, "valid-key", "value", 0).Err())
	})

	t.Run("write rejected after lease expiry", func(t *testing.T) {
		// Renew with a very short lease.
		require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, shortLeaseMs, 1).Err())
		// Wait for the lease to expire.
		time.Sleep(time.Duration(shortLeaseMs+100) * time.Millisecond)
		// Write should be rejected with the specific lease-expired error.
		err := rdb.Set(ctx, "expired-key", "value", 0).Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "master lease expired")
	})

	t.Run("write succeeds again after lease is renewed", func(t *testing.T) {
		// Renew again with a generous lease.
		require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 5000, 1).Err())
		require.NoError(t, rdb.Set(ctx, "renewed-key", "value", 0).Err())
	})
}

// TestLeaseLogOnlyMode verifies that writes succeed after expiry in log-only mode.
func TestLeaseLogOnlyMode(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"cluster-enabled":   "yes",
		"master-lease-mode": "log-only",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	initClusterMaster(t, srv)

	// Renew with a very short lease.
	require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, shortLeaseMs, 1).Err())
	// Wait for the lease to expire.
	time.Sleep(time.Duration(shortLeaseMs+100) * time.Millisecond)

	// Write should succeed even though the lease is expired.
	require.NoError(t, rdb.Set(ctx, "log-only-key", "value", 0).Err())
}

// TestHeartbeatElectionVersionMismatch verifies that a stale election_version is rejected.
func TestHeartbeatElectionVersionMismatch(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"cluster-enabled":   "yes",
		"master-lease-mode": "block-write",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	initClusterMaster(t, srv)

	// First, renew with version 5 to establish local_election_version = 5.
	require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 5000, 5).Err())

	// Now send a stale version (< 5). The lease must not be renewed and an error returned.
	err := rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 5000, 4).Err()
	require.Error(t, err)
	require.Contains(t, err.Error(), "election version mismatch")
}

// TestHeartbeatNonMatchingNodeID verifies that HEARTBEAT with a mismatched master_node_id
// does not renew the lease but returns a normal info response (no error).
func TestHeartbeatNonMatchingNodeID(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"cluster-enabled":   "yes",
		"master-lease-mode": "block-write",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	initClusterMaster(t, srv)

	// Use a different node ID so the node does not renew its lease.
	differentNodeID := "aabbccddeeff00112233445566778899aabbccdd"
	result, err := rdb.Do(ctx, "clusterx", "HEARTBEAT", differentNodeID, 5000, 1).Text()
	require.NoError(t, err)
	// Normal INFO response returned; no lease renewal.
	require.Contains(t, result, "role:")

	// Wait for any hypothetical lease to expire (there should be none since deadline == 0).
	time.Sleep(time.Duration(shortLeaseMs+100) * time.Millisecond)

	// Writes should still succeed because no lease was renewed (cold start: deadline == 0).
	require.NoError(t, rdb.Set(ctx, "no-lease-key", "value", 0).Err())
}

// TestHeartbeatLeaseResetOnSlaveOf verifies that becoming a slave resets the lease so that
// replication writes are not blocked.
func TestHeartbeatLeaseResetOnSlaveOf(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{
		"cluster-enabled":   "yes",
		"master-lease-mode": "block-write",
	})
	defer srv.Close()

	masterSrv := util.StartServer(t, map[string]string{})
	defer masterSrv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	masterRdb := masterSrv.NewClient()
	defer func() { require.NoError(t, masterRdb.Close()) }()

	initClusterMaster(t, srv)

	// Establish a short-lived lease on the node (which is currently a cluster master).
	require.NoError(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, shortLeaseMs, 1).Err())
	// Wait for the lease to expire so block-write mode is active.
	time.Sleep(time.Duration(shortLeaseMs+100) * time.Millisecond)
	err := rdb.Set(ctx, "pre-slaveof-key", "blocked-value", 0).Err()
	require.Error(t, err)
	require.Contains(t, err.Error(), "master lease expired")

	// Demote the node to slave. This must reset the lease.
	util.SlaveOf(t, rdb, masterSrv)
	util.WaitForSync(t, rdb)

	// After becoming a slave, the lease is reset (deadline == 0). Replication writes
	// go through writeToDB() and must not be blocked. We verify by writing to master and
	// confirming the replica syncs successfully.
	require.NoError(t, masterRdb.Set(ctx, "master-key", "replicated-value", 0).Err())
	util.WaitForOffsetSync(t, masterRdb, rdb, 5*time.Second)
}

// TestHeartbeatInvalidArgs verifies validation of HEARTBEAT arguments.
func TestHeartbeatInvalidArgs(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	initClusterMaster(t, srv)

	t.Run("missing arguments", func(t *testing.T) {
		require.Error(t, rdb.Do(ctx, "clusterx", "HEARTBEAT").Err())
		require.Error(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID).Err())
		require.Error(t, rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 1000).Err())
	})

	t.Run("lease_ms zero", func(t *testing.T) {
		err := rdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 0, 1).Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid lease_ms")
	})

	t.Run("cluster not enabled", func(t *testing.T) {
		noClusterSrv := util.StartServer(t, map[string]string{})
		defer noClusterSrv.Close()
		noClusterRdb := noClusterSrv.NewClient()
		defer func() { require.NoError(t, noClusterRdb.Close()) }()
		require.ErrorContains(t, noClusterRdb.Do(ctx, "clusterx", "HEARTBEAT", testNodeID, 1000, 1).Err(), "not enabled")
	})
}
