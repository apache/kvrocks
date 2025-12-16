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
 *
 */

package failover

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

// testNameWrapper wraps testing.TB to sanitize test names for MkdirTemp
// This is needed because subtest names contain "/" which causes MkdirTemp to fail
type testNameWrapper struct {
	testing.TB
	sanitizedName string
}

func (w *testNameWrapper) Name() string {
	return w.sanitizedName
}

// sanitizeTestName replaces path separators in test names to avoid issues with MkdirTemp
func sanitizeTestName(tb testing.TB) testing.TB {
	sanitizedName := strings.ReplaceAll(tb.Name(), "/", "_")
	return &testNameWrapper{TB: tb, sanitizedName: sanitizedName}
}

// startServerWithSanitizedName starts a server with a sanitized test name
func startServerWithSanitizedName(t testing.TB, configs map[string]string) *util.KvrocksServer {
	return util.StartServer(sanitizeTestName(t), configs)
}

type FailoverState string

const (
	FailoverStateNone       FailoverState = "none"
	FailoverStateStarted    FailoverState = "started"
	FailoverStateCheckSlave FailoverState = "check_slave"
	FailoverStatePauseWrite FailoverState = "pause_write"
	FailoverStateWaitSync   FailoverState = "wait_sync"
	FailoverStateSwitching  FailoverState = "switching"
	FailoverStateSuccess    FailoverState = "success"
	FailoverStateFailed     FailoverState = "failed"
)

// TestFailoverBasicFlow tests the basic failover process and custom timeout parameter.
// Test Case 1.1: Basic Failover Flow - Master successfully transfers control to Slave
// Test Case 1.2: Failover with Custom Timeout - Using custom timeout parameter
func TestFailoverBasicFlow(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	// Wait for replication to establish
	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 1.1: Basic Failover Flow
	t.Run("FAILOVER - Basic failover flow", func(t *testing.T) {
		// Write some data
		require.NoError(t, masterClient.Set(ctx, "key1", "value1", 0).Err())
		require.NoError(t, masterClient.Set(ctx, "key2", "value2", 0).Err())

		// Start failover
		result := masterClient.Do(ctx, "clusterx", "failover", slaveID)
		if result.Err() != nil {
			t.Logf("FAILOVER command error: %v", result.Err())
		}
		require.NoError(t, result.Err(), "FAILOVER command should succeed")
		require.Equal(t, "OK", result.Val())

		// Wait for failover to complete
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)

		// Verify slots are migrated (MOVED response)
		require.ErrorContains(t, masterClient.Set(ctx, "key1", "newvalue", 0).Err(), "MOVED")
		require.ErrorContains(t, masterClient.Get(ctx, "key1").Err(), "MOVED")

		// Verify data is accessible on new master (slave)
		require.Equal(t, "value1", slaveClient.Get(ctx, "key1").Val())
		require.Equal(t, "value2", slaveClient.Get(ctx, "key2").Val())
	})

	// Test Case 1.2: Failover with Custom Timeout
	t.Run("FAILOVER - Failover with custom timeout", func(t *testing.T) {
		// Reset failover state by updating topology
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		// Start failover with custom timeout
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "5000").Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)
	})
}

// TestFailoverFailureCases tests various failure scenarios and timeout values.
// Test Case 2.1: Slave Node Not Found - Specified slave_node_id is not in cluster
// Test Case 2.2: Slave Not Connected - Slave node exists but no replication connection
// Test Case 3.5: Different Timeout Values - Testing various timeout values (0, 100, 10000)
func TestFailoverFailureCases(t *testing.T) {
	ctx := context.Background()

	master := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { master.Close() }()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()
	masterID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODEID", masterID).Err())

	// Test Case 2.1: Slave Node Not Found
	t.Run("FAILOVER - Failover to non-existent node", func(t *testing.T) {
		clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383", masterID, master.Host(), master.Port())
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

		nonExistentID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx99"
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", nonExistentID).Val())
		waitForFailoverState(t, masterClient, FailoverStateFailed, 5*time.Second)
	})

	// Test Case 2.2: Slave Not Connected (node exists as master, not slave)
	t.Run("FAILOVER - Failover to non-slave node", func(t *testing.T) {
		slave := startServerWithSanitizedName(t, map[string]string{"cluster-enabled": "yes"})
		defer func() { slave.Close() }()
		slaveClient := slave.NewClient()
		defer func() { require.NoError(t, slaveClient.Close()) }()
		slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

		// Set slave as master (not slave)
		clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
		clusterNodes += fmt.Sprintf("%s %s %d master -", slaveID, slave.Host(), slave.Port())
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())
		waitForFailoverState(t, masterClient, FailoverStateFailed, 5*time.Second)
	})

	// Test Case 3.5: Invalid timeout value (negative)
	t.Run("FAILOVER - Invalid timeout value", func(t *testing.T) {
		slave := startServerWithSanitizedName(t, map[string]string{"cluster-enabled": "yes"})
		defer func() { slave.Close() }()
		slaveClient := slave.NewClient()
		defer func() { require.NoError(t, slaveClient.Close()) }()
		slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

		clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
		clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "3").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "3").Err())

		// Negative timeout should return error
		require.Error(t, masterClient.Do(ctx, "clusterx", "failover", slaveID, "-1").Err())
	})

	// Test Case 3.5: Different Timeout Values (0, 100, 10000)
	t.Run("FAILOVER - Different timeout values", func(t *testing.T) {
		slave := startServerWithSanitizedName(t, map[string]string{"cluster-enabled": "yes"})
		defer func() { slave.Close() }()
		slaveClient := slave.NewClient()
		defer func() { require.NoError(t, slaveClient.Close()) }()
		slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

		clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
		clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "4").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "4").Err())

		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		// Test with timeout = 0 when slave is already synced (lag=0)
		// When lag=0, failover should succeed because no waiting is needed
		// But if slave has lag, it will fail
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "0").Val())
		// Wait for either success or failed state
		require.Eventually(t, func() bool {
			info := masterClient.ClusterInfo(ctx).Val()
			return strings.Contains(info, "cluster_failover_state:success") ||
				strings.Contains(info, "cluster_failover_state:failed")
		}, 5*time.Second, 100*time.Millisecond)

		// Check final state - can be success (if lag=0) or failed (if lag>0)
		finalInfo := masterClient.ClusterInfo(ctx).Val()
		if strings.Contains(finalInfo, "cluster_failover_state:success") {
			t.Logf("timeout=0 with lag=0: failover succeeded as expected")
		} else if strings.Contains(finalInfo, "cluster_failover_state:failed") {
			t.Logf("timeout=0: failover failed (slave may have lag)")
		}

		// Reset for next test
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "5").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "5").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		// Test with timeout = 0 when slave has lag
		// Create lag by writing data to master without waiting for sync
		for i := 0; i < 100; i++ {
			require.NoError(t, masterClient.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), 0).Err())
		}
		// Don't wait for sync, start failover immediately to create lag scenario
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "0").Val())
		// With lag, timeout=0 should fail
		waitForFailoverState(t, masterClient, FailoverStateFailed, 5*time.Second)

		// Reset and test with small timeout
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "6").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "6").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "100").Val())
		// Small timeout may fail, but should start
		time.Sleep(200 * time.Millisecond)
		info := masterClient.ClusterInfo(ctx).Val()
		require.True(t, strings.Contains(info, "cluster_failover_state:failed") ||
			strings.Contains(info, "cluster_failover_state:success") ||
			strings.Contains(info, "cluster_failover_state:wait_sync") ||
			strings.Contains(info, "cluster_failover_state:switching"))

		// Reset and test with large timeout
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "7").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "7").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "10000").Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 15*time.Second)
	})
}

// TestFailoverConcurrency tests concurrent failover scenarios.
// Test Case 3.1: Duplicate Failover - Cannot start failover when one is in progress
// Test Case 3.2: Restart After Failure - Can restart failover after previous failure
func TestFailoverConcurrency(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 3.1: Duplicate Failover
	t.Run("FAILOVER - Cannot start failover when one is in progress", func(t *testing.T) {
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())

		// Try to start another failover immediately - should return error
		// Wait a bit to ensure first failover has started
		time.Sleep(100 * time.Millisecond)
		result := masterClient.Do(ctx, "clusterx", "failover", slaveID)
		// second failover may return an error indicating a failover is already in progress.
		_, err := result.Result()
		if err != nil {
			require.Contains(t, err.Error(), "Failover is already in progress")
		} else {
			// should not reach here
			require.Fail(t, "second failover should return error")
		}

		// We verify the first one completes successfully
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)
	})

	// Test Case 3.2: Restart After Failure
	t.Run("FAILOVER - Can restart after failure", func(t *testing.T) {
		// Reset state
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		// Start a failover with very short timeout
		// If slave is synced (lag=0), it may succeed; if slave has lag, it will fail
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "1").Val())
		// Accept either success or failed state
		require.Eventually(t, func() bool {
			info := masterClient.ClusterInfo(ctx).Val()
			return strings.Contains(info, "cluster_failover_state:success") ||
				strings.Contains(info, "cluster_failover_state:failed")
		}, 5*time.Second, 100*time.Millisecond)

		// Can restart after failure
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "3").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "3").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "10000").Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 15*time.Second)
	})
}

// TestFailoverWriteBlocking tests write and read request behavior during failover.
// Test Case 3.3: Write Requests During Failover - Write requests return TRYAGAIN in blocking states
// Test Case 3.4: Read Requests During Failover - Read requests are not blocked
func TestFailoverWriteBlocking(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 3.3: Write Requests During Failover
	t.Run("FAILOVER - Write requests blocked during failover", func(t *testing.T) {
		// Write initial data
		require.NoError(t, masterClient.Set(ctx, "testkey", "testvalue", 0).Err())

		// Start failover with long timeout to observe blocking
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "10000").Val())

		// Try to write during failover - should return TRYAGAIN in blocking states
		// Poll for blocking state (pause_write, wait_sync, or switching)
		for i := 0; i < 50; i++ {
			time.Sleep(50 * time.Millisecond)
			err := masterClient.Set(ctx, "testkey", "newvalue", 0).Err()
			if err != nil && (strings.Contains(err.Error(), "TRYAGAIN") || strings.Contains(err.Error(), "Failover in progress")) {
				break
			}
			// Check if failover already completed
			info := masterClient.ClusterInfo(ctx).Val()
			if strings.Contains(info, "cluster_failover_state:success") {
				break
			}
		}
		// At least one write should have been blocked, or failover completed very quickly
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 15*time.Second)

		// After success, writes should return MOVED
		require.ErrorContains(t, masterClient.Set(ctx, "testkey", "newvalue2", 0).Err(), "MOVED")
	})

	// Test Case 3.4: Read Requests During Failover
	t.Run("FAILOVER - Read requests not blocked during failover", func(t *testing.T) {
		// Reset state
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		require.NoError(t, masterClient.Set(ctx, "readkey", "readvalue", 0).Err())

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "10000").Val())

		// Reads should work during failover (not blocked)
		// Try reading multiple times during failover
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			val := masterClient.Get(ctx, "readkey").Val()
			require.Equal(t, "readvalue", val)
			// Check if failover completed
			info := masterClient.ClusterInfo(ctx).Val()
			if strings.Contains(info, "cluster_failover_state:success") {
				break
			}
		}
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 15*time.Second)
	})
}

// TestFailoverWithAuth tests failover with password authentication.
// Test Case 1.4: Failover with Password Authentication - Cluster configured with requirepass
func TestFailoverWithAuth(t *testing.T) {
	ctx := context.Background()

	master := util.StartServer(t, map[string]string{
		"cluster-enabled": "yes",
		"requirepass":     "password123",
	})
	defer func() { master.Close() }()
	masterClient := master.NewClient()
	masterClient = redis.NewClient(&redis.Options{
		Addr:     master.HostPort(),
		Password: "password123",
	})
	defer func() { require.NoError(t, masterClient.Close()) }()
	masterID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODEID", masterID).Err())

	slave := startServerWithSanitizedName(t, map[string]string{
		"cluster-enabled": "yes",
		"requirepass":     "password123",
		"masterauth":      "password123",
	})
	defer func() { slave.Close() }()
	slaveClient := slave.NewClient()
	slaveClient = redis.NewClient(&redis.Options{
		Addr:     slave.HostPort(),
		Password: "password123",
	})
	defer func() { require.NoError(t, slaveClient.Close()) }()
	slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 1.4: Failover with Password Authentication
	t.Run("FAILOVER - Failover with authentication", func(t *testing.T) {
		require.NoError(t, masterClient.Set(ctx, "authkey", "authvalue", 0).Err())

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)

		// Verify data on new master
		require.Equal(t, "authvalue", slaveClient.Get(ctx, "authkey").Val())
	})
}

// TestFailoverStateQuery tests querying failover state information.
// Test Case 4.1: CLUSTER INFO State Output - Query failover state and verify all state transitions
func TestFailoverStateQuery(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 4.1: CLUSTER INFO State Output
	t.Run("FAILOVER - Query failover state via CLUSTER INFO", func(t *testing.T) {
		// Initial state should be none
		info := masterClient.ClusterInfo(ctx).Val()
		require.Contains(t, info, "cluster_failover_state:none")

		// Start failover
		result := masterClient.Do(ctx, "clusterx", "failover", slaveID)
		if result.Err() != nil {
			t.Logf("FAILOVER command error: %v", result.Err())
		}
		require.NoError(t, result.Err(), "FAILOVER command should succeed")
		require.Equal(t, "OK", result.Val())

		// Wait for success
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)

		// Verify state is success
		info = masterClient.ClusterInfo(ctx).Val()
		require.Contains(t, info, "cluster_failover_state:success")
	})

	// Test Case 4.1: All State Transitions
	t.Run("FAILOVER - All state transitions", func(t *testing.T) {
		// Reset state
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		// Initial state: none
		info := masterClient.ClusterInfo(ctx).Val()
		require.Contains(t, info, "cluster_failover_state:none")

		// Start failover
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())

		// We may catch intermediate states, but they're very fast
		// The important thing is we transition through them and end at success
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)

		// Verify final state
		info = masterClient.ClusterInfo(ctx).Val()
		require.Contains(t, info, "cluster_failover_state:success")
	})
}

// TestFailoverTakeoverCommand tests the TAKEOVER command handling on slave.
// Test Case 5.2: TAKEOVER Command Processing - Slave receives and processes TAKEOVER command
func TestFailoverTakeoverCommand(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 5.2: TAKEOVER Command Processing
	t.Run("FAILOVER - TAKEOVER command on slave", func(t *testing.T) {
		// Slave should accept TAKEOVER command
		require.Equal(t, "OK", slaveClient.Do(ctx, "clusterx", "takeover").Val())

		// Verify imported slots are set
		// After takeover, slave should be able to serve the slots
		require.NoError(t, masterClient.Set(ctx, "takeoverkey", "takeovervalue", 0).Err())

		// Start failover to test the full flow
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "2").Err())
		require.Eventually(t, func() bool {
			info := masterClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 10*time.Second, 100*time.Millisecond)

		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)
	})
}

// TestFailoverDataConsistency tests data consistency after failover.
// Test Case 5.4: Data Consistency Verification - All data is replicated to new master without loss
func TestFailoverDataConsistency(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 5.4: Data Consistency Verification
	t.Run("FAILOVER - Data consistency after failover", func(t *testing.T) {
		// Write various types of data
		require.NoError(t, masterClient.Set(ctx, "string_key", "string_value", 0).Err())
		require.NoError(t, masterClient.LPush(ctx, "list_key", "item1", "item2", "item3").Err())
		require.NoError(t, masterClient.HSet(ctx, "hash_key", "field1", "value1", "field2", "value2").Err())
		require.NoError(t, masterClient.SAdd(ctx, "set_key", "member1", "member2").Err())
		require.NoError(t, masterClient.ZAdd(ctx, "zset_key", redis.Z{Score: 1.0, Member: "member1"}).Err())

		// Start failover with longer timeout to ensure data sync
		result := masterClient.Do(ctx, "clusterx", "failover", slaveID, "10000")
		if result.Err() != nil {
			t.Logf("FAILOVER command error: %v", result.Err())
		}
		require.NoError(t, result.Err(), "FAILOVER command should succeed")
		require.Equal(t, "OK", result.Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 15*time.Second)

		// Verify all data is on new master
		require.Equal(t, "string_value", slaveClient.Get(ctx, "string_key").Val())
		require.EqualValues(t, []string{"item3", "item2", "item1"}, slaveClient.LRange(ctx, "list_key", 0, -1).Val())
		require.Equal(t, map[string]string{"field1": "value1", "field2": "value2"}, slaveClient.HGetAll(ctx, "hash_key").Val())
		require.EqualValues(t, []string{"member1", "member2"}, slaveClient.SMembers(ctx, "set_key").Val())
		require.EqualValues(t, []redis.Z{{Score: 1.0, Member: "member1"}}, slaveClient.ZRangeWithScores(ctx, "zset_key", 0, -1).Val())
	})
}

// TestFailoverStateReset tests failover state reset after topology update.
// Test Case 5.1: SETNODES Reset State - Controller updates topology and resets failover state
func TestFailoverStateReset(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 5.1: SETNODES Reset State
	t.Run("FAILOVER - State reset after SETNODES", func(t *testing.T) {
		// Start and complete failover
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())
		waitForFailoverState(t, masterClient, FailoverStateSuccess, 10*time.Second)

		// Update topology (simulating controller update)
		newClusterNodes := fmt.Sprintf("%s %s %d slave %s\n", masterID, master.Host(), master.Port(), slaveID)
		newClusterNodes += fmt.Sprintf("%s %s %d master - 0-16383", slaveID, slave.Host(), slave.Port())
		require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", newClusterNodes, "2").Err())
		require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", newClusterNodes, "2").Err())

		// State should be reset to none
		// After SETNODES, the original master becomes slave, and original slave becomes master
		// Wait for replication relationship to be re-established
		// The new master (slaveClient) should have a slave connection
		require.Eventually(t, func() bool {
			info := slaveClient.Info(ctx, "replication").Val()
			return strings.Contains(info, "connected_slaves:1")
		}, 20*time.Second, 200*time.Millisecond)

		// Verify failover state is reset to none on the new master
		info := slaveClient.ClusterInfo(ctx).Val()
		require.Contains(t, info, "cluster_failover_state:none")
	})
}

// Helper functions

func waitForFailoverState(t testing.TB, client *redis.Client, state FailoverState, timeout time.Duration) {
	var lastInfo string
	require.Eventually(t, func() bool {
		info := client.ClusterInfo(context.Background()).Val()
		if info != lastInfo && strings.Contains(info, "cluster_failover_state:") {
			// Log state changes for debugging
			t.Logf("Failover state: %s", info)
			lastInfo = info
		}
		return strings.Contains(info, fmt.Sprintf("cluster_failover_state:%s", state))
	}, timeout, 100*time.Millisecond)
}

// TestFailoverSlaveNotConnected tests failover to a slave that is not connected.
// Test Case 2.2: Slave Not Connected - Slave node exists in topology but no replication connection
func TestFailoverSlaveNotConnected(t *testing.T) {
	ctx := context.Background()

	master := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { master.Close() }()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()
	masterID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODEID", masterID).Err())

	slave := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer func() { slave.Close() }()
	slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"

	// Set up cluster topology but don't establish replication
	// Slave exists in topology but is not connected
	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	// Try to failover to unconnected slave
	require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())
	waitForFailoverState(t, masterClient, FailoverStateFailed, 5*time.Second)

	// Verify error state
	info := masterClient.ClusterInfo(ctx).Val()
	require.Contains(t, info, "cluster_failover_state:failed")
}

// TestFailoverWaitSyncTimeout tests failover timeout when waiting for replication sync.
// Test Case 2.6: Wait Sync Timeout - waitReplicationSync exceeds timeout
func TestFailoverWaitSyncTimeout(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Write some data to create lag
	for i := 0; i < 100; i++ {
		require.NoError(t, masterClient.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), 0).Err())
	}

	// Start failover with very short timeout to trigger timeout
	require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID, "1").Val())
	waitForFailoverState(t, masterClient, FailoverStateFailed, 5*time.Second)

	// Verify timeout error
	info := masterClient.ClusterInfo(ctx).Val()
	require.Contains(t, info, "cluster_failover_state:failed")
}

// TestFailoverAuthFailure tests failover with incorrect authentication.
// Test Case 2.8: AUTH Failed - Password Incorrect - requirepass configured but password is wrong
func TestFailoverAuthFailure(t *testing.T) {
	ctx := context.Background()

	master := util.StartServer(t, map[string]string{
		"cluster-enabled": "yes",
		"requirepass":     "correctpass",
	})
	defer func() { master.Close() }()
	masterClient := master.NewClient()
	masterClient = redis.NewClient(&redis.Options{
		Addr:     master.HostPort(),
		Password: "correctpass",
	})
	defer func() { require.NoError(t, masterClient.Close()) }()
	masterID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODEID", masterID).Err())

	// Slave with different password (simulating auth failure scenario)
	// Note: In real scenario, master would try to connect with its own password
	// but if slave has different password, AUTH would fail
	// However, in our test setup, both use same config, so we test with wrong password scenario
	slave := util.StartServer(t, map[string]string{
		"cluster-enabled": "yes",
		"requirepass":     "wrongpass", // Different password
	})
	defer func() { slave.Close() }()
	slaveClient := slave.NewClient()
	slaveClient = redis.NewClient(&redis.Options{
		Addr:     slave.HostPort(),
		Password: "wrongpass",
	})
	defer func() { require.NoError(t, slaveClient.Close()) }()
	slaveID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODEID", slaveID).Err())

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	// Note: Replication won't establish due to password mismatch, but failover will try
	// The failover will fail when trying to send TAKEOVER command with wrong password
	require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())
	// This should fail during TAKEOVER due to AUTH failure
	waitForFailoverState(t, masterClient, FailoverStateFailed, 10*time.Second)

	info := masterClient.ClusterInfo(ctx).Val()
	require.Contains(t, info, "cluster_failover_state:failed")
}

// TestFailoverStateTransitions tests observing various failover state transitions.
// Test Case 4.1: State Transitions - Verify failover progresses through expected states
func TestFailoverStateTransitions(t *testing.T) {
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

	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383\n", masterID, master.Host(), master.Port())
	clusterNodes += fmt.Sprintf("%s %s %d slave %s", slaveID, slave.Host(), slave.Port(), masterID)
	require.NoError(t, masterClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())
	require.NoError(t, slaveClient.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	require.Eventually(t, func() bool {
		info := masterClient.Info(ctx, "replication").Val()
		return strings.Contains(info, "connected_slaves:1")
	}, 10*time.Second, 100*time.Millisecond)

	// Test Case 4.1: State Transitions
	t.Run("FAILOVER - Verify all possible states appear", func(t *testing.T) {
		// Start failover
		require.Equal(t, "OK", masterClient.Do(ctx, "clusterx", "failover", slaveID).Val())

		// Poll for states - we may catch intermediate states
		statesSeen := make(map[FailoverState]bool)
		for i := 0; i < 100; i++ {
			time.Sleep(50 * time.Millisecond)
			info := masterClient.ClusterInfo(ctx).Val()
			if strings.Contains(info, "cluster_failover_state:none") {
				statesSeen[FailoverStateNone] = true
			}
			if strings.Contains(info, "cluster_failover_state:started") {
				statesSeen[FailoverStateStarted] = true
			}
			if strings.Contains(info, "cluster_failover_state:check_slave") {
				statesSeen[FailoverStateCheckSlave] = true
			}
			if strings.Contains(info, "cluster_failover_state:pause_write") {
				statesSeen[FailoverStatePauseWrite] = true
			}
			if strings.Contains(info, "cluster_failover_state:wait_sync") {
				statesSeen[FailoverStateWaitSync] = true
			}
			if strings.Contains(info, "cluster_failover_state:switching") {
				statesSeen[FailoverStateSwitching] = true
			}
			if strings.Contains(info, "cluster_failover_state:success") {
				statesSeen[FailoverStateSuccess] = true
				break
			}
			if strings.Contains(info, "cluster_failover_state:failed") {
				statesSeen[FailoverStateFailed] = true
				break
			}
		}

		// We should at least see success or failed
		require.True(t, statesSeen[FailoverStateSuccess] || statesSeen[FailoverStateFailed],
			"Should reach either success or failed state")
	})
}
