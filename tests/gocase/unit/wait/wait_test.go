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

package wait

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestWaitCommand(t *testing.T) {
	// Start master server
	masterSrv := util.StartServer(t, map[string]string{})
	defer masterSrv.Close()

	// Start slave server
	slaveSrv := util.StartServer(t, map[string]string{})
	defer slaveSrv.Close()

	ctx := context.Background()
	masterRdb := masterSrv.NewClient()
	defer func() { require.NoError(t, masterRdb.Close()) }()

	slaveRdb := slaveSrv.NewClient()
	defer func() { require.NoError(t, slaveRdb.Close()) }()

	// Set up replication
	util.SlaveOf(t, slaveRdb, masterSrv)
	util.WaitForSync(t, slaveRdb)

	t.Run("WAIT with negative number should return error", func(t *testing.T) {
		result := masterRdb.Do(ctx, "WAIT", "-1")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "numreplicas should be a positive integer")
	})

	t.Run("WAIT with invalid arguments should return error", func(t *testing.T) {
		result := masterRdb.Do(ctx, "WAIT")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")

		result = masterRdb.Do(ctx, "WAIT", "1", "1000")
		require.Error(t, result.Err())
		require.Contains(t, result.Err().Error(), "wrong number of arguments")
	})

	t.Run("WAIT should not block indefinitely", func(t *testing.T) {
		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			require.NoError(t, masterRdb.Do(ctx, "SET", "k1", "v1").Err())
			require.NoError(t, masterRdb.Do(ctx, "WAIT", "1").Err())
			done <- true
		}()

		// Wait for the command to complete (should be immediate)
		select {
		case <-done:
			// Success - command completed immediately
		case <-time.After(5 * time.Second):
			t.Fatal("WAIT command blocked indefinitely")
		}
	})

	t.Run("WAIT should block until enough replicas acknowledge", func(t *testing.T) {
		// Disconnect the slave
		require.NoError(t, slaveRdb.Do(ctx, "SLAVEOF", "NO", "ONE").Err())

		// Master remove the slave from the replication list periodically
		// so we need to wait for the master to detect the disconnection
		require.Eventually(t, func() bool {
			info := masterRdb.Info(ctx, "replication").Val()
			return !strings.Contains(info, "connected_slaves:1")
		}, 50*time.Second, 100*time.Millisecond)

		// Start a goroutine to execute WAIT
		done := make(chan bool, 1)
		go func() {
			require.NoError(t, masterRdb.Do(ctx, "SET", "k1", "v1").Err())
			require.NoError(t, masterRdb.Do(ctx, "WAIT", "1").Err())
			done <- true
		}()

		select {
		case <-done:
			t.Fatal("WAIT command did not block")
		case <-time.After(1 * time.Second):
			// Success - command blocked
		}

		// Reconnect the slave
		util.SlaveOf(t, slaveRdb, masterSrv)
		util.WaitForSync(t, slaveRdb)

		// Now WAIT should complete
		select {
		case <-done:
			// Success - command completed after replica connected
		case <-time.After(5 * time.Second):
			t.Fatal("WAIT command did not complete after replica connected")
		}
	})
}
