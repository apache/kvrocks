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
	"strconv"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestReplicationAuthRuntimeChanges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	master := util.StartServer(t, map[string]string{})
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()
	replica := util.StartServer(t, map[string]string{})
	defer replica.Close()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	// Keep the same replication thread throughout all authentication changes.
	util.SlaveOf(t, replicaClient, master)
	util.WaitForSync(t, replicaClient)
	require.NoError(t, masterClient.Set(ctx, "auth-marker", "initial", 0).Err())
	require.Eventually(t, func() bool {
		value, err := replicaClient.Get(ctx, "auth-marker").Result()
		return err == nil && value == "initial"
	}, 10*time.Second, 20*time.Millisecond)

	for _, phase := range []struct {
		name     string
		password string
	}{
		{name: "enable", password: "password-a"},
		{name: "rotate", password: "password-b"},
		{name: "clear"},
	} {
		if !t.Run(phase.name, func(t *testing.T) {
			previousPSyncs, err := strconv.ParseUint(util.FindInfoEntry(masterClient, "sync_partial_ok"), 10, 64)
			require.NoError(t, err)
			require.NoError(t, replicaClient.ConfigSet(ctx, "masterauth", phase.password).Err())
			require.NoError(t, masterClient.ConfigSet(ctx, "requirepass", phase.password).Err())
			require.NoError(t, masterClient.Close())
			masterClient = master.NewClientWithOption(&redis.Options{Password: phase.password})

			killed, err := masterClient.ClientKillByFilter(ctx, "type", "slave").Result()
			require.NoError(t, err)
			require.EqualValues(t, 1, killed)
			require.NoError(t, masterClient.Set(ctx, "auth-marker", phase.name, 0).Err())

			// A new PSYNC and a new value exclude a stale "up" state on the old connection.
			require.Eventually(t, func() bool {
				psyncs, err := strconv.ParseUint(util.FindInfoEntry(masterClient, "sync_partial_ok"), 10, 64)
				if err != nil || psyncs <= previousPSyncs {
					return false
				}
				value, err := replicaClient.Get(ctx, "auth-marker").Result()
				return err == nil && value == phase.name && util.FindInfoEntry(replicaClient, "master_link_status") == "up"
			}, 15*time.Second, 20*time.Millisecond)
		}) {
			return
		}
	}
}

func TestReplicationAuthFullSync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const password = "fullsync-password"
	master := util.StartServer(t, map[string]string{"requirepass": password})
	defer master.Close()
	masterClient := master.NewClientWithOption(&redis.Options{Password: password})
	defer func() { require.NoError(t, masterClient.Close()) }()
	replica := util.StartServer(t, map[string]string{"masterauth": password})
	defer replica.Close()
	replicaClient := replica.NewClient()
	defer func() { require.NoError(t, replicaClient.Close()) }()

	require.NoError(t, masterClient.Set(ctx, "fullsync-marker", "snapshot", 0).Err())
	// A replica sequence beyond the master's WAL forces the authenticated full-sync path.
	util.Populate(t, replicaClient, "replica-only-", 32, 1)
	masterOffset, err := strconv.ParseUint(util.FindInfoEntry(masterClient, "master_repl_offset"), 10, 64)
	require.NoError(t, err)
	replicaOffset, err := strconv.ParseUint(util.FindInfoEntry(replicaClient, "master_repl_offset"), 10, 64)
	require.NoError(t, err)
	require.Greater(t, replicaOffset, masterOffset)
	util.SlaveOf(t, replicaClient, master)

	require.Eventually(t, func() bool {
		fullSyncs, err := strconv.ParseUint(util.FindInfoEntry(masterClient, "sync_full"), 10, 64)
		if err != nil || fullSyncs == 0 {
			return false
		}
		psyncs, err := strconv.ParseUint(util.FindInfoEntry(masterClient, "sync_partial_ok"), 10, 64)
		if err != nil || psyncs == 0 {
			return false
		}
		value, err := replicaClient.Get(ctx, "fullsync-marker").Result()
		return err == nil && value == "snapshot" && util.FindInfoEntry(replicaClient, "master_link_status") == "up"
	}, 30*time.Second, 20*time.Millisecond)
	exists, err := replicaClient.Exists(ctx, "replica-only-0").Result()
	require.NoError(t, err)
	require.EqualValues(t, 0, exists)

	require.NoError(t, masterClient.Set(ctx, "fullsync-marker", "incremental", 0).Err())
	require.Eventually(t, func() bool {
		value, err := replicaClient.Get(ctx, "fullsync-marker").Result()
		return err == nil && value == "incremental"
	}, 10*time.Second, 20*time.Millisecond)
}
