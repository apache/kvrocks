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

package monitor

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func TestMonitor(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("MONITOR can log executed commands", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("MONITOR"))
		c.MustRead(t, "+OK")
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Get(ctx, "foo").Err())
		c.MustMatch(t, ".*hello.*")
		c.MustMatch(t, ".*set.*foo.*bar.*")
		c.MustMatch(t, ".*get.*foo.*")
	})

	t.Run("MONITOR should skip commands with 'skip-monitor' flag", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("MONITOR"))
		c.MustRead(t, "+OK")

		// Execute commands that should be skipped
		require.NoError(t, rdb.ConfigSet(ctx, "slave-read-only", "yes").Err())
		require.NoError(t, rdb.ConfigGet(ctx, "slave-read-only").Err())

		require.NoError(t, rdb.Ping(ctx).Err())
		require.NoError(t, rdb.FlushAll(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "test", "value", 0).Err())
		require.NoError(t, rdb.Get(ctx, "test").Err())
		require.NoError(t, rdb.Info(ctx, "server").Err())
		c.MustMatch(t, ".*ping.*")
		c.MustMatch(t, ".*flushall.*")
		c.MustMatch(t, ".*set.*test.*value.*")
		c.MustMatch(t, ".*get.*test.*")
		c.MustMatch(t, ".*info.*server.*")
	})
}

func TestMonitorRedactPassword(t *testing.T) {
	srv := util.StartServer(t, map[string]string{"requirepass": "testpass"})
	defer srv.Close()

	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()
	require.NoError(t, c.WriteArgs("AUTH", "testpass"))
	c.MustRead(t, "+OK")

	require.NoError(t, c.WriteArgs("MONITOR"))
	c.MustRead(t, "+OK")

	rdb := srv.NewClientWithOption(&redis.Options{
		Password: "testpass",
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	ctx := context.Background()
	require.NoError(t, rdb.Do(ctx, "AUTH", "testpass").Err())
	require.NoError(t, rdb.Do(ctx, "HELLO", "3", "AUTH", "testpass").Err())
	require.NoError(t, rdb.Do(ctx, "HELLO", "3", "AUTH", "default", "testpass").Err())

	// Client uses HELLO to AUTH, so it will have an extra HELLO command
	c.MustMatch(t, `.*hello.*3.*auth.*default.*\(redacted\).*`)
	c.MustMatch(t, `.*AUTH.*\(redacted\).*`)
	c.MustMatch(t, `.*HELLO.*3.*AUTH.*\(redacted\).*`)
	c.MustMatch(t, `.*HELLO.*3.*AUTH.*default.*\(redacted\).*`)
}
