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

package keyspacenotify

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// drainSubscribeConfirms waits for subscription confirmations.
func drainSubscribeConfirms(t *testing.T, ctx context.Context, pubsub *redis.PubSub, n int) {
	t.Helper()
	for range n {
		msg, err := pubsub.ReceiveTimeout(ctx, 2*time.Second)
		require.NoError(t, err)
		require.IsType(t, &redis.Subscription{}, msg)
	}
}

// expectMessage checks the next pubsub message.
func expectMessage(t *testing.T, ctx context.Context, pubsub *redis.PubSub, channel, payload string) {
	t.Helper()
	msg, err := pubsub.ReceiveTimeout(ctx, 2*time.Second)
	require.NoError(t, err)
	m, ok := msg.(*redis.Message)
	require.Truef(t, ok, "expected *redis.Message, got %T", msg)
	require.Equal(t, channel, m.Channel)
	require.Equal(t, payload, m.Payload)
}

// expectNoMessage checks that no message arrives soon.
func expectNoMessage(t *testing.T, ctx context.Context, pubsub *redis.PubSub) {
	t.Helper()
	msg, err := pubsub.ReceiveTimeout(ctx, 300*time.Millisecond)
	require.Errorf(t, err, "expected no message, got %v", msg)
}

func TestKeyspaceNotify(t *testing.T) {
	srv := util.StartServer(t, map[string]string{"notify-keyspace-events": "KEA"})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	// Subscribe to both channel forms.
	pubsub := rdb.PSubscribe(ctx, "__keyspace@0__:*", "__keyevent@0__:*")
	defer func() { require.NoError(t, pubsub.Close()) }()
	drainSubscribeConfirms(t, ctx, pubsub, 2)

	t.Run("SET publishes set", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		// Keyspace is published before keyevent.
		expectMessage(t, ctx, pubsub, "__keyspace@0__:foo", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "foo")
	})

	t.Run("SETEX publishes set from the shared Set API", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "SETEX", "setex-key", 60, "value").Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:setex-key", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "setex-key")
	})

	t.Run("SET NX on existing key publishes nothing", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "nxkey", "v1", 0).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:nxkey", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "nxkey")

		// NX fails, so nothing is published.
		require.NoError(t, rdb.SetNX(ctx, "nxkey", "v2", 0).Err())
		expectNoMessage(t, ctx, pubsub)
	})

	t.Run("SET GET with conditions publishes only on write", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "getnx", "v1", 0).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:getnx", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "getnx")

		cmd := rdb.Do(ctx, "SET", "getnx", "v2", "GET", "NX")
		require.NoError(t, cmd.Err())
		require.Equal(t, "v1", cmd.Val())
		expectNoMessage(t, ctx, pubsub)

		cmd = rdb.Do(ctx, "SET", "getnx-missing", "v1", "GET", "NX")
		require.ErrorIs(t, cmd.Err(), redis.Nil)
		expectMessage(t, ctx, pubsub, "__keyspace@0__:getnx-missing", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "getnx-missing")

		cmd = rdb.Do(ctx, "SET", "getxx-missing", "v1", "GET", "XX")
		require.ErrorIs(t, cmd.Err(), redis.Nil)
		expectNoMessage(t, ctx, pubsub)

		require.NoError(t, rdb.Set(ctx, "ifeq", "v1", 0).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:ifeq", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "ifeq")

		cmd = rdb.Do(ctx, "SET", "ifeq", "v2", "GET", "IFEQ", "other")
		require.NoError(t, cmd.Err())
		require.Equal(t, "v1", cmd.Val())
		expectNoMessage(t, ctx, pubsub)

		cmd = rdb.Do(ctx, "SET", "ifeq", "v2", "GET", "IFEQ", "v1")
		require.NoError(t, cmd.Err())
		require.Equal(t, "v1", cmd.Val())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:ifeq", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "ifeq")
	})

	t.Run("DEL publishes one del per deleted key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "d1", "x", 0).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:d1", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "d1")

		// Only d1 is deleted.
		require.EqualValues(t, 1, rdb.Del(ctx, "d1", "d2").Val())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:d1", "del")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:del", "d1")
		expectNoMessage(t, ctx, pubsub)

		require.NoError(t, rdb.Set(ctx, "ddup", "x", 0).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:ddup", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "ddup")

		require.EqualValues(t, 1, rdb.Del(ctx, "ddup", "ddup").Val())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:ddup", "del")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:del", "ddup")
		expectNoMessage(t, ctx, pubsub)
	})

	t.Run("UNLINK publishes del", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "unlink-key", "x", 0).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:unlink-key", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "unlink-key")

		require.EqualValues(t, 1, rdb.Unlink(ctx, "unlink-key").Val())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:unlink-key", "del")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:del", "unlink-key")
	})

	t.Run("Lua nested commands publish events", func(t *testing.T) {
		script := `
			redis.call("SET", KEYS[1], "v")
			redis.call("DEL", KEYS[1])
			return 1
		`
		require.NoError(t, rdb.Eval(ctx, script, []string{"lua-key"}).Err())
		expectMessage(t, ctx, pubsub, "__keyspace@0__:lua-key", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "lua-key")
		expectMessage(t, ctx, pubsub, "__keyspace@0__:lua-key", "del")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:del", "lua-key")
		expectNoMessage(t, ctx, pubsub)
	})

	t.Run("MULTI/EXEC publishes queued events after commit", func(t *testing.T) {
		_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "m1", "v", 0)
			pipe.Del(ctx, "m1")
			return nil
		})
		require.NoError(t, err)
		// Events publish after commit.
		expectMessage(t, ctx, pubsub, "__keyspace@0__:m1", "set")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "m1")
		expectMessage(t, ctx, pubsub, "__keyspace@0__:m1", "del")
		expectMessage(t, ctx, pubsub, "__keyevent@0__:del", "m1")
		expectNoMessage(t, ctx, pubsub)
	})

	t.Run("MULTI/EXEC preserves per-command notification config", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "notify-keyspace-events", "E$").Err())
		_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "event-before-disable", "v", 0)
			pipe.ConfigSet(ctx, "notify-keyspace-events", "")
			pipe.Set(ctx, "event-while-disabled", "v", 0)
			pipe.ConfigSet(ctx, "notify-keyspace-events", "K$")
			pipe.Set(ctx, "event-after-enable", "v", 0)
			return nil
		})
		require.NoError(t, err)

		expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "event-before-disable")
		expectMessage(t, ctx, pubsub, "__keyspace@0__:event-after-enable", "set")
		expectNoMessage(t, ctx, pubsub)
	})
}

func TestKeyspaceNotifyDisabled(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	pubsub := rdb.PSubscribe(ctx, "__keyspace@0__:*", "__keyevent@0__:*")
	defer func() { require.NoError(t, pubsub.Close()) }()
	drainSubscribeConfirms(t, ctx, pubsub, 2)

	require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
	require.NoError(t, rdb.Del(ctx, "foo").Err())
	expectNoMessage(t, ctx, pubsub)

	// CONFIG SET enables notifications at runtime.
	require.NoError(t, rdb.ConfigSet(ctx, "notify-keyspace-events", "KEA").Err())
	require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
	expectMessage(t, ctx, pubsub, "__keyspace@0__:foo", "set")
	expectMessage(t, ctx, pubsub, "__keyevent@0__:set", "foo")

	// CONFIG SET rejects unsupported flags.
	require.Error(t, rdb.ConfigSet(ctx, "notify-keyspace-events", "Kx").Err())
	require.Error(t, rdb.ConfigSet(ctx, "notify-keyspace-events", "KEl").Err())
}

func TestKeyspaceNotifyRedisDatabases(t *testing.T) {
	srv := util.StartServer(t, map[string]string{"notify-keyspace-events": "KEA", "redis-databases": "16"})
	defer srv.Close()

	ctx := context.Background()
	sub := srv.NewClient()
	defer func() { require.NoError(t, sub.Close()) }()
	writer := srv.NewClient()
	defer func() { require.NoError(t, writer.Close()) }()

	pubsub := sub.PSubscribe(ctx, "__keyspace@1__:*", "__keyevent@1__:*")
	defer func() { require.NoError(t, pubsub.Close()) }()
	drainSubscribeConfirms(t, ctx, pubsub, 2)

	require.NoError(t, writer.Do(ctx, "SELECT", 1).Err())
	require.NoError(t, writer.Set(ctx, "db-key", "v", 0).Err())
	expectMessage(t, ctx, pubsub, "__keyspace@1__:db-key", "set")
	expectMessage(t, ctx, pubsub, "__keyevent@1__:set", "db-key")

	require.EqualValues(t, 1, writer.Del(ctx, "db-key").Val())
	expectMessage(t, ctx, pubsub, "__keyspace@1__:db-key", "del")
	expectMessage(t, ctx, pubsub, "__keyevent@1__:del", "db-key")
	expectNoMessage(t, ctx, pubsub)
}

func TestKeyspaceNotifyNamespace(t *testing.T) {
	const (
		adminPassword  = "admin-password"
		namespace      = "tenant:1"
		namespaceToken = "tenant-token"
	)

	srv := util.StartServer(t, map[string]string{
		"notify-keyspace-events": "KEA",
		"requirepass":            adminPassword,
	})
	defer srv.Close()

	ctx := context.Background()
	admin := srv.NewClientWithOption(&redis.Options{Password: adminPassword})
	defer func() { require.NoError(t, admin.Close()) }()
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", namespace, namespaceToken).Err())

	sub := srv.NewClientWithOption(&redis.Options{Password: namespaceToken})
	defer func() { require.NoError(t, sub.Close()) }()
	writer := srv.NewClientWithOption(&redis.Options{Password: namespaceToken})
	defer func() { require.NoError(t, writer.Close()) }()

	pubsub := sub.PSubscribe(ctx, "__keyspace@tenant:1__:*", "__keyevent@tenant:1__:*")
	defer func() { require.NoError(t, pubsub.Close()) }()
	drainSubscribeConfirms(t, ctx, pubsub, 2)

	require.NoError(t, writer.Set(ctx, "namespace-key", "v", 0).Err())
	expectMessage(t, ctx, pubsub, "__keyspace@tenant:1__:namespace-key", "set")
	expectMessage(t, ctx, pubsub, "__keyevent@tenant:1__:set", "namespace-key")
}
