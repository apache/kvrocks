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

package limits

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func getClientOutputBufferLimitDisconnections(t *testing.T, srv *util.KvrocksServer) int {
	t.Helper()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	count, err := strconv.Atoi(util.FindInfoEntry(rdb, "client_output_buffer_limit_disconnections"))
	require.NoError(t, err)
	return count
}

func TestClientOutputBufferLimitConfig(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("limits are disabled by default", func(t *testing.T) {
		v, err := rdb.ConfigGet(ctx, "client-output-buffer-limit").Result()
		require.NoError(t, err)
		require.Equal(t, "normal 0 0 0 slave 0 0 0 pubsub 0 0 0", v["client-output-buffer-limit"])
	})

	t.Run("partial spec only changes the specified classes", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "client-output-buffer-limit", "pubsub 256k 128k 30").Err())
		v, err := rdb.ConfigGet(ctx, "client-output-buffer-limit").Result()
		require.NoError(t, err)
		require.Equal(t, "normal 0 0 0 slave 0 0 0 pubsub 262144 131072 30", v["client-output-buffer-limit"])

		// 'replica' is an alias of the slave class
		require.NoError(t, rdb.ConfigSet(ctx, "client-output-buffer-limit", "replica 1m 512k 10").Err())
		v, err = rdb.ConfigGet(ctx, "client-output-buffer-limit").Result()
		require.NoError(t, err)
		require.Equal(t, "normal 0 0 0 slave 1048576 524288 10 pubsub 262144 131072 30", v["client-output-buffer-limit"])
	})

	t.Run("invalid spec is rejected and keeps the old value", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "client-output-buffer-limit", "normal 100m 0 0").Err())
		for _, spec := range []string{
			"",
			"normal 0 0",
			"unknown 0 0 0",
			"normal x 0 0",
			"normal 0 x 0",
			"normal 0 0 x",
			"normal 0 0 0 pubsub 1m 512k",
			// hard/soft limits and soft seconds are limited to INT64_MAX
			"normal 9223372036854775808 0 0",
			"normal 0 9223372036854775808 0",
			"normal 0 0 9223372036854775808",
			"normal 0 0 -1",
			// the soft limit should be less than the hard limit
			"normal 1m 2m 0",
			"normal 1m 1m 0",
		} {
			require.Error(t, rdb.ConfigSet(ctx, "client-output-buffer-limit", spec).Err())
		}
		v, err := rdb.ConfigGet(ctx, "client-output-buffer-limit").Result()
		require.NoError(t, err)
		require.Equal(t, "normal 104857600 0 0 slave 1048576 524288 10 pubsub 262144 131072 30",
			v["client-output-buffer-limit"])
		require.NoError(t, rdb.ConfigSet(ctx, "client-output-buffer-limit", "normal 0 0 0").Err())
	})

	t.Run("config rewrite and restart keep the value", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "client-output-buffer-limit", "pubsub 4m 1m 20").Err())
		require.NoError(t, rdb.ConfigRewrite(ctx).Err())
		srv.Restart()

		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		v, err := c.ConfigGet(ctx, "client-output-buffer-limit").Result()
		require.NoError(t, err)
		require.Equal(t, "normal 0 0 0 slave 1048576 524288 10 pubsub 4194304 1048576 20",
			v["client-output-buffer-limit"])
	})
}

func TestClientOutputBufferLimitPubsubHardLimit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 0 0 0 slave 0 0 0 pubsub 1m 0 0",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("slow subscriber is disconnected once the hard limit is reached", func(t *testing.T) {
		sub := srv.NewTCPClient()
		defer func() { require.NoError(t, sub.Close()) }()
		require.NoError(t, sub.WriteArgs("SUBSCRIBE", "ch"))
		sub.MustRead(t, "*3")
		sub.MustRead(t, "$9")
		sub.MustRead(t, "subscribe")
		sub.MustRead(t, "$2")
		sub.MustRead(t, "ch")
		sub.MustRead(t, ":1")

		// A single message bigger than the hard limit must kill the subscriber
		// right when it's appended to the output buffer. The message is made
		// much bigger than the limit so that the kernel socket buffers cannot
		// concurrently drain the output buffer below it.
		payload := strings.Repeat("x", 8*1024*1024)
		res := rdb.Publish(ctx, "ch", payload)
		require.NoError(t, res.Err())
		// The message was already appended to the output buffer of the
		// subscriber before it was scheduled to close, so the subscriber is
		// still counted as a receiver of the message, the same as Redis.
		require.EqualValues(t, 1, res.Val())

		require.Eventually(t, func() bool {
			return getClientOutputBufferLimitDisconnections(t, srv) == 1
		}, 5*time.Second, 100*time.Millisecond)
	})
}

func TestClientOutputBufferLimitPubsubSoftLimit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 0 0 0 slave 0 0 0 pubsub 0 512k 1",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("slow subscriber is disconnected after staying over the soft limit", func(t *testing.T) {
		sub := srv.NewTCPClient()
		defer func() { require.NoError(t, sub.Close()) }()
		require.NoError(t, sub.WriteArgs("SUBSCRIBE", "ch"))
		sub.MustRead(t, "*3")
		sub.MustRead(t, "$9")
		sub.MustRead(t, "subscribe")
		sub.MustRead(t, "$2")
		sub.MustRead(t, "ch")
		sub.MustRead(t, ":1")

		// The subscriber stops reading: the first big message crosses the soft
		// limit and starts the clock, but must not kill the connection yet.
		// The message is made much bigger than the limit so that the kernel
		// socket buffers cannot drain the output buffer below it in between.
		payload := strings.Repeat("x", 8*1024*1024)
		require.NoError(t, rdb.Publish(ctx, "ch", payload).Err())
		require.Equal(t, 0, getClientOutputBufferLimitDisconnections(t, srv))

		// Still over the soft limit after more than soft-seconds: the next
		// append must disconnect the subscriber.
		time.Sleep(2 * time.Second)
		require.NoError(t, rdb.Publish(ctx, "ch", "ping").Err())

		require.Eventually(t, func() bool {
			return getClientOutputBufferLimitDisconnections(t, srv) == 1
		}, 5*time.Second, 100*time.Millisecond)
	})
}

func TestBlockedClientOutputBufferLimit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 0 0 0 slave 0 0 0 pubsub 1m 0 0",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("subscriber blocked on BLPOP is disconnected once the hard limit is reached", func(t *testing.T) {
		sub := srv.NewTCPClient()
		defer func() { require.NoError(t, sub.Close()) }()
		require.NoError(t, sub.WriteArgs("SUBSCRIBE", "ch"))
		sub.MustRead(t, "*3")
		sub.MustRead(t, "$9")
		sub.MustRead(t, "subscribe")
		sub.MustRead(t, "$2")
		sub.MustRead(t, "ch")
		sub.MustRead(t, ":1")

		// Block the subscriber on a key that will never be written, so that the
		// blocking command replaces the bufferevent callbacks of the connection.
		require.NoError(t, sub.WriteArgs("BLPOP", "blocked-key", "0"))
		require.Eventually(t, func() bool {
			return strings.Contains(rdb.ClientList(ctx).Val(), "cmd=blpop")
		}, 5*time.Second, 100*time.Millisecond)
		require.Equal(t, "1", util.FindInfoEntry(rdb, "blocked_clients"))

		// The blocked client must be closed instead of only being counted as
		// disconnected while lingering with a full output buffer.
		payload := strings.Repeat("x", 8*1024*1024)
		require.NoError(t, rdb.Publish(ctx, "ch", payload).Err())

		require.Eventually(t, func() bool {
			return !strings.Contains(rdb.ClientList(ctx).Val(), "cmd=blpop")
		}, 5*time.Second, 100*time.Millisecond)
		require.Equal(t, 1, getClientOutputBufferLimitDisconnections(t, srv))

		// The disconnected client must be unblocked as well, so it should no
		// longer be counted as a blocked client.
		require.Equal(t, "0", util.FindInfoEntry(rdb, "blocked_clients"))

		// The stale registration of the disconnected client should not consume
		// the pushed element: it must still be delivered to a new consumer.
		require.NoError(t, rdb.LPush(ctx, "blocked-key", "value").Err())
		require.Equal(t, []string{"blocked-key", "value"}, rdb.BLPop(ctx, time.Second, "blocked-key").Val())
	})
}

func TestNormalClientOutputBufferLimit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"client-output-buffer-limit": "normal 1m 0 0 slave 0 0 0 pubsub 0 0 0",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("normal client exceeding the hard limit is disconnected", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "big", strings.Repeat("x", 2*1024*1024), 0).Err())

		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("GET", "big"))

		require.Eventually(t, func() bool {
			return getClientOutputBufferLimitDisconnections(t, srv) == 1
		}, 5*time.Second, 100*time.Millisecond)
	})
}
