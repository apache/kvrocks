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

package client

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// pauseWriteFunctionLib is a minimal Lua library used to exercise the FUNCTION subcommands.
const pauseWriteFunctionLib = "#!lua name=pause_write_lib\n" +
	"local function inc(keys, args)\n" +
	"    return args[1] + 1\n" +
	"end\n" +
	"redis.register_function(\"inc\", inc)\n"

// TestClientPauseWriteDefersWriteCommands verifies that the commands which write to RocksDB
// while declaring no write flag (FUNCTION FLUSH, MPUBLISH and the mutating NAMESPACE
// subcommands) are deferred by CLIENT PAUSE WRITE, while the read-only subcommands
// are still served during the pause. See https://github.com/apache/kvrocks/issues/3828.
func TestClientPauseWriteDefersWriteCommands(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass": "admin",
	})
	defer srv.Close()

	ctx := context.Background()

	adminClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
	defer func() { require.NoError(t, adminClient.Close()) }()

	// unpauseClient is a separate connection used to send CLIENT UNPAUSE and to
	// issue the read-only commands while other connections are paused.
	unpauseClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
	defer func() { require.NoError(t, unpauseClient.Close()) }()

	t.Run("CLIENT PAUSE WRITE defers FUNCTION FLUSH but not FUNCTION LIST", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "FUNCTION", "LOAD", pauseWriteFunctionLib).Err())
		libraries, err := adminClient.FunctionList(ctx, redis.FunctionListQuery{WithCode: true}).Result()
		require.NoError(t, err)
		require.Len(t, libraries, 1)
		require.Equal(t, "pause_write_lib", libraries[0].Name)

		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "3000", "WRITE").Err())

		flushClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, flushClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, flushClient.Do(ctx, "FUNCTION", "FLUSH").Err())
		}()

		// FUNCTION FLUSH must be deferred while the write pause is active, so the
		// loaded library is still visible. The read-only FUNCTION LIST subcommand
		// is served immediately on a separate connection.
		time.Sleep(200 * time.Millisecond)
		start := time.Now()
		libraries, err = unpauseClient.FunctionList(ctx, redis.FunctionListQuery{WithCode: true}).Result()
		require.NoError(t, err)
		require.Len(t, libraries, 1)
		require.Less(t, time.Since(start).Milliseconds(), int64(1000))

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()

		// Once the pause is lifted, the deferred FUNCTION FLUSH completes and the
		// library is gone.
		libraries, err = unpauseClient.FunctionList(ctx, redis.FunctionListQuery{WithCode: true}).Result()
		require.NoError(t, err)
		require.Empty(t, libraries)
	})

	t.Run("CLIENT PAUSE WRITE defers MPUBLISH until UNPAUSE", func(t *testing.T) {
		subscriber := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, subscriber.Close()) }()
		pubsub := subscriber.Subscribe(ctx, "pause-write-mpublish")
		defer func() { require.NoError(t, pubsub.Close()) }()
		subscription, err := pubsub.ReceiveTimeout(ctx, 2*time.Second)
		require.NoError(t, err)
		require.IsType(t, &redis.Subscription{}, subscription)

		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "3000", "WRITE").Err())

		publishClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, publishClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, publishClient.Do(ctx, "MPUBLISH", "pause-write-mpublish", "m1", "m2").Err())
		}()

		// MPUBLISH writes each message to RocksDB on the master to replicate it,
		// so it must be deferred while the write pause is active and no message
		// should reach the subscriber yet.
		time.Sleep(300 * time.Millisecond)
		msg, err := pubsub.ReceiveTimeout(ctx, 300*time.Millisecond)
		require.Errorf(t, err, "expected no message while CLIENT PAUSE WRITE is active, got %v", msg)

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()

		// Both messages are delivered once the pause is lifted.
		for _, expected := range []string{"m1", "m2"} {
			msg, err = pubsub.ReceiveTimeout(ctx, 2*time.Second)
			require.NoError(t, err)
			message, ok := msg.(*redis.Message)
			require.Truef(t, ok, "expected *redis.Message, got %T", msg)
			require.Equal(t, "pause-write-mpublish", message.Channel)
			require.Equal(t, expected, message.Payload)
		}
	})

	t.Run("CLIENT PAUSE WRITE defers NAMESPACE ADD/SET/DEL but not NAMESPACE GET/CURRENT", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "3000", "WRITE").Err())

		nsClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, nsClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, nsClient.Do(ctx, "NAMESPACE", "ADD", "paused_ns", "token1").Err())
		}()

		// The mutating NAMESPACE subcommands must be deferred while the write
		// pause is active, while the read-only subcommands are served immediately
		// and the new namespace is not visible yet.
		time.Sleep(200 * time.Millisecond)
		start := time.Now()
		r := unpauseClient.Do(ctx, "NAMESPACE", "GET", "paused_ns")
		require.Equal(t, redis.Nil, r.Err())
		require.Equal(t, "default", unpauseClient.Do(ctx, "NAMESPACE", "CURRENT").Val())
		require.Less(t, time.Since(start).Milliseconds(), int64(1000))

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()

		r = unpauseClient.Do(ctx, "NAMESPACE", "GET", "paused_ns")
		require.NoError(t, r.Err())
		require.Equal(t, "token1", r.Val())

		// NAMESPACE SET is deferred as well.
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "3000", "WRITE").Err())
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, nsClient.Do(ctx, "NAMESPACE", "SET", "paused_ns", "token2").Err())
		}()
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, "token1", unpauseClient.Do(ctx, "NAMESPACE", "GET", "paused_ns").Val())
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()
		require.Equal(t, "token2", unpauseClient.Do(ctx, "NAMESPACE", "GET", "paused_ns").Val())

		// NAMESPACE DEL is deferred as well.
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "3000", "WRITE").Err())
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, nsClient.Do(ctx, "NAMESPACE", "DEL", "paused_ns").Err())
		}()
		time.Sleep(200 * time.Millisecond)
		require.Equal(t, "token2", unpauseClient.Do(ctx, "NAMESPACE", "GET", "paused_ns").Val())
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()
		require.Equal(t, redis.Nil, unpauseClient.Do(ctx, "NAMESPACE", "GET", "paused_ns").Err())
	})
}
