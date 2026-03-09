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

func TestClientPause(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass": "admin",
	})
	defer srv.Close()

	ctx := context.Background()

	adminClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
	defer func() { require.NoError(t, adminClient.Close()) }()

	// unpauseClient is a separate connection used to send CLIENT UNPAUSE.
	// It must be a different connection from the one being paused, because
	// the paused connection has its read events disabled and cannot receive
	// any new commands until it is resumed.
	unpauseClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
	defer func() { require.NoError(t, unpauseClient.Close()) }()

	t.Run("CLIENT PAUSE blocks write commands in ALL mode", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "500").Err())

		var wg sync.WaitGroup
		wg.Add(1)
		start := time.Now()
		go func() {
			defer wg.Done()
			writeClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
			defer func() { require.NoError(t, writeClient.Close()) }()
			require.NoError(t, writeClient.Set(ctx, "k1", "v1", 0).Err())
		}()
		wg.Wait()
		require.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(400))

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
	})

	t.Run("CLIENT UNPAUSE releases paused clients immediately", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "10000", "WRITE").Err())

		writeClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, writeClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		start := time.Now()
		go func() {
			defer wg.Done()
			require.NoError(t, writeClient.Set(ctx, "k2", "v2", 0).Err())
		}()

		time.Sleep(100 * time.Millisecond)
		// k2 should not exist yet because the SET is still blocked by the pause.
		require.Equal(t, redis.Nil, unpauseClient.Get(ctx, "k2").Err())

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()
		require.Less(t, time.Since(start).Milliseconds(), int64(5000))
		// After UNPAUSE the blocked SET should have completed, so k2 must be "v2".
		val, err := unpauseClient.Get(ctx, "k2").Result()
		require.NoError(t, err)
		require.Equal(t, "v2", val)
	})

	t.Run("CLIENT PAUSE WRITE blocks write but not read commands", func(t *testing.T) {
		require.NoError(t, adminClient.Set(ctx, "readkey", "hello", 0).Err())
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "2000", "WRITE").Err())

		// Read commands on a separate connection should complete immediately.
		readClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, readClient.Close()) }()
		start := time.Now()
		val, err := readClient.Get(ctx, "readkey").Result()
		require.NoError(t, err)
		require.Equal(t, "hello", val)
		require.Less(t, time.Since(start).Milliseconds(), int64(1000))

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
	})

	t.Run("CLIENT PAUSE requires admin permission", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "NAMESPACE", "ADD", "test_ns", "test_token").Err())

		userClient := srv.NewClientWithOption(&redis.Options{Password: "test_token"})
		defer func() { require.NoError(t, userClient.Close()) }()

		r := userClient.Do(ctx, "CLIENT", "PAUSE", "1000")
		require.ErrorContains(t, r.Err(), "admin permission required")

		r = userClient.Do(ctx, "CLIENT", "UNPAUSE")
		require.ErrorContains(t, r.Err(), "admin permission required")
	})

	t.Run("CLIENT PAUSE with invalid arguments", func(t *testing.T) {
		r := adminClient.Do(ctx, "CLIENT", "PAUSE")
		require.Error(t, r.Err())

		r = adminClient.Do(ctx, "CLIENT", "PAUSE", "notanumber")
		require.Error(t, r.Err())

		r = adminClient.Do(ctx, "CLIENT", "PAUSE", "1000", "READ")
		require.Error(t, r.Err())

		r = adminClient.Do(ctx, "CLIENT", "UNPAUSE", "extra")
		require.Error(t, r.Err())
	})

	t.Run("CLIENT LIST shows z flag for paused connections", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "10000").Err())

		pausedClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, pausedClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, pausedClient.Set(ctx, "flagtest", "v", 0).Err())
		}()

		// Give the goroutine time to send SET and get suspended.
		time.Sleep(100 * time.Millisecond)

		list, err := unpauseClient.Do(ctx, "CLIENT", "LIST").Text()
		require.NoError(t, err)
		require.Contains(t, list, "flags=z")

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()
	})

	t.Run("CLIENT UNPAUSE from a different connection is not blocked", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "30000").Err())

		pausedClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, pausedClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, pausedClient.Set(ctx, "unpause_regression", "v", 0).Err())
		}()

		time.Sleep(100 * time.Millisecond)

		// CLIENT UNPAUSE must complete immediately from a separate connection,
		// not get blocked. This is the regression test for the original bug.
		start := time.Now()
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		require.Less(t, time.Since(start).Milliseconds(), int64(1000))

		wg.Wait()
	})

	t.Run("CLIENT PAUSE blocks EXEC in MULTI/EXEC", func(t *testing.T) {
		multiClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, multiClient.Close()) }()

		require.NoError(t, multiClient.Do(ctx, "MULTI").Err())
		require.NoError(t, multiClient.Set(ctx, "multi_pause_key", "1", 0).Err())

		// EXEC has no "write" flag (exclusive bypass-multi slow), so use ALL mode to block it.
		go func() {
			time.Sleep(50 * time.Millisecond)
			require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "3000").Err())
		}()
		time.Sleep(150 * time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(1)
		var execStart time.Time
		var execErr error
		go func() {
			defer wg.Done()
			execStart = time.Now()
			execErr = multiClient.Do(ctx, "EXEC").Err()
		}()

		time.Sleep(400 * time.Millisecond)
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()

		require.NoError(t, execErr)
		require.GreaterOrEqual(t, time.Since(execStart).Milliseconds(), int64(400))
		val, err := unpauseClient.Get(ctx, "multi_pause_key").Result()
		require.NoError(t, err)
		require.Equal(t, "1", val)
	})

	t.Run("CLIENT PAUSE blocks BLPOP wakeup until UNPAUSE", func(t *testing.T) {
		blpopClient := srv.NewTCPClient()
		defer func() { require.NoError(t, blpopClient.Close()) }()
		require.NoError(t, blpopClient.WriteArgs("AUTH", "admin"))
		blpopClient.MustRead(t, "+OK")

		require.NoError(t, adminClient.Del(ctx, "blist").Err())
		require.NoError(t, blpopClient.WriteArgs("BLPOP", "blist", "0"))

		time.Sleep(50 * time.Millisecond)
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "5000", "WRITE").Err())

		pushClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, pushClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		var blpopDone time.Time
		go func() {
			defer wg.Done()
			blpopClient.MustReadStrings(t, []string{"blist", "data"})
			blpopDone = time.Now()
		}()

		var pushWg sync.WaitGroup
		pushWg.Add(1)
		go func() {
			defer pushWg.Done()
			require.NoError(t, pushClient.RPush(ctx, "blist", "data").Err())
		}()

		time.Sleep(200 * time.Millisecond)
		unpauseStart := time.Now()
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		pushWg.Wait()
		wg.Wait()

		require.GreaterOrEqual(t, blpopDone.Sub(unpauseStart).Milliseconds(), int64(0))
	})

	t.Run("CLIENT PAUSE WRITE blocks EVAL until UNPAUSE", func(t *testing.T) {
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "2000", "WRITE").Err())

		evalClient := srv.NewClientWithOption(&redis.Options{Password: "admin"})
		defer func() { require.NoError(t, evalClient.Close()) }()

		var wg sync.WaitGroup
		wg.Add(1)
		start := time.Now()
		var evalErr error
		var evalVal interface{}
		go func() {
			defer wg.Done()
			evalVal, evalErr = evalClient.Eval(ctx, `return redis.call('ping')`, []string{}).Result()
		}()

		time.Sleep(400 * time.Millisecond)
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		wg.Wait()

		require.NoError(t, evalErr)
		require.Equal(t, "PONG", evalVal)
		require.GreaterOrEqual(t, time.Since(start).Milliseconds(), int64(400))
	})
}
