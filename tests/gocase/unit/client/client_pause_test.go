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
	"strings"
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

	newAuthedTCPClient := func(t *testing.T, name string) *util.TCPClient {
		t.Helper()
		c := srv.NewTCPClient()
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		require.NoError(t, c.WriteArgs("AUTH", "admin"))
		c.MustRead(t, "+OK")
		require.NoError(t, c.WriteArgs("CLIENT", "SETNAME", name))
		c.MustRead(t, "+OK")
		return c
	}

	queueTx := func(t *testing.T, c *util.TCPClient, cmds ...[]string) {
		t.Helper()
		require.NoError(t, c.WriteArgs("MULTI"))
		c.MustRead(t, "+OK")
		for _, cmd := range cmds {
			require.NoError(t, c.WriteArgs(cmd...))
			c.MustRead(t, "+QUEUED")
		}
	}

	waitClientPaused := func(t *testing.T, name string) {
		t.Helper()
		require.Eventually(t, func() bool {
			list, err := unpauseClient.Do(ctx, "CLIENT", "LIST").Text()
			if err != nil {
				return false
			}
			for _, line := range strings.Split(list, "\n") {
				if strings.Contains(line, "name="+name+" ") && strings.Contains(line, "flags=z") {
					return true
				}
			}
			return false
		}, 2*time.Second, 10*time.Millisecond, "expected client %s to enter paused state", name)
	}

	readLineAsync := func(c *util.TCPClient) <-chan string {
		ch := make(chan string, 1)
		go func() {
			line, err := c.ReadLine()
			if err != nil {
				ch <- "ERR:" + err.Error()
				return
			}
			ch <- line
		}()
		return ch
	}

	t.Run("CLIENT PAUSE WRITE holds pure-write EXEC and preserves pipelined commands", func(t *testing.T) {
		require.NoError(t, adminClient.Del(ctx, "pw_a", "pw_b", "pw_seq").Err())
		txConn := newAuthedTCPClient(t, "tx_pure_write")
		queueTx(t, txConn, []string{"SET", "pw_a", "1"}, []string{"SET", "pw_b", "2"})

		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "10000", "WRITE").Err())
		defer func() { _ = unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err() }()

		require.NoError(t, txConn.WriteArgs("EXEC"))
		require.NoError(t, txConn.WriteArgs("INCR", "pw_seq"))
		execCh := readLineAsync(txConn)

		waitClientPaused(t, "tx_pure_write")
		select {
		case early := <-execCh:
			require.FailNowf(t, "unexpected EXEC reply before UNPAUSE", "got %q", early)
		case <-time.After(50 * time.Millisecond):
		}
		require.Equal(t, redis.Nil, unpauseClient.Get(ctx, "pw_a").Err())

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		require.Equal(t, "*2", <-execCh)
		txConn.MustRead(t, "+OK")
		txConn.MustRead(t, "+OK")
		txConn.MustRead(t, ":1")
		require.Equal(t, "1", unpauseClient.Get(ctx, "pw_a").Val())
		require.Equal(t, "2", unpauseClient.Get(ctx, "pw_b").Val())

		require.NoError(t, txConn.WriteArgs("INCR", "pw_seq"))
		txConn.MustRead(t, ":2")
	})

	t.Run("CLIENT PAUSE WRITE holds read-then-write EXEC until pause expires", func(t *testing.T) {
		require.NoError(t, adminClient.Del(ctx, "rw_k0", "rw_k1").Err())
		require.NoError(t, adminClient.Set(ctx, "rw_k0", "v0", 0).Err())
		rwConn := newAuthedTCPClient(t, "tx_read_write")
		queueTx(t, rwConn, []string{"GET", "rw_k0"}, []string{"SET", "rw_k1", "v1"})

		execStart := time.Now()
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "1000", "WRITE").Err())
		defer func() { _ = unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err() }()
		require.NoError(t, rwConn.WriteArgs("EXEC"))
		rwCh := readLineAsync(rwConn)

		waitClientPaused(t, "tx_read_write")
		select {
		case early := <-rwCh:
			require.FailNowf(t, "unexpected truncated EXEC reply before expiry", "got %q", early)
		default:
		}
		require.Equal(t, redis.Nil, unpauseClient.Get(ctx, "rw_k1").Err())

		require.Equal(t, "*2", <-rwCh)
		rwConn.MustReadBulkString(t, "v0")
		rwConn.MustRead(t, "+OK")
		require.GreaterOrEqual(t, time.Since(execStart).Milliseconds(), int64(800))
		require.Equal(t, "v1", unpauseClient.Get(ctx, "rw_k1").Val())
	})

	t.Run("CLIENT PAUSE queued inside MULTI commits full transaction atomically", func(t *testing.T) {
		defer func() { _ = unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err() }()
		require.NoError(t, adminClient.Del(ctx, "tc_a1", "tc_a2", "tc_post").Err())

		tcConn := newAuthedTCPClient(t, "tx_in_exec_pause")
		queueTx(t, tcConn,
			[]string{"SET", "tc_a1", "v1"},
			[]string{"CLIENT", "PAUSE", "10000", "ALL"},
			[]string{"GET", "tc_a1"},
			[]string{"SET", "tc_a2", "v2"},
		)
		require.NoError(t, tcConn.WriteArgs("EXEC"))
		require.NoError(t, tcConn.WriteArgs("SET", "tc_post", "v3"))

		hdr, err := tcConn.ReadLine()
		require.NoError(t, err)
		if hdr != "*4" {
			_ = unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err()
			require.Equal(t, "*4", hdr, "tc_a1=%q tc_a2_err=%v",
				unpauseClient.Get(ctx, "tc_a1").Val(), unpauseClient.Get(ctx, "tc_a2").Err())
		}
		tcConn.MustRead(t, "+OK")
		tcConn.MustRead(t, "+OK")
		tcConn.MustReadBulkString(t, "v1")
		tcConn.MustRead(t, "+OK")

		postCh := readLineAsync(tcConn)
		waitClientPaused(t, "tx_in_exec_pause")
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		require.Equal(t, "+OK", <-postCh)
		require.Equal(t, "v1", unpauseClient.Get(ctx, "tc_a1").Val())
		require.Equal(t, "v2", unpauseClient.Get(ctx, "tc_a2").Val())
		require.Equal(t, "v3", unpauseClient.Get(ctx, "tc_post").Val())
	})

	t.Run("CLIENT PAUSE WRITE lets read-only EXEC pass and holds dynamic or watched write EXEC", func(t *testing.T) {
		defer func() { _ = unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err() }()
		require.NoError(t, adminClient.Del(ctx, "ro_k", "w_k", "hl_fe").Err())
		require.NoError(t, adminClient.Set(ctx, "ro_k", "abc", 0).Err())
		require.NoError(t, adminClient.Set(ctx, "w_k", "init", 0).Err())

		roConn := newAuthedTCPClient(t, "tx_readonly")
		wConn := newAuthedTCPClient(t, "tx_watch")
		require.NoError(t, wConn.WriteArgs("WATCH", "w_k"))
		wConn.MustRead(t, "+OK")
		queueTx(t, wConn, []string{"SET", "w_k", "tx_val"})
		require.NoError(t, adminClient.Set(ctx, "w_k", "other", 0).Err())

		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "10000", "WRITE").Err())

		// Read-only MULTI/EXEC passes immediately during WRITE pause.
		t0 := time.Now()
		queueTx(t, roConn, []string{"GET", "ro_k"}, []string{"STRLEN", "ro_k"})
		require.NoError(t, roConn.WriteArgs("EXEC"))
		roConn.MustRead(t, "*2")
		roConn.MustReadBulkString(t, "abc")
		roConn.MustRead(t, ":3")
		require.Less(t, time.Since(t0).Milliseconds(), int64(1000))

		// Write EXEC with dirty WATCH still waits for UNPAUSE before returning nil.
		require.NoError(t, wConn.WriteArgs("EXEC"))
		wCh := readLineAsync(wConn)
		select {
		case early := <-wCh:
			require.FailNowf(t, "dirty-WATCH write EXEC should wait for UNPAUSE", "got %q", early)
		case <-time.After(50 * time.Millisecond):
		}
		waitClientPaused(t, "tx_watch")
		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		require.Equal(t, "$-1", <-wCh)
		require.Equal(t, "other", unpauseClient.Get(ctx, "w_k").Val())

		defer func() {
			_ = adminClient.ConfigSet(ctx, "hash-encoding-mode", "legacy").Err()
			_ = adminClient.ConfigSet(ctx, "hash-length-mode", "accurate").Err()
		}()
		require.NoError(t, adminClient.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
		require.NoError(t, adminClient.ConfigSet(ctx, "hash-length-mode", "approximate").Err())
		require.NoError(t, adminClient.HSet(ctx, "hl_fe", "f1", "v1").Err())

		cfgConn := newAuthedTCPClient(t, "tx_cfg_hlen")
		queueTx(t, cfgConn, []string{"CONFIG", "SET", "hash-length-mode", "accurate"}, []string{"HLEN", "hl_fe"})
		require.NoError(t, adminClient.Do(ctx, "CLIENT", "PAUSE", "10000", "WRITE").Err())
		require.NoError(t, cfgConn.WriteArgs("EXEC"))
		cfgCh := readLineAsync(cfgConn)
		select {
		case early := <-cfgCh:
			require.FailNowf(t, "in-MULTI CONFIG SET + HLEN EXEC should pause", "got %q", early)
		case <-time.After(50 * time.Millisecond):
		}
		waitClientPaused(t, "tx_cfg_hlen")

		queueTx(t, roConn, []string{"GET", "ro_k"}, []string{"HLEN", "hl_fe"})
		require.NoError(t, roConn.WriteArgs("EXEC"))
		roConn.MustRead(t, "*2")
		roConn.MustReadBulkString(t, "abc")
		roConn.MustRead(t, ":1")

		require.NoError(t, unpauseClient.Do(ctx, "CLIENT", "UNPAUSE").Err())
		require.Equal(t, "*2", <-cfgCh)
		cfgConn.MustRead(t, "+OK")
		cfgConn.MustRead(t, ":1")
	})
}
