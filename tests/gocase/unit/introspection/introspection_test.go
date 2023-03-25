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

package introspection

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestIntrospection(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("CLIENT LIST", func(t *testing.T) {
		v := rdb.ClientList(ctx).Val()
		require.Regexp(t, "id=.* addr=.*:.* fd=.* name=.* age=.* idle=.* flags=N namespace=.* qbuf=.* .*obuf=.* cmd=client.*", v)
	})

	t.Run("CLIENT INFO", func(t *testing.T) {
		v := rdb.Do(ctx, "CLIENT", "INFO").Val()
		require.Regexp(t, "id=.* addr=.*:.* fd=.* name=.* age=.* idle=.* flags=N namespace=.* qbuf=.* .*obuf=.* cmd=client.*", v)
	})

	t.Run("MONITOR can log executed commands", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("MONITOR"))
		c.MustRead(t, "+OK")
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Get(ctx, "foo").Err())
		c.MustMatch(t, ".*set.*foo.*bar.*")
		c.MustMatch(t, ".*get.*foo.*")
	})

	t.Run("CLIENT GETNAME should return NIL if name is not assigned", func(t *testing.T) {
		require.EqualError(t, rdb.ClientGetName(ctx).Err(), redis.Nil.Error())
	})

	t.Run("CLIENT LIST shows empty fields for unassigned names", func(t *testing.T) {
		require.Regexp(t, ".*name= .*", rdb.ClientList(ctx).Val())
	})

	t.Run("CLIENT INFO shows empty fields for unassigned names", func(t *testing.T) {
		require.Regexp(t, ".*name= .*", rdb.Do(ctx, "CLIENT", "INFO").Val())
	})

	t.Run("CLIENT SETNAME does not accept spaces", func(t *testing.T) {
		require.Error(t, rdb.Do(ctx, "CLIENT", "SETNAME", "foo bar").Err())
	})

	t.Run("CLIENT SETNAME can assign a name to this connection", func(t *testing.T) {
		r := rdb.Do(ctx, "CLIENT", "SETNAME", "myname")
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
		require.Regexp(t, ".*name=myname.*", rdb.ClientList(ctx).Val())
	})

	t.Run("CLIENT SETNAME can assign a name to this connection", func(t *testing.T) {
		r := rdb.Do(ctx, "CLIENT", "SETNAME", "myname")
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
		require.Regexp(t, ".*name=myname.*", rdb.ClientList(ctx).Val())
	})

	t.Run("CLIENT SETNAME can change the name of an existing connection", func(t *testing.T) {
		r := rdb.Do(ctx, "CLIENT", "SETNAME", "someothername")
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
		require.Regexp(t, ".*name=someothername.*", rdb.ClientList(ctx).Val())
	})

	t.Run("After CLIENT SETNAME, connection can still be closed", func(t *testing.T) {
		func() {
			c := srv.NewClient()
			defer func() { require.NoError(t, c.Close()) }()
			r := c.Do(ctx, "CLIENT", "SETNAME", "foobar")
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
			require.Regexp(t, ".*name=foobar.*", rdb.ClientList(ctx).Val())
		}()

		// now the client should no longer be listed
		require.Eventually(t, func() bool {
			r := rdb.ClientList(ctx).Val()
			return !strings.Contains(r, "foobar")
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("Kill normal client", func(t *testing.T) {
		defer func() { rdb = srv.NewClient() }()

		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		r := c.Do(ctx, "CLIENT", "SETNAME", "normal")
		require.NoError(t, r.Err())
		require.Equal(t, "OK", r.Val())
		require.Regexp(t, ".*name=normal.*", rdb.ClientList(ctx).Val())

		require.EqualValues(t, 1, rdb.ClientKillByFilter(ctx, "skipme", "yes", "type", "normal").Val())
		require.EqualValues(t, 1, rdb.ClientKillByFilter(ctx, "skipme", "no", "type", "normal").Val())

		// reconnect
		require.NoError(t, rdb.Close())
		rdb = srv.NewClient()

		// now the client should no longer be listed
		require.Eventually(t, func() bool {
			r := rdb.ClientList(ctx).Val()
			return !strings.Contains(r, "normal")
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("Kill pubsub client", func(t *testing.T) {
		// subscribe clients
		c0 := srv.NewClient()
		defer func() { require.NoError(t, c0.Close()) }()
		require.NoError(t, c0.Do(ctx, "CLIENT", "SETNAME", "pubsub").Err())
		r := c0.Do(ctx, "SUBSCRIBE", "foo")
		require.NoError(t, r.Err())
		require.Equal(t, "[subscribe foo 1]", fmt.Sprintf("%v", r.Val()))

		// psubscribe clients
		c1 := srv.NewClient()
		defer func() { require.NoError(t, c1.Close()) }()
		require.NoError(t, c1.Do(ctx, "CLIENT", "SETNAME", "pubsub_patterns").Err())
		r = c1.Do(ctx, "PSUBSCRIBE", "bar.*")
		require.NoError(t, r.Err())
		require.Equal(t, "[psubscribe bar.* 1]", fmt.Sprintf("%v", r.Val()))

		// normal clients
		c2 := srv.NewClient()
		require.NoError(t, c2.Do(ctx, "CLIENT", "SETNAME", "normal").Err())
		defer func() { require.NoError(t, c2.Close()) }()

		require.EqualValues(t, 2, rdb.ClientKillByFilter(ctx, "type", "pubsub").Val())

		// now the pubsub client should no longer be listed
		// but normal client should not be dropped
		require.Eventually(t, func() bool {
			r := rdb.ClientList(ctx).Val()
			return strings.Count(r, "pubsub") == 0 && strings.Count(r, "normal") == 1
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("DEBUG will freeze server", func(t *testing.T) {
		// use TCPClient to avoid waiting for reply
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.WriteArgs("DEBUG", "sleep", "2.5"))

		// sleep 100ms to prevent the successive set command to be executed
		// before the debug command since there are in the different connection.
		time.Sleep(100 * time.Millisecond)

		now := time.Now()
		require.NoError(t, rdb.Set(ctx, "a", "b", 0).Err())
		require.GreaterOrEqual(t, time.Since(now).Seconds(), 2.0)
	})
}

func TestMultiServerIntrospection(t *testing.T) {
	master := util.StartServer(t, map[string]string{})
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()

	slave := util.StartServer(t, map[string]string{})
	defer slave.Close()
	slaveClient := slave.NewClient()
	defer func() { require.NoError(t, slaveClient.Close()) }()

	ctx := context.Background()

	util.SlaveOf(t, slaveClient, master)
	util.WaitForSync(t, slaveClient)

	t.Run("Kill slave client", func(t *testing.T) {
		count, err := strconv.Atoi(util.FindInfoEntry(masterClient, "sync_partial_ok"))
		require.NoError(t, err)

		// kill slave connection
		require.EqualValues(t, 1, masterClient.ClientKillByFilter(ctx, "type", "slave").Val())

		// incr sync_partial_ok since slave reconnects
		require.Eventually(t, func() bool {
			newCount, err := strconv.Atoi(util.FindInfoEntry(masterClient, "sync_partial_ok"))
			require.NoError(t, err)
			return newCount == count+1
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("Kill master client", func(t *testing.T) {
		count, err := strconv.Atoi(util.FindInfoEntry(masterClient, "sync_partial_ok"))
		require.NoError(t, err)

		// kill master connection by type
		require.EqualValues(t, 1, slaveClient.ClientKillByFilter(ctx, "type", "master").Val())

		// incr sync_partial_ok since slave reconnects
		require.Eventually(t, func() bool {
			newCount, err := strconv.Atoi(util.FindInfoEntry(masterClient, "sync_partial_ok"))
			require.NoError(t, err)
			return newCount == count+1
		}, 5*time.Second, 100*time.Millisecond)

		count, err = strconv.Atoi(util.FindInfoEntry(masterClient, "sync_partial_ok"))
		require.NoError(t, err)

		// kill master connection by addr
		require.Equal(t, "OK", slaveClient.ClientKill(ctx, master.HostPort()).Val())

		// incr sync_partial_ok since slave reconnects
		require.Eventually(t, func() bool {
			newCount, err := strconv.Atoi(util.FindInfoEntry(masterClient, "sync_partial_ok"))
			require.NoError(t, err)
			return newCount == count+1
		}, 5*time.Second, 100*time.Millisecond)
	})
}
