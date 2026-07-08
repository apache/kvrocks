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
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// kvrocks' internal name for the default (admin) namespace.
const defaultNS = "__namespace"

// tenantConn is a TCP-level connection authenticated against a namespace
// token (or requirepass, for admin). TCP rather than go-redis is used so
// that a server-side kill is directly observable — go-redis transparently
// reconnects, masking the close.
type tenantConn struct {
	*util.TCPClient
}

// dial opens a new authenticated TCP connection.
func dial(t *testing.T, srv *util.KvrocksServer, password string) *tenantConn {
	t.Helper()
	c := srv.NewTCPClient()
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.WriteArgs("AUTH", password))
	c.MustRead(t, "+OK")
	return &tenantConn{c}
}

// info returns one parsed field from CLIENT INFO (e.g. "id", "addr").
func (c *tenantConn) info(t *testing.T, key string) string {
	t.Helper()
	require.NoError(t, c.WriteArgs("CLIENT", "INFO"))
	// CLIENT INFO returns a bulk string. Connection::ToString already ends
	// with \n, so the RESP frame is "$<len>\r\n<body>\n\r\n" and ReadLine
	// breaks at the embedded \n. Consume header, body, then trailer to
	// realign the buffer.
	_, err := c.ReadLine()
	require.NoError(t, err)
	body, err := c.ReadLine()
	require.NoError(t, err)
	_, err = c.ReadLine()
	require.NoError(t, err)
	for field := range strings.FieldsSeq(body) {
		if v, ok := strings.CutPrefix(field, key+"="); ok {
			return v
		}
	}
	t.Fatalf("no %s= field in CLIENT INFO: %q", key, body)
	return ""
}

// requireAlive asserts the connection still responds to PING.
func (c *tenantConn) requireAlive(t *testing.T) {
	t.Helper()
	require.NoError(t, c.WriteArgs("PING"))
	c.MustRead(t, "+PONG")
}

// requireKilled asserts the server has (or imminently will) close the connection.
func (c *tenantConn) requireKilled(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		if err := c.WriteArgs("PING"); err != nil {
			return true
		}
		_, err := c.ReadLine()
		return err != nil
	}, 5*time.Second, 100*time.Millisecond, "connection was expected to be killed")
}

// countNamespaceLines counts CLIENT LIST rows whose `namespace=` field equals ns.
func countNamespaceLines(list, ns string) int {
	count := 0
	for line := range strings.SplitSeq(list, "\n") {
		if strings.Contains(line, " namespace="+ns+" ") {
			count++
		}
	}
	return count
}

// TestClientCommandNamespaceIsolation verifies that CLIENT LIST / INFO / KILL
// are scoped to the caller's namespace for non-admin (tenant) connections,
// while admin connections (authenticated via requirepass / default namespace)
// retain server-wide visibility and control.
//
// These tests cover the cross-namespace isolation bypass on CLIENT LIST /
// INFO / KILL: without filtering, a tenant authenticated against a
// non-default namespace can both enumerate and terminate connections that
// belong to other namespaces (including the admin namespace).
func TestClientCommandNamespaceIsolation(t *testing.T) {
	const adminPass = "adminpass"
	srv := util.StartServer(t, map[string]string{"requirepass": adminPass})
	defer srv.Close()

	ctx := context.Background()

	admin := srv.NewClientWithOption(&redis.Options{Password: adminPass})
	defer func() { require.NoError(t, admin.Close()) }()
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns1", "token1").Err())
	require.NoError(t, admin.Do(ctx, "NAMESPACE", "ADD", "ns2", "token2").Err())

	t.Run("CLIENT LIST: tenant only sees its own namespace", func(t *testing.T) {
		_ = dial(t, srv, "token1")
		_ = dial(t, srv, "token1")
		_ = dial(t, srv, "token2")

		ns1 := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		defer func() { require.NoError(t, ns1.Close()) }()

		list := ns1.ClientList(ctx).Val()
		require.NotEmpty(t, list)
		require.GreaterOrEqual(t, countNamespaceLines(list, "ns1"), 2,
			"ns1 tenant should see at least its own connections, got:\n%s", list)
		require.Equal(t, 0, countNamespaceLines(list, "ns2"),
			"ns1 tenant must not see ns2 connections, got:\n%s", list)
		require.Equal(t, 0, countNamespaceLines(list, defaultNS),
			"ns1 tenant must not see default-namespace (admin) connections, got:\n%s", list)
	})

	t.Run("CLIENT LIST: admin sees every namespace", func(t *testing.T) {
		_ = dial(t, srv, "token1")
		_ = dial(t, srv, "token2")

		list := admin.ClientList(ctx).Val()
		require.GreaterOrEqual(t, countNamespaceLines(list, "ns1"), 1, list)
		require.GreaterOrEqual(t, countNamespaceLines(list, "ns2"), 1, list)
		require.GreaterOrEqual(t, countNamespaceLines(list, defaultNS), 1, list)
	})

	t.Run("CLIENT INFO: only describes the caller's own connection", func(t *testing.T) {
		ns1 := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		defer func() { require.NoError(t, ns1.Close()) }()

		info, err := ns1.Do(ctx, "CLIENT", "INFO").Text()
		require.NoError(t, err)
		require.Contains(t, info, " namespace=ns1 ")
		require.NotContains(t, info, " namespace="+defaultNS+" ")
		require.NotContains(t, info, " namespace=ns2 ")
	})

	t.Run("CLIENT KILL by ID: tenant cannot kill another namespace", func(t *testing.T) {
		conn2 := dial(t, srv, "token2")
		attacker := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		defer func() { require.NoError(t, attacker.Close()) }()

		killed := attacker.ClientKillByFilter(ctx, "id", conn2.info(t, "id")).Val()
		require.EqualValues(t, 0, killed,
			"ns1 tenant must not be able to kill a ns2 connection by ID")
		conn2.requireAlive(t)
	})

	t.Run("CLIENT KILL by ID: tenant cannot kill an admin connection", func(t *testing.T) {
		adminConn := dial(t, srv, adminPass)
		attacker := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		defer func() { require.NoError(t, attacker.Close()) }()

		killed := attacker.ClientKillByFilter(ctx, "id", adminConn.info(t, "id")).Val()
		require.EqualValues(t, 0, killed,
			"ns1 tenant must not be able to kill an admin/default-namespace connection")
		adminConn.requireAlive(t)
	})

	t.Run("CLIENT KILL by ADDR: tenant cannot kill another namespace", func(t *testing.T) {
		conn2 := dial(t, srv, "token2")
		attacker := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		defer func() { require.NoError(t, attacker.Close()) }()

		// The legacy "CLIENT KILL <addr>" form should reply with an error
		// ("No such client") because, from ns1's perspective, the ns2
		// connection does not exist.
		err := attacker.ClientKill(ctx, conn2.info(t, "addr")).Err()
		require.Error(t, err, "ns1 tenant must not be able to kill a ns2 connection by ADDR")
		conn2.requireAlive(t)
	})

	t.Run("CLIENT KILL TYPE normal: tenant only affects its own namespace", func(t *testing.T) {
		conn1 := dial(t, srv, "token1")
		conn2 := dial(t, srv, "token2")
		adminConn := dial(t, srv, adminPass)

		attacker := srv.NewClientWithOption(&redis.Options{Password: "token1"})
		defer func() { require.NoError(t, attacker.Close()) }()

		killed := attacker.ClientKillByFilter(ctx, "skipme", "yes", "type", "normal").Val()
		require.GreaterOrEqual(t, killed, int64(1))

		conn1.requireKilled(t)
		conn2.requireAlive(t)
		adminConn.requireAlive(t)
	})

	t.Run("CLIENT KILL: admin retains full server-wide power", func(t *testing.T) {
		conn2 := dial(t, srv, "token2")

		killed := admin.ClientKillByFilter(ctx, "id", conn2.info(t, "id")).Val()
		require.EqualValues(t, 1, killed,
			"admin must be able to kill a connection in any namespace by ID")
		conn2.requireKilled(t)
	})
}
