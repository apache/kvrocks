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
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func readReplicationAuthCommand(t *testing.T, reader *bufio.Reader) []string {
	t.Helper()
	readLength := func(prefix byte) int {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasSuffix(line, "\r\n"), "invalid RESP length: %q", line)
		require.Equal(t, prefix, line[0])
		length, err := strconv.Atoi(strings.TrimSuffix(line[1:], "\r\n"))
		require.NoError(t, err)
		require.GreaterOrEqual(t, length, 0)
		return length
	}

	count := readLength('*')
	args := make([]string, count)
	for index := range args {
		length := readLength('$')
		value := make([]byte, length+2)
		_, err := io.ReadFull(reader, value)
		require.NoError(t, err)
		require.Equal(t, "\r\n", string(value[length:]))
		args[index] = string(value[:length])
	}
	return args
}

func readReplicationAuthHandshake(t *testing.T, conn net.Conn, dbName string) [][]string {
	t.Helper()
	reader := bufio.NewReader(conn)
	var commands [][]string
	for {
		args := readReplicationAuthCommand(t, reader)
		require.NotEmpty(t, args)
		commands = append(commands, args)
		var reply string
		switch strings.ToLower(args[0]) {
		case "auth", "replconf":
			reply = "+OK\r\n"
		case "_db_name":
			reply = dbName + "\r\n"
		case "psync", "_fetch_meta":
			return commands
		default:
			t.Fatalf("unexpected replication handshake command: %q", args)
		}
		_, err := io.WriteString(conn, reply)
		require.NoError(t, err)
	}
}

func TestReplicationAuthOnPSyncReconnect(t *testing.T) {
	t.Parallel()
	testReplicationAuthOnReconnect(t, false, false)
}

func TestReplicationAuthOnFullSyncReconnect(t *testing.T) {
	t.Parallel()
	testReplicationAuthOnReconnect(t, true, false)
}

func TestReplicationAuthOnPSyncRestart(t *testing.T) {
	t.Parallel()
	testReplicationAuthOnReconnect(t, false, true)
}

func TestReplicationAuthOnFullSyncRestart(t *testing.T) {
	t.Parallel()
	testReplicationAuthOnReconnect(t, true, true)
}

func testReplicationAuthOnReconnect(t *testing.T, fullSync, rejectAuth bool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	accept := func() net.Conn {
		t.Helper()
		// Authentication failures use RESTART's ten-second delay before reconnecting.
		require.NoError(t, listener.(*net.TCPListener).SetDeadline(time.Now().Add(30*time.Second)))
		conn, err := listener.Accept()
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		require.NoError(t, conn.SetDeadline(time.Now().Add(30*time.Second)))
		return conn
	}

	const password = "replication-secret"
	const dbName = "replication-auth-test"
	replica := util.StartServer(t, map[string]string{
		"masterauth":     password,
		"db-name":        dbName,
		"use-rsid-psync": "no",
	})
	defer replica.Close()
	client := replica.NewClient()
	defer func() { require.NoError(t, client.Close()) }()
	ctx := context.Background()
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, client.SlaveOf(ctx, "127.0.0.1", strconv.Itoa(port)).Err())

	psyncCommands := [][]string{
		{"AUTH", password},
		{"_db_name"},
		{"replconf", "listening-port", fmt.Sprint(replica.Port())},
		{"PSYNC", "1"},
	}
	if fullSync {
		conn := accept()
		require.Equal(t, psyncCommands, readReplicationAuthHandshake(t, conn, dbName))
		_, err := io.WriteString(conn, "-ERR full synchronization required\r\n")
		require.NoError(t, err)
	}

	attempts := 3
	if rejectAuth {
		conn := accept()
		reader := bufio.NewReader(conn)
		require.Equal(t, []string{"AUTH", password}, readReplicationAuthCommand(t, reader))
		_, err := io.WriteString(conn, "-ERR invalid password\r\n")
		require.NoError(t, err)
		// Keep our side open: the replica must close it through the RESTART path.
		_, err = reader.ReadByte()
		require.ErrorIs(t, err, io.EOF)
		attempts = 1
	}

	for attempt := 0; attempt < attempts; attempt++ {
		conn := accept()
		commands := readReplicationAuthHandshake(t, conn, dbName)
		if fullSync {
			require.Equal(t, [][]string{{"AUTH", password}, {"_fetch_meta"}}, commands,
				"fullsync connection %d must authenticate exactly once before fetching metadata", attempt+1)
		} else {
			require.Equal(t, psyncCommands, commands,
				"psync connection %d must authenticate exactly once before its handshake", attempt+1)
			_, err := io.WriteString(conn, "+OK\r\n")
			require.NoError(t, err)
			util.WaitForSync(t, client)
		}
		if attempt+1 < attempts {
			// EOF restarts the existing state machine; another SLAVEOF would replace it.
			require.NoError(t, conn.Close())
		}
	}
}
