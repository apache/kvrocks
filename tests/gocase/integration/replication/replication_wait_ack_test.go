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
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type waitAckPeer struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newWaitAckPeer(t *testing.T, conn net.Conn) *waitAckPeer {
	t.Helper()
	t.Cleanup(func() { _ = conn.Close() })
	peer := &waitAckPeer{conn: conn, reader: bufio.NewReader(conn)}
	peer.resetDeadline(t)
	return peer
}

func dialWaitAckPeer(t *testing.T, address string) *waitAckPeer {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	require.NoError(t, err)
	return newWaitAckPeer(t, conn)
}

func listenWaitAckPeer(t *testing.T) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func acceptWaitAckPeer(t *testing.T, listener *net.TCPListener) *waitAckPeer {
	t.Helper()
	require.NoError(t, listener.SetDeadline(time.Now().Add(5*time.Second)))
	conn, err := listener.Accept()
	require.NoError(t, err)
	return newWaitAckPeer(t, conn)
}

func (peer *waitAckPeer) resetDeadline(t *testing.T) {
	t.Helper()
	require.NoError(t, peer.conn.SetDeadline(time.Now().Add(5*time.Second)))
}

func (peer *waitAckPeer) readLine() (string, error) {
	line, err := peer.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	if !strings.HasSuffix(line, "\r\n") {
		return "", fmt.Errorf("invalid RESP line: %q", line)
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

func (peer *waitAckPeer) readLength(prefix byte, maximum int) (int, error) {
	line, err := peer.readLine()
	if err != nil {
		return 0, err
	}
	if len(line) < 2 || line[0] != prefix {
		return 0, fmt.Errorf("expected RESP prefix %q, got %q", prefix, line)
	}
	length, err := strconv.Atoi(line[1:])
	if err != nil || length < 0 || length > maximum {
		return 0, fmt.Errorf("invalid RESP length: %q", line)
	}
	return length, nil
}

func (peer *waitAckPeer) readBulk() (string, error) {
	length, err := peer.readLength('$', 64<<20)
	if err != nil {
		return "", err
	}
	data := make([]byte, length+2)
	if _, err := io.ReadFull(peer.reader, data); err != nil {
		return "", err
	}
	if string(data[length:]) != "\r\n" {
		return "", fmt.Errorf("invalid bulk string terminator")
	}
	return string(data[:length]), nil
}

func (peer *waitAckPeer) readCommand() ([]string, error) {
	count, err := peer.readLength('*', 32)
	if err != nil {
		return nil, err
	}
	args := make([]string, count)
	for index := range args {
		args[index], err = peer.readBulk()
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}

func waitAckBulk(value string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
}

func (peer *waitAckPeer) write(value string) error {
	n, err := io.WriteString(peer.conn, value)
	if err == nil && n != len(value) {
		return io.ErrShortWrite
	}
	return err
}

func (peer *waitAckPeer) writeCommand(args ...string) error {
	value := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		value += waitAckBulk(arg)
	}
	return peer.write(value)
}

func startWaitAckServer(t *testing.T, config map[string]string) (*util.KvrocksServer, *redis.Client) {
	t.Helper()
	server := util.StartServer(t, config)
	t.Cleanup(server.Close)
	client := server.NewClient()
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	return server, client
}

func waitAckSequence(t *testing.T, client *redis.Client) uint64 {
	t.Helper()
	sequence, err := strconv.ParseUint(util.FindInfoEntry(client, "master_repl_offset"), 10, 64)
	require.NoError(t, err)
	return sequence
}

func setWaitAckValue(t *testing.T, peer *waitAckPeer, key, value string) {
	t.Helper()
	peer.resetDeadline(t)
	require.NoError(t, peer.writeCommand("SET", key, value))
	response, err := peer.readLine()
	require.NoError(t, err)
	require.Equal(t, "+OK", response)
}

func startBlockedWait(t *testing.T, waiter *waitAckPeer, client *redis.Client) {
	t.Helper()
	waiter.resetDeadline(t)
	require.NoError(t, waiter.writeCommand("WAIT", "1", "10000"))
	require.Eventually(t, func() bool {
		return util.FindInfoEntry(client, "blocked_clients") == "1"
	}, 5*time.Second, 10*time.Millisecond)
}

func readWaitGetAck(t *testing.T, peer *waitAckPeer) {
	t.Helper()
	peer.resetDeadline(t)
	for {
		value, err := peer.readBulk()
		require.NoError(t, err, "master must request an ACK without another write")
		if value == "ping" {
			continue
		}
		require.Equal(t, "_getack", value)
		return
	}
}

func TestReplicationGetAckOnOuterIdle(t *testing.T) {
	t.Parallel()
	testReplicationGetAckOnIdle(t, false)
}

func TestReplicationGetAckOnInnerIdle(t *testing.T) {
	t.Parallel()
	testReplicationGetAckOnIdle(t, true)
}

func testReplicationGetAckOnIdle(t *testing.T, sendBatch bool) {
	t.Helper()
	master, client := startWaitAckServer(t, map[string]string{"max-replication-lag": "0"})
	waiter := dialWaitAckPeer(t, master.HostPort())
	setWaitAckValue(t, waiter, "seed", "value")
	sequence := waitAckSequence(t, client)
	require.Positive(t, sequence)
	peer := dialWaitAckPeer(t, master.HostPort())
	require.NoError(t, peer.writeCommand("PSYNC", strconv.FormatUint(sequence+1, 10)))
	response, err := peer.readLine()
	require.NoError(t, err)
	require.Equal(t, "+OK", response)

	if sendBatch {
		setWaitAckValue(t, waiter, "marker", "value")
		sequence = waitAckSequence(t, client)
		for {
			batch, err := peer.readBulk()
			require.NoError(t, err)
			if batch != "ping" {
				require.NotEqual(t, "_getack", batch)
				break
			}
		}
	}

	// Register WAIT only after the requested sequence has already been sent.
	startBlockedWait(t, waiter, client)
	readWaitGetAck(t, peer)
	// Ignoring ping prevents the periodic heartbeat from satisfying this test.
	require.NoError(t, peer.writeCommand("replconf", "ack", strconv.FormatUint(sequence, 10)))
	waiter.resetDeadline(t)
	response, err = waiter.readLine()
	require.NoError(t, err)
	require.Equal(t, ":1", response)
}

func TestReplicationStandaloneGetAck(t *testing.T) {
	t.Parallel()
	testReplicaGetAck(t, false, false)
}

func TestReplicationStandaloneGetAckGroupSync(t *testing.T) {
	t.Parallel()
	testReplicaGetAck(t, false, true)
}

func TestReplicationGetAckBeforePartialBatch(t *testing.T) {
	t.Parallel()
	testReplicaGetAck(t, true, false)
}

func TestReplicationGetAckBeforePartialBatchGroupSync(t *testing.T) {
	t.Parallel()
	testReplicaGetAck(t, true, true)
}

func testReplicaGetAck(t *testing.T, partialBatch, groupSync bool) {
	t.Helper()
	listener := listenWaitAckPeer(t)
	config := map[string]string{"db-name": "wait-ack-test", "use-rsid-psync": "no"}
	if groupSync {
		config["replication-group-sync"] = "yes"
		config["rocksdb.write_options.sync"] = "yes"
	}
	replica, client := startWaitAckServer(t, config)
	require.NoError(t, client.SlaveOf(context.Background(), "127.0.0.1", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)).Err())
	peer := acceptWaitAckPeer(t, listener)
	for _, step := range []struct {
		command []string
		reply   string
	}{
		{[]string{"_db_name"}, "wait-ack-test\r\n"},
		{[]string{"replconf", "listening-port", strconv.FormatUint(replica.Port(), 10)}, "+OK\r\n"},
		{[]string{"PSYNC", "1"}, "+OK\r\n"},
	} {
		command, err := peer.readCommand()
		require.NoError(t, err)
		require.Equal(t, step.command, command)
		require.NoError(t, peer.write(step.reply))
	}

	// Consume the initial ACK so later responses cannot come from this ping.
	require.NoError(t, peer.write(waitAckBulk("ping")))
	ack, err := peer.readCommand()
	require.NoError(t, err)
	require.Equal(t, []string{"replconf", "ack", "0"}, ack)

	if partialBatch {
		require.NoError(t, peer.write(waitAckBulk("_getack")+"$4\r\npi"))
		ack, err = peer.readCommand()
		require.NoError(t, err, "GETACK must be acknowledged before the following bulk is complete")
		require.Equal(t, []string{"replconf", "ack", "0"}, ack)
		require.NoError(t, peer.write("ng\r\n"))
		return
	}

	for request := 0; request < 3; request++ {
		peer.resetDeadline(t)
		require.NoError(t, peer.write(waitAckBulk("_getack")))
		ack, err = peer.readCommand()
		require.NoError(t, err, "standalone GETACK request %d must receive an ACK", request+1)
		require.Equal(t, []string{"replconf", "ack", "0"}, ack)
	}
}

func TestReplicationWaitAfterAppliedWrite(t *testing.T) {
	t.Parallel()
	master, masterClient := startWaitAckServer(t, map[string]string{})
	waiter := dialWaitAckPeer(t, master.HostPort())
	_, replicaClient := startWaitAckServer(t, map[string]string{"use-rsid-psync": "no"})
	listener := listenWaitAckPeer(t)
	ctx := context.Background()
	require.NoError(t, replicaClient.SlaveOf(ctx, "127.0.0.1", strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)).Err())
	downstream := acceptWaitAckPeer(t, listener)
	upstream := dialWaitAckPeer(t, master.HostPort())

	// Relay the real handshake, then control the two replication directions.
	for _, name := range []string{"_db_name", "replconf", "PSYNC"} {
		command, err := downstream.readCommand()
		require.NoError(t, err)
		require.NotEmpty(t, command)
		require.Equal(t, name, command[0])
		require.NoError(t, upstream.writeCommand(command...))
		response, err := upstream.readLine()
		require.NoError(t, err)
		require.NoError(t, downstream.write(response+"\r\n"))
	}

	setWaitAckValue(t, waiter, "marker", "already-applied")
	sequence := strconv.FormatUint(waitAckSequence(t, masterClient), 10)
	for {
		batch, err := upstream.readBulk()
		require.NoError(t, err)
		if batch == "ping" {
			continue
		}
		require.NotEqual(t, "_getack", batch)
		require.NoError(t, downstream.write(waitAckBulk(batch)))
		break
	}
	// The first batch produces an ordinary ACK. Withhold it from the master.
	ack, err := downstream.readCommand()
	require.NoError(t, err)
	require.Equal(t, []string{"replconf", "ack", sequence}, ack)
	value, err := replicaClient.Get(ctx, "marker").Result()
	require.NoError(t, err)
	require.Equal(t, "already-applied", value)
	require.Contains(t, util.FindInfoEntry(masterClient, "slave0"), "offset=0,")

	// No further writes or heartbeat messages reach the replica from this point.
	startBlockedWait(t, waiter, masterClient)
	readWaitGetAck(t, upstream)
	downstream.resetDeadline(t)
	require.NoError(t, downstream.write(waitAckBulk("_getack")))
	ack, err = downstream.readCommand()
	require.NoError(t, err)
	require.Equal(t, []string{"replconf", "ack", sequence}, ack)
	require.NoError(t, upstream.writeCommand(ack...))
	waiter.resetDeadline(t)
	response, err := waiter.readLine()
	require.NoError(t, err)
	require.Equal(t, ":1", response)
	require.Equal(t, sequence, strconv.FormatUint(waitAckSequence(t, replicaClient), 10))
}
