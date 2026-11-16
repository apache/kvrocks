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
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestReplicationSendTimeout(t *testing.T) {
	t.Parallel()
	master := util.StartServer(t, map[string]string{
		"replication-send-timeout-ms": "1000",
		"max-replication-lag":         "0",
	})
	defer master.Close()
	masterClient := master.NewClient()
	defer func() { require.NoError(t, masterClient.Close()) }()

	conn, err := net.DialTimeout("tcp", master.HostPort(), 5*time.Second)
	require.NoError(t, err)
	// Close the peer before the server even when a blocked send fails the test.
	defer func() { require.NoError(t, conn.Close()) }()
	peer := conn.(*net.TCPConn)
	require.NoError(t, peer.SetReadBuffer(1024))
	require.NoError(t, peer.SetDeadline(time.Now().Add(5*time.Second)))

	sequence, err := strconv.ParseUint(util.FindInfoEntry(masterClient, "master_repl_offset"), 10, 64)
	require.NoError(t, err)
	_, err = fmt.Fprintf(peer, "PSYNC %d\r\n", sequence+1)
	require.NoError(t, err)
	response, err := bufio.NewReader(peer).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "+OK\r\n", response)
	require.Eventually(t, func() bool {
		return util.FindInfoEntry(masterClient, "connected_slaves") == "1"
	}, 5*time.Second, 50*time.Millisecond)

	// Keep the peer open without reading, so one batch fills the socket buffers.
	require.NoError(t, masterClient.Set(context.Background(), "large-batch", strings.Repeat("x", 16<<20), 0).Err())
	require.Eventually(t, func() bool {
		return util.FindInfoEntry(masterClient, "connected_slaves") == "0" &&
			master.LogFileMatches(t, "Write error while sending batch to slave .*send timeout")
	}, 5*time.Second, 50*time.Millisecond, "the send deadline should disconnect a replica that stops reading")
}
