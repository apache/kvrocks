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

package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testConcurrentProxies(
	t *testing.T,
	newProxy func(ctx context.Context, t testing.TB, target string) uint64,
	extraCheck func(t *testing.T, ports []uint64),
) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = backend.Close() }()

	go func() {
		for {
			conn, err := backend.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()

	const numProxies = 8
	ports := make([]uint64, numProxies)
	for i := range ports {
		ports[i] = newProxy(ctx, t, backend.Addr().String())
	}

	seen := make(map[uint64]struct{}, numProxies)
	for _, port := range ports {
		_, reused := seen[port]
		require.False(t, reused, "proxy bound port %d more than once", port)
		seen[port] = struct{}{}
	}

	for i, port := range ports {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		require.NoError(t, err)

		payload := []byte(fmt.Sprintf("proxy-%d", i))
		_, err = conn.Write(payload)
		require.NoError(t, err)

		echoed := make([]byte, len(payload))
		_, err = io.ReadFull(conn, echoed)
		require.NoError(t, err)
		require.Equal(t, payload, echoed)

		require.NoError(t, conn.Close())
	}

	if extraCheck != nil {
		extraCheck(t, ports)
	}
}

func TestSimpleTCPProxyConcurrent(t *testing.T) {
	testConcurrentProxies(t, func(ctx context.Context, t testing.TB, target string) uint64 {
		return SimpleTCPProxy(ctx, t, target, false)
	}, nil)
}

func TestPausableTCPProxyConcurrent(t *testing.T) {
	pauseChs := make([]chan bool, 0, 8)
	testConcurrentProxies(t, func(ctx context.Context, t testing.TB, target string) uint64 {
		pauseCh := make(chan bool)
		pauseChs = append(pauseChs, pauseCh)
		return PausableTCPProxy(ctx, t, target, pauseCh)
	}, func(t *testing.T, ports []uint64) {
		for i, port := range ports {
			pauseChs[i] <- true

			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
			require.NoError(t, err)

			payload := []byte(fmt.Sprintf("paused-%d", i))
			_, err = conn.Write(payload)
			require.NoError(t, err)

			require.NoError(t, conn.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
			_, err = conn.Read(make([]byte, len(payload)))
			var netErr net.Error
			require.Error(t, err)
			require.True(t, errors.As(err, &netErr) && netErr.Timeout(),
				"expected the read to time out while the proxy is paused, got %v", err)

			pauseChs[i] <- false
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))

			echoed := make([]byte, len(payload))
			_, err = io.ReadFull(conn, echoed)
			require.NoError(t, err)
			require.Equal(t, payload, echoed)

			require.NoError(t, conn.Close())
		}
	})
}
