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
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func checkServerShutdown(t *testing.T, command bool, signal os.Signal) {
	t.Helper()
	srv := StartServer(t, map[string]string{})
	exited := make(chan error, 1)
	go func() { exited <- srv.cmd.Wait() }()
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = srv.cmd.Process.Kill()
			err := <-exited
			t.Logf("server exit during cleanup: %v", err)
		}
		srv.clean(false)
	})

	if command {
		conn, err := net.DialTimeout("tcp", srv.HostPort(), time.Second)
		require.NoError(t, err)
		defer conn.Close()
		require.NoError(t, conn.SetWriteDeadline(time.Now().Add(time.Second)))
		_, err = io.WriteString(conn, "*1\r\n$8\r\nSHUTDOWN\r\n")
		require.NoError(t, err)
	}
	if signal != nil {
		err := srv.cmd.Process.Signal(signal)
		// SHUTDOWN may have already reaped the process through Wait.
		if command && err != nil {
			require.ErrorIs(t, err, os.ErrProcessDone)
		} else {
			require.NoError(t, err)
		}
	}

	select {
	case err := <-exited:
		waited = true
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("server did not complete shutdown")
	}
}

func TestServerShutdownBySIGTERM(t *testing.T) {
	checkServerShutdown(t, false, syscall.SIGTERM)
}

func TestServerShutdownBySIGINT(t *testing.T) {
	checkServerShutdown(t, false, syscall.SIGINT)
}

func TestServerShutdownByCommand(t *testing.T) {
	checkServerShutdown(t, true, nil)
}

func TestServerShutdownByCommandAndSignal(t *testing.T) {
	checkServerShutdown(t, true, syscall.SIGTERM)
}
