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
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

type KvrocksServer struct {
	t    *testing.T
	cmd  *exec.Cmd
	addr net.Addr

	clean func()
}

func (s *KvrocksServer) NewClient() *redis.Client {
	return redis.NewClient(&redis.Options{Addr: s.addr.String()})
}

func (s *KvrocksServer) NewTcpClient() *tcpClient {
	c, err := net.Dial(s.addr.Network(), s.addr.String())
	require.NoError(s.t, err)
	return newTcpClient(c)
}

func (s *KvrocksServer) Close() {
	require.NoError(s.t, s.cmd.Process.Kill())
	require.EqualError(s.t, s.cmd.Wait(), "signal: killed")
	s.clean()
}

func StartServer(t *testing.T, configs map[string]string) (*KvrocksServer, error) {
	b := os.Getenv("KVROCKS_BIN_PATH")
	cmd := exec.Command(b)

	addr, err := findFreePort()
	if err != nil {
		return nil, err
	}
	configs["bind"] = addr.IP.String()
	configs["port"] = fmt.Sprintf("%d", addr.Port)

	dir := os.Getenv("GO_CASE_WORKSPACE")
	require.NoError(t, err)
	dir, err = os.MkdirTemp(dir, "Server-*")
	require.NoError(t, err)
	configs["dir"] = dir

	f, err := os.Create(filepath.Join(dir, "kvrocks.conf"))
	if err != nil {
		return nil, err
	}

	for k := range configs {
		_, err := f.WriteString(fmt.Sprintf("%s %s\n", k, configs[k]))
		if err != nil {
			return nil, err
		}
	}

	cmd.Args = append(cmd.Args, "-c", f.Name())

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	require.NoError(t, err)
	cmd.Stdout = stdout
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(t, err)
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	c := redis.NewClient(&redis.Options{Addr: addr.String()})
	defer func() { require.NoError(t, c.Close()) }()
	require.Eventually(t, func() bool {
		return c.Ping(context.Background()).Err() == nil
	}, time.Minute, time.Second)

	return &KvrocksServer{
		t:    t,
		cmd:  cmd,
		addr: addr,
		clean: func() {
			require.NoError(t, stdout.Close())
			require.NoError(t, stderr.Close())
		},
	}, nil
}

func findFreePort() (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	lis, err := net.ListenTCP("tcp", addr)
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr), nil
}
