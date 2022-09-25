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
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

type IServer interface {
	Host() string
	Port() uint16
	NewClient() *redis.Client
	NewClientWithOption(options *redis.Options) *redis.Client
	NewTCPClient() *tcpClient
	Close()
}

type Server struct {
	t    testing.TB
	cmd  *exec.Cmd
	addr *net.TCPAddr

	clean func()
}
type KvrocksServer struct {
	Server
}

type RedisServer struct {
	Server
}

func (s *Server) Host() string {
	return s.addr.AddrPort().Addr().String()
}

func (s *Server) Port() uint16 {
	return s.addr.AddrPort().Port()
}

func (s *Server) NewClient() *redis.Client {
	return s.NewClientWithOption(&redis.Options{Addr: s.addr.String()})
}

func (s *Server) NewClientWithOption(options *redis.Options) *redis.Client {
	if options.Addr == "" {
		options.Addr = s.addr.String()
	}
	return redis.NewClient(options)
}

func (s *Server) NewTCPClient() *tcpClient {
	c, err := net.Dial(s.addr.Network(), s.addr.String())
	require.NoError(s.t, err)
	return newTCPClient(c)
}

func (s *Server) Close() {
	require.NoError(s.t, s.cmd.Process.Signal(syscall.SIGTERM))
	timer := time.AfterFunc(k8sDefaultGracePeriod*time.Second, func() {
		require.NoError(s.t, s.cmd.Process.Kill())
	})
	defer timer.Stop()
	require.NoError(s.t, s.cmd.Wait())
	s.clean()
}

func StartServer(t testing.TB, st ServerType, configs map[string]string) IServer {
	require.Contains(t, []ServerType{TypeKvrocks, TypeRedis}, st)
	var b, confName, typeName string
	switch st {
	case TypeKvrocks:
		{
			b = os.Getenv("KVROCKS_BIN_PATH")
			require.NotEmpty(t, b, "please set the environment variable `KVROCKS_BIN_PATH`")
			confName = "kvrocks.conf"
			typeName = "kvrocks"
		}
	case TypeRedis:
		{
			b = os.Getenv("REDIS_BIN_PATH")
			require.NotEmpty(t, b, "please set the environment variable `REDIS_BIN_PATH`")
			confName = "redis.conf"
			typeName = "redis"
			configs["logfile"] = "redis.log"
			configs["save"] = "900 1"
		}
	}

	cmd := exec.Command(b)

	addr, err := findFreePort()
	require.NoError(t, err)
	configs["bind"] = addr.IP.String()
	configs["port"] = fmt.Sprintf("%d", addr.Port)

	dir := os.Getenv("GO_CASE_WORKSPACE")
	require.NotEmpty(t, dir, "please set the environment variable `GO_CASE_WORKSPACE`")
	dir, err = os.MkdirTemp(dir, fmt.Sprintf("%s-%d-*-%s", t.Name(), time.Now().UnixMilli(), typeName))
	require.NoError(t, err)
	configs["dir"] = dir

	f, err := os.Create(filepath.Join(dir, confName))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	for k := range configs {
		_, err := f.WriteString(fmt.Sprintf("%s %s\n", k, configs[k]))
		require.NoError(t, err)
	}

	if st == TypeKvrocks {
		cmd.Args = append(cmd.Args, "-c", f.Name())
	} else {
		cmd.Args = append(cmd.Args, f.Name())
	}

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	require.NoError(t, err)
	cmd.Stdout = stdout
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	require.NoError(t, err)
	cmd.Stderr = stderr

	require.NoError(t, cmd.Start())

	c := redis.NewClient(&redis.Options{Addr: addr.String()})
	defer func() { require.NoError(t, c.Close()) }()
	require.Eventually(t, func() bool {
		err := c.Ping(context.Background()).Err()
		return err == nil || err.Error() == "NOAUTH Authentication required."
	}, time.Minute, time.Second)
	if st == TypeKvrocks {
		return &KvrocksServer{
			Server: Server{
				t:    t,
				cmd:  cmd,
				addr: addr,
				clean: func() {
					require.NoError(t, stdout.Close())
					require.NoError(t, stderr.Close())
				},
			},
		}
	} else {
		return &RedisServer{
			Server: Server{
				t:    t,
				cmd:  cmd,
				addr: addr,
				clean: func() {
					require.NoError(t, stdout.Close())
					require.NoError(t, stderr.Close())
				},
			},
		}
	}
}

func findFreePort() (*net.TCPAddr, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return nil, err
	}
	lis, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().(*net.TCPAddr), nil
}
