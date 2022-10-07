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
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

var (
	binPath      = flag.String("binPath", "", "path of Kvrocks binary")
	redisBinPath = flag.String("redisBinPath", "", "path of Redis binary")
	workspace    = flag.String("workspace", "", "directory of cases workspace")
	deleteOnExit = flag.Bool("deleteOnExit", false, "whether to delete workspace on exit")
)

type ServerType uint8

const (
	ServerTypeKvrocks ServerType = iota
	ServerTypeRedis
)

type Server struct {
	t    testing.TB
	cmd  *exec.Cmd
	addr *net.TCPAddr
	dir  string

	clean func()
}

func (s *Server) HostPort() string {
	return s.addr.AddrPort().String()
}

func (s *Server) Host() string {
	return s.addr.AddrPort().Addr().String()
}

func (s *Server) Port() uint16 {
	return s.addr.AddrPort().Port()
}

func (s *Server) GetDir() string {
	return s.dir
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

func (s *Server) NewTCPClient() *TCPClient {
	c, err := net.Dial(s.addr.Network(), s.addr.String())
	require.NoError(s.t, err)
	return newTCPClient(c)
}

func (s *Server) Close() {
	require.NoError(s.t, s.cmd.Process.Signal(syscall.SIGTERM))
	timer := time.AfterFunc(defaultGracePeriod, func() {
		require.NoError(s.t, s.cmd.Process.Kill())
	})
	defer timer.Stop()
	require.NoError(s.t, s.cmd.Wait())
	s.clean()
}

func StartServer(t testing.TB, configs map[string]string) *Server {
	return startServer(t, ServerTypeKvrocks, configs)
}

func StartRedisServer(t testing.TB, configs map[string]string) *Server {
	return startServer(t, ServerTypeRedis, configs)
}

func startServer(t testing.TB, tp ServerType, configs map[string]string) *Server {
	addr, err := findFreePort()
	require.NoError(t, err)
	configs["bind"] = addr.IP.String()
	configs["port"] = fmt.Sprintf("%d", addr.Port)

	dir := *workspace
	require.NotEmpty(t, dir, "please set the workspace by `-workspace`")
	dir, err = os.MkdirTemp(dir, fmt.Sprintf("%s-%d-*-%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixMilli(), tp))
	require.NoError(t, err)
	configs["dir"] = dir

	f, err := os.Create(filepath.Join(dir, "default.conf"))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	var cmd *exec.Cmd
	switch tp {
	case ServerTypeKvrocks:
		b := *binPath
		require.NotEmpty(t, b, "please set the binary path of Kvrocks by `-binPath`")
		cmd = exec.Command(b)
		cmd.Args = append(cmd.Args, "-c", f.Name())
	case ServerTypeRedis:
		if k, ok := configs["kvrocks-dir"]; ok {
			rdbPath := k + "/dump.rdb"
			_, err := os.Stat(rdbPath)
			require.NoError(t, err)
			cmd := exec.Command("cp", rdbPath, dir)
			require.NoError(t, cmd.Run())
			delete(configs, "kvrocks-dir")
		}
		b := *redisBinPath
		require.NotEmpty(t, b, "please set the binary path of Redis by `-redisBinPath`")
		cmd = exec.Command(b)
		cmd.Args = append(cmd.Args, f.Name())
	}

	for k := range configs {
		_, err := f.WriteString(fmt.Sprintf("%s %s\n", k, configs[k]))
		require.NoError(t, err)
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

	return &Server{
		t:    t,
		cmd:  cmd,
		addr: addr,
		dir:  dir,
		clean: func() {
			require.NoError(t, stdout.Close())
			require.NoError(t, stderr.Close())
			if *deleteOnExit {
				require.NoError(t, os.RemoveAll(dir))
			}
		},
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
