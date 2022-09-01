package util

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

type KvrocksServer struct {
	t   *testing.T
	cmd *exec.Cmd
	r   *redis.Client
}

func (s *KvrocksServer) Client() *redis.Client {
	return s.r
}

func (s *KvrocksServer) Close() {
	require.NoError(s.t, s.cmd.Process.Kill())
	require.EqualError(s.t, s.cmd.Wait(), "signal: killed")
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

	d := os.TempDir()
	f, err := os.CreateTemp(d, "*.conf")
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
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, err
	}

	r := redis.NewClient(&redis.Options{Addr: addr.String()})
	require.Eventually(t, func() bool {
		return r.Ping(context.Background()).Err() == nil
	}, time.Minute, time.Second)

	return &KvrocksServer{
		t:   t,
		cmd: cmd,
		r:   r,
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
