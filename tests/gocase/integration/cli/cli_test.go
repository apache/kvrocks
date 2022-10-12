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

package cli

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

const defaultByteBufLen = math.MaxUint16

var cliPath = flag.String("cliPath", "redis-cli", "path to redis-cli")

type interactiveCli struct {
	c *exec.Cmd
	r *bufio.Reader
	w *bufio.Writer
}

func createInteractiveCli(t *testing.T, srv *util.KvrocksServer) *interactiveCli {
	c := exec.Command(*cliPath)
	c.Args = append(c.Args, "-h", srv.Host(), "-p", fmt.Sprintf("%d", srv.Port()))
	w, err := c.StdinPipe()
	require.NoError(t, err)
	r, err := c.StdoutPipe()
	require.NoError(t, err)
	require.NoError(t, c.Start())
	return &interactiveCli{
		c: c,
		r: bufio.NewReader(r),
		w: bufio.NewWriter(w),
	}
}

func (c *interactiveCli) Close() error {
	if err := c.c.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	return c.c.Wait()
}

func (c *interactiveCli) Read() (string, error) {
	b := make([]byte, defaultByteBufLen)
	for {
		n, err := c.r.Read(b)
		if err != nil {
			return "", err
		}
		if n > 0 {
			r := string(bytes.Trim(b, "\x00"))
			r = strings.ReplaceAll(r, "\r", "")
			r = strings.TrimSuffix(r, "\n")
			return r, nil
		}
	}
}

func (c *interactiveCli) Write(s string) error {
	_, err := c.w.WriteString(s + "\n")
	if err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *interactiveCli) MustEqual(t *testing.T, cmd, expected string) {
	require.NoError(t, c.Write(cmd))
	r, err := c.Read()
	require.NoError(t, err)
	require.Equal(t, expected, r)
}

func TestRedisCli(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	require.NoError(t, os.Setenv("TERM", "dumb"))
	defer func() { require.NoError(t, os.Unsetenv("TERM")) }()

	t.Run("test_interactive_cli", func(t *testing.T) {
		require.NoError(t, os.Setenv("FAKETTY", "1"))
		defer func() { require.NoError(t, os.Unsetenv("FAKETTY")) }()

		cli := createInteractiveCli(t, srv)
		defer func() { require.EqualError(t, cli.Close(), "signal: terminated") }()

		t.Run("INFO response should be printed raw", func(t *testing.T) {
			require.NoError(t, cli.Write("info"))
			r, err := cli.Read()
			require.NoError(t, err)
			for _, line := range strings.Split(r, "\n") {
				require.Regexp(t, `^$|^#|^[^#:]+:`, line)
			}
		})

		t.Run("Status reply", func(t *testing.T) {
			cli.MustEqual(t, "set key foo", "OK")
		})

		t.Run("Integer reply", func(t *testing.T) {
			cli.MustEqual(t, "incr counter", "(integer) 1")
		})

		t.Run("Bulk reply", func(t *testing.T) {
			require.NoError(t, rdb.Set(ctx, "key", "foo", 0).Err())
			cli.MustEqual(t, "get key", `"foo"`)
		})

		t.Run("Multi-bulk reply", func(t *testing.T) {
			require.NoError(t, rdb.RPush(ctx, "list", "foo").Err())
			require.NoError(t, rdb.RPush(ctx, "list", "bar").Err())
			cli.MustEqual(t, "lrange list 0 -1", strings.Trim(`
1) "foo"
2) "bar"
`, "\n"))
		})

		t.Run("Parsing quotes", func(t *testing.T) {
			cli.MustEqual(t, `set key "bar"`, "OK")
			require.Equal(t, "bar", rdb.Get(ctx, "key").Val())
			cli.MustEqual(t, `set key " bar "`, "OK")
			require.Equal(t, " bar ", rdb.Get(ctx, "key").Val())
			cli.MustEqual(t, `set key "\"bar\""`, "OK")
			require.Equal(t, `"bar"`, rdb.Get(ctx, "key").Val())
			cli.MustEqual(t, "set key \"\tbar\t\"", "OK")
			require.Equal(t, "\tbar\t", rdb.Get(ctx, "key").Val())

			// invalid quotation
			cli.MustEqual(t, `get ""key`, "Invalid argument(s)")
			cli.MustEqual(t, `get "key"x`, "Invalid argument(s)")

			// quotes after the argument are weird, but should be allowed
			cli.MustEqual(t, `set key"" bar`, "OK")
			require.Equal(t, "bar", rdb.Get(ctx, "key").Val())
		})
	})
}
