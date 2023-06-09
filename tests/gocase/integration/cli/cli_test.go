//go:build !ignore_when_tsan

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
	"errors"
	"fmt"
	"io"
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

type interactiveCli struct {
	c *exec.Cmd
	r *bufio.Reader
	w *bufio.Writer
}

func createInteractiveCli(t *testing.T, srv *util.KvrocksServer) *interactiveCli {
	c := exec.Command(util.CLIPath())
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
	pos := 0
	b := make([]byte, defaultByteBufLen)
	for {
		if pos >= defaultByteBufLen {
			return "", errors.New("exceed read buffer size")
		}
		n, err := c.r.Read(b[pos:])
		if err != nil {
			return "", err
		}
		pos += n
		if n > 0 {
			// For the big response size scenario, it may need multiple times
			// to read all bytes, but it's no a good way to wait for the entire
			// response except parsing the Redis protocol. To make this simple,
			// we can just check whether the response has the newline or not.
			if pos < 2 || b[pos-1] != '\n' {
				continue
			}
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

type result struct {
	t *testing.T

	b   []byte
	err error
}

func (res *result) Success() string {
	require.NoError(res.t, res.err)
	r := string(bytes.Trim(res.b, "\x00"))
	r = strings.ReplaceAll(r, "\r", "")
	r = strings.TrimSuffix(r, "\n")
	return r
}

func (res *result) Failed() {
	require.Error(res.t, res.err)
}

func runCli(t *testing.T, srv *util.KvrocksServer, in io.Reader, args ...string) *result {
	c := exec.Command(util.CLIPath())
	c.Stdin = in
	c.Args = append(c.Args, "-h", srv.Host(), "-p", fmt.Sprintf("%d", srv.Port()))
	c.Args = append(c.Args, args...)
	b, err := c.CombinedOutput()
	return &result{
		t:   t,
		b:   b,
		err: err,
	}
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

	t.Run("test_tty_cli", func(t *testing.T) {
		require.NoError(t, os.Setenv("FAKETTY", "1"))
		defer func() { require.NoError(t, os.Unsetenv("FAKETTY")) }()

		t.Run("Status reply", func(t *testing.T) {
			require.Equal(t, "OK", runCli(t, srv, nil, "set", "key", "bar").Success())
			require.Equal(t, "bar", rdb.Get(ctx, "key").Val())
		})

		t.Run("Integer reply", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "counter").Err())
			require.Equal(t, "(integer) 1", runCli(t, srv, nil, "incr", "counter").Success())
		})

		t.Run("Bulk reply", func(t *testing.T) {
			require.NoError(t, rdb.Set(ctx, "key", "tab\tnewline\n", 0).Err())
			require.Equal(t, `"tab\tnewline\n"`, runCli(t, srv, nil, "get", "key").Success())
		})

		t.Run("Multi-bulk reply", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "list").Err())
			require.NoError(t, rdb.RPush(ctx, "list", "foo").Err())
			require.NoError(t, rdb.RPush(ctx, "list", "bar").Err())
			expected := strings.Trim(`
1) "foo"
2) "bar"
`, "\n")
			require.Equal(t, expected, runCli(t, srv, nil, "lrange", "list", "0", "-1").Success())
		})

		t.Run("Read last argument from pipe", func(t *testing.T) {
			cmd := exec.Command("echo", "foo")
			out, err := cmd.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			defer func() { require.NoError(t, cmd.Wait()) }()
			require.Equal(t, "OK", runCli(t, srv, out, "-x", "set", "key").Success())
			require.Equal(t, "foo\n", rdb.Get(ctx, "key").Val())
		})

		t.Run("Read last argument from file", func(t *testing.T) {
			f, err := os.CreateTemp("", "")
			require.NoError(t, err)
			defer func() { require.NoError(t, f.Close()) }()
			_, err = f.WriteString("from file")
			require.NoError(t, err)
			require.NoError(t, f.Sync())
			_, err = f.Seek(0, io.SeekStart)
			require.NoError(t, err)
			require.Equal(t, "OK", runCli(t, srv, f, "-x", "set", "key").Success())
			require.Equal(t, "from file", rdb.Get(ctx, "key").Val())
		})
	})

	t.Run("test_notty_cli", func(t *testing.T) {
		t.Run("Status reply", func(t *testing.T) {
			require.Equal(t, "OK", runCli(t, srv, nil, "set", "key", "bar").Success())
			require.Equal(t, "bar", rdb.Get(ctx, "key").Val())
		})

		t.Run("Integer reply", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "counter").Err())
			require.Equal(t, "1", runCli(t, srv, nil, "incr", "counter").Success())
		})

		t.Run("Bulk reply", func(t *testing.T) {
			require.NoError(t, rdb.Set(ctx, "key", "tab\tnewline\n", 0).Err())
			require.Equal(t, "tab\tnewline\n", runCli(t, srv, nil, "get", "key").Success())
		})

		t.Run("Multi-bulk reply", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, "list").Err())
			require.NoError(t, rdb.RPush(ctx, "list", "foo").Err())
			require.NoError(t, rdb.RPush(ctx, "list", "bar").Err())
			require.Equal(t, "foo\nbar", runCli(t, srv, nil, "lrange", "list", "0", "-1").Success())
		})

		t.Run("Quoted input arguments", func(t *testing.T) {
			require.NoError(t, rdb.Set(ctx, "\x00\x00", "value", 0).Err())
			require.Equal(t, "value", runCli(t, srv, nil, "--quoted-input", "get", `"\x00\x00"`).Success())
		})

		t.Run("No accidental unquoting of input arguments", func(t *testing.T) {
			require.Equal(t, "OK", runCli(t, srv, nil, "--quoted-input", "set", `"\x41\x41"`, "quoted-val").Success())
			require.Equal(t, "OK", runCli(t, srv, nil, "set", `"\x41\x41"`, "unquoted-val").Success())
			require.Equal(t, "quoted-val", rdb.Get(ctx, "AA").Val())
			require.Equal(t, "unquoted-val", rdb.Get(ctx, `"\x41\x41"`).Val())
		})

		t.Run("Invalid quoted input arguments", func(t *testing.T) {
			runCli(t, srv, nil, "--quoted-input", "set", `"Unterminated`).Failed()
			// a single arg that unquotes to two arguments is also not expected
			runCli(t, srv, nil, "--quoted-input", "set", `"arg1" "arg2"`).Failed()
		})

		t.Run("Read last argument from pipe", func(t *testing.T) {
			cmd := exec.Command("echo", "foo")
			out, err := cmd.StdoutPipe()
			require.NoError(t, err)
			require.NoError(t, cmd.Start())
			defer func() { require.NoError(t, cmd.Wait()) }()
			require.Equal(t, "OK", runCli(t, srv, out, "-x", "set", "key").Success())
			require.Equal(t, "foo\n", rdb.Get(ctx, "key").Val())
		})

		t.Run("Read last argument from file", func(t *testing.T) {
			f, err := os.CreateTemp("", "")
			require.NoError(t, err)
			defer func() { require.NoError(t, f.Close()) }()
			_, err = f.WriteString("from file")
			require.NoError(t, err)
			require.NoError(t, f.Sync())
			_, err = f.Seek(0, io.SeekStart)
			require.NoError(t, err)
			require.Equal(t, "OK", runCli(t, srv, f, "-x", "set", "key").Success())
			require.Equal(t, "from file", rdb.Get(ctx, "key").Val())
		})
	})

	t.Run("Scan mode", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "key:", 10, 1)

		// basic use
		r := runCli(t, srv, nil, "--scan").Success()
		require.Len(t, strings.Split(r, "\n"), 10)
	})

	t.Run("Bigkeys", func(t *testing.T) {
		runCli(t, srv, nil, "--bigkeys").Success()
	})

	t.Run("Memkeys", func(t *testing.T) {
		runCli(t, srv, nil, "--memkeys").Success()
	})

	formatArgs := func(args ...string) string {
		cmd := fmt.Sprintf("*%d\r\n", len(args))
		for _, arg := range args {
			cmd = cmd + fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
		}
		return cmd
	}

	t.Run("Piping raw protocol", func(t *testing.T) {
		f, err := os.CreateTemp("", "")
		require.NoError(t, err)
		defer func() { require.NoError(t, f.Close()) }()
		cmd := formatArgs("select", "9")
		cmd += formatArgs("del", "test-counter")
		for i := 0; i < 1000; i++ {
			cmd += formatArgs("incr", "test-counter")
			cmd += formatArgs("set", "large-key", strings.Repeat("x", 20000))
		}
		for i := 0; i < 100; i++ {
			cmd += formatArgs("set", "very-large-key", strings.Repeat("x", 512000))
		}
		_, err = f.WriteString(cmd)
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		_, err = f.Seek(0, io.SeekStart)
		require.NoError(t, err)

		r := runCli(t, srv, f, "--pipe").Success()
		require.Equal(t, "1000", rdb.Get(ctx, "test-counter").Val())
		require.Regexp(t, "(?s).*All data transferred.*errors: 0.*replies: 2102.*", r)
	})
}
