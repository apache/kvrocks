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

package protocol

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestProtocolNetwork(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("empty bulk array command", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\n"))
		require.NoError(t, c.Write("*0\r\n"))
		require.NoError(t, c.Write("\r\n"))
		require.NoError(t, c.Write("*1\r\n$4\r\nPING\r\n"))
		c.MustRead(t, "+PONG")
	})

	t.Run("empty inline command", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write(" \r\n"))
		require.NoError(t, c.Write("*1\r\n$4\r\nPING\r\n"))
		c.MustRead(t, "+PONG")
	})

	t.Run("out of range multibulk length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*20000000\r\n"))
		c.MustMatch(t, "invalid multibulk length")
	})

	t.Run("wrong multibulk payload header", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfoo\r\n"))
		c.MustMatch(t, "expected '\\$'")
	})

	t.Run("negative multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n"))
		c.MustMatch(t, "invalid bulk length")
	})

	t.Run("out of range multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n"))
		c.MustMatch(t, "invalid bulk length")
	})

	t.Run("non-number multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$foo\r\n"))
		c.MustMatch(t, "invalid bulk length")
	})

	t.Run("multibulk request not followed by bulk arguments", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*1\r\nfoo\r\n"))
		c.MustMatch(t, "expected '\\$'")
	})

	t.Run("generic wrong number of args", func(t *testing.T) {
		rdb := srv.NewClient()
		defer func() { require.NoError(t, rdb.Close()) }()
		v := rdb.Do(context.Background(), "ping", "x", "y")
		require.EqualError(t, v.Err(), "ERR wrong number of arguments")
	})

	t.Run("empty array parsed", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		c.MustRead(t, "+OK")
	})

	t.Run("allow only LF protocol separator", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("set foo 123\n"))
		c.MustRead(t, "+OK")
	})

	t.Run("mix LF/CRLF protocol separator", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\nset foo 123\nget foo\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		for _, res := range []string{"+OK", "$3", "123", "+OK"} {
			c.MustRead(t, res)
		}
	})

	t.Run("invalid LF in multi bulk protocol", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		c.MustMatch(t, "invalid multibulk length")
	})

	t.Run("command type should return the simple string", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("set foo bar\n"))
		c.MustRead(t, "+OK")
		require.NoError(t, c.Write("type foo\n"))
		c.MustRead(t, "+string")
	})
}

func TestProtocolRESP2(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "no",
	})
	defer srv.Close()

	c := srv.NewTCPClient()
	defer func() {
		require.NoError(t, c.Close())
	}()

	t.Run("debug protocol string", func(t *testing.T) {
		types := map[string][]string{
			"string":  {"$11", "Hello World"},
			"integer": {":12345"},
			"array":   {"*3", ":0", ":1", ":2"},
			"set":     {"*3", ":0", ":1", ":2"},
			"true":    {":1"},
			"false":   {":0"},
			"null":    {"$-1"},
		}
		for typ, expected := range types {
			args := []string{"DEBUG", "PROTOCOL", typ}
			require.NoError(t, c.WriteArgs(args...))
			for _, line := range expected {
				c.MustRead(t, line)
			}
		}
	})

	t.Run("multi bulk strings with null string", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("HSET", "hash", "f1", "v1"))
		c.MustRead(t, ":1")

		require.NoError(t, c.WriteArgs("HMGET", "hash", "f1", "f2"))
		c.MustRead(t, "*2")
		c.MustRead(t, "$2")
		c.MustRead(t, "v1")
		c.MustRead(t, "$-1")
	})

	t.Run("null array", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("ZRANK", "no-exists-zset", "m0", "WITHSCORE"))
		c.MustRead(t, "*-1")
	})
}

func TestProtocolRESP3(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "yes",
	})
	defer srv.Close()

	c := srv.NewTCPClient()
	defer func() {
		require.NoError(t, c.Close())
	}()

	t.Run("debug protocol string", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("HELLO", "3"))
		values := []string{"*6", "$6", "server", "$5", "redis", "$5", "proto", ":3", "$4", "mode", "$10", "standalone"}
		for _, line := range values {
			c.MustRead(t, line)
		}

		types := map[string][]string{
			"string":  {"$11", "Hello World"},
			"integer": {":12345"},
			"array":   {"*3", ":0", ":1", ":2"},
			"set":     {"~3", ":0", ":1", ":2"},
			"true":    {"#t"},
			"false":   {"#f"},
			"null":    {"_"},
		}
		for typ, expected := range types {
			args := []string{"DEBUG", "PROTOCOL", typ}
			require.NoError(t, c.WriteArgs(args...))
			for _, line := range expected {
				c.MustRead(t, line)
			}
		}
	})

	t.Run("multi bulk strings with null", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("HSET", "hash", "f1", "v1"))
		c.MustRead(t, ":1")

		require.NoError(t, c.WriteArgs("HMGET", "hash", "f1", "f2"))
		c.MustRead(t, "*2")
		c.MustRead(t, "$2")
		c.MustRead(t, "v1")
		c.MustRead(t, "_")
	})

	t.Run("null array", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("ZRANK", "no-exists-zset", "m0", "WITHSCORE"))
		c.MustRead(t, "_")
	})
}
