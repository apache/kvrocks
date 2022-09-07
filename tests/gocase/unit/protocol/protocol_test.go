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

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestProtocolNetwork(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	t.Run("handle an empty array", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("\r\n"))
		require.NoError(t, c.Write("*1\r\n$4\r\nPING\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+PONG", r)
	})

	t.Run("out of range multibulk length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*20000000\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid multibulk length")
	})

	t.Run("wrong multibulk payload header", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\nfoo\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "expected '$'")
	})

	t.Run("negative multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$-10\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid bulk length")
	})

	t.Run("out of range multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$2000000000\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid bulk length")
	})

	t.Run("non-number multibulk payload length", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\r\n$3\r\nSET\r\n$1\r\nx\r\n$foo\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid bulk length")
	})

	t.Run("multibulk request not followed by bulk arguments", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*1\r\nfoo\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "expected '$'")
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
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, r, "+OK")
	})

	t.Run("allow only LF protocol separator", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("set foo 123\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, r, "+OK")
	})

	t.Run("mix LF/CRLF protocol separator", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*-1\r\nset foo 123\nget foo\r\n*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		for _, res := range []string{"+OK", "$3", "123", "+OK"} {
			r, err := c.ReadLine()
			require.NoError(t, err)
			require.Equal(t, res, r)
		}
	})

	t.Run("invalid LF in multi bulk protocol", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*3\n$3\r\nset\r\n$3\r\nkey\r\n$3\r\nval\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid multibulk length")
	})
}
