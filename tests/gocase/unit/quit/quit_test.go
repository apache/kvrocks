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

package quit

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestPipeQuit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()

	t.Run("QUIT returns OK", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()

		require.NoError(t, c.WriteArgs("QUIT"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		require.NoError(t, c.WriteArgs("PING"))
		r, err = c.ReadLine()
		require.Error(t, err)
		require.Empty(t, r)
	})

	t.Run("Pipelined commands after QUIT must not be executed", func(t *testing.T) {
		rdb1 := srv.NewClient()
		defer func() { require.NoError(t, rdb1.Close()) }()
		pipe := rdb1.Pipeline()
		cmd1 := pipe.Do(ctx, "quit")
		cmd2 := pipe.Set(ctx, "foo", "bar", 0)
		_, err := pipe.Exec(ctx)
		require.Error(t, err)
		require.Equal(t, "OK", cmd1.Val())
		require.Empty(t, cmd2.Val())
		require.EqualError(t, cmd2.Err(), "EOF")
		rdb2 := srv.NewClient()
		defer func() { require.NoError(t, rdb2.Close()) }()
		cmd3 := rdb2.Get(ctx, "foo")
		require.Equal(t, "", cmd3.Val())
	})

	t.Run("Pipelined commands after QUIT that exceed read buffer size", func(t *testing.T) {
		rdb1 := srv.NewClient()
		defer func() { require.NoError(t, rdb1.Close()) }()
		pipe := rdb1.Pipeline()
		cmd1 := pipe.Do(ctx, "quit")
		cmd2 := pipe.Set(ctx, "foo", strings.Repeat("x", 1024), 0)
		_, err := pipe.Exec(ctx)
		require.Error(t, err)
		require.Equal(t, "OK", cmd1.Val())
		require.Empty(t, cmd2.Val())
		require.EqualError(t, cmd2.Err(), "EOF")
		rdb2 := srv.NewClient()
		defer func() { require.NoError(t, rdb2.Close()) }()
		cmd3 := rdb2.Get(ctx, "foo")
		require.Equal(t, "", cmd3.Val())
	})
}
