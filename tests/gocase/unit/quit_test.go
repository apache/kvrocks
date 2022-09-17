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
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func reconnect(srv *util.KvrocksServer, rdb **redis.Client) error {
	if err := (*rdb).Close(); err != nil {
		return err
	}
	*rdb = srv.NewClient()
	return nil
}

func TestPipeQuit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	rdb := srv.NewClient()
	ctx := context.Background()

	t.Run("QUIT returns OK", func(t *testing.T) {
		require.NoError(t, reconnect(srv, &rdb))
		pipe := rdb.Pipeline()
		cmd1 := pipe.Do(ctx, "quit")
		cmd2 := pipe.Do(ctx, "ping")
		_, err := pipe.Exec(ctx)
		require.Equal(t, "OK", cmd1.Val())
		require.Equal(t, nil, cmd2.Val())
		require.Error(t, err)
	})

	t.Run("Pipelined commands after QUIT must not be executed", func(t *testing.T) {
		// reconnect
		require.NoError(t, reconnect(srv, &rdb))
		pipe := rdb.Pipeline()
		cmd1 := pipe.Do(ctx, "quit")
		cmd2 := pipe.Do(ctx, "set", "foo", "bar")
		_, err := pipe.Exec(ctx)
		require.Equal(t, "OK", cmd1.Val())
		require.Equal(t, nil, cmd2.Val())
		require.Error(t, err)
		require.NoError(t, reconnect(srv, &rdb))
		cmd3 := rdb.Get(ctx, "foo")
		require.Equal(t, "", cmd3.Val())
	})

	t.Run("Pipelined commands after QUIT that exceed read buffer size", func(t *testing.T) {
		require.NoError(t, reconnect(srv, &rdb))
		pipe := rdb.Pipeline()
		cmd1 := pipe.Do(ctx, "quit")
		cmd2 := pipe.Do(ctx, "set", "foo", strings.Repeat("x", 1024))
		_, err := pipe.Exec(ctx)
		require.Equal(t, "OK", cmd1.Val())
		require.Equal(t, nil, cmd2.Val())
		require.Error(t, err)
		require.NoError(t, reconnect(srv, &rdb))
		cmd3 := rdb.Get(ctx, "foo")
		require.Equal(t, "", cmd3.Val())
	})

	require.NoError(t, rdb.Close())
}
