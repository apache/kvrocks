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

package multi

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestMulti(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("MUTLI / EXEC basics", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "a").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "b").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "c").Err())

		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		v1 := rdb.Do(ctx, "LRANGE", "mylist", 0, -1).Val()
		v2 := rdb.Do(ctx, "PING").Val()
		v3 := rdb.Do(ctx, "EXEC").Val()
		require.Equal(t, "QUEUED", fmt.Sprintf("%v", v1))
		require.Equal(t, "QUEUED", fmt.Sprintf("%v", v2))
		require.Equal(t, "[[a b c] PONG]", fmt.Sprintf("%v", v3))
	})

	t.Run("DISCARD", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "a").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "b").Err())
		require.NoError(t, rdb.RPush(ctx, "mylist", "c").Err())

		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		v1 := rdb.Do(ctx, "DEL", "mylist").Val()
		v2 := rdb.Do(ctx, "DISCARD").Val()
		require.Equal(t, "QUEUED", fmt.Sprintf("%v", v1))
		require.Equal(t, "OK", fmt.Sprintf("%v", v2))

		require.Equal(t, []string{"a", "b", "c"}, rdb.LRange(ctx, "mylist", 0, -1).Val())
	})

	t.Run("Nested MULTI are not allowed", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.EqualError(t, rdb.Do(ctx, "MULTI").Err(), "ERR MULTI calls can not be nested")
		require.NoError(t, rdb.Do(ctx, "EXEC").Err())
	})

	t.Run("MULTI where commands alter argc/argv", func(t *testing.T) {
		require.NoError(t, rdb.SAdd(ctx, "myset", "a").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SPOP", "myset").Err())
		v := rdb.Do(ctx, "EXEC").Val()
		require.Equal(t, "[a]", fmt.Sprintf("%v", v))
		require.Zero(t, rdb.Exists(ctx, "myset").Val())
	})

	t.Run("EXEC fails if there are errors while queueing commands #1", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo1", "foo2").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo1", "bar1").Err())
		require.Error(t, rdb.Do(ctx, "non-existing-command").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo2", "bar2").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
		require.Zero(t, rdb.Exists(ctx, "foo1").Val())
		require.Zero(t, rdb.Exists(ctx, "foo2").Val())
	})

	t.Run("If EXEC aborts, the client MULTI state is cleared", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo1", "foo2").Err())
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo1", "bar1").Err())
		require.Error(t, rdb.Do(ctx, "non-existing-command").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo2", "bar2").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
		require.Equal(t, "PONG", rdb.Ping(ctx).Val())
	})

	t.Run("Blocking commands ignores the timeout", func(t *testing.T) {
		v1 := rdb.Do(ctx, "MULTI").Val()
		require.NoError(t, rdb.Do(ctx, "BLPOP", "empty_list", "0").Err())
		require.NoError(t, rdb.Do(ctx, "BRPOP", "empty_list", "0").Err())
		v2 := rdb.Do(ctx, "EXEC").Val().([]interface{})

		require.Equal(t, "OK", fmt.Sprintf("%v", v1))
		require.Len(t, v2, 2)
		require.Nil(t, v2[0])
		require.Nil(t, v2[1])
	})

	t.Run("If EXEC aborts if commands are not allowed in MULTI-EXEC", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "MULTI").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo1", "bar1").Err())
		require.Error(t, rdb.Do(ctx, "monitor").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo2", "bar2").Err())
		require.EqualError(t, rdb.Do(ctx, "EXEC").Err(), "EXECABORT Transaction discarded")
		require.Equal(t, "PONG", rdb.Ping(ctx).Val())
	})

	func() {
		newSrv := util.StartServer(t, map[string]string{})
		defer newSrv.Close()

		t.Run("MULTI-EXEC used in redis-sentinel for failover", func(t *testing.T) {
			{
				rdb := newSrv.NewClient()
				defer func() { require.NoError(t, rdb.Close()) }()
				require.NoError(t, rdb.Do(ctx, "MULTI").Err())
				util.SlaveOf(t, rdb, srv)
				require.NoError(t, rdb.ConfigRewrite(ctx).Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "PUBSUB").Err())
				require.NoError(t, rdb.Do(ctx, "EXEC").Err())
				require.Equal(t, "slave", util.FindInfoEntry(rdb, "role"))
			}

			{
				rdb := newSrv.NewClient()
				defer func() { require.NoError(t, rdb.Close()) }()
				require.NoError(t, rdb.Do(ctx, "MULTI").Err())
				require.NoError(t, rdb.SlaveOf(ctx, "NO", "ONE").Err())
				require.NoError(t, rdb.ConfigRewrite(ctx).Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err())
				require.NoError(t, rdb.Do(ctx, "CLIENT", "KILL", "TYPE", "PUBSUB").Err())
				require.NoError(t, rdb.Do(ctx, "EXEC").Err())
				require.Equal(t, "master", util.FindInfoEntry(rdb, "role"))
			}
		})
	}()
}
