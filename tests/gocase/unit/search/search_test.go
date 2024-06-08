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

package search

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSearch(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("FT.CREATE", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "FT.CREATE", "testidx1", "ON", "JSON", "PREFIX", "1", "test1:", "SCHEMA", "a", "TAG", "b", "NUMERIC").Err())

		verify := func(t *testing.T) {
			require.Equal(t, []interface{}{"testidx1"}, rdb.Do(ctx, "FT._LIST").Val())
			infoRes := rdb.Do(ctx, "FT.INFO", "testidx1")
			require.NoError(t, infoRes.Err())
			idxInfo := infoRes.Val().([]interface{})
			require.Equal(t, "index_name", idxInfo[0])
			require.Equal(t, "testidx1", idxInfo[1])
			require.Equal(t, "on_data_type", idxInfo[2])
			require.Equal(t, "ReJSON-RL", idxInfo[3])
			require.Equal(t, "prefixes", idxInfo[4])
			require.Equal(t, []interface{}{"test1:"}, idxInfo[5])
			require.Equal(t, "fields", idxInfo[6])
			require.Equal(t, []interface{}{"a", "tag"}, idxInfo[7].([]interface{})[0])
			require.Equal(t, []interface{}{"b", "numeric"}, idxInfo[7].([]interface{})[1])
		}
		verify(t)

		srv.Restart()
		verify(t)
	})

	t.Run("FT.SEARCH", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test1:k1", "$", `{"a": "x,y", "b": 11}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test1:k2", "$", `{"a": "x,z", "b": 22}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test1:k3", "$", `{"a": "y,z", "b": 33}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test2:k4", "$", `{"a": "x,y,z", "b": 44}`).Err())

		verify := func(t *testing.T, res *redis.Cmd) {
			require.NoError(t, res.Err())
			require.Equal(t, 7, len(res.Val().([]interface{})))
			require.Equal(t, int64(3), res.Val().([]interface{})[0])
			require.Equal(t, "test1:k1", res.Val().([]interface{})[1])
			require.Equal(t, "test1:k2", res.Val().([]interface{})[3])
			require.Equal(t, "test1:k3", res.Val().([]interface{})[5])
		}

		res := rdb.Do(ctx, "FT.SEARCHSQL", "select * from testidx1")
		verify(t, res)
		res = rdb.Do(ctx, "FT.SEARCH", "testidx1", "*")
		verify(t, res)

		verify = func(t *testing.T, res *redis.Cmd) {
			require.NoError(t, res.Err())
			require.Equal(t, 3, len(res.Val().([]interface{})))
			require.Equal(t, int64(1), res.Val().([]interface{})[0])
			require.Equal(t, "test1:k2", res.Val().([]interface{})[1])
			fields := res.Val().([]interface{})[2].([]interface{})
			if fields[0] == "a" {
				require.Equal(t, "x,z", fields[1])
				require.Equal(t, "b", fields[2])
				require.Equal(t, "22", fields[3])
			} else if fields[0] == "b" {
				require.Equal(t, "22", fields[1])
				require.Equal(t, "a", fields[2])
				require.Equal(t, "x,z", fields[3])
			} else {
				require.Fail(t, "not started with a or b")
			}
		}

		res = rdb.Do(ctx, "FT.SEARCHSQL", `select * from testidx1 where a hastag "z" and b < 30`)
		verify(t, res)
		res = rdb.Do(ctx, "FT.SEARCH", "testidx1", `@a:{z} @b:[-inf (30]`)
		verify(t, res)
	})

	t.Run("FT.DROPINDEX", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "FT.DROPINDEX", "testidx1").Err())

		verify := func(t *testing.T) {
			require.Equal(t, []interface{}{}, rdb.Do(ctx, "FT._LIST").Val())
			infoRes := rdb.Do(ctx, "FT.INFO", "testidx1")
			require.Equal(t, "ERR index not found", infoRes.Err().Error())
		}
		verify(t)

		srv.Restart()
		verify(t)
	})
}
