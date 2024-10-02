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
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func SetBinaryBuffer(buf *bytes.Buffer, vec []float64) error {
	buf.Reset()

	for _, v := range vec {
		if err := binary.Write(buf, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	return nil
}

func TestSearch(t *testing.T) {
	t.Skip("search commands is disabled")

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("FT.CREATE", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "FT.CREATE", "testidx1", "ON", "JSON", "PREFIX", "1", "test1:", "SCHEMA", "a", "TAG", "b", "NUMERIC",
			"c", "VECTOR", "HNSW", "6", "TYPE", "FLOAT64", "DIM", "3", "DISTANCE_METRIC", "L2").Err())

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
			require.Equal(t, []interface{}{"c", "vector"}, idxInfo[7].([]interface{})[2])
		}
		verify(t)

		srv.Restart()
		verify(t)

		require.NoError(t, rdb.Do(ctx, "FT.CREATE", "testidx2", "SCHEMA", "x", "NUMERIC").Err())
		require.NoError(t, rdb.Do(ctx, "FT.DROPINDEX", "testidx2").Err())
	})

	t.Run("FT.SEARCH", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test1:k1", "$", `{"a": "x,y", "b": 11, "c": [2,3,4]}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test1:k2", "$", `{"a": "x,z", "b": 22, "c": [12,13,14]}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test1:k3", "$", `{"a": "y,z", "b": 33, "c": [23,24,25]}`).Err())
		require.NoError(t, rdb.Do(ctx, "JSON.SET", "test2:k4", "$", `{"a": "x,y,z", "b": 44, "c": [33,34,35]}`).Err())

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
			fieldMap := make(map[string]string)
			for i := 0; i < len(fields); i += 2 {
				fieldMap[fields[i].(string)] = fields[i+1].(string)
			}

			_, aExists := fieldMap["a"]
			_, bExists := fieldMap["b"]
			_, cExists := fieldMap["c"]

			require.True(t, aExists, "'a' should exist in the result")
			require.True(t, bExists, "'b' should exist in the result")
			require.True(t, cExists, "'c' should exist in the result")

			require.Equal(t, "x,z", fieldMap["a"])
			require.Equal(t, "22", fieldMap["b"])
			require.Equal(t, "12.000000, 13.000000, 14.000000", fieldMap["c"])
		}

		res = rdb.Do(ctx, "FT.SEARCHSQL", `select * from testidx1 where a hastag "z" and b < 30`)
		verify(t, res)
		res = rdb.Do(ctx, "FT.SEARCH", "testidx1", `@a:{z} @b:[-inf (30]`)
		verify(t, res)
		res = rdb.Do(ctx, "FT.SEARCHSQL", `select * from testidx1 order by c <-> [13,14,15] limit 1`)
		verify(t, res)
		res = rdb.Do(ctx, "FT.SEARCHSQL", `select * from testidx1 where c <-> [16,17,18] < 7`)
		verify(t, res)
		res = rdb.Do(ctx, "FT.SEARCHSQL", `select * from testidx1 where a hastag "z" and c <-> [2,3,4] < 18`)
		verify(t, res)

		var buf bytes.Buffer

		vec := []float64{13, 14, 15}
		require.NoError(t, SetBinaryBuffer(&buf, vec), "Failed to set binary buffer")
		vecBinary := buf.Bytes()
		res = rdb.Do(ctx, "FT.SEARCH", "testidx1", `*=>[KNN 1 @c $BLOB]`, "PARAMS", "2", "BLOB", vecBinary)
		verify(t, res)

		vec = []float64{16, 17, 18}
		require.NoError(t, SetBinaryBuffer(&buf, vec), "Failed to set binary buffer")
		vecBinary = buf.Bytes()
		res = rdb.Do(ctx, "FT.SEARCH", "testidx1", `@c:[VECTOR_RANGE 7 $BLOB]`, "PARAMS", "2", "BLOB", vecBinary)
		verify(t, res)

		vec = []float64{2, 3, 4}
		require.NoError(t, SetBinaryBuffer(&buf, vec), "Failed to set binary buffer")
		vecBinary = buf.Bytes()
		res = rdb.Do(ctx, "FT.SEARCH", "testidx1", `@a:{z} @c:[VECTOR_RANGE 18 $BLOB]`, "PARAMS", "2", "BLOB", vecBinary)
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
