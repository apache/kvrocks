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

package command

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("acquire GET command info by COMMAND INFO", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "INFO", "GET")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
		require.Equal(t, "get", v[0])
		require.EqualValues(t, 2, v[1])
		require.Equal(t, []interface{}{"readonly"}, v[2])
		require.EqualValues(t, 1, v[3])
		require.EqualValues(t, 1, v[4])
		require.EqualValues(t, 1, v[5])
	})

	t.Run("command entry length check", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND")
		vs, err := r.Slice()
		require.NoError(t, err)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
	})

	t.Run("get keys of commands by COMMAND GETKEYS", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "GET", "test")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Equal(t, "test", vs[0])
	})

    t.Run("COMMAND GETKEYS SINTERCARD", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "SINTERCARD", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZINTER", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZINTER", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZINTERSTORE", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZINTERSTORE", "dst", "2", "src1", "src2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "src1", vs[0])
        require.Equal(t, "src2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZINTERCARD", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZINTERCARD", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZUNION", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZUNION", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZUNIONSTORE", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZUNIONSTORE", "dst", "2", "src1", "src2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "src1", vs[0])
        require.Equal(t, "src2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZDIFF", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZDIFF", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZDIFFSTORE", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZDIFFSTORE", "dst", "2", "src1", "src2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "src1", vs[0])
        require.Equal(t, "src2", vs[1])
    })

    t.Run("COMMAND GETKEYS ZMPOP", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "ZMPOP", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS BZMPOP", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "BZMPOP", "0", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS LMPOP", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "LMPOP", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })

    t.Run("COMMAND GETKEYS BLMPOP", func(t *testing.T) {
        r := rdb.Do(ctx, "COMMAND", "GETKEYS", "BLMPOP", "0", "2", "key1", "key2")
        vs, err := r.Slice()
        require.NoError(t, err)
        require.Len(t, vs, 2)
        require.Equal(t, "key1", vs[0])
        require.Equal(t, "key2", vs[1])
    })
}
