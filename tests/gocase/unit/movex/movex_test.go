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

package movex

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestMoveDummy(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Dummy move", func(t *testing.T) {
		r := rdb.Move(ctx, "key1", 1)
		require.NoError(t, r.Err())
		require.Equal(t, false, r.Val())

		require.NoError(t, rdb.Del(ctx, "key1").Err())
		require.NoError(t, rdb.Set(ctx, "key1", "value1", 0).Err())
		r = rdb.Move(ctx, "key1", 1)
		require.NoError(t, r.Err())
		require.Equal(t, true, r.Val())
	})
}

func TestMoveXWithoutPwd(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("MoveX without password", func(t *testing.T) {
		r := rdb.Do(ctx, "MOVEX", "key1", "ns1", "token1")
		require.EqualError(t, r.Err(), "ERR Forbidden to move key when requirepass is empty")
	})
}

func TestMoveX(t *testing.T) {
	token := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": token,
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClientWithOption(&redis.Options{
		Password: token,
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("MoveX test", func(t *testing.T) {
		nsTokens := map[string]string{
			"ns1": "token1",
			"ns2": "token2",
			"ns3": "token3",
		}
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.NoError(t, r.Err())
			require.Equal(t, token, r.Val())
		}

		// add 3 kvs to default namespace
		require.NoError(t, rdb.Del(ctx, "key1", "key2", "key3").Err())
		require.NoError(t, rdb.Set(ctx, "key1", "value1", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key2", "value2", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key3", "value3", 0).Err())
		require.EqualValues(t, "value1", rdb.Get(ctx, "key1").Val())
		require.EqualValues(t, "value2", rdb.Get(ctx, "key2").Val())
		require.EqualValues(t, "value3", rdb.Get(ctx, "key3").Val())

		// move key1 to ns1
		r := rdb.Do(ctx, "MOVEX", "key1", "ns1", "token1")
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(1), r.Val())
		require.EqualValues(t, "", rdb.Get(ctx, "key1").Val())
		require.NoError(t, rdb.Do(ctx, "AUTH", "token1").Err())
		require.EqualValues(t, "value1", rdb.Get(ctx, "key1").Val())
		require.NoError(t, rdb.Do(ctx, "AUTH", token).Err())

		// move key2 to ns2, with wrong token first
		r = rdb.Do(ctx, "MOVEX", "key2", "ns2", "token1")
		require.EqualError(t, r.Err(), "ERR Incorrect namespace or token")
		r = rdb.Do(ctx, "MOVEX", "key2", "ns2", "token2")
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(1), r.Val())
		require.EqualValues(t, "", rdb.Get(ctx, "key2").Val())
		require.NoError(t, rdb.Do(ctx, "AUTH", "token2").Err())
		require.EqualValues(t, "value2", rdb.Get(ctx, "key2").Val())
		require.NoError(t, rdb.Do(ctx, "AUTH", token).Err())

		// move non-existent keys
		r = rdb.Do(ctx, "MOVEX", "key2", "ns2", "token2")
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(0), r.Val())

		// move key that exists in the target namespace
		require.NoError(t, rdb.Set(ctx, "key1", "value4", 0).Err())
		r = rdb.Do(ctx, "MOVEX", "key1", "ns1", "token1")
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(0), r.Val())

		// move key3 to ns3, and move back
		r = rdb.Do(ctx, "MOVEX", "key3", "ns3", "token3")
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(1), r.Val())
		require.EqualValues(t, "", rdb.Get(ctx, "key3").Val())
		require.NoError(t, rdb.Do(ctx, "AUTH", "token3").Err())
		require.EqualValues(t, "value3", rdb.Get(ctx, "key3").Val())
		r = rdb.Do(ctx, "MOVEX", "key3", "__namespace", token)
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(1), r.Val())
		require.EqualValues(t, "", rdb.Get(ctx, "key3").Val())
		require.NoError(t, rdb.Do(ctx, "AUTH", token).Err())
		require.EqualValues(t, "value3", rdb.Get(ctx, "key3").Val())

		// move in place
		require.NoError(t, rdb.Do(ctx, "AUTH", "token1").Err())
		require.EqualValues(t, "value1", rdb.Get(ctx, "key1").Val())
		r = rdb.Do(ctx, "MOVEX", "key1", "ns1", "token1")
		require.NoError(t, r.Err())
		require.EqualValues(t, int64(0), r.Val())
	})
}
