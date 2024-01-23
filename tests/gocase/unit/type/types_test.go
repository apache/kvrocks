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

package types

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestTypesError(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Operate with wrong type", func(t *testing.T) {
		message := "ERR Invalid argument: WRONGTYPE Operation against a key holding the wrong kind of value"
		require.NoError(t, rdb.Set(ctx, "a", "hello", 0).Err())
		require.EqualError(t, rdb.Do(ctx, "XADD", "a", "*", "a", "test").Err(), message)
		require.EqualError(t, rdb.Do(ctx, "LPUSH", "a", 1).Err(), message)
		require.EqualError(t, rdb.Do(ctx, "HSET", "a", "1", "2").Err(), message)
		require.EqualError(t, rdb.Do(ctx, "SADD", "a", "1", "2").Err(), message)
		require.EqualError(t, rdb.Do(ctx, "ZADD", "a", "1", "2").Err(), message)
		require.EqualError(t, rdb.Do(ctx, "JSON.SET", "a", "$", "{}").Err(), message)
		require.EqualError(t, rdb.Do(ctx, "BF.ADD", "a", "test").Err(), message)
		require.EqualError(t, rdb.Do(ctx, "SADD", "a", 100).Err(), message)

		require.NoError(t, rdb.LPush(ctx, "a1", "hello", 0).Err())
		require.EqualError(t, rdb.Do(ctx, "SETBIT", "a1", 1, 1).Err(), message)
		require.EqualError(t, rdb.Do(ctx, "GET", "a1").Err(), message)

	})
}
