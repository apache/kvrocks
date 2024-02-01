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

package reset

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestReset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("reset with ongoing txn", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "30", 0).Err())
		require.NoError(t, rdb.Do(ctx, "multi").Err())
		require.NoError(t, rdb.Set(ctx, "x", "40", 0).Err())
		require.NoError(t, rdb.Do(ctx, "reset").Err())

		v1 := rdb.Do(ctx, "get", "x").Val()
		require.Equal(t, "30", fmt.Sprintf("%v", v1))
	})

	t.Run("unwatch keys", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", 30, 0).Err())
		require.NoError(t, rdb.Do(ctx, "watch", "x").Err())
		require.NoError(t, rdb.Do(ctx, "multi").Err())
		require.NoError(t, rdb.Do(ctx, "ping").Err())
		require.NoError(t, rdb.Do(ctx, "reset").Err())

		require.NoError(t, rdb.Set(ctx, "x", 40, 0).Err())
		require.NoError(t, rdb.Do(ctx, "multi").Err())
		require.NoError(t, rdb.Do(ctx, "ping").Err())
		require.Equal(t, rdb.Do(ctx, "exec").Val(), []interface{}{"PONG"})
	})

	t.Run("unsub and punsub", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "subscribe", "chan1").Err())
		require.NoError(t, rdb.Do(ctx, "reset").Err())
		require.Equal(t, rdb.Do(ctx, "subscribe", "chan2").Val(), []interface{}{"subscribe", "chan2", (int64)(1)})
	})
}
