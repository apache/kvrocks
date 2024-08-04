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

package hyperloglog

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestHyperLogLog(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("basic add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, card)
		addCnt, err := rdb.PFAdd(ctx, "hll", "foo").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)
		card, err = rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, card)
	})

	t.Run("duplicate add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, card)
		addCnt, err := rdb.PFAdd(ctx, "hll", "foo").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)
		addCnt, err = rdb.PFAdd(ctx, "hll", "foo").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, addCnt)
	})

	t.Run("empty add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		addCnt, err := rdb.PFAdd(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, addCnt)

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, card)
	})

	t.Run("multiple add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		addCnt, err := rdb.PFAdd(ctx, "hll", "a", "b", "c", "d").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		addCnt, err = rdb.PFAdd(ctx, "hll", "a", "b", "c").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, addCnt)

		addCnt, err = rdb.PFAdd(ctx, "hll", "a", "f", "c").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err = rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 5, card)
	})
}
