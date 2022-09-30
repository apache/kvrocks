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

package scan

import (
	"context"
	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
	"testing"
)

func TestScan(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SCAN Basic", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "", 1000, 10)
		keys := scanAll(t, rdb)
		slices.Compact(keys)
		require.Len(t, keys, 1000)
	})

	t.Run("SCAN COUNT", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "", 1000, 10)
		keys := scanAll(t, rdb, "count", 5)
		slices.Compact(keys)
		require.Len(t, keys, 1000)
	})

	t.Run("SCAN MATCH", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "key:", 1000, 10)
		keys := scanAll(t, rdb, "match", "key:*")
		slices.Compact(keys)
		require.Len(t, keys, 1000)
	})
}

func scan(t testing.TB, rdb *redis.Client, c string, args ...interface{}) (cursor string, keys []string) {
	args = append([]interface{}{"SCAN", c}, args...)
	r := rdb.Do(context.Background(), args...)
	require.NoError(t, r.Err())
	require.Len(t, r.Val(), 2)

	rs := r.Val().([]interface{})
	cursor = rs[0].(string)

	for _, key := range rs[1].([]interface{}) {
		keys = append(keys, key.(string))
	}

	return
}

func scanAll(t testing.TB, rdb *redis.Client, args ...interface{}) (keys []string) {
	c := "0"
	for {
		cursor, keyList := scan(t, rdb, c, args...)

		c = cursor
		keys = append(keys, keyList...)

		if c == "0" {
			return
		}
	}
}
