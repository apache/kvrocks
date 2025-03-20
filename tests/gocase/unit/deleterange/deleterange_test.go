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

package deleterange

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestDeleteRange(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	DeleteRangeTest(t, rdb, ctx)
}

func DeleteRangeTest(t *testing.T, rdb *redis.Client, ctx context.Context) {
	t.Run("DELETERANGE ALL", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		util.Populate(t, rdb, "key:", 1000, 10)
		require.NoError(t, rdb.Do(ctx, "deleterange", "*").Err())
		keys := scanAll(t, rdb)
		require.Len(t, keys, 0)
	})

	t.Run("DELETERANGE BY PERFERIX", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())

		for _, key := range []string{"aa", "aab", "aabb", "ab", "abb", "ba", "cc", "cd", "dd"} {
			require.NoError(t, rdb.Set(ctx, key, "hello", 0).Err())
		}
		deleterange(t, rdb, "aa*")
		keys := scanAll(t, rdb)
		require.Equal(t, []string{"ab", "abb", "ba", "cc", "cd", "dd"}, keys)

		deleterange(t, rdb, "c*")
		keys = scanAll(t, rdb)
		require.Equal(t, []string{"ab", "abb", "ba", "dd"}, keys)

		deleterange(t, rdb, "d*")
		keys = scanAll(t, rdb)
		require.Equal(t, []string{"ab", "abb", "ba"}, keys)

		deleterange(t, rdb, "a*")
		keys = scanAll(t, rdb)
		require.Equal(t, []string{"ba"}, keys)

		deleterange(t, rdb, "*")
		keys = scanAll(t, rdb)
		require.Equal(t, []string(nil), keys)
	})

	t.Run("DELETERANGE with multi namespace", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.ConfigSet(ctx, "requirepass", "foobared").Err())

		tokens := []string{"test_ns_token1", "test_ns_token2"}
		keyPrefixes := []string{"key1*", "key2*"}
		namespaces := []string{"test_ns1", "test_ns2"}

		for i := 0; i < 2; i++ {
			require.NoError(t, rdb.Do(ctx, "AUTH", "foobared").Err())
			require.NoError(t, rdb.Do(ctx, "NAMESPACE", "ADD", namespaces[i], tokens[i]).Err())
			require.NoError(t, rdb.Do(ctx, "AUTH", tokens[i]).Err())

			for k := 0; k < 1000; k++ {
				require.NoError(t, rdb.Set(ctx, fmt.Sprintf("%s:%d", keyPrefixes[i], k), "hello", 0).Err())
			}
			for k := 0; k < 100; k++ {
				require.NoError(t, rdb.Set(ctx, strconv.Itoa(k), "hello", 0).Err())
			}
		}

		for i := 0; i < 2; i++ {
			require.NoError(t, rdb.Do(ctx, "AUTH", tokens[i]).Err())
			require.NoError(t, rdb.Do(ctx, "deleterange", keyPrefixes[i]).Err())

			keys := scanAll(t, rdb, "match", keyPrefixes[i])
			require.Len(t, keys, 0)

			keys = scanAll(t, rdb)
			require.Len(t, keys, 100)
		}
	})

	t.Run("Deleterange reject invalid input", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "hello").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "hel*o").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "*hello").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "[").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "\\").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "[a").Err(), ".*syntax error.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "DELETERANGE", "[a-]").Err(), ".*syntax error.*")
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

func deleterange(t testing.TB, rdb *redis.Client, c string, args ...interface{}) {
	args = append([]interface{}{"DELETERANGE", c}, args...)
	r := rdb.Do(context.Background(), args...)
	require.NoError(t, r.Err())
}

func scanAll(t testing.TB, rdb *redis.Client, args ...interface{}) (keys []string) {
	c := "0"
	for {
		cursor, keyList := scan(t, rdb, c, args...)

		c = cursor
		keys = append(keys, keyList...)

		if c == "0" {
			slices.Sort(keys)
			keys = slices.Compact(keys)
			return
		}
	}
}
