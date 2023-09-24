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

package namespace

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestNamespace(t *testing.T) {
	password := "pwd"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Basic operation", func(t *testing.T) {
		nsTokens := map[string]string{
			"ns1": "token1",
			"ns2": "token2",
			"ns3": "token3",
			"ns4": "token4",
		}
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
		// duplicate add the same namespace
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, "new"+token)
			util.ErrorRegexp(t, r.Err(), ".*ERR the namespace already exists.*")
		}
		for ns, token := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "GET", ns)
			require.NoError(t, r.Err())
			require.Equal(t, token, r.Val())
		}
		for ns := range nsTokens {
			r := rdb.Do(ctx, "NAMESPACE", "DEL", ns)
			require.NoError(t, r.Err())
			require.Equal(t, "OK", r.Val())
		}
	})

	t.Run("Concurrent creating namespaces", func(t *testing.T) {
		threads := 4
		countPerThread := 10

		var wg sync.WaitGroup
		var nsTokens sync.Map
		for i := 0; i < threads; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				for j := 0; j < countPerThread; j++ {
					ns := "ns" + util.RandString(16, 16, util.Alpha)
					token := util.RandString(16, 16, util.Alpha)
					nsTokens.Store(ns, token)
					r := rdb.Do(ctx, "NAMESPACE", "ADD", ns, token)
					require.NoError(t, r.Err())
					require.Equal(t, "OK", r.Val())
				}
			}(i)
		}
		wg.Wait()

		nsTokens.Range(func(key, value interface{}) bool {
			r := rdb.Do(ctx, "NAMESPACE", "GET", key)
			require.NoError(t, r.Err())
			require.Equal(t, value, r.Val())
			return true
		})
	})
}
