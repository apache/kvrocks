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

package debug

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestDebugProtocolV2(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "no",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("debug protocol type", func(t *testing.T) {
		types := map[string]interface{}{
			"string":  "Hello World",
			"integer": int64(12345),
			"array":   []interface{}{int64(0), int64(1), int64(2)},
			"true":    int64(1),
			"false":   int64(0),
		}
		for typ, expectedValue := range types {
			r := rdb.Do(ctx, "DEBUG", "PROTOCOL", typ)
			require.NoError(t, r.Err())
			require.EqualValues(t, expectedValue, r.Val())
		}
	})

	t.Run("lua script return value type", func(t *testing.T) {
		var returnValueScript = redis.NewScript(`
			return ARGV[1]
		`)
		val, err := returnValueScript.Run(ctx, rdb, []string{}, true).Int()
		require.NoError(t, err)
		require.EqualValues(t, 1, val)
		val, err = returnValueScript.Run(ctx, rdb, []string{}, false).Int()
		require.NoError(t, err)
		require.EqualValues(t, 0, val)
	})
}

func TestDebugProtocolV3(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": "yes",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("debug protocol type", func(t *testing.T) {
		types := map[string]interface{}{
			"string":  "Hello World",
			"integer": int64(12345),
			"array":   []interface{}{int64(0), int64(1), int64(2)},
			"true":    true,
			"false":   false,
		}
		for typ, expectedValue := range types {
			r := rdb.Do(ctx, "DEBUG", "PROTOCOL", typ)
			require.NoError(t, r.Err())
			require.EqualValues(t, expectedValue, r.Val())
		}
	})

	t.Run("lua script return value type", func(t *testing.T) {
		var returnValueScript = redis.NewScript(`
			return ARGV[1]
		`)
		val, err := returnValueScript.Run(ctx, rdb, []string{}, true).Bool()
		require.NoError(t, err)
		require.EqualValues(t, true, val)
		val, err = returnValueScript.Run(ctx, rdb, []string{}, false).Bool()
		require.NoError(t, err)
		require.EqualValues(t, false, val)
	})
}
