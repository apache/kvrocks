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

package digest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func TestDigest(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("DIGEST with existing string key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "Hello world", 0).Err())

		digest := rdb.Do(ctx, "DIGEST", "key1").Val().(string)
		require.Equal(t, "b6acb9d84a38ff74", digest)
	})

	t.Run("DIGEST with non-existent key", func(t *testing.T) {
		digest := rdb.Do(ctx, "DIGEST", "nonexistent").Val()
		require.Nil(t, digest)
	})

	t.Run("DIGEST with different string values produces different hashes", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "Hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key2", "World", 0).Err())

		digest1 := rdb.Do(ctx, "DIGEST", "key1").Val().(string)
		digest2 := rdb.Do(ctx, "DIGEST", "key2").Val().(string)

		require.NotEqual(t, digest1, digest2)
	})

	t.Run("DIGEST with same string value produces same hash", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "consistent", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key2", "consistent", 0).Err())

		digest1 := rdb.Do(ctx, "DIGEST", "key1").Val().(string)
		digest2 := rdb.Do(ctx, "DIGEST", "key2").Val().(string)

		require.Equal(t, digest1, digest2)
	})

	t.Run("DIGEST with empty string", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "empty", "", 0).Err())

		digest := rdb.Do(ctx, "DIGEST", "empty").Val().(string)
		require.NotEmpty(t, digest)
		require.Len(t, digest, 16)
	})

	t.Run("DIGEST with binary data", func(t *testing.T) {
		binaryData := "\x00\x01\x02\xff\xfe\xfd"
		require.NoError(t, rdb.Set(ctx, "binary", binaryData, 0).Err())

		digest := rdb.Do(ctx, "DIGEST", "binary").Val().(string)
		require.NotEmpty(t, digest)
		require.Len(t, digest, 16)
	})

	t.Run("DIGEST with large string", func(t *testing.T) {
		largeString := make([]byte, 10240)
		for i := range largeString {
			largeString[i] = byte(i % 256)
		}

		require.NoError(t, rdb.Set(ctx, "large", string(largeString), 0).Err())

		digest := rdb.Do(ctx, "DIGEST", "large").Val().(string)
		require.NotEmpty(t, digest)
		require.Len(t, digest, 16)
	})

	t.Run("DIGEST wrong number of arguments", func(t *testing.T) {
		err := rdb.Do(ctx, "DIGEST").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "wrong number of arguments")

		err = rdb.Do(ctx, "DIGEST", "key1", "extra").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "wrong number of arguments")
	})

	t.Run("DIGEST with wrong key type should fail", func(t *testing.T) {
		require.NoError(t, rdb.LPush(ctx, "list_key", "value").Err())

		err := rdb.Do(ctx, "DIGEST", "list_key").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "WRONGTYPE")
	})
}

func TestDigestCompatibility(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	testCases := []struct {
		name     string
		value    string
		expected string
	}{
		{"simple string", "hello", "9555e8555c62dcfd"},
		{"number as string", "123", "404a763b3f4c8c9a"},
		{"special chars", "!@#$%^&*()", "b492113f83b73532"},
		{"unicode", "こんにちは", "37267692105b8cbf"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, rdb.Set(ctx, "test_key", tc.value, 0).Err())

			digest := rdb.Do(ctx, "DIGEST", "test_key").Val().(string)
			require.Equal(t, tc.expected, digest)
		})
	}
}
