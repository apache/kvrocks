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

package string

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
		
		// DIGEST should return hex hash
		digest := rdb.Do(ctx, "DIGEST", "key1").String()
		require.Equal(t, "b6acb9d84a38ff74", digest)
	})

	t.Run("DIGEST with non-existent key", func(t *testing.T) {
		// DIGEST should return nil for non-existent key
		digest := rdb.Do(ctx, "DIGEST", "nonexistent").Val()
		require.Nil(t, digest)
	})

	t.Run("DIGEST with different string values produces different hashes", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "Hello", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key2", "World", 0).Err())
		
		digest1 := rdb.Do(ctx, "DIGEST", "key1").String()
		digest2 := rdb.Do(ctx, "DIGEST", "key2").String()
		
		require.NotEqual(t, digest1, digest2)
	})

	t.Run("DIGEST with same string value produces same hash", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "key1", "consistent", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key2", "consistent", 0).Err())
		
		digest1 := rdb.Do(ctx, "DIGEST", "key1").String()
		digest2 := rdb.Do(ctx, "DIGEST", "key2").String()
		
		require.Equal(t, digest1, digest2)
	})

	t.Run("DIGEST with empty string", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "empty", "", 0).Err())
		
		digest := rdb.Do(ctx, "DIGEST", "empty").String()
		require.NotEmpty(t, digest)
		
		// Should still return a valid hex string
		require.Len(t, digest, 16)
	})

	t.Run("DIGEST with binary data", func(t *testing.T) {
		binaryData := "\x00\x01\x02\xff\xfe\xfd"
		require.NoError(t, rdb.Set(ctx, "binary", binaryData, 0).Err())
		
		digest := rdb.Do(ctx, "DIGEST", "binary").String()
		require.NotEmpty(t, digest)
		
		require.Len(t, digest, 16)
	})

	t.Run("DIGEST with large string", func(t *testing.T) {
		// Create a large string
		largeString := make([]byte, 10240)
		for i := range largeString {
			largeString[i] = byte(i % 256)
		}
		
		require.NoError(t, rdb.Set(ctx, "large", string(largeString), 0).Err())
		
		digest := rdb.Do(ctx, "DIGEST", "large").String()
		require.NotEmpty(t, digest)
		
		require.Len(t, digest, 16)
	})

	t.Run("DIGEST wrong number of arguments", func(t *testing.T) {
		// Too few arguments
		err := rdb.Do(ctx, "DIGEST").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "wrong number of arguments")

		// Too many arguments  
		err = rdb.Do(ctx, "DIGEST", "key1", "extra").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "wrong number of arguments")
	})

	t.Run("DIGEST with wrong key type should fail", func(t *testing.T) {
		// Set up a non-string key
		require.NoError(t, rdb.LPush(ctx, "list_key", "value").Err())
		
		// DIGEST should fail on non-string keys
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

	// Test compatibility with Redis command syntax
	testCases := []struct {
		name     string
		value    string
		expected string
	}{
		{"simple string", "hello", ""},
		{"number as string", "123", ""},
		{"special chars", "!@#$%^&*()", ""},
		{"unicode", "こんにちは", ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, rdb.Set(ctx, "test_key", tc.value, 0).Err())
			
			digest := rdb.Do(ctx, "DIGEST", "test_key").String()
			require.NotEmpty(t, digest)
			
			require.Len(t, digest, 16)
			
			if tc.expected != "" {
				require.Equal(t, tc.expected, digest)
			}
		})
	}
}

