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

package cli

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util" // Utilize common utilities
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelPrefix(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6666", // Ensure this matches the Kvrocks port
	})
	defer client.Close()

	// Set up test keys
	client.Set(ctx, "test:1", "value1", 0)
	client.Set(ctx, "test:2", "value2", 0)
	client.Set(ctx, "other:1", "value3", 0)

	// Run the DELPREFIX command
	res, err := client.Do(ctx, "DELPREFIX", "test:").Int()
	assert.NoError(t, err)
	assert.Equal(t, 2, res, "Expected to delete 2 keys")

	// Ensure the prefixed keys are deleted, but others remain
	existsTest1, _ := client.Exists(ctx, "test:1").Result()
	existsTest2, _ := client.Exists(ctx, "test:2").Result()
	existsOther1, _ := client.Exists(ctx, "other:1").Result()

	assert.Equal(t, int64(0), existsTest1, "test:1 should be deleted")
	assert.Equal(t, int64(0), existsTest2, "test:2 should be deleted")
	assert.Equal(t, int64(1), existsOther1, "other:1 should still exist")
}

func TestDelPrefixClusterMode(t *testing.T) {
	t.Parallel()
	// Start server with cluster mode enabled
	srv := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer srv.Close()

	ctx := context.Background()
	client := srv.NewClient()
	defer func() { require.NoError(t, client.Close()) }()

	// Simulate cluster mode by adding keys with hash prefixes
	client.Set(ctx, "{hash1}test:1", "value1", 0)
	client.Set(ctx, "{hash1}test:2", "value2", 0)
	client.Set(ctx, "{hash2}other:1", "value3", 0)

	// Run the DELPREFIX command (expect it to be disabled)
	_, err := client.Do(ctx, "DELPREFIX", "{hash1}test:").Result()
	require.ErrorContains(t, err, "disabled in cluster mode")

	// Ensure no keys are deleted
	existsTest1, _ := client.Exists(ctx, "{hash1}test:1").Result()
	existsTest2, _ := client.Exists(ctx, "{hash1}test:2").Result()
	existsOther1, _ := client.Exists(ctx, "{hash2}other:1").Result()

	assert.Equal(t, int64(1), existsTest1, "{hash1}test:1 should still exist")
	assert.Equal(t, int64(1), existsTest2, "{hash1}test:2 should still exist")
	assert.Equal(t, int64(1), existsOther1, "{hash2}other:1 should still exist")
}

func TestDelPrefixDisabledInClusterMode(t *testing.T) {
	t.Parallel()
	// Start server with cluster mode enabled
	srv := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer srv.Close()

	ctx := context.Background()
	client := srv.NewClient()
	defer func() { require.NoError(t, client.Close()) }()

	// Simulate cluster mode by adding keys with hash prefixes
	client.Set(ctx, "{hash1}test:1", "value1", 0)
	client.Set(ctx, "{hash1}test:2", "value2", 0)

	// Assume DELPREFIX command is disabled in cluster mode, expect an error
	_, err := client.Do(ctx, "DELPREFIX", "{hash1}test:").Result()
	require.ErrorContains(t, err, "disabled in cluster mode")
}
