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

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestDelPrefix(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6666", // Update this if Kvrocks runs on a different port
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
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6666", // Update this if Kvrocks runs on a different port
	})
	defer client.Close()

	// Simulate cluster mode by adding keys with hash prefixes
	client.Set(ctx, "{hash1}test:1", "value1", 0)
	client.Set(ctx, "{hash1}test:2", "value2", 0)
	client.Set(ctx, "{hash2}other:1", "value3", 0)

	// Run the DELPREFIX command
	res, err := client.Do(ctx, "DELPREFIX", "{hash1}test:").Int()
	assert.NoError(t, err)
	assert.Equal(t, 2, res, "Expected to delete 2 keys with hash prefix")

	// Ensure the prefixed keys are deleted, but others remain
	existsTest1, _ := client.Exists(ctx, "{hash1}test:1").Result()
	existsTest2, _ := client.Exists(ctx, "{hash1}test:2").Result()
	existsOther1, _ := client.Exists(ctx, "{hash2}other:1").Result()

	assert.Equal(t, int64(0), existsTest1, "{hash1}test:1 should be deleted")
	assert.Equal(t, int64(0), existsTest2, "{hash1}test:2 should be deleted")
	assert.Equal(t, int64(1), existsOther1, "{hash2}other:1 should still exist")
}

func TestDelPrefixDisabledInClusterMode(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6666", // Update this if Kvrocks runs on a different port
	})
	defer client.Close()

	// Simulate cluster mode by adding keys with hash prefixes
	client.Set(ctx, "{hash1}test:1", "value1", 0)
	client.Set(ctx, "{hash1}test:2", "value2", 0)

	// Assume DELPREFIX command is disabled in cluster mode, expect an error
	_, err := client.Do(ctx, "DELPREFIX", "{hash1}test:").Result()
	assert.Error(t, err, "Expected error when running DELPREFIX in cluster mode")
}
