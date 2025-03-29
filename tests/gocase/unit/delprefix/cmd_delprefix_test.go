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
 *
 */

package deleteprefix

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var instance *util.KvrocksServer

func setup(t *testing.T) *redis.Client {
	instance = util.StartServer(t, map[string]string{})

	// Initialize client with authentication if needed
	client := instance.NewClientWithOption(&redis.Options{
		Addr: instance.HostPort(),
	})
	require.Eventually(t, func() bool {
		err := client.Ping(context.Background()).Err()
		return err == nil || err.Error() == "NOAUTH Authentication required."
	}, time.Minute, time.Second)

	return client
}

func teardown() {
	if instance != nil {
		instance.Close()
	}
}

func TestDelPrefix(t *testing.T) {
	client := setup(t)
	defer teardown()

	// Test cases
	t.Run("DELPREFIX_ALL", func(t *testing.T) {
		require.NoError(t, client.Set(context.Background(), "test:key1", "value1", 0).Err())
		require.NoError(t, client.Set(context.Background(), "test:key2", "value2", 0).Err())

		_, err := client.Do(context.Background(), "DELPREFIX", "test").Result()
		require.NoError(t, err)
	})

	t.Run("DELPREFIX_BY_PREFIX", func(t *testing.T) {
		require.NoError(t, client.Set(context.Background(), "sample:key1", "value1", 0).Err())
		require.NoError(t, client.Set(context.Background(), "sample:key2", "value2", 0).Err())

		_, err := client.Do(context.Background(), "DELPREFIX", "sample").Result()
		require.NoError(t, err)
	})

	t.Run("Delprefix_reject_invalid_input", func(t *testing.T) {
		_, err := client.Do(context.Background(), "DELPREFIX").Result()
		require.Error(t, err)
	})
}
