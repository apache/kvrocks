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

package config

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestReplicaofConfig(t *testing.T) {
	t.Parallel()
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("valid host and port", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "127.0.0.1 1234").Err())
		val := rdb.ConfigGet(ctx, "replicaof").Val()
		require.EqualValues(t, "127.0.0.1 1234", val["replicaof"])
	})

	t.Run("no one clears master config", func(t *testing.T) {
		// First set a master
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "127.0.0.1 1234").Err())
		val := rdb.ConfigGet(ctx, "replicaof").Val()
		require.EqualValues(t, "127.0.0.1 1234", val["replicaof"])

		// Clear with "no one"
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "no one").Err())
		val = rdb.ConfigGet(ctx, "replicaof").Val()
		require.EqualValues(t, "no one", val["replicaof"])
	})

	t.Run("NO ONE (uppercase) also clears master config", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "127.0.0.1 1234").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "NO ONE").Err())
		val := rdb.ConfigGet(ctx, "replicaof").Val()
		require.EqualValues(t, "NO ONE", val["replicaof"])
	})

	t.Run("mixed case No One also clears master config", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "127.0.0.1 1234").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "replicaof", "No One").Err())
		val := rdb.ConfigGet(ctx, "replicaof").Val()
		require.EqualValues(t, "No One", val["replicaof"])
	})

	t.Run("invalid: keyword no with numeric port", func(t *testing.T) {
		err := rdb.ConfigSet(ctx, "replicaof", "no 1234").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("invalid: uppercase NO with numeric port", func(t *testing.T) {
		err := rdb.ConfigSet(ctx, "replicaof", "NO 1234").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("invalid: non-no host with keyword one", func(t *testing.T) {
		err := rdb.ConfigSet(ctx, "replicaof", "foo one").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("invalid: non-no host with uppercase ONE", func(t *testing.T) {
		err := rdb.ConfigSet(ctx, "replicaof", "foo ONE").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid")
	})

	t.Run("invalid: typo in no", func(t *testing.T) {
		err := rdb.ConfigSet(ctx, "replicaof", "noo one").Err()
		require.Error(t, err)
	})

	t.Run("invalid: typo in one", func(t *testing.T) {
		err := rdb.ConfigSet(ctx, "replicaof", "no onee").Err()
		require.Error(t, err)
	})
}
