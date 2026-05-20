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

package bloom

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestCuckooFilter(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Add creates filter", func(t *testing.T) {
		key := "test_cuckoo_filter_add_create"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "cf.add", key, "item").Val())
		require.Equal(t, "MBbloomCF", rdb.Type(ctx, key).Val())
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err(), "already exists")
	})

	t.Run("Wrong type", func(t *testing.T) {
		key := "test_cuckoo_filter_wrong_type"
		require.NoError(t, rdb.Set(ctx, key, "value", 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.add", key, "item").Err(), "WRONGTYPE")
	})

	t.Run("Reserve expansion", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "cf.reserve", "test_cuckoo_filter_expansion_256", "1000", "EXPANSION", "256").Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", "test_cuckoo_filter_expansion_max", "1000", "EXPANSION", "32768").Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "test_cuckoo_filter_expansion_too_large", "1000", "EXPANSION", "32769").Err(), "expansion must be between 0 and 32768")
	})

	t.Run("Reserve creates cuckoo filter type", func(t *testing.T) {
		key := "test_cuckoo_filter_reserve_type"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		require.Equal(t, "MBbloomCF", rdb.Type(ctx, key).Val())
	})
}
