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
	"fmt"
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

	t.Run("Reserve invalid capacity", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_cap_str", "abc").Err(), "invalid capacity")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_cap_neg", "-1").Err(), "invalid capacity")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_cap_zero", "0").Err(), "capacity must be larger than 0")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_cap_one", "1").Err(), "capacity must be at least 2")
	})

	t.Run("Reserve invalid bucket size", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_bs_str", "1000", "BUCKETSIZE", "abc").Err(), "invalid bucket size")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_bs_zero", "1000", "BUCKETSIZE", "0").Err(), "bucket size must be between 1 and 255")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_bs_neg", "1000", "BUCKETSIZE", "-1").Err(), "invalid bucket size")
	})

	t.Run("Reserve invalid max iterations", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_mi_str", "1000", "MAXITERATIONS", "abc").Err(), "invalid max iterations")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_mi_zero", "1000", "MAXITERATIONS", "0").Err(), "max iterations must be larger than 0")
	})

	t.Run("Reserve invalid expansion", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_exp_str", "1000", "EXPANSION", "abc").Err(), "invalid expansion factor")
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_exp_big", "1000", "EXPANSION", "32769").Err(), "expansion must be between 0 and 32768")
	})

	t.Run("Reserve invalid syntax", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", "cf_bad_syntax", "1000", "UNKNOWNOPT").Err(), "syntax error")
	})

	t.Run("Reserve duplicate key", func(t *testing.T) {
		key := "test_cuckoo_filter_reserve_dup"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.reserve", key, "2000").Err(), "already exists")
	})

	t.Run("Reserve wrong number of arguments", func(t *testing.T) {
		require.Error(t, rdb.Do(ctx, "cf.reserve").Err())
		require.Error(t, rdb.Do(ctx, "cf.reserve", "key_only").Err())
	})

	t.Run("Add wrong number of arguments", func(t *testing.T) {
		require.Error(t, rdb.Do(ctx, "cf.add").Err())
		require.Error(t, rdb.Do(ctx, "cf.add", "key_only").Err())
		require.Error(t, rdb.Do(ctx, "cf.add", "key", "item1", "item2").Err())
	})

	t.Run("Add many items", func(t *testing.T) {
		key := "test_cuckoo_filter_add_many"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		for i := 0; i < 500; i++ {
			result := rdb.Do(ctx, "cf.add", key, fmt.Sprintf("item_%d", i))
			require.NoError(t, result.Err())
			require.Equal(t, int64(1), result.Val())
		}
	})

	t.Run("Add duplicate items allowed", func(t *testing.T) {
		key := "test_cuckoo_filter_add_dup"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		for i := 0; i < 10; i++ {
			result := rdb.Do(ctx, "cf.add", key, "same_item")
			require.NoError(t, result.Err())
			require.Equal(t, int64(1), result.Val())
		}
	})

	t.Run("Add triggers expansion", func(t *testing.T) {
		key := "test_cuckoo_filter_add_expansion"
		require.NoError(t, rdb.Del(ctx, key).Err())
		// Small capacity with expansion enabled to trigger expansion quickly
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "4", "BUCKETSIZE", "1", "MAXITERATIONS", "1", "EXPANSION", "2").Err())
		// Add enough items to trigger expansion
		for i := 0; i < 20; i++ {
			result := rdb.Do(ctx, "cf.add", key, fmt.Sprintf("expand_item_%d", i))
			require.NoError(t, result.Err())
			require.Equal(t, int64(1), result.Val())
		}
	})

	t.Run("Add to full non-scaling filter returns error", func(t *testing.T) {
		key := "test_cuckoo_filter_full_nonscaling"
		require.NoError(t, rdb.Del(ctx, key).Err())
		// expansion=0 disables scaling; small capacity fills quickly
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "4", "BUCKETSIZE", "1", "MAXITERATIONS", "1", "EXPANSION", "0").Err())
		full := false
		for i := 0; i < 100; i++ {
			result := rdb.Do(ctx, "cf.add", key, fmt.Sprintf("full_item_%d", i))
			if result.Err() != nil {
				require.ErrorContains(t, result.Err(), "filter is full")
				full = true
				break
			}
		}
		require.True(t, full, "Non-scaling filter should eventually become full")
	})

	t.Run("Reserve with all optional params", func(t *testing.T) {
		key := "test_cuckoo_filter_all_opts"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "5000", "BUCKETSIZE", "8", "MAXITERATIONS", "100", "EXPANSION", "4").Err())
		require.Equal(t, "MBbloomCF", rdb.Type(ctx, key).Val())
	})

	t.Run("Add empty string item", func(t *testing.T) {
		key := "test_cuckoo_filter_empty_item"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "100").Err())
		result := rdb.Do(ctx, "cf.add", key, "")
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("Add large item", func(t *testing.T) {
		key := "test_cuckoo_filter_large_item"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "100").Err())
		largeItem := make([]byte, 10000)
		for i := range largeItem {
			largeItem[i] = byte('a' + i%26)
		}
		result := rdb.Do(ctx, "cf.add", key, string(largeItem))
		require.NoError(t, result.Err())
		require.Equal(t, int64(1), result.Val())
	})

	t.Run("Info of no exists key", func(t *testing.T) {
		key := "no_exist_key"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.info", key).Err(), "filter is not found")
	})

	t.Run("Info empty filter", func(t *testing.T) {
		key := "test_cf_info_empty"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1024").Err())

		all := rdb.Do(ctx, "cf.info", key).Val().([]interface{})
		require.Equal(t, []interface{}{
			"Size", int64(2048),
			"Number of buckets", int64(1024),
			"Number of filters", int64(1),
			"Number of items inserted", int64(0),
			"Number of items deleted", int64(0),
			"Bucket size", int64(2),
			"Expansion rate", int64(1),
			"Max iterations", int64(20),
		}, all)
	})

	t.Run("Info wrong type key", func(t *testing.T) {
		key := "test_cf_info_wrong_type"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Set(ctx, key, "value", 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.info", key).Err(), "WRONGTYPE")
	})

	t.Run("Info invalid argument", func(t *testing.T) {
		key := "test_cf_info_invalid_arg"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.info", key, "invalid").Err(), "Invalid info argument")
	})

	t.Run("Info items after add", func(t *testing.T) {
		key := "test_cf_info_items_add"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "cf.info", key, "items").Val().([]interface{})[1])
		require.NoError(t, rdb.Do(ctx, "cf.add", key, "item1").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "cf.info", key, "items").Val().([]interface{})[1])
		require.NoError(t, rdb.Do(ctx, "cf.add", key, "item2").Err())
		require.Equal(t, int64(2), rdb.Do(ctx, "cf.info", key, "items").Val().([]interface{})[1])
	})

	t.Run("Info duplicate items increments", func(t *testing.T) {
		key := "test_cf_info_items_dup"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "1000").Err())
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "cf.add", key, "dup_item").Err())
		}
		items := rdb.Do(ctx, "cf.info", key, "items").Val().([]interface{})
		require.Equal(t, int64(3), items[1])
		require.Equal(t, int64(0), items[3])
	})

	t.Run("Info filters after expansion", func(t *testing.T) {
		key := "test_cf_info_expansion"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "4", "BUCKETSIZE", "1", "MAXITERATIONS", "1", "EXPANSION", "2").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "cf.info", key, "filters").Val())
		for i := 0; i < 10; i++ {
			require.NoError(t, rdb.Do(ctx, "cf.add", key, fmt.Sprintf("expand_%d", i)).Err())
		}
		require.Greater(t, rdb.Do(ctx, "cf.info", key, "filters").Val(), int64(1))
	})

	t.Run("Info individual fields", func(t *testing.T) {
		key := "test_cf_info_fields"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "500", "BUCKETSIZE", "4", "MAXITERATIONS", "20", "EXPANSION", "0").Err())
		require.Equal(t, int64(1024), rdb.Do(ctx, "cf.info", key, "size").Val())
		require.Equal(t, int64(256), rdb.Do(ctx, "cf.info", key, "buckets").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "cf.info", key, "filters").Val())
		require.Equal(t, int64(0), rdb.Do(ctx, "cf.info", key, "items").Val().([]interface{})[1])
		require.Equal(t, int64(4), rdb.Do(ctx, "cf.info", key, "bucket_size").Val())
		require.Equal(t, interface{}(nil), rdb.Do(ctx, "cf.info", key, "expansion").Val())
		require.Equal(t, int64(20), rdb.Do(ctx, "cf.info", key, "max_iterations").Val())
	})

	t.Run("Count no exists key returns zero", func(t *testing.T) {
		key := "no_exist_key_count"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "cf.count", key, "item").Val())
	})

	t.Run("Count empty filter", func(t *testing.T) {
		key := "test_cf_count_empty"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "100").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "cf.count", key, "item").Val())
	})

	t.Run("Count after add", func(t *testing.T) {
		key := "test_cf_count_after_add"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "100").Err())
		require.NoError(t, rdb.Do(ctx, "cf.add", key, "my_item").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "cf.count", key, "my_item").Val())
	})

	t.Run("Count duplicate items", func(t *testing.T) {
		key := "test_cf_count_dup"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "100").Err())
		for i := 0; i < 5; i++ {
			require.NoError(t, rdb.Do(ctx, "cf.add", key, "same_item").Err())
		}
		require.Equal(t, int64(5), rdb.Do(ctx, "cf.count", key, "same_item").Val())
	})

	t.Run("Count non-existent item", func(t *testing.T) {
		key := "test_cf_count_not_exist_item"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cf.reserve", key, "100").Err())
		require.NoError(t, rdb.Do(ctx, "cf.add", key, "item1").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "cf.count", key, "item2").Val())
	})

	t.Run("Count wrong type key", func(t *testing.T) {
		key := "test_cf_count_wrong_type"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Set(ctx, key, "value", 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "cf.count", key, "item").Err(), "WRONGTYPE")
	})

	t.Run("Count wrong number of arguments", func(t *testing.T) {
		require.Error(t, rdb.Do(ctx, "cf.count").Err())
		require.Error(t, rdb.Do(ctx, "cf.count", "key_only").Err())
		require.Error(t, rdb.Do(ctx, "cf.count", "key", "item1", "item2").Err())
	})
}
