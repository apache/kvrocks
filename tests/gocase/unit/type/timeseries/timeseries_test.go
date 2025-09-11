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
package timeseries

import (
	"context"
	"math"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeSeries(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		testTimeSeries(t, configs)
	}
}

func testTimeSeries(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "test_ts_key"
	t.Run("TS.CREATE Basic Creation", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key, "retention", "3600", "chunk_size", "2048", "encoding", "uncompressed", "duplicate_policy", "last", "labels", "label1", "value1").Err())
	})

	t.Run("TS.CREATE Invalid RETENTION", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.create", key, "retention", "abc").Err(), "Couldn't parse RETENTION")
		require.ErrorContains(t, rdb.Do(ctx, "ts.create", key, "retention", "-100").Err(), "Couldn't parse RETENTION")
	})

	t.Run("TS.CREATE Invalid CHUNK_SIZE", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.create", key, "chunk_size", "abc").Err(), "invalid CHUNK_SIZE")
		require.ErrorContains(t, rdb.Do(ctx, "ts.create", key, "chunk_size", "-1024").Err(), "invalid CHUNK_SIZE")
	})

	t.Run("TS.CREATE Invalid ENCODING", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.create", key, "encoding", "invalid").Err(), "unknown ENCODING parameter")
	})

	t.Run("TS.CREATE Invalid DUPLICATE_POLICY", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.create", key, "duplicate_policy", "invalid").Err(), "Unknown DUPLICATE_POLICY")
	})

	// Test non-existent key
	t.Run("TS.INFO Non-Existent Key", func(t *testing.T) {
		_, err := rdb.Do(ctx, "ts.info", "test_info_key").Result()
		require.ErrorContains(t, err, "the key is not a TSDB key")
	})

	t.Run("TS.INFO Initial State", func(t *testing.T) {
		key := "test_info_key"
		// Create timeseries with custom options
		require.NoError(t, rdb.Do(ctx, "ts.create", key, "retention", "10", "chunk_size", "3",
			"labels", "k1", "v1", "k2", "v2").Err())
		vals, err := rdb.Do(ctx, "ts.info", key).Slice()
		require.NoError(t, err)
		require.Equal(t, 24, len(vals))

		// totalSamples = 0
		require.Equal(t, "totalSamples", vals[0])
		require.Equal(t, int64(0), vals[1])

		// memoryUsage = 0
		require.Equal(t, "memoryUsage", vals[2])
		require.Equal(t, int64(0), vals[3])

		// retentionTime = 10
		require.Equal(t, "retentionTime", vals[8])
		require.Equal(t, int64(10), vals[9])

		// chunkSize = 3
		require.Equal(t, "chunkSize", vals[12])
		require.Equal(t, int64(3), vals[13])

		// chunkType = uncompressed
		require.Equal(t, "chunkType", vals[14])
		require.Equal(t, "uncompressed", vals[15])

		// duplicatePolicy = block
		require.Equal(t, "duplicatePolicy", vals[16])
		require.Equal(t, "block", vals[17])

		// labels = [(k1,v1), (k2,v2)]
		require.Equal(t, "labels", vals[18])
		labels := vals[19].([]interface{})
		require.Equal(t, 2, len(labels))
		for i, expected := range [][]string{{"k1", "v1"}, {"k2", "v2"}} {
			pair := labels[i].([]interface{})
			require.Equal(t, expected[0], pair[0])
			require.Equal(t, expected[1], pair[1])
		}

		// sourceKey = nil
		require.Equal(t, "sourceKey", vals[20])
		require.Nil(t, []byte(nil), vals[21])

		// rules = empty array
		require.Equal(t, "rules", vals[22])
		require.Empty(t, vals[23])
	})

	t.Run("TS.INFO After Adding Data", func(t *testing.T) {
		key := "test_info_key"
		// Add samples
		require.NoError(t, rdb.Do(ctx, "ts.madd", key, "1", "10", key, "3", "10", key, "2", "20",
			key, "3", "20", key, "4", "20", key, "13", "20", key, "1", "20", key, "14", "20").Err())

		vals, err := rdb.Do(ctx, "ts.info", key).Slice()
		require.NoError(t, err)

		// totalSamples = 6
		require.Equal(t, "totalSamples", vals[0])
		require.Equal(t, int64(6), vals[1])

		// firstTimestamp = 4 (earliest after retention)
		require.Equal(t, "firstTimestamp", vals[4])
		require.Equal(t, int64(4), vals[5])

		// lastTimestamp = 14
		require.Equal(t, "lastTimestamp", vals[6])
		require.Equal(t, int64(14), vals[7])

		// chunkCount = 2
		require.Equal(t, "chunkCount", vals[10])
		require.Equal(t, int64(2), vals[11])
	})

	t.Run("TS.ADD Basic Add", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", key, "1000", "12.3").Val())
		require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", "autocreate", "1000", "12.3").Val())
	})

	t.Run("TS.ADD Invalid Timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.add", key, "abc", "12.3").Err(), "invalid timestamp")
		require.ErrorContains(t, rdb.Do(ctx, "ts.add", key, "-100", "12.3").Err(), "invalid timestamp")
	})

	t.Run("TS.ADD Invalid Value", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.add", key, "1000", "abc").Err(), "invalid value")
	})

	t.Run("TS.ADD Duplicate Policy Block", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key, "duplicate_policy", "block").Err())
		require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", key, "1000", "12.3").Val())
		require.ErrorContains(t, rdb.Do(ctx, "ts.add", key, "1000", "13.4").Err(), "update is not supported when DUPLICATE_POLICY is set to BLOCK mode")
	})

	t.Run("TS.ADD With Retention", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key, "retention", "1000").Err())
		currentTs := time.Now().UnixMilli()
		require.Equal(t, int64(currentTs), rdb.Do(ctx, "ts.add", key, strconv.FormatInt(currentTs, 10), "12.3").Val())
		oldTs := currentTs - 2000
		require.ErrorContains(t, rdb.Do(ctx, "ts.add", key, strconv.FormatInt(oldTs, 10), "12.3").Err(), "Timestamp is older than retention")
	})

	t.Run("TS.MADD Basic Test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.Equal(t, []interface{}{int64(1000), int64(2000)}, rdb.Do(ctx, "ts.madd", key, "1000", "12.3", key, "2000", "13.4").Val())
	})

	t.Run("TS.MADD Invalid Arguments", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.madd", key, "abc", "12.3").Err(), "invalid timestamp")
		require.ErrorContains(t, rdb.Do(ctx, "ts.madd", key, "1000", "12.3", "invalidkey").Err(), "wrong number of arguments")
	})

	t.Run("TS.MADD Duplicate Handling", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key, "duplicate_policy", "block").Err())
		require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", key, "1000", "12.3").Val())
		res := rdb.Do(ctx, "ts.madd", key, "1000", "13.4", key, "1000", "14.5").Val().([]interface{})
		assert.Contains(t, res[0], "update is not supported when DUPLICATE_POLICY is set to BLOCK mode")
		assert.Contains(t, res[1], "update is not supported when DUPLICATE_POLICY is set to BLOCK mode")
	})

	t.Run("TS.MADD Nonexistent Key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nonexistent").Err())
		require.NoError(t, rdb.Del(ctx, "existent").Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", "existent").Err())
		res := rdb.Do(ctx, "ts.madd", "nonexistent", "1000", "12.3", "existent", "1000", "13.4").Val().([]interface{})
		assert.Contains(t, res[0], "the key is not a TSDB key")
		assert.Equal(t, res[1], int64(1000))
	})

	t.Run("TS.RANGE Invalid Timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "abc", "1000").Err(), "wrong fromTimestamp")
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "1000", "xyz").Err(), "wrong toTimestamp")
	})

	t.Run("TS.RANGE No Data", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		res := rdb.Do(ctx, "ts.range", key, "-", "+").Val().([]interface{})
		assert.Empty(t, res)
	})

	t.Run("TS.RANGE Nonexistent Key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nonexistent").Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", "nonexistent", "-", "+").Err(), "key does not exist")
	})

	t.Run("TS.RANGE Invalid Aggregation Type", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "invalid", "1000").Err(), "Invalid aggregator type")
	})

	t.Run("TS.RANGE Invalid Aggregation Duration", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "avg", "0").Err(), "bucketDuration must be greater than zero")
	})

	t.Run("TS.RANGE Invalid Count", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "COUNT", "0").Err(), "Invalid COUNT value")
	})

	t.Run("TS.RANGE Invalid Align Parameter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "avg", "1000", "ALIGN", "invalid").Err(), "unknown ALIGN parameter")
	})

	t.Run("TS.RANGE Align Without Aggregation", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "ALIGN", "1000").Err(), "ALIGN parameter can only be used with AGGREGATION")
	})

	t.Run("TS.RANGE BucketTimestamp Without Aggregation", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "BUCKETTIMESTAMP", "START").Err(), "BUCKETTIMESTAMP flag should be the 3rd or 4th flag after AGGREGATION flag")
	})

	t.Run("TS.RANGE Empty Without Aggregation", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "-", "+", "EMPTY").Err(), "EMPTY flag should be the 3rd or 5th flag after AGGREGATION flag")
	})

	t.Run("TS.RANGE Comprehensive Test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key, "labels", "type", "stock", "name", "A").Err())

		// Add samples in three batches
		samples := []struct {
			ts  int64
			val float64
		}{
			{1000, 100}, {1010, 110}, {1020, 120},
			{2000, 200}, {2010, 210}, {2020, 220},
			{3000, 300}, {3010, 310}, {3020, 320},
		}
		for _, s := range samples {
			require.Equal(t, s.ts, rdb.Do(ctx, "ts.add", key, s.ts, s.val).Val())
		}

		// Test basic range without aggregation
		res := rdb.Do(ctx, "ts.range", key, "-", "+").Val().([]interface{})
		assert.Equal(t, len(samples), len(res))
		for i, s := range samples {
			arr := res[i].([]interface{})
			assert.Equal(t, s.ts, arr[0])
			assert.Equal(t, s.val, arr[1])
		}

		// Test MIN aggregation with 20ms bucket
		res = rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "MIN", 20).Val().([]interface{})
		assert.Equal(t, 6, len(res))
		expected := []struct {
			ts  int64
			val float64
		}{
			{1000, 100}, {1020, 120},
			{2000, 200}, {2020, 220},
			{3000, 300}, {3020, 320},
		}
		for i, exp := range expected {
			arr := res[i].([]interface{})
			assert.Equal(t, exp.ts, arr[0])
			assert.Equal(t, exp.val, arr[1])
		}

		// Test alignment with 10ms offset
		res = rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "MIN", 20, "ALIGN", 10).Val().([]interface{})
		assert.Equal(t, 6, len(res))
		expected = []struct {
			ts  int64
			val float64
		}{
			{990, 100}, {1010, 110},
			{1990, 200}, {2010, 210},
			{2990, 300}, {3010, 310},
		}
		for i, exp := range expected {
			arr := res[i].([]interface{})
			assert.Equal(t, exp.ts, arr[0])
			assert.Equal(t, exp.val, arr[1])
		}

		// Test mid bucket timestamp
		res = rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "MIN", 20, "ALIGN", 10, "BUCKETTIMESTAMP", "MID").Val().([]interface{})
		assert.Equal(t, 6, len(res))
		expected = []struct {
			ts  int64
			val float64
		}{
			{1000, 100}, {1020, 110},
			{2000, 200}, {2020, 210},
			{3000, 300}, {3020, 310},
		}
		for i, exp := range expected {
			arr := res[i].([]interface{})
			assert.Equal(t, exp.ts, arr[0])
			assert.Equal(t, exp.val, arr[1])
		}

		// Test empty buckets
		res = rdb.Do(ctx, "ts.range", key, 1500, 2500, "AGGREGATION", "MIN", 5, "EMPTY").Val().([]interface{})
		assert.Equal(t, 5, len(res))
		expected = []struct {
			ts  int64
			val float64
		}{
			{2000, 200}, {2005, 0},
			{2010, 210}, {2015, 0},
			{2020, 220},
		}
		for i, exp := range expected {
			arr := res[i].([]interface{})
			assert.Equal(t, exp.ts, arr[0])
			if i == 1 || i == 3 {
				assert.True(t, math.IsNaN(arr[1].(float64)))
			} else {
				assert.Equal(t, exp.val, arr[1])
			}
		}

		// Test value filtering
		res = rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "MIN", 20, "FILTER_BY_VALUE", 200, 300).Val().([]interface{})
		assert.Equal(t, 3, len(res))
		for _, arr := range res {
			val := arr.([]interface{})[1].(float64)
			assert.True(t, val >= 200 && val <= 300)
		}

		// Test ts filtering
		res = rdb.Do(ctx, "ts.range", key, "-", "+", "FILTER_BY_TS", "1000", "3000").Val().([]interface{})
		assert.Equal(t, 2, len(res))
		for _, arr := range res {
			ts := arr.([]interface{})[0].(int64)
			assert.True(t, ts == 1000 || ts == 3000)
		}

		// Test count limit
		res = rdb.Do(ctx, "ts.range", key, "-", "+", "AGGREGATION", "MIN", 20, "COUNT", 1).Val().([]interface{})
		assert.Equal(t, 1, len(res))
	})

	t.Run("TS.GET Basic", func(t *testing.T) {
		key := "test_get_key"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		// Test GET on empty timeseries
		res := rdb.Do(ctx, "ts.get", key).Val().([]interface{})
		require.Equal(t, 0, len(res))

		// Add samples
		require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", key, "1000", "12.3").Val())
		require.Equal(t, int64(2000), rdb.Do(ctx, "ts.add", key, "2000", "15.6").Val())

		// Test basic GET
		res = rdb.Do(ctx, "ts.get", key).Val().([]interface{})
		require.Equal(t, 1, len(res))
		require.Equal(t, int64(2000), res[0].([]interface{})[0])
		require.Equal(t, 15.6, res[0].([]interface{})[1])

		// Test GET on non-existent key
		_, err := rdb.Do(ctx, "ts.get", "nonexistent_key").Result()
		require.ErrorContains(t, err, "key does not exist")
	})

	t.Run("TS.CREATERULE Error Cases", func(t *testing.T) {
		srcKey := "error_src"
		dstKey := "error_dst"
		anotherKey := "another_dst"
		anotherSrc := "another_src"
		srcOfSrc := "src_of_src"

		// 1. Source key equals destination key
		t.Run("SourceEqualsDestination", func(t *testing.T) {
			_, err := rdb.Do(ctx, "ts.createrule", srcKey, srcKey, "aggregation", "avg", "1000").Result()
			assert.Contains(t, err, "the source key and destination key should be different")
		})

		// 2. Source key does not exist
		t.Run("SourceNotExists", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, srcKey).Err())
			_, err := rdb.Do(ctx, "ts.createrule", srcKey, dstKey, "aggregation", "avg", "1000").Result()
			assert.Contains(t, err, "the key is not a TSDB key")
		})

		// Create source key
		require.NoError(t, rdb.Do(ctx, "ts.create", srcKey).Err())

		// 3. Destination key does not exist
		t.Run("DestinationNotExists", func(t *testing.T) {
			require.NoError(t, rdb.Del(ctx, dstKey).Err())
			_, err := rdb.Do(ctx, "ts.createrule", srcKey, dstKey, "aggregation", "avg", "1000").Result()
			assert.Contains(t, err, "the key is not a TSDB key")
		})

		// Create destination key
		require.NoError(t, rdb.Do(ctx, "ts.create", dstKey).Err())

		// 4. Source key already has a source rule
		t.Run("SourceHasSourceRule", func(t *testing.T) {

			require.NoError(t, rdb.Do(ctx, "ts.create", srcOfSrc).Err())

			// Create a rule from srcOfSrc to srcKey
			require.NoError(t, rdb.Do(ctx, "ts.createrule", srcOfSrc, srcKey, "aggregation", "avg", "1000").Err())

			require.NoError(t, rdb.Do(ctx, "ts.create", anotherKey).Err())
			// Try to create rule from srcKey to anotherKey
			_, err := rdb.Do(ctx, "ts.createrule", srcKey, anotherKey, "aggregation", "avg", "1000").Result()
			assert.Contains(t, err, "the source key already has a source rule")
		})

		// 5. Destination key already has a source rule
		t.Run("DestinationHasSourceRule", func(t *testing.T) {
			require.NoError(t, rdb.Do(ctx, "ts.create", "src_for_dst").Err())

			// Create a rule from src_for_dst to dstKey
			require.NoError(t, rdb.Do(ctx, "ts.createrule", "src_for_dst", dstKey, "aggregation", "avg", "1000").Err())

			// Try to create rule from another_src to dstKey
			require.NoError(t, rdb.Do(ctx, "ts.create", anotherSrc).Err())
			_, err := rdb.Do(ctx, "ts.createrule", anotherSrc, dstKey, "aggregation", "avg", "1000").Result()
			assert.Contains(t, err, "the destination key already has a src rule")
		})

		// 6. Destination key already has downstream rules
		t.Run("DestinationHasDownstreamRules", func(t *testing.T) {
			// Create a rule from another_src to anotherKey
			require.NoError(t, rdb.Do(ctx, "ts.createrule", anotherSrc, anotherKey, "aggregation", "avg", "1000").Err())

			// Try to create rule from another_src to srcOfSrc
			_, err := rdb.Do(ctx, "ts.createrule", anotherSrc, srcOfSrc, "aggregation", "avg", "1000").Result()
			assert.Contains(t, err, "the destination key already has a dst rule")
		})
	})
	t.Run("TS.CREATERULE DownStream Write", func(t *testing.T) {
		test2 := "test2"
		test3 := "test3"

		// Create test2 with CHUNK_SIZE 3
		require.NoError(t, rdb.Do(ctx, "ts.create", test2, "CHUNK_SIZE", "3").Err())
		// Create test3
		require.NoError(t, rdb.Do(ctx, "ts.create", test3).Err())
		// Create rule with MIN aggregation
		require.NoError(t, rdb.Do(ctx, "ts.createrule", test2, test3, "aggregation", "min", "10").Err())

		// First batch of writes
		res := rdb.Do(ctx, "ts.madd", test2, "1", "1", test2, "2", "2", test2, "3", "6", test2, "5", "7", test2, "10", "11", test2, "11", "17").Val().([]interface{})
		assert.Equal(t, []interface{}{int64(1), int64(2), int64(3), int64(5), int64(10), int64(11)}, res)

		// Second batch of writes
		res = rdb.Do(ctx, "ts.madd", test2, "4", "-0.2", test2, "12", "55", test2, "20", "65").Val().([]interface{})
		assert.Equal(t, []interface{}{int64(4), int64(12), int64(20)}, res)

		// Verify test3 results
		vals := rdb.Do(ctx, "ts.range", test3, "-", "+").Val().([]interface{})
		require.Equal(t, 2, len(vals))
		assert.Equal(t, []interface{}{int64(0), -0.2}, vals[0])
		assert.Equal(t, []interface{}{int64(10), float64(11)}, vals[1])
	})

	t.Run("TS.MGET Filter Expression Parsing", func(t *testing.T) {
		// Clean up existing keys
		require.NoError(t, rdb.Del(ctx, "temp:TLV", "temp:JLM").Err())

		// Create the time series with labels as in the example
		require.NoError(t, rdb.Do(ctx, "ts.create", "temp:TLV", "LABELS", "type", "temp", "location", "TLV").Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", "temp:JLM", "LABELS", "type", "temp", "location", "JLM").Err())

		// Add a sample to each time series
		require.NoError(t, rdb.Do(ctx, "ts.add", "temp:TLV", "1000", "30").Err())
		require.NoError(t, rdb.Do(ctx, "ts.add", "temp:JLM", "1005", "30").Err())

		// Test cases
		tests := []struct {
			name           string
			filters        []string
			expectedKeys   []string
			expectError    bool
			errorSubstring string
		}{
			{
				name:           "Empty Filter",
				filters:        []string{},
				expectError:    true,
				errorSubstring: "wrong number of arguments",
			},
			{
				name:           "No Matcher",
				filters:        []string{"type="},
				expectError:    true,
				errorSubstring: "please provide at least one matcher",
			},
			{
				name:         "Filter with trailing comma - type=(temp,)",
				filters:      []string{"type=(temp,)"},
				expectError:  false,
				expectedKeys: []string{"temp:TLV", "temp:JLM"},
			},
			{
				name:         "Basic equality - type=temp",
				filters:      []string{"type=temp"},
				expectError:  false,
				expectedKeys: []string{"temp:TLV", "temp:JLM"},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				args := []interface{}{"ts.mget", "FILTER"}
				for _, f := range tc.filters {
					args = append(args, f)
				}

				result, err := rdb.Do(ctx, args...).Result()
				if tc.expectError {
					require.Error(t, err)
					if tc.errorSubstring != "" {
						require.Contains(t, err.Error(), tc.errorSubstring)
					}
					return
				}

				require.NoError(t, err)
				resultArray, ok := result.([]interface{})
				require.True(t, ok, "Expected array result")

				foundKeys := make([]string, 0)
				for _, item := range resultArray {
					itemArray, ok := item.([]interface{})
					require.True(t, ok, "Expected item to be an array")
					require.True(t, len(itemArray) >= 1, "Expected item array to have at least 1 element")

					key, ok := itemArray[0].(string)
					require.True(t, ok, "Expected key to be a string")
					foundKeys = append(foundKeys, key)
				}

				// Sort both expected and found keys for consistent comparison
				sort.Strings(tc.expectedKeys)
				sort.Strings(foundKeys)

				require.Equal(t, tc.expectedKeys, foundKeys,
					"Expected keys %v but got %v", tc.expectedKeys, foundKeys)
			})
		}

		// Test WITHLABELS option
		t.Run("WITHLABELS Option", func(t *testing.T) {
			result, err := rdb.Do(ctx, "ts.mget", "WITHLABELS", "FILTER", "type=temp").Result()
			require.NoError(t, err)

			resultArray, ok := result.([]interface{})
			require.True(t, ok, "Expected array result")

			foundKeys := make([]string, 0)
			for _, item := range resultArray {
				itemArray, ok := item.([]interface{})
				require.True(t, ok, "Expected item to be an array")
				require.GreaterOrEqual(t, len(itemArray), 3, "Expected item array to have at least 3 elements")

				// Extract key
				key, ok := itemArray[0].(string)
				require.True(t, ok, "Expected key to be a string")
				foundKeys = append(foundKeys, key)

				// Extract labels - labels are a nested array of [key, value] pairs
				labels, ok := itemArray[1].([]interface{})
				require.True(t, ok, "Expected labels to be an array")

				// Create a map to store label key-value pairs
				labelMap := make(map[string]string)

				// Loop through each label pair in the array
				for _, labelPair := range labels {
					pair, ok := labelPair.([]interface{})
					require.True(t, ok, "Expected label pair to be an array")
					require.Equal(t, 2, len(pair), "Expected label pair to have 2 elements")

					labelKey, ok := pair[0].(string)
					require.True(t, ok, "Expected label key to be a string")

					labelValue, ok := pair[1].(string)
					require.True(t, ok, "Expected label value to be a string")

					labelMap[labelKey] = labelValue
				}

				// Verify labels
				require.Equal(t, "temp", labelMap["type"])
				switch key {
				case "temp:TLV":
					require.Equal(t, "TLV", labelMap["location"])
				case "temp:JLM":
					require.Equal(t, "JLM", labelMap["location"])
				}

				// Extract and verify sample data - sample is a nested array
				samples, _ := itemArray[2].([]interface{})
				sample, _ := samples[0].([]interface{})

				// Check timestamp and value
				switch key {
				case "temp:TLV":
					require.Equal(t, int64(1000), sample[0])
					require.Equal(t, float64(30), sample[1])
				case "temp:JLM":
					require.Equal(t, int64(1005), sample[0])
					require.Equal(t, float64(30), sample[1])
				}
			}

			// Check that we have both keys
			sort.Strings(foundKeys)
			require.Equal(t, []string{"temp:JLM", "temp:TLV"}, foundKeys)
		})

		// Test SELECTED_LABELS option
		t.Run("SELECTED_LABELS Option", func(t *testing.T) {
			result, err := rdb.Do(ctx, "ts.mget", "SELECTED_LABELS", "location", "FILTER", "type=temp").Result()
			require.NoError(t, err)

			resultArray, ok := result.([]interface{})
			require.True(t, ok, "Expected array result")

			// Debug the structure
			t.Logf("SELECTED_LABELS Result structure: %#v", resultArray)

			for _, item := range resultArray {
				itemArray, ok := item.([]interface{})
				require.True(t, ok, "Expected item to be an array")
				require.GreaterOrEqual(t, len(itemArray), 3, "Expected item array to have at least 3 elements")

				// Extract key
				key, ok := itemArray[0].(string)
				require.True(t, ok, "Expected key to be a string")

				// Extract labels - labels are a nested array of [key, value] pairs
				labels, ok := itemArray[1].([]interface{})
				require.True(t, ok, "Expected labels to be an array")

				// Create a map to store label key-value pairs
				labelMap := make(map[string]string)

				// Loop through each label pair in the array
				for _, labelPair := range labels {
					pair, ok := labelPair.([]interface{})
					require.True(t, ok, "Expected label pair to be an array")
					require.Equal(t, 2, len(pair), "Expected label pair to have 2 elements")

					labelKey, ok := pair[0].(string)
					require.True(t, ok, "Expected label key to be a string")

					labelValue, ok := pair[1].(string)
					require.True(t, ok, "Expected label value to be a string")

					labelMap[labelKey] = labelValue
				}

				// Verify that only location label is present
				require.Equal(t, 1, len(labelMap), "Should have exactly one label")
				require.Contains(t, labelMap, "location")
				require.NotContains(t, labelMap, "type")

				switch key {
				case "temp:TLV":
					require.Equal(t, "TLV", labelMap["location"])
				case "temp:JLM":
					require.Equal(t, "JLM", labelMap["location"])
				}

				// Extract and verify sample data
				samples, _ := itemArray[2].([]interface{})
				sample, _ := samples[0].([]interface{})

				// Check timestamp and value
				switch key {
				case "temp:TLV":
					require.Equal(t, int64(1000), sample[0])
					require.Equal(t, float64(30), sample[1])
				case "temp:JLM":
					require.Equal(t, int64(1005), sample[0])
					require.Equal(t, float64(30), sample[1])
				}
			}
		})
	})
	t.Run("TS.MRange Test", func(t *testing.T) {
		t.Run("Basic", func(t *testing.T) {
			keyA, keyB := "stock:A_MRange", "stock:B_MRange"
			type_label := "stock_MRange"
			require.NoError(t, rdb.Do(ctx, "ts.create", keyA, "LABELS", "type", type_label, "name", "A").Err())
			require.NoError(t, rdb.Do(ctx, "ts.create", keyB, "LABELS", "type", type_label, "name", "B").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyA, "1000", "100", keyA, "1010", "110", keyA, "1020", "120").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyB, "1000", "120", keyB, "1010", "110", keyB, "1020", "100").Err())

			res := rdb.Do(ctx, "ts.mrange", "-", "+", "WITHLABELS", "FILTER", "type="+type_label, "GROUPBY", "type", "REDUCE", "max").Val().([]interface{})
			require.Equal(t, 1, len(res))

			group := res[0].([]interface{})
			require.Equal(t, "type=stock_MRange", group[0])

			metadata := group[1].([]interface{})
			labels := metadata[0].([]interface{})
			require.Equal(t, []interface{}{"type", type_label}, labels)
			require.Equal(t, "max", metadata[1].([]interface{})[1])

			samples := group[2].([]interface{})
			require.Equal(t, 3, len(samples))
			expectSamples := [][]interface{}{
				{int64(1000), 120.0}, {int64(1010), 110.0}, {int64(1020), 120.0},
			}
			for i, s := range samples {
				require.Equal(t, expectSamples[i], s.([]interface{}))
			}
		})

		t.Run("With Aggregation", func(t *testing.T) {
			keyA, keyB := "stock:A_WithAggregation", "stock:B_WithAggregation"
			type_label := "stock_WithAggregation"
			require.NoError(t, rdb.Do(ctx, "ts.create", keyA, "LABELS", "type", type_label, "name", "A").Err())
			require.NoError(t, rdb.Do(ctx, "ts.create", keyB, "LABELS", "type", type_label, "name", "B").Err())

			require.NoError(t, rdb.Do(ctx, "ts.madd", keyA, "1000", "100", keyA, "1010", "110", keyA, "1020", "120").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyB, "1000", "120", keyB, "1010", "110", keyB, "1020", "100").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyA, "2000", "200", keyA, "2010", "210", keyA, "2020", "220").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyB, "2000", "220", keyB, "2010", "210", keyB, "2020", "200").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyA, "3000", "300", keyA, "3010", "310", keyA, "3020", "320").Err())
			require.NoError(t, rdb.Do(ctx, "ts.madd", keyB, "3000", "320", keyB, "3010", "310", keyB, "3020", "300").Err())

			res := rdb.Do(ctx, "ts.mrange", "-", "+", "WITHLABELS", "AGGREGATION", "avg", "1000", "FILTER", "type="+type_label, "GROUPBY", "type", "REDUCE", "max").Val().([]interface{})
			require.Equal(t, 1, len(res))

			name := res[0].([]interface{})[0].(string)
			require.Equal(t, "type="+type_label, name)

			labels := res[0].([]interface{})[1].([]interface{})
			require.Equal(t, 3, len(labels))
			require.Equal(t, []interface{}{"type", type_label}, labels[0].([]interface{}))
			require.Equal(t, []interface{}{"__reducer__", "max"}, labels[1].([]interface{}))
			require.Equal(t, []interface{}{"__source__", keyA + "," + keyB}, labels[2].([]interface{}))

			samples := res[0].([]interface{})[2].([]interface{})
			require.Equal(t, 3, len(samples))
			expectSamples := [][]interface{}{
				{int64(1000), 110.0}, {int64(2000), 210.0}, {int64(3000), 310.0},
			}
			for i, s := range samples {
				require.Equal(t, expectSamples[i], s.([]interface{}))
			}
		})

		t.Run("Filter By Value", func(t *testing.T) {
			keyA, keyB := "ts1_MRange_FilterByValue", "ts2_MRange_FilterByValue"
			label_spec := "metric_MRange_FilterByValue"
			require.NoError(t, rdb.Do(ctx, "ts.add", keyA, "1548149180000", "90", "labels", "metric", label_spec, "metric_name", "system").Err())
			require.NoError(t, rdb.Do(ctx, "ts.add", keyB, "1548149180000", "99", "labels", "metric", label_spec, "metric_name", "user").Err())

			res := rdb.Do(ctx, "ts.mrange", "-", "+", "FILTER_BY_VALUE", "90", "100", "WITHLABELS", "FILTER", "metric="+label_spec).Val().([]interface{})
			require.Equal(t, 2, len(res))

			results := map[string][]interface{}{}
			for _, item := range res {
				arr := item.([]interface{})
				results[arr[0].(string)] = arr[2].([]interface{})
			}

			ts1 := results[keyA]
			require.Equal(t, 1, len(ts1))
			require.Equal(t, int64(1548149180000), ts1[0].([]interface{})[0])
			require.Equal(t, 90.0, ts1[0].([]interface{})[1])

			ts2 := results[keyB]
			require.Equal(t, 1, len(ts2))
			require.Equal(t, int64(1548149180000), ts2[0].([]interface{})[0])
			require.Equal(t, 99.0, ts2[0].([]interface{})[1])
		})
	})

	t.Run("TS.INCRBY/DECRBY Test", func(t *testing.T) {
		key := "key_Incrby"
		require.NoError(t, rdb.Del(ctx, key).Err())
		// Test initial INCRBY creates key
		require.Equal(t, int64(1657811829000), rdb.Do(ctx, "ts.incrby", key, "232", "TIMESTAMP", "1657811829000").Val())
		// Verify range after first increment
		res := rdb.Do(ctx, "ts.range", key, "-", "+").Val().([]interface{})
		require.Equal(t, 1, len(res))
		require.Equal(t, []interface{}{int64(1657811829000), 232.0}, res[0])

		// Test incrementing same timestamp
		require.Equal(t, int64(1657811829000), rdb.Do(ctx, "ts.incrby", key, "157", "TIMESTAMP", "1657811829000").Val())
		res = rdb.Do(ctx, "ts.range", key, "-", "+").Val().([]interface{})
		require.Equal(t, 1, len(res))
		require.Equal(t, []interface{}{int64(1657811829000), 389.0}, res[0])

		// Test additional increment
		require.Equal(t, int64(1657811829000), rdb.Do(ctx, "ts.incrby", key, "432", "TIMESTAMP", "1657811829000").Val())
		res = rdb.Do(ctx, "ts.range", key, "-", "+").Val().([]interface{})
		require.Equal(t, 1, len(res))
		require.Equal(t, []interface{}{int64(1657811829000), 821.0}, res[0])

		// Test error with earlier timestamp
		_, err := rdb.Do(ctx, "ts.incrby", key, "100", "TIMESTAMP", "50").Result()
		require.ErrorContains(t, err, "timestamp must be equal to or higher than the maximum existing timestamp")

		// Test  decrementing
		require.Equal(t, int64(1657811829000), rdb.Do(ctx, "ts.decrby", key, "432", "TIMESTAMP", "1657811829000").Val())
		res = rdb.Do(ctx, "ts.range", key, "-", "+").Val().([]interface{})
		require.Equal(t, 1, len(res))
		require.Equal(t, []interface{}{int64(1657811829000), 389.0}, res[0])
	})
}
