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
}
