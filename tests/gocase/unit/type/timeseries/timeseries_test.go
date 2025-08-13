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

	t.Run("TS.RANGE Basic Query", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		// Add sample data points
		require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", key, "1000", "12.3").Val())
		require.Equal(t, int64(2000), rdb.Do(ctx, "ts.add", key, "2000", "13.4").Val())
		require.Equal(t, int64(3000), rdb.Do(ctx, "ts.add", key, "3000", "14.5").Val())
		
		// Query full range
		res := rdb.Do(ctx, "ts.range", key, "-", "+").Val()
		assert.Len(t, res, 3)
		assert.Equal(t, []interface{}{
			[]interface{}{int64(1000), 12.3},
			[]interface{}{int64(2000), 13.4},
			[]interface{}{int64(3000), 14.5},
		}, res)
	})

	t.Run("TS.RANGE Invalid Timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "abc", "1000").Err(), "wrong fromTimestamp")
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", key, "1000", "xyz").Err(), "wrong toTimestamp")
	})

	t.Run("TS.RANGE No Data", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
		res := rdb.Do(ctx, "ts.range", key, "0", "+").Val().([]interface{})
		assert.Empty(t, res)
	})

	// t.Run("TS.RANGE Filter By TS", func(t *testing.T) {
	// 	require.NoError(t, rdb.Del(ctx, key).Err())
	// 	require.NoError(t, rdb.Do(ctx, "ts.create", key).Err())
	// 	require.Equal(t, int64(1000), rdb.Do(ctx, "ts.add", key, "1000", "12.3").Val())
	// 	require.Equal(t, int64(2000), rdb.Do(ctx, "ts.add", key, "2000", "13.4").Val())
	// 	require.Equal(t, int64(3000), rdb.Do(ctx, "ts.add", key, "3000", "14.5").Val())
		
	// 	// Filter by specific timestamps
	// 	res := rdb.Do(ctx, "ts.range", key, "0", "+", "FILTER_BY_TS", "1000", "3000").Val().([]interface{})
	// 	assert.Len(t, res, 2)
	// 	assert.Equal(t, []interface{}{int64(1000), "12.3", int64(3000), "14.5"}, res)
	// })

	t.Run("TS.RANGE Nonexistent Key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nonexistent").Err())
		require.ErrorContains(t, rdb.Do(ctx, "ts.range", "nonexistent", "-", "+").Err(), "key does not exist")
	})
}
