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

package latency

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func isEmptyResult(val interface{}) bool {
	switch v := val.(type) {
	case []interface{}:
		return len(v) == 0
	case map[interface{}]interface{}:
		return len(v) == 0
	default:
		return val == nil
	}
}

func TestLatencyHelp(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LATENCY HELP returns help text", func(t *testing.T) {
		result, err := rdb.Do(ctx, "LATENCY", "HELP").StringSlice()
		require.NoError(t, err)
		require.NotEmpty(t, result)
		require.Contains(t, result[0], "HELP")
	})

	t.Run("LATENCY HELP is case-insensitive", func(t *testing.T) {
		result, err := rdb.Do(ctx, "latency", "help").StringSlice()
		require.NoError(t, err)
		require.NotEmpty(t, result)
	})
}

func TestLatencyReset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LATENCY RESET always returns 0", func(t *testing.T) {
		val, err := rdb.Do(ctx, "LATENCY", "RESET").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 0, val)
	})

	t.Run("LATENCY RESET with event arguments still returns 0", func(t *testing.T) {
		val, err := rdb.Do(ctx, "LATENCY", "RESET", "event1", "event2").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 0, val)
	})
}

func TestLatencyErrors(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LATENCY without subcommand returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "LATENCY").Err()
		require.Error(t, err)
	})

	t.Run("LATENCY with unknown subcommand returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "LATENCY", "UNKNOWN").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Unknown LATENCY subcommand")
	})
}

func TestLatencyHistogramWithoutBuckets(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LATENCY HISTOGRAM returns empty when histogram-bucket-boundaries is not set", func(t *testing.T) {
		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM").Result()
		require.NoError(t, err)
		require.True(t, isEmptyResult(val))
	})
}

func TestLatencyHistogram(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"histogram-bucket-boundaries": "10,20,30,50,100,200,500,1000",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("LATENCY HISTOGRAM returns data after running commands", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			require.NoError(t, rdb.Set(ctx, fmt.Sprintf("key-%d", i), "value", 0).Err())
		}
		for i := 0; i < 10; i++ {
			require.NoError(t, rdb.Get(ctx, fmt.Sprintf("key-%d", i)).Err())
		}

		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM").Result()
		require.NoError(t, err)
		require.False(t, isEmptyResult(val))
	})

	t.Run("LATENCY HISTOGRAM filters by command name", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			require.NoError(t, rdb.Set(ctx, fmt.Sprintf("filter-key-%d", i), "value", 0).Err())
		}

		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "set").Result()
		require.NoError(t, err)
		require.False(t, isEmptyResult(val))
	})

	t.Run("LATENCY HISTOGRAM for nonexistent command returns empty", func(t *testing.T) {
		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "nonexistent_command").Result()
		require.NoError(t, err)
		require.True(t, isEmptyResult(val))
	})

	t.Run("LATENCY HISTOGRAM skips commands with zero calls", func(t *testing.T) {
		// hset has never been called in this test, so it should have calls==0
		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "hset").Result()
		require.NoError(t, err)
		require.True(t, isEmptyResult(val))
	})

	t.Run("LATENCY HISTOGRAM is case-insensitive for command names", func(t *testing.T) {
		valLower, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "set").Result()
		require.NoError(t, err)

		valUpper, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "SET").Result()
		require.NoError(t, err)

		require.False(t, isEmptyResult(valLower))
		require.False(t, isEmptyResult(valUpper))
	})

	t.Run("LATENCY HISTOGRAM with multiple command filters", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "multi-key", "value", 0).Err())
		_, err := rdb.Get(ctx, "multi-key").Result()
		require.NoError(t, err)

		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "set", "get").Result()
		require.NoError(t, err)
		require.False(t, isEmptyResult(val))
	})

	t.Run("LATENCY HISTOGRAM contains calls and histogram_usec fields", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "field-check-key", "value", 0).Err())

		val, err := rdb.Do(ctx, "LATENCY", "HISTOGRAM", "set").Result()
		require.NoError(t, err)

		// RESP2 returns flat array: ["set", ["calls", N, "histogram_usec", [...]]]
		// RESP3 returns map: {"set": {"calls": N, "histogram_usec": {...}}}
		switch v := val.(type) {
		case []interface{}:
			require.GreaterOrEqual(t, len(v), 2)
			innerSlice, ok := v[1].([]interface{})
			require.True(t, ok)
			require.GreaterOrEqual(t, len(innerSlice), 4)
			require.Equal(t, "calls", innerSlice[0])
			calls, ok := innerSlice[1].(int64)
			require.True(t, ok)
			require.Greater(t, calls, int64(0))
		case map[interface{}]interface{}:
			setData, ok := v["set"]
			require.True(t, ok)
			innerMap, ok := setData.(map[interface{}]interface{})
			require.True(t, ok)
			calls, ok := innerMap["calls"]
			require.True(t, ok)
			require.NotNil(t, calls)
		default:
			t.Fatalf("unexpected result type: %T", val)
		}
	})
}
