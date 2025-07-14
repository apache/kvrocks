//go:build !ignore_when_tsan

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

package tdigest

import (
	"context"
	"strconv"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

const (
	errMsgWrongNumberArg   = "wrong number of arguments"
	errMsgParseCompression = "error parsing compression parameter"
	errMsgNeedToBePositive = "compression parameter needs to be a positive integer"
	errMsgMustInRange      = "compression must be between 1 and 1000"
	errMsgKeyAlreadyExists = "key already exists"
	errMsgKeyNotExist      = "key does not exist"
)

type tdigestInfo struct {
	Compression       int64
	Capacity          int64
	MergedNodes       int64
	UnmergedNodes     int64
	MergedWeight      int64
	UnmergedWeight    int64
	Observations      int64
	TotalCompressions int64
	// memory usgae is not useful, we do not support it now
}

func toTdigestInfo(t *testing.T, value interface{}) tdigestInfo {
	require.IsType(t, map[interface{}]interface{}{}, value)
	v := value.(map[interface{}]interface{})
	return tdigestInfo{
		Compression:       v["Compression"].(int64),
		Capacity:          v["Capacity"].(int64),
		MergedNodes:       v["Merged nodes"].(int64),
		UnmergedNodes:     v["Unmerged nodes"].(int64),
		MergedWeight:      v["Merged weight"].(int64),
		UnmergedWeight:    v["Unmerged weight"].(int64),
		Observations:      v["Observations"].(int64),
		TotalCompressions: v["Total compressions"].(int64),
	}
}

func TestTDigest(t *testing.T) {
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
		tdigestTests(t, configs)
	}
}

func tdigestTests(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("tdigest.create with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_create_"
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "hahah").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "1", "hahah").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "hahah").Err(), errMsgParseCompression)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "0").Err(), errMsgNeedToBePositive)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "-1").Err(), errMsgNeedToBePositive)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "0.1").Err(), errMsgParseCompression)

		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key0", "compression", "1").Err())
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key0", "compression", "1").Err(), errMsgKeyAlreadyExists)
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key1", "compression", "1000").Err())
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key", "compression", "1001").Err(), errMsgMustInRange)
	})

	t.Run("tdigest.info with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_info_"
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.INFO").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key", "hahah").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key").Err(), errMsgKeyNotExist)
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key0", "compression", "1").Err())
		{
			rsp := rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key0")
			require.NoError(t, rsp.Err())
			info := toTdigestInfo(t, rsp.Val())
			require.EqualValues(t, 1, info.Compression)
			require.EqualValues(t, 1*6+10, info.Capacity)
			require.EqualValues(t, 0, info.MergedNodes)
			require.EqualValues(t, 0, info.UnmergedNodes)
			require.EqualValues(t, 0, info.MergedWeight)
			require.EqualValues(t, 0, info.UnmergedWeight)
			require.EqualValues(t, 0, info.Observations)
			require.EqualValues(t, 0, info.TotalCompressions)
		}

		{
			require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"key1", "compression", "1000").Err())
			rsp := rdb.Do(ctx, "TDIGEST.INFO", keyPrefix+"key1")
			require.NoError(t, rsp.Err())
			info := toTdigestInfo(t, rsp.Val())
			require.EqualValues(t, 1000, info.Compression)
			require.EqualValues(t, 1024, info.Capacity) // max is 1024
			require.EqualValues(t, 0, info.MergedNodes)
			require.EqualValues(t, 0, info.UnmergedNodes)
			require.EqualValues(t, 0, info.MergedWeight)
			require.EqualValues(t, 0, info.UnmergedWeight)
			require.EqualValues(t, 0, info.Observations)
			require.EqualValues(t, 0, info.TotalCompressions)
		}
	})

	t.Run("tdigest.add with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_add_"

		// Satisfy the number of parameters
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.ADD").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.ADD", keyPrefix+"key").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.ADD", keyPrefix+"key", "abc").Err(), "not a valid float")
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.ADD", keyPrefix+"nonexistent", "1.0").Err(), errMsgKeyNotExist)

		// Test adding values to a key
		key := keyPrefix + "test1"
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key, "compression", "100").Err())
		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "42.0").Err())

		rsp := rdb.Do(ctx, "TDIGEST.INFO", key)
		require.NoError(t, rsp.Err())
		info := toTdigestInfo(t, rsp.Val())
		require.EqualValues(t, 1, info.UnmergedNodes)
		require.EqualValues(t, 1, info.Observations)

		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "1.0", "2.0", "3.0", "4.0", "5.0").Err())

		rsp = rdb.Do(ctx, "TDIGEST.INFO", key)
		require.NoError(t, rsp.Err())
		info = toTdigestInfo(t, rsp.Val())
		require.EqualValues(t, 6, info.Observations)

		// Test adding values to a key with compression
		key2 := keyPrefix + "test2"
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key2, "compression", "100").Err())

		args := []interface{}{key2}
		for i := 1; i <= 1000; i++ {
			args = append(args, float64(i))
		}
		require.NoError(t, rdb.Do(ctx, append([]interface{}{"TDIGEST.ADD"}, args...)...).Err())

		rsp = rdb.Do(ctx, "TDIGEST.INFO", key2)
		require.NoError(t, rsp.Err())
		info = toTdigestInfo(t, rsp.Val())
		require.EqualValues(t, 1000, info.Observations)

		// Test adding values to a key with compression and merge node
		key3 := keyPrefix + "test3"
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key3, "compression", "10").Err())

		args = []interface{}{key3}
		for i := 1; i <= 100; i++ {
			args = append(args, float64(i%10))
		}
		require.NoError(t, rdb.Do(ctx, append([]interface{}{"TDIGEST.ADD"}, args...)...).Err())

		rsp = rdb.Do(ctx, "TDIGEST.INFO", key3)
		require.NoError(t, rsp.Err())
		info = toTdigestInfo(t, rsp.Val())

		require.Greater(t, info.MergedNodes, int64(0))
		require.Greater(t, info.MergedWeight, int64(0))
		require.EqualValues(t, 100, info.Observations)
		require.Greater(t, info.TotalCompressions, int64(0))
	})

	t.Run("tdigest.max with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_max_"

		// Test invalid arguments
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.MAX").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.MAX", keyPrefix+"nonexistent").Err(), errMsgKeyNotExist)

		// Test with empty tdigest
		key := keyPrefix + "test1"
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key, "compression", "100").Err())
		rsp := rdb.Do(ctx, "TDIGEST.MAX", key)
		require.NoError(t, rsp.Err())
		require.EqualValues(t, rsp.Val(), "nan")

		// Test with single value
		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "42.5").Err())
		rsp = rdb.Do(ctx, "TDIGEST.MAX", key)
		require.NoError(t, rsp.Err())
		require.Equal(t, "42.5", rsp.Val())

		// Test with multiple values
		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "1.0", "100.5", "50.5", "-10.5").Err())
		rsp = rdb.Do(ctx, "TDIGEST.MAX", key)
		require.NoError(t, rsp.Err())
		require.Equal(t, "100.5", rsp.Val())
	})

	t.Run("tdigest.min with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_min_"

		// Test invalid arguments
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.MIN").Err(), errMsgWrongNumberArg)
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.MIN", keyPrefix+"nonexistent").Err(), errMsgKeyNotExist)

		// Test with empty tdigest
		key := keyPrefix + "test1"
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key, "compression", "100").Err())
		rsp := rdb.Do(ctx, "TDIGEST.MIN", key)
		require.NoError(t, rsp.Err())
		require.EqualValues(t, rsp.Val(), "nan")

		// Test with single value
		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "42.5").Err())
		rsp = rdb.Do(ctx, "TDIGEST.MIN", key)
		require.NoError(t, rsp.Err())
		require.Equal(t, "42.5", rsp.Val())

		// Test with multiple values
		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "1.0", "100.5", "50.5", "-10.5").Err())
		rsp = rdb.Do(ctx, "TDIGEST.MIN", key)
		require.NoError(t, rsp.Err())
		require.Equal(t, "-10.5", rsp.Val())
	})
	t.Run("tdigest.reset with different arguments", func(t *testing.T) {
		keyPrefix := "tdigest_reset_"

		// Testing with no arguments to .RESET
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.RESET").Err(), errMsgWrongNumberArg)

		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", keyPrefix+"mydigest", "compression", "101").Err())

		key := keyPrefix + "mydigest"
		// Adding some data to digest
		require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key, "-84.3", "199.3", "343.34", "12.34").Err())

		// Checking MIN value to ensure data was added
		rsp := rdb.Do(ctx, "TDIGEST.MIN", key)
		require.NoError(t, rsp.Err())
		require.EqualValues(t, rsp.Val(), "-84.3")

		// Reset on a non-existent key
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.RESET", keyPrefix+"notexist").Err(), errMsgKeyNotExist)

		// Get TDIGEST.INFO before reset
		rsp = rdb.Do(ctx, "TDIGEST.INFO", key)
		require.NoError(t, rsp.Err())
		infoBeforeReset := toTdigestInfo(t, rsp.Val())

		// Perform the reset
		require.NoError(t, rdb.Do(ctx, "TDIGEST.RESET", key).Err())

		// Get TDIGEST.INFO after reset
		rsp = rdb.Do(ctx, "TDIGEST.INFO", key)
		require.NoError(t, rsp.Err())
		infoAfterReset := toTdigestInfo(t, rsp.Val())

		// Ensure capacity remains unchanged
		require.EqualValues(t, infoBeforeReset.Capacity, infoAfterReset.Capacity)
		require.EqualValues(t, 101, infoAfterReset.Compression)
		require.EqualValues(t, 0, infoAfterReset.MergedNodes)
		require.EqualValues(t, 0, infoAfterReset.UnmergedNodes)
		require.EqualValues(t, 0, infoAfterReset.Observations)
		require.EqualValues(t, 0, infoAfterReset.TotalCompressions)

		// Reset on an empty digest
		emptyDigestKey := keyPrefix + "empty"
		require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", emptyDigestKey, "COMPRESSION", "100").Err())
		rsp = rdb.Do(ctx, "TDIGEST.RESET", emptyDigestKey)
		require.NoError(t, rsp.Err())

		// Ensure empty digest's capacity remains the same
		rsp = rdb.Do(ctx, "TDIGEST.INFO", emptyDigestKey)
		require.NoError(t, rsp.Err())
		infoAfterEmptyReset := toTdigestInfo(t, rsp.Val())
		require.EqualValues(t, 100, infoAfterEmptyReset.Compression)
	})
	t.Run("tdigest.quantile with different arguments", func(t *testing.T) {
		keyPrefix := "t_qt_"

		// No arguments
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.QUANTILE").Err(), errMsgWrongNumberArg)

		// Non-existent key
		require.ErrorContains(t, rdb.Do(ctx, "TDIGEST.QUANTILE", keyPrefix+"iDoNotExist", "0.5").Err(), errMsgKeyNotExist)

		{
			// Quantile on empry tdigest
			key0 := keyPrefix + "00"
			require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key0, "compression", "100").Err())
			rsp := rdb.Do(ctx, "TDIGEST.QUANTILE", key0, "0.4", "0.1", "0.2", "0.3", "0.7", "0.5")
			require.NoError(t, rsp.Err())
			vals, err := rsp.Slice()
			require.NoError(t, err)
			require.Equal(t, 6, len(vals))
			for _, v := range vals {
				s, ok := v.(string)
				require.True(t, ok, "Expected string but got %T", v)
				require.Equal(t, "nan", s, "Expected value to be 'nan'")
			}
		}
		{
			// Quantiles on positive data
			key1 := keyPrefix + "01"
			require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key1, "compression", "100").Err())
			require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key1, "1", "2", "2", "3", "3", "3", "4", "4", "4", "4", "5", "5", "5", "5", "5").Err())
			rsp := rdb.Do(ctx, "TDIGEST.QUANTILE", key1, "0", "0.1", "0.2", "0.3", "0.4", "0.5", "0.6", "0.7", "0.8", "0.9", "1")
			require.NoError(t, rsp.Err())
			vals, err := rsp.Slice()
			require.NoError(t, err)
			require.Len(t, vals, 11)
			expected := []float64{1.0, 2.0, 2.5, 3.0, 3.5, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0}
			for i, v := range vals {
				str, ok := v.(string)
				require.True(t, ok, "expected string but got %T at index %d", v, i)

				got, err := strconv.ParseFloat(str, 64)
				require.NoError(t, err, "could not parse value at index %d", i)

				require.InEpsilon(t, expected[i], got, 0.0001, "mismatch at index %d", i)
			}
		}
		{
			// Quantiles on negative data
			key2 := keyPrefix + "02"
			require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key2, "compression", "100").Err())
			require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key2, "-1", "-2", "-3", "-4", "-5", "-6", "-7", "-8", "-9", "-10").Err())
			rsp := rdb.Do(ctx, "TDIGEST.QUANTILE", key2, "0", "0.25", "0.5", "0.75", "1")
			require.NoError(t, rsp.Err())

			vals, err := rsp.Slice()
			require.NoError(t, err)
			require.Len(t, vals, 5)

			expected := []float64{-10.0, -8.0, -5.5, -3.0, -1.0}
			for i, v := range vals {
				str, ok := v.(string)
				require.True(t, ok, "expected string but got %T at index %d", v, i)

				got, err := strconv.ParseFloat(str, 64)
				require.NoError(t, err, "could not parse value at index %d", i)

				require.InEpsilon(t, expected[i], got, 0.0001, "mismatch at index %d", i)
			}
		}
		{
			// Query with unordered quantiles
			key3 := keyPrefix + "03"
			require.NoError(t, rdb.Do(ctx, "TDIGEST.CREATE", key3, "compression", "100").Err())
			require.NoError(t, rdb.Do(ctx, "TDIGEST.ADD", key3,
				"3", "12", "-3", "-19", "13", "4", "14", "18", "-1", "-5", "15", "-10", "33", "17", "-20",
			).Err())
			rsp := rdb.Do(ctx, "TDIGEST.QUANTILE", key3,
				"0.9", "0.1", "0.7", "0.3", "0.6", "0.0", "0.55", "0.65", "0.34", "0.88",
			)
			require.NoError(t, rsp.Err())
			vals, err := rsp.Slice()
			require.NoError(t, err)
			require.Equal(t, 10, len(vals))

			expected := []float64{
				18.0,
				-19.0,
				14.0,
				-3.0,
				12.5,
				-20.0,
				12.0,
				13.0,
				-1.0,
				18.0,
			}
			for i, v := range vals {
				strVal, ok := v.(string)
				require.True(t, ok, "Expected string at index %d but got %T", i, v)
				numVal, err := strconv.ParseFloat(strVal, 64)
				require.NoError(t, err, "Failed to parse value at index %d: %s", i, strVal)
				require.InDelta(t, expected[i], numVal, 1e-6, "Mismatch at index %d", i)
			}
		}
	})
}
