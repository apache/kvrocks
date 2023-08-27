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
	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBloom(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "test_bloom_key"
	t.Run("Reserve a bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000").Err())
	})

	t.Run("Reserve a bloom filter with wrong error_rate", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "abc", "1000").Err(), "ERR value is not a valid float")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "-0.03", "1000").Err(), "ERR error rate should be between 0 and 1")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "1", "1000").Err(), "ERR error rate should be between 0 and 1")
	})

	t.Run("Reserve a bloom filter with wrong capacity", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "qwe").Err(), "ERR value is not an integer")
		// capacity stored in uint32_t, if input is negative, the parser will make an error.
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "-1000").Err(), "ERR value is not an integer or out of range")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "0").Err(), "ERR capacity should be larger than 0")
	})

	t.Run("Reserve a bloom filter with nonscaling", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000", "nonscaling").Err())
	})

	t.Run("Reserve a bloom filter with expansion", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000", "expansion", "1").Err())
	})

	t.Run("Reserve a bloom filter with wrong expansion", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion").Err(), "ERR no more item to parse")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion", "0").Err(), "ERR expansion should be greater or equal to 1")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion", "asd").Err(), "ERR not started as an integer")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion", "-1").Err(), "ERR out of range of integer type")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion", "1.5").Err(), "ERR encounter non-integer characters")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion", "123asd").Err(), "ERR encounter non-integer characters")
	})

	t.Run("Reserve a bloom filter with nonscaling and expansion", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "expansion", "1", "nonscaling").Err(), "ERR nonscaling filters cannot expand")
		require.ErrorContains(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "1000", "nonscaling", "expansion", "1").Err(), "ERR nonscaling filters cannot expand")
	})

	t.Run("Check no exists key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.exists", "no_exist_key", "item1").Err(), "ERR NotFound: key is not found")
		require.NoError(t, rdb.Del(ctx, "no_exist_key").Err())
	})

	t.Run("BasicAddAndCheck", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		var insertItems []string
		for i := 0; i < 100; i++ {
			buf := util.RandString(1, 100000, util.Alpha)
			insertItems = append(insertItems, buf)
			require.Equal(t, int64(1), rdb.Do(ctx, "bf.add", key, buf).Val())
		}

		for i := 0; i < 1000; i++ {
			index := util.RandomInt(100)
			require.Equal(t, int64(1), rdb.Do(ctx, "bf.exists", key, insertItems[index]).Val())
		}

		for i := 0; i < 1000; i++ {
			index := util.RandomInt(100)
			require.Equal(t, int64(0), rdb.Do(ctx, "bf.exists", key, "no_exist"+insertItems[index]).Val())
		}
	})
}
