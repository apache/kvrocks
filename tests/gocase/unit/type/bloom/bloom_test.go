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
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	t.Run("Check no exist key and no exist item", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "no_exist_key").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.exists", "no_exist_key", "item1").Val())
		require.NoError(t, rdb.Del(ctx, "no_exist_key").Err())

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.exists", key, "item1").Val())
	})

	t.Run("Add the same value", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.add", key, "xxx").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.info", key, "items").Val())
		// Add the same value would return 0
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.add", key, "xxx").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.info", key, "items").Val())
		// Add the distinct value would return 1
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.add", key, "yyy").Val())
		require.Equal(t, int64(2), rdb.Do(ctx, "bf.info", key, "items").Val())
	})

	t.Run("BasicAddAndCheck", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		var totalCount = 10000
		var fpp = 0.01
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, fpp, totalCount).Err())

		// insert items
		var insertItems []string
		var falseExist = 0
		for i := 0; i < totalCount; i++ {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.add", key, buf)
			require.NoError(t, Add.Err())
			if Add.Val() == int64(0) {
				falseExist += 1
			} else {
				insertItems = append(insertItems, buf)
			}
		}
		require.Equal(t, int64(totalCount-falseExist), rdb.Do(ctx, "bf.info", key, "items").Val())
		require.LessOrEqual(t, float64(falseExist), fpp*float64(totalCount))

		// check exist items
		for i := 0; i < totalCount; i++ {
			index := util.RandomInt(int64(totalCount - falseExist))
			require.Equal(t, int64(1), rdb.Do(ctx, "bf.exists", key, insertItems[index]).Val())
		}

		// check no exist items
		falseExist = 0
		for i := 0; i < totalCount; i++ {
			buf := util.RandString(9, 10, util.Alpha)
			check := rdb.Do(ctx, "bf.exists", key, buf)
			require.NoError(t, check.Err())
			if check.Val() == int64(1) {
				falseExist += 1
			}
		}
		require.LessOrEqual(t, float64(falseExist), fpp*float64(totalCount))
	})

	t.Run("MAdd Basic Test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Equal(t, []interface{}{int64(0), int64(0), int64(0)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())

		require.Equal(t, []interface{}{int64(1), int64(1)}, rdb.Do(ctx, "bf.madd", key, "xxx", "zzz").Val())
		require.Equal(t, int64(2), rdb.Do(ctx, "bf.card", key).Val())
		require.Equal(t, []interface{}{int64(1), int64(0), int64(1)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())

		// add the existed value
		require.Equal(t, []interface{}{int64(0)}, rdb.Do(ctx, "bf.madd", key, "zzz").Val())
		require.Equal(t, []interface{}{int64(1), int64(0), int64(1)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())

		// add the same value
		require.Equal(t, []interface{}{int64(1), int64(0)}, rdb.Do(ctx, "bf.madd", key, "yyy", "yyy").Val())
		require.Equal(t, []interface{}{int64(1), int64(1), int64(1)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())
	})

	t.Run("MAdd nonscaling Test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.0001", "25", "nonscaling").Err())

		// insert items
		var insertNum int64 = 0
		require.Equal(t, []interface{}{int64(1), int64(1), int64(1), int64(1)}, rdb.Do(ctx, "bf.madd", key, "x", "y", "z", "k").Val())
		for insertNum < 24 {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.madd", key, buf, buf+"xx")
			require.NoError(t, Add.Err())
			insertNum = rdb.Do(ctx, "bf.card", key).Val().(int64)
		}
		require.Equal(t, int64(24), rdb.Do(ctx, "bf.card", key).Val())

		Add := rdb.Do(ctx, "bf.madd", key, "a", "x", "xxx", "y", "z").Val()
		ret := make([]interface{}, 0, 5)
		for _, value := range Add.([]interface{}) {
			switch v := value.(type) {
			case int64:
				ret = append(ret, v)
			case error:
				ret = append(ret, v.Error())
			default:
			}
		}
		assert.Equal(t, []interface{}{int64(1), int64(0), "ERR nonscaling filter is full", int64(0), int64(0)}, ret)
		require.Equal(t, int64(25), rdb.Do(ctx, "bf.card", key).Val())
	})

	t.Run("MAdd scaling Test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.0001", "30", "expansion", "2").Err())

		// insert items
		var insertNum int64 = 0
		for insertNum < 30 {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.madd", key, buf, buf+"xx", buf+"yy")
			require.NoError(t, Add.Err())
			insertNum = rdb.Do(ctx, "bf.card", key).Val().(int64)
		}
		require.Equal(t, []interface{}{"Capacity", int64(30), "Size", int64(128), "Number of filters", int64(1), "Number of items inserted", int64(30), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())

		// bloom filter is full and scaling
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "xxx").Err())
		require.Equal(t, []interface{}{"Capacity", int64(90), "Size", int64(384), "Number of filters", int64(2), "Number of items inserted", int64(31), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())

		// insert items
		for insertNum < 90 {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.add", key, buf)
			require.NoError(t, Add.Err())
			insertNum = rdb.Do(ctx, "bf.card", key).Val().(int64)
		}
		require.Equal(t, []interface{}{"Capacity", int64(90), "Size", int64(384), "Number of filters", int64(2), "Number of items inserted", int64(90), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())
		// add the existed value would not scaling
		require.NoError(t, rdb.Do(ctx, "bf.madd", key, "xxx").Err())
		require.Equal(t, []interface{}{"Capacity", int64(90), "Size", int64(384), "Number of filters", int64(2), "Number of items inserted", int64(90), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())
		// bloom filter is full and scaling
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "xxxx").Err())
		require.Equal(t, []interface{}{"Capacity", int64(210), "Size", int64(896), "Number of filters", int64(3), "Number of items inserted", int64(91), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())
	})

	t.Run("MExists Basic Test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Equal(t, []interface{}{int64(0), int64(0), int64(0)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())

		require.Equal(t, int64(1), rdb.Do(ctx, "bf.add", key, "xxx").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.card", key).Val())
		require.Equal(t, []interface{}{int64(1), int64(0)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy").Val())

		require.Equal(t, int64(1), rdb.Do(ctx, "bf.add", key, "zzz").Val())
		require.Equal(t, []interface{}{int64(1), int64(0), int64(1)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())

		require.Equal(t, int64(1), rdb.Do(ctx, "bf.add", key, "yyy").Val())
		require.Equal(t, []interface{}{int64(1), int64(1), int64(1)}, rdb.Do(ctx, "bf.mexists", key, "xxx", "yyy", "zzz").Val())
	})

	t.Run("Get info of no exists key ", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "no_exist_key").Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.info", "no_exist_key").Err(), "ERR key is not found")
		require.NoError(t, rdb.Del(ctx, "no_exist_key").Err())
	})

	t.Run("Get info but wrong arguments", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "2000", "nonscaling").Err())
		require.ErrorContains(t, rdb.Do(ctx, "bf.info", key, "xxx").Err(), "Invalid info argument")
		require.ErrorContains(t, rdb.Do(ctx, "bf.info", key, "capacity", "items").Err(), "wrong number of arguments")
	})

	t.Run("Get all info of bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000", "expansion", "3").Err())
		require.Equal(t, []interface{}{"Capacity", int64(1000), "Size", int64(2048), "Number of filters", int64(1), "Number of items inserted", int64(0), "Expansion rate", int64(3)}, rdb.Do(ctx, "bf.info", key).Val())
	})

	t.Run("Get capacity of bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "2000").Err())
		require.Equal(t, int64(2000), rdb.Do(ctx, "bf.info", key, "capacity").Val())
	})

	t.Run("Get expansion of bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "2000", "expansion", "1").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.info", key, "expansion").Val())
	})

	t.Run("Get reserve default expansion", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000").Err())
		// if not specified expansion, the default expansion value is 2.
		require.Equal(t, int64(2), rdb.Do(ctx, "bf.info", key, "expansion").Val())
	})

	t.Run("Get expansion of nonscaling bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.01", "2000", "nonscaling").Err())
		require.Equal(t, redis.Nil, rdb.Do(ctx, "bf.info", key, "expansion").Err())
	})

	t.Run("Get size of bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000", "expansion", "1").Err())
		require.Equal(t, int64(2048), rdb.Do(ctx, "bf.info", key, "size").Val())
	})

	t.Run("Get items of bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000", "expansion", "1").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.info", key, "items").Val())
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "item").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.info", key, "items").Val())
	})

	t.Run("Bloom filter full and nonscaling", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.0001", "50", "nonscaling").Err())

		// insert items
		var insertNum int64 = 0
		for insertNum < 50 {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.add", key, buf)
			require.NoError(t, Add.Err())
			insertNum = rdb.Do(ctx, "bf.card", key).Val().(int64)
		}
		require.Equal(t, int64(50), rdb.Do(ctx, "bf.info", key, "items").Val())
		require.ErrorContains(t, rdb.Do(ctx, "bf.add", key, "xxx").Err(), "ERR nonscaling filter is full")
		require.Equal(t, int64(50), rdb.Do(ctx, "bf.info", key, "items").Val())
	})

	t.Run("Bloom filter full and scaling", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.0001", "50", "expansion", "2").Err())

		// insert items
		var insertNum int64 = 0
		for insertNum < 50 {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.add", key, buf)
			require.NoError(t, Add.Err())
			insertNum = rdb.Do(ctx, "bf.card", key).Val().(int64)
		}
		require.Equal(t, []interface{}{"Capacity", int64(50), "Size", int64(256), "Number of filters", int64(1), "Number of items inserted", int64(50), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())

		// bloom filter is full and scaling
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "xxx").Err())
		require.Equal(t, []interface{}{"Capacity", int64(150), "Size", int64(768), "Number of filters", int64(2), "Number of items inserted", int64(51), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())

		// insert items
		for insertNum < 150 {
			buf := util.RandString(7, 8, util.Alpha)
			Add := rdb.Do(ctx, "bf.add", key, buf)
			require.NoError(t, Add.Err())
			insertNum = rdb.Do(ctx, "bf.card", key).Val().(int64)
		}
		require.Equal(t, []interface{}{"Capacity", int64(150), "Size", int64(768), "Number of filters", int64(2), "Number of items inserted", int64(150), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())

		// bloom filter is full and scaling
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "xxxx").Err())
		require.Equal(t, []interface{}{"Capacity", int64(350), "Size", int64(1792), "Number of filters", int64(3), "Number of items inserted", int64(151), "Expansion rate", int64(2)}, rdb.Do(ctx, "bf.info", key).Val())
	})

	t.Run("Get type of bloom filter", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000").Err())
		require.Equal(t, "MBbloom--", rdb.Type(ctx, key).Val())
	})

	t.Run("Get Card of bloom filter", func(t *testing.T) {
		// if bf.card no exist key, it would return 0
		require.NoError(t, rdb.Del(ctx, "no_exist_key").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.card", "no_exist_key").Val())

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "bf.reserve", key, "0.02", "1000", "expansion", "1").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.card", key).Val())
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "item1").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.card", key).Val())
		// insert the duplicate key, insert would return 0 and the card of bloom filter would not change
		require.Equal(t, int64(0), rdb.Do(ctx, "bf.add", key, "item1").Val())
		require.Equal(t, int64(1), rdb.Do(ctx, "bf.card", key).Val())
		require.NoError(t, rdb.Do(ctx, "bf.add", key, "item2").Err())
		require.Equal(t, int64(2), rdb.Do(ctx, "bf.card", key).Val())
	})

}
