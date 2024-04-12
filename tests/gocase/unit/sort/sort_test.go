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

package sort

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestSortParser(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SORT Parser", func(t *testing.T) {
		rdb.RPush(ctx, "bad-case-key", 5, 4, 3, 2, 1)

		_, err := rdb.Do(ctx, "Sort").Result()
		require.EqualError(t, err, "ERR wrong number of arguments")

		_, err = rdb.Do(ctx, "Sort", "bad-case-key", "BadArg").Result()
		require.EqualError(t, err, "ERR syntax error")

		_, err = rdb.Do(ctx, "Sort", "bad-case-key", "LIMIT").Result()
		require.EqualError(t, err, "ERR syntax error")

		_, err = rdb.Do(ctx, "Sort", "bad-case-key", "LIMIT", 1).Result()
		require.EqualError(t, err, "ERR syntax error")

		_, err = rdb.Do(ctx, "Sort", "bad-case-key", "LIMIT", 1, "not-number").Result()
		require.EqualError(t, err, "ERR not started as an integer")

		_, err = rdb.Do(ctx, "Sort", "bad-case-key", "STORE").Result()
		require.EqualError(t, err, "ERR syntax error")

		_, err = rdb.Do(ctx, "Sort_RO", "bad-case-key", "STORE", "store_ro_key").Result()
		require.EqualError(t, err, "ERR syntax error")
	})
}

func TestListSort(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SORT Basic", func(t *testing.T) {
		rdb.LPush(ctx, "today_cost", 30, 1.5, 10, 8)

		sortResult, err := rdb.Sort(ctx, "today_cost", &redis.Sort{}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1.5", "8", "10", "30"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "today_cost", &redis.Sort{Order: "ASC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1.5", "8", "10", "30"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "today_cost", &redis.Sort{Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"30", "10", "8", "1.5"}, sortResult)

		sortResult, err = rdb.SortRO(ctx, "today_cost", &redis.Sort{Order: "ASC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1.5", "8", "10", "30"}, sortResult)

		sortResult, err = rdb.SortRO(ctx, "today_cost", &redis.Sort{Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"30", "10", "8", "1.5"}, sortResult)
	})

	t.Run("SORT ALPHA", func(t *testing.T) {
		rdb.LPush(ctx, "website", "www.reddit.com", "www.slashdot.com", "www.infoq.com")

		sortResult, err := rdb.Sort(ctx, "website", &redis.Sort{Alpha: true}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"www.infoq.com", "www.reddit.com", "www.slashdot.com"}, sortResult)

		_, err = rdb.Sort(ctx, "website", &redis.Sort{Alpha: false}).Result()
		require.EqualError(t, err, "One or more scores can't be converted into double")
	})

	t.Run("SORT LIMIT", func(t *testing.T) {
		rdb.RPush(ctx, "rank", 1, 3, 5, 7, 9, 2, 4, 6, 8, 10)

		sortResult, err := rdb.Sort(ctx, "rank", &redis.Sort{Offset: 0, Count: 5}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 0, Count: 5, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"10", "9", "8", "7", "6"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 10, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 10, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 11, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: 2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 11}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: -1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: -2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)
	})

	t.Run("SORT BY + GET", func(t *testing.T) {
		rdb.LPush(ctx, "uid", 1, 2, 3, 4)
		rdb.MSet(ctx, "user_name_1", "admin", "user_name_2", "jack", "user_name_3", "peter", "user_name_4", "mary")
		rdb.MSet(ctx, "user_level_1", 9999, "user_level_2", 10, "user_level_3", 25, "user_level_4", 70)

		sortResult, err := rdb.Sort(ctx, "uid", &redis.Sort{By: "user_level_*"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"admin", "jack", "peter", "mary"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_level_*", Get: []string{"user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"jack", "peter", "mary", "admin"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"9999", "admin", "10", "jack", "25", "peter", "70", "mary"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"#", "user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "9999", "admin", "2", "10", "jack", "3", "25", "peter", "4", "70", "mary"}, sortResult)

		// not sorted
		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4", "3", "2", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 0, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: 2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"3", "2"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: -1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"3", "2", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 0, Count: 1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: 2, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 0, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: -1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Get: []string{"#", "user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4", "70", "mary", "3", "25", "peter", "2", "10", "jack", "1", "9999", "admin"}, sortResult)

		// pattern with hash tag
		rdb.HMSet(ctx, "user_info_1", "name", "admin", "level", 9999)
		rdb.HMSet(ctx, "user_info_2", "name", "jack", "level", 10)
		rdb.HMSet(ctx, "user_info_3", "name", "peter", "level", 25)
		rdb.HMSet(ctx, "user_info_4", "name", "mary", "level", 70)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_info_*->level"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_info_*->level", Get: []string{"user_info_*->name"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"jack", "peter", "mary", "admin"}, sortResult)
	})

	t.Run("SORT STORE", func(t *testing.T) {
		rdb.RPush(ctx, "numbers", 1, 3, 5, 7, 9, 2, 4, 6, 8, 10)

		storedLen, err := rdb.Do(ctx, "Sort", "numbers", "STORE", "sorted-numbers").Result()
		require.NoError(t, err)
		require.Equal(t, int64(10), storedLen)

		sortResult, err := rdb.LRange(ctx, "sorted-numbers", 0, -1).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)
	})
}

func TestSetSort(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SORT Basic", func(t *testing.T) {
		rdb.SAdd(ctx, "today_cost", 30, 1.5, 10, 8)

		sortResult, err := rdb.Sort(ctx, "today_cost", &redis.Sort{}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1.5", "8", "10", "30"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "today_cost", &redis.Sort{Order: "ASC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1.5", "8", "10", "30"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "today_cost", &redis.Sort{Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"30", "10", "8", "1.5"}, sortResult)

		sortResult, err = rdb.SortRO(ctx, "today_cost", &redis.Sort{Order: "ASC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1.5", "8", "10", "30"}, sortResult)

		sortResult, err = rdb.SortRO(ctx, "today_cost", &redis.Sort{Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"30", "10", "8", "1.5"}, sortResult)
	})

	t.Run("SORT ALPHA", func(t *testing.T) {
		rdb.SAdd(ctx, "website", "www.reddit.com", "www.slashdot.com", "www.infoq.com")

		sortResult, err := rdb.Sort(ctx, "website", &redis.Sort{Alpha: true}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"www.infoq.com", "www.reddit.com", "www.slashdot.com"}, sortResult)

		_, err = rdb.Sort(ctx, "website", &redis.Sort{Alpha: false}).Result()
		require.EqualError(t, err, "One or more scores can't be converted into double")
	})

	t.Run("SORT LIMIT", func(t *testing.T) {
		rdb.SAdd(ctx, "rank", 1, 3, 5, 7, 9, 2, 4, 6, 8, 10)

		sortResult, err := rdb.Sort(ctx, "rank", &redis.Sort{Offset: 0, Count: 5}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 0, Count: 5, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"10", "9", "8", "7", "6"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 10, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 10, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 11, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: 2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 11}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: -1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: -2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)
	})

	t.Run("SORT BY + GET", func(t *testing.T) {
		rdb.SAdd(ctx, "uid", 4, 3, 2, 1)
		rdb.MSet(ctx, "user_name_1", "admin", "user_name_2", "jack", "user_name_3", "peter", "user_name_4", "mary")
		rdb.MSet(ctx, "user_level_1", 9999, "user_level_2", 10, "user_level_3", 25, "user_level_4", 70)

		sortResult, err := rdb.Sort(ctx, "uid", &redis.Sort{By: "user_level_*"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"admin", "jack", "peter", "mary"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_level_*", Get: []string{"user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"jack", "peter", "mary", "admin"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"9999", "admin", "10", "jack", "25", "peter", "70", "mary"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"#", "user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "9999", "admin", "2", "10", "jack", "3", "25", "peter", "4", "70", "mary"}, sortResult)

		// not sorted
		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 0, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: 2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: -1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 0, Count: 1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: 2, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 0, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: -1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Get: []string{"#", "user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "9999", "admin", "2", "10", "jack", "3", "25", "peter", "4", "70", "mary"}, sortResult)

		// pattern with hash tag
		rdb.HMSet(ctx, "user_info_1", "name", "admin", "level", 9999)
		rdb.HMSet(ctx, "user_info_2", "name", "jack", "level", 10)
		rdb.HMSet(ctx, "user_info_3", "name", "peter", "level", 25)
		rdb.HMSet(ctx, "user_info_4", "name", "mary", "level", 70)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_info_*->level"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_info_*->level", Get: []string{"user_info_*->name"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"jack", "peter", "mary", "admin"}, sortResult)
	})

	t.Run("SORT STORE", func(t *testing.T) {
		rdb.SAdd(ctx, "numbers", 1, 3, 5, 7, 9, 2, 4, 6, 8, 10)

		storedLen, err := rdb.Do(ctx, "Sort", "numbers", "STORE", "sorted-numbers").Result()
		require.NoError(t, err)
		require.Equal(t, int64(10), storedLen)

		sortResult, err := rdb.LRange(ctx, "sorted-numbers", 0, -1).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)
	})
}

func TestZSetSort(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SORT Basic", func(t *testing.T) {
		rdb.ZAdd(ctx, "today_cost", redis.Z{Score: 30, Member: "1"}, redis.Z{Score: 1.5, Member: "2"}, redis.Z{Score: 10, Member: "3"}, redis.Z{Score: 8, Member: "4"})

		sortResult, err := rdb.Sort(ctx, "today_cost", &redis.Sort{}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "today_cost", &redis.Sort{Order: "ASC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "today_cost", &redis.Sort{Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4", "3", "2", "1"}, sortResult)

		sortResult, err = rdb.SortRO(ctx, "today_cost", &redis.Sort{Order: "ASC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.SortRO(ctx, "today_cost", &redis.Sort{Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4", "3", "2", "1"}, sortResult)
	})

	t.Run("SORT ALPHA", func(t *testing.T) {
		rdb.ZAdd(ctx, "website", redis.Z{Score: 1, Member: "www.reddit.com"}, redis.Z{Score: 2, Member: "www.slashdot.com"}, redis.Z{Score: 3, Member: "www.infoq.com"})

		sortResult, err := rdb.Sort(ctx, "website", &redis.Sort{Alpha: true}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"www.infoq.com", "www.reddit.com", "www.slashdot.com"}, sortResult)

		_, err = rdb.Sort(ctx, "website", &redis.Sort{Alpha: false}).Result()
		require.EqualError(t, err, "One or more scores can't be converted into double")
	})

	t.Run("SORT LIMIT", func(t *testing.T) {
		rdb.ZAdd(ctx, "rank",
			redis.Z{Score: 1, Member: "1"},
			redis.Z{Score: 2, Member: "3"},
			redis.Z{Score: 3, Member: "5"},
			redis.Z{Score: 4, Member: "7"},
			redis.Z{Score: 5, Member: "9"},
			redis.Z{Score: 6, Member: "2"},
			redis.Z{Score: 7, Member: "4"},
			redis.Z{Score: 8, Member: "6"},
			redis.Z{Score: 9, Member: "8"},
			redis.Z{Score: 10, Member: "10"},
		)

		sortResult, err := rdb.Sort(ctx, "rank", &redis.Sort{Offset: 0, Count: 5}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 0, Count: 5, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"10", "9", "8", "7", "6"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 10, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 10, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: 11, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: 2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -1, Count: 11}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: -1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "rank", &redis.Sort{Offset: -2, Count: -2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)
	})

	t.Run("SORT BY + GET", func(t *testing.T) {
		rdb.ZAdd(ctx, "uid",
			redis.Z{Score: 1, Member: "4"},
			redis.Z{Score: 2, Member: "3"},
			redis.Z{Score: 3, Member: "2"},
			redis.Z{Score: 4, Member: "1"})

		rdb.MSet(ctx, "user_name_1", "admin", "user_name_2", "jack", "user_name_3", "peter", "user_name_4", "mary")
		rdb.MSet(ctx, "user_level_1", 9999, "user_level_2", 10, "user_level_3", 25, "user_level_4", 70)

		sortResult, err := rdb.Sort(ctx, "uid", &redis.Sort{By: "user_level_*"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"admin", "jack", "peter", "mary"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_level_*", Get: []string{"user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"jack", "peter", "mary", "admin"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"9999", "admin", "10", "jack", "25", "peter", "70", "mary"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{Get: []string{"#", "user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "9999", "admin", "2", "10", "jack", "3", "25", "peter", "4", "70", "mary"}, sortResult)

		// not sorted
		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4", "3", "2", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 0, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: 2}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"3", "2"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 0}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: -1}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"3", "2", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 0, Count: 1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: 2, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 4, Count: 0, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Offset: 1, Count: -1, Order: "DESC"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "not-exists-key", Get: []string{"#", "user_level_*", "user_name_*"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"4", "70", "mary", "3", "25", "peter", "2", "10", "jack", "1", "9999", "admin"}, sortResult)

		// pattern with hash tag
		rdb.HMSet(ctx, "user_info_1", "name", "admin", "level", 9999)
		rdb.HMSet(ctx, "user_info_2", "name", "jack", "level", 10)
		rdb.HMSet(ctx, "user_info_3", "name", "peter", "level", 25)
		rdb.HMSet(ctx, "user_info_4", "name", "mary", "level", 70)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_info_*->level"}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "4", "1"}, sortResult)

		sortResult, err = rdb.Sort(ctx, "uid", &redis.Sort{By: "user_info_*->level", Get: []string{"user_info_*->name"}}).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"jack", "peter", "mary", "admin"}, sortResult)
	})

	t.Run("SORT STORE", func(t *testing.T) {
		rdb.ZAdd(ctx, "numbers",
			redis.Z{Score: 1, Member: "1"},
			redis.Z{Score: 2, Member: "3"},
			redis.Z{Score: 3, Member: "5"},
			redis.Z{Score: 4, Member: "7"},
			redis.Z{Score: 5, Member: "9"},
			redis.Z{Score: 6, Member: "2"},
			redis.Z{Score: 7, Member: "4"},
			redis.Z{Score: 8, Member: "6"},
			redis.Z{Score: 9, Member: "8"},
			redis.Z{Score: 10, Member: "10"},
		)

		storedLen, err := rdb.Do(ctx, "Sort", "numbers", "STORE", "sorted-numbers").Result()
		require.NoError(t, err)
		require.Equal(t, int64(10), storedLen)

		sortResult, err := rdb.LRange(ctx, "sorted-numbers", 0, -1).Result()
		require.NoError(t, err)
		require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, sortResult)
	})
}
