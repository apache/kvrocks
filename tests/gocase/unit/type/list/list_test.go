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

package list

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"modernc.org/mathutil"
)

// We need a value larger than list-max-ziplist-value to make sure
// the list has the right encoding when it is swapped in again.
var largeValue = map[string]string{
	"zipList":    "hello",
	"linkedList": strings.Repeat("hello", 4),
}

func TestLTRIM(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"list-max-ziplist-size": "4",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "myList"
	startLen := int64(32)

	rand.Seed(0)
	for typ, value := range largeValue {
		t.Run(fmt.Sprintf("LTRIM stress testing - %s", typ), func(t *testing.T) {
			var myList []string
			require.NoError(t, rdb.Del(ctx, key).Err())
			require.NoError(t, rdb.RPush(ctx, key, value).Err())
			myList = append(myList, value)

			for i := int64(0); i < startLen; i++ {
				s := strconv.FormatInt(rand.Int63(), 10)
				require.NoError(t, rdb.RPush(ctx, key, s).Err())
				myList = append(myList, s)
			}

			for i := 0; i < 1000; i++ {
				lo := int64(rand.Float64() * float64(startLen))
				hi := int64(float64(lo) + rand.Float64()*float64(startLen))

				myList = myList[lo:mathutil.Min(int(hi+1), len(myList))]
				require.NoError(t, rdb.LTrim(ctx, key, lo, hi).Err())
				require.Equal(t, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed trim")

				starting := rdb.LLen(ctx, key).Val()
				for j := starting; j < startLen; j++ {
					s := strconv.FormatInt(rand.Int63(), 10)
					require.NoError(t, rdb.RPush(ctx, key, s).Err())
					myList = append(myList, s)
					require.Equal(t, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed append match")
				}
			}
		})
	}
}

func TestZipList(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"list-max-ziplist-size": "16",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClientWithOption(&redis.Options{
		ReadTimeout: 10 * time.Second,
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	rand.Seed(0)

	t.Run("Explicit regression for a list bug", func(t *testing.T) {
		key := "l"
		myList := []string{
			"49376042582", "BkG2o\\pIC]4YYJa9cJ4GWZalG[4tin;1D2whSkCOW`mX;SFXGyS8sedcff3fQI^tgPCC@^Nu1J6o]meM@Lko]t_jRyo<xSJ1oObDYd`ppZuW6P@fS278YaOx=s6lvdFlMbP0[SbkI^Kr\\HBXtuFaA^mDx:yzS4a[skiiPWhT<nNfAf=aQVfclcuwDrfe;iVuKdNvB9kbfq>tK?tH[\\EvWqS]b`o2OCtjg:?nUTwdjpcUm]y:pg5q24q7LlCOwQE^",
		}
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.RPush(ctx, key, myList[0]).Err())
		require.NoError(t, rdb.RPush(ctx, key, myList[1]).Err())
		require.Equal(t, myList[0], rdb.LIndex(ctx, key, 0).Val())
		require.Equal(t, myList[1], rdb.LIndex(ctx, key, 1).Val())
	})

	t.Run("Regression for quicklist #3343 bug", func(t *testing.T) {
		key := "myList"
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.LPush(ctx, key, "401").Err())
		require.NoError(t, rdb.LPush(ctx, key, "392").Err())

		require.NoError(t, rdb.RPush(ctx, key, fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 5105), "799")).Err())
		require.NoError(t, rdb.LSet(ctx, key, -1, fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 1014), "702")).Err())

		require.NoError(t, rdb.LPop(ctx, key).Err())

		require.NoError(t, rdb.LSet(ctx, key, -1, fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 4149), "852")).Err())

		require.NoError(t, rdb.LInsert(ctx, key, "before", "401", fmt.Sprintf("%s\"%s\"", strings.Repeat("x", 9927), "12")).Err())
		require.NoError(t, rdb.LRange(ctx, key, 0, -1).Err())
		require.Equal(t, rdb.Ping(ctx).Val(), "PONG")
	})

	t.Run("Stress tester for #3343-alike bugs", func(t *testing.T) {
		key := "key"
		require.NoError(t, rdb.Del(ctx, key).Err())
		for i := 0; i < 10000; i++ {
			op := rand.Int63n(6)
			randCnt := 5 - rand.Int63n(10)
			var ele string
			if rand.Int31n(2) == 0 {
				ele = fmt.Sprintf("%d", rand.Int63n(1000))
			} else {
				ele = fmt.Sprintf("%s%d", strings.Repeat("x", int(rand.Int63n(10000))), 1)
			}
			switch op {
			case 0:
				require.NoError(t, rdb.LPush(ctx, key, ele).Err())
			case 1:
				require.NoError(t, rdb.RPush(ctx, key, ele).Err())
			case 2:
				rdb.LPop(ctx, key)
			case 3:
				rdb.RPop(ctx, key)
			case 4:
				rdb.LSet(ctx, key, randCnt, ele)
			case 5:
				otherEle := fmt.Sprintf("%d", rand.Int63n(1000))
				var where string
				if rand.Int31n(2) == 0 {
					where = "before"
				} else {
					where = "after"
				}
				require.NoError(t, rdb.LInsert(ctx, key, where, otherEle, ele).Err())
			}
		}
	})

	t.Run("ziplist implementation: value encoding and backlink", func(t *testing.T) {
		iterations := 100
		key := "l"
		for j := 0; j < iterations; j++ {
			require.NoError(t, rdb.Del(ctx, key).Err())
			var lis []string
			for i := 0; i < 200; i++ {
				op := rand.Int63n(7)
				data := ""
				switch op {
				case 0:
					data = strings.Repeat("x", int(rand.Int63n(1000000)))
				case 1:
					data = fmt.Sprintf("%d", rand.Int63n(65536))
				case 2:
					data = fmt.Sprintf("%d", rand.Int63n(4294967296))
				case 3:
					data = fmt.Sprintf("%d", rand.Uint64())
				case 4:
					data = fmt.Sprintf("-%d", rand.Int63n(65536))
					if data == "-0" {
						data = "0"
					}
				case 5:
					data = fmt.Sprintf("-%d", rand.Int63n(4294967296))
					if data == "-0" {
						data = "0"
					}
				case 6:
					data = fmt.Sprintf("-%d", rand.Uint64())
					if data == "-0" {
						data = "0"
					}
				}
				lis = append(lis, data)
				require.NoError(t, rdb.RPush(ctx, key, data).Err())
			}
			require.Equal(t, int64(len(lis)), rdb.LLen(ctx, key).Val())

			for i := 199; i >= 0; i-- {
				require.Equal(t, lis[i], rdb.LIndex(ctx, key, int64(i)).Val())
			}
		}
	})

	t.Run("ziplist implementation: encoding stress testing", func(t *testing.T) {
		key := "l"
		for j := 0; j < 200; j++ {
			require.NoError(t, rdb.Del(ctx, key).Err())
			var lis []string
			l := int(rand.Int63n(400))
			for i := 0; i < l; i++ {
				rv := util.RandomValue()
				util.RandPathNoResult(
					func() {
						lis = append(lis, rv)
						require.NoError(t, rdb.RPush(ctx, key, rv).Err())
					},
					func() {
						lis = append([]string{rv}, lis...)
						require.NoError(t, rdb.LPush(ctx, key, rv).Err())
					},
				)
			}
			require.Equal(t, int64(len(lis)), rdb.LLen(ctx, key).Val())
			for i := 0; i < l; i++ {
				require.Equal(t, lis[i], rdb.LIndex(ctx, key, int64(i)).Val())
			}
		}
	})
}
