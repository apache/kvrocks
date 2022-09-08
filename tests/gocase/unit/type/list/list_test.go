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

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
	"modernc.org/mathutil"
)

// We need a value larger than list-max-ziplist-value to make sure
// the list has the right encoding when it is swapped in again.
var largeValue = map[string]string{
	"zipList":    "hello",
	"linkedList": strings.Repeat("hello", 4),
}

func BenchmarkLTRIM(b *testing.B) {
	srv := util.StartServer(b, map[string]string{
		"list-max-ziplist-size": "4",
	})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(b, rdb.Close()) }()

	key := "myList"
	startLen := int64(32)

	rand.Seed(0)
	for typ, value := range largeValue {
		b.Run(fmt.Sprintf("LTRIM stress testing - %s", typ), func(b *testing.B) {
			var myList []string
			require.NoError(b, rdb.Del(ctx, key).Err())
			require.NoError(b, rdb.RPush(ctx, key, value).Err())
			myList = append(myList, value)

			for i := int64(0); i < startLen; i++ {
				s := strconv.FormatInt(rand.Int63(), 10)
				require.NoError(b, rdb.RPush(ctx, key, s).Err())
				myList = append(myList, s)
			}

			for i := 0; i < 1000; i++ {
				lo := int64(rand.Float64() * float64(startLen))
				hi := int64(float64(lo) + rand.Float64()*float64(startLen))

				myList = myList[lo:mathutil.Min(int(hi+1), len(myList))]
				require.NoError(b, rdb.LTrim(ctx, key, lo, hi).Err())
				require.Equal(b, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed trim")

				starting := rdb.LLen(ctx, key).Val()
				for j := starting; j < startLen; j++ {
					s := strconv.FormatInt(rand.Int63(), 10)
					require.NoError(b, rdb.RPush(ctx, key, s).Err())
					myList = append(myList, s)
					require.Equal(b, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed append match")
				}
			}
		})
	}
}
