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
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func randInt64Str() string {
	return strconv.FormatInt(rand.Int63(), 10)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Test(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "myList"
	rand.Seed(0)
	for typ, largeStr := range LargeValue {
		t.Run(fmt.Sprintf("LTRIM stress testing - %s", typ), func(t *testing.T) {
			myList := []string{}
			startLen := 32
			rdb.Del(ctx, key)
			rdb.RPush(ctx, key, largeStr)
			myList = append(myList, largeStr)
			for i := 0; i < 1000; i++ {
				s := randInt64Str()
				rdb.RPush(ctx, key, s)
				myList = append(myList, s)
			}
			for i := 0; i < 1000; i++ {
				low := int(rand.Float64() * float64(startLen))
				high := int(float64(low) + rand.Float64()*float64(startLen))
				myList = myList[low:min(high+1, len(myList))]
				rdb.Do(ctx, "LTRIM", key, low, high)
				require.Equal(t, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed trim")
				starting := rdb.LLen(ctx, key).Val()
				for j := starting; j < int64(startLen); j++ {
					s := randInt64Str()
					rdb.RPush(ctx, key, s)
					myList = append(myList, s)
					require.Equal(t, myList, rdb.LRange(ctx, key, 0, -1).Val(), "failed append match")
				}
			}
		})
	}

}
