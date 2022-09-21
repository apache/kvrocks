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

package command

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestInfo(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	MustAtoi := func(t *testing.T, s string) int {
		i, err := strconv.Atoi(s)
		require.NoError(t, err)
		return i
	}

	t.Run("get rocksdb ops by INFO", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			k := fmt.Sprintf("key%d", i)
			v := fmt.Sprintf("value%d", i)
			for j := 0; j < 500; j++ {
				rdb.LPush(ctx, k, v)
				rdb.LRange(ctx, k, 0, 1)
			}
			time.Sleep(time.Second)
		}

		r := util.FindInfoEntry(t, ctx, rdb, "put_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
		r = util.FindInfoEntry(t, ctx, rdb, "get_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
		r = util.FindInfoEntry(t, ctx, rdb, "seek_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
		r = util.FindInfoEntry(t, ctx, rdb, "next_per_sec", "rocksdb")
		require.Greater(t, MustAtoi(t, r), 0)
	})

	t.Run("get bgsave information by INFO", func(t *testing.T) {
		require.Equal(t, "0", util.FindInfoEntry(t, ctx, rdb, "bgsave_in_progress", "persistence"))
		require.Equal(t, "-1", util.FindInfoEntry(t, ctx, rdb, "last_bgsave_time", "persistence"))
		require.Equal(t, "ok", util.FindInfoEntry(t, ctx, rdb, "last_bgsave_status", "persistence"))
		require.Equal(t, "-1", util.FindInfoEntry(t, ctx, rdb, "last_bgsave_time_sec", "persistence"))

		r := rdb.Do(ctx, "bgsave")
		v, err := r.Text()
		require.NoError(t, err)
		require.Equal(t, "OK", v)

		require.Eventually(t, func() bool {
			e := MustAtoi(t, util.FindInfoEntry(t, ctx, rdb, "bgsave_in_progress", "persistence"))
			return e == 0
		}, 5*time.Second, 100*time.Millisecond)

		lastBgsaveTime := MustAtoi(t, util.FindInfoEntry(t, ctx, rdb, "last_bgsave_time", "persistence"))
		require.Greater(t, lastBgsaveTime, 1640507660)
		require.Equal(t, "ok", util.FindInfoEntry(t, ctx, rdb, "last_bgsave_status", "persistence"))
		lastBgsaveTimeSec := MustAtoi(t, util.FindInfoEntry(t, ctx, rdb, "last_bgsave_time_sec", "persistence"))
		require.GreaterOrEqual(t, lastBgsaveTimeSec, 0)
		require.Less(t, lastBgsaveTimeSec, 3)
	})
}
