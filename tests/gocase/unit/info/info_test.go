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
	"regexp"
	"strconv"
	"strings"
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

	FindInfoEntry := func(t *testing.T, section string, key string) string {
		r := rdb.Info(ctx, section)
		p := regexp.MustCompile(fmt.Sprintf("%s:(.+)", key))
		ms := p.FindStringSubmatch(r.Val())
		require.Len(t, ms, 2)
		return strings.TrimSpace(ms[1])
	}

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

		r := FindInfoEntry(t, "rocksdb", "put_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
		r = FindInfoEntry(t, "rocksdb", "get_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
		r = FindInfoEntry(t, "rocksdb", "seek_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
		r = FindInfoEntry(t, "rocksdb", "next_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
	})

	t.Run("get bgsave information by INFO", func(t *testing.T) {
		require.Equal(t, "0", FindInfoEntry(t, "persistence", "bgsave_in_progress"))
		require.Equal(t, "-1", FindInfoEntry(t, "persistence", "last_bgsave_time"))
		require.Equal(t, "ok", FindInfoEntry(t, "persistence", "last_bgsave_status"))
		require.Equal(t, "-1", FindInfoEntry(t, "persistence", "last_bgsave_time_sec"))

		r := rdb.Do(ctx, "bgsave")
		v, err := r.Text()
		require.NoError(t, err)
		require.Equal(t, "OK", v)

		require.Eventually(t, func() bool {
			e := MustAtoi(t, FindInfoEntry(t, "persistence", "bgsave_in_progress"))
			return e == 0
		}, 5*time.Second, 100*time.Millisecond)

		lastBgsaveTime := MustAtoi(t, FindInfoEntry(t, "persistence", "last_bgsave_time"))
		require.Greater(t, lastBgsaveTime, 1640507660)
		require.Equal(t, "ok", FindInfoEntry(t, "persistence", "last_bgsave_status"))
		lastBgsaveTimeSec := MustAtoi(t, FindInfoEntry(t, "persistence", "last_bgsave_time_sec"))
		require.GreaterOrEqual(t, lastBgsaveTimeSec, 0)
		require.Less(t, lastBgsaveTimeSec, 3)
	})
}
