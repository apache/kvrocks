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

package stream

import (
	"context"
	"fmt"
	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStreamOffset(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XADD advances the entries-added counter and sets the recorded-first-entry-id", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "x",
			ID:     "1-0",
			Values: []string{"data", "a"},
		}).Err())
		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 1, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "x",
			ID:     "2-0",
			Values: []string{"data", "a"},
		}).Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 2, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)
	})

	t.Run("XDEL/TRIM are reflected by recorded first entry", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 0; i < 5; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i+1),
				Values: []string{"data", "a"},
			}).Err())
		}

		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.EqualValues(t, 5, r.EntriesAdded)
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "2-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "1-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "1-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "3-0", r.RecordedFirstEntryID)

		require.NoError(t, rdb.XTrimMaxLen(ctx, "x", 2).Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "4-0", r.RecordedFirstEntryID)
	})

	t.Run("Maximum XDEL ID behaves correctly", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())

		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: "x",
				ID:     fmt.Sprintf("%d-0", i+1),
				Values: []string{"data", fmt.Sprintf("%c", 'a'+i)},
			}).Err())
		}

		r := rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "0-0", r.MaxDeletedEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "2-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "2-0", r.MaxDeletedEntryID)

		require.NoError(t, rdb.XDel(ctx, "x", "1-0").Err())
		r = rdb.XInfoStreamFull(ctx, "x", 0).Val()
		require.Equal(t, "2-0", r.MaxDeletedEntryID)
	})
}
