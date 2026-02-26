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

package flushslots

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestFlushSlots(t *testing.T) {
	srv := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer srv.Close()

	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	ctx := context.Background()
	nodeID := "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
	require.NoError(t, rdb.Do(ctx, "clusterx", "SETNODEID", nodeID).Err())
	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383", nodeID, srv.Host(), srv.Port())
	require.NoError(t, rdb.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	slotKeys := []string{"{3560}key", "{22179}key", "{48756}key", "{2977}key", "{4569}key",
		"{460}key", "{4384}key", "{41432}key", "{46920}key", "{9073}key"}
	initKeys := func(cli *redis.Client) error {
		if err := cli.FlushDB(ctx).Err(); err != nil {
			return err
		}
		for _, key := range slotKeys {
			if err := cli.Set(ctx, key, "value", 0).Err(); err != nil {
				return err
			}
		}
		return nil
	}

	t.Run("Flush slots", func(t *testing.T) {
		require.NoError(t, initKeys(rdb))

		r, err := rdb.Do(ctx, "flushslots", "0 2 4 6-9").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", r)

		expectedRemainingKeys := []string{slotKeys[1], slotKeys[3], slotKeys[5]}
		keys, err := rdb.Keys(ctx, "*").Result()
		require.NoError(t, err)
		require.ElementsMatch(t, keys, expectedRemainingKeys)
	})
}
