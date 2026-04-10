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

package hyperloglog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestHyperLogLog(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("basic add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, card)
		addCnt, err := rdb.PFAdd(ctx, "hll", "foo").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)
		card, err = rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, card)
	})

	t.Run("duplicate add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, card)
		addCnt, err := rdb.PFAdd(ctx, "hll", "foo").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)
		addCnt, err = rdb.PFAdd(ctx, "hll", "foo").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, addCnt)
	})

	t.Run("empty add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		addCnt, err := rdb.PFAdd(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, addCnt)

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, card)
	})

	t.Run("multiple add", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())

		addCnt, err := rdb.PFAdd(ctx, "hll", "a", "b", "c", "d").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		addCnt, err = rdb.PFAdd(ctx, "hll", "a", "b", "c").Result()
		require.NoError(t, err)
		require.EqualValues(t, 0, addCnt)

		addCnt, err = rdb.PFAdd(ctx, "hll", "a", "f", "c").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err = rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 5, card)
	})

	t.Run("multiple count", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())
		// Delete hll1, hll2, hll3
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "DEL", fmt.Sprintf("hll%d", i)).Err())
		}

		addCnt, err := rdb.PFAdd(ctx, "hll", "a", "b", "c", "d").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		addCnt, err = rdb.PFAdd(ctx, "hll1", "a", "b", "c", "d").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err := rdb.PFCount(ctx, "hll", "hll1").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		// Order doesn't matter
		card, err = rdb.PFCount(ctx, "hll1", "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		// Count non-exist key
		card, err = rdb.PFCount(ctx, "hll1", "hll2", "hll3").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		addCnt, err = rdb.PFAdd(ctx, "hll2", "1", "2", "3").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err = rdb.PFCount(ctx, "hll", "hll1", "hll2").Result()
		require.NoError(t, err)
		require.EqualValues(t, 7, card)

		// add with overlap
		addCnt, err = rdb.PFAdd(ctx, "hll3", "a", "3", "5").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err = rdb.PFCount(ctx, "hll", "hll1", "hll2", "hll3").Result()
		require.NoError(t, err)
		require.EqualValues(t, 8, card)
	})

	t.Run("basic merge", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "hll").Err())
		// Delete hll1, hll2, hll3, hll4
		for i := 0; i < 4; i++ {
			require.NoError(t, rdb.Do(ctx, "DEL", fmt.Sprintf("hll%d", i)).Err())
		}

		addCnt, err := rdb.PFAdd(ctx, "hll", "a", "b", "c", "d").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		// Empty merge
		mergeCmd, err := rdb.PFMerge(ctx, "hll").Result()
		require.NoError(t, err)
		// mergeCmd result is always "OK"
		require.EqualValues(t, "OK", mergeCmd)

		// Count the merged key
		card, err := rdb.PFCount(ctx, "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		// Merge to hll1
		mergeCmd, err = rdb.PFMerge(ctx, "hll1", "hll").Result()
		require.NoError(t, err)
		require.EqualValues(t, "OK", mergeCmd)

		// Count the merged key
		card, err = rdb.PFCount(ctx, "hll1").Result()
		require.NoError(t, err)
		require.EqualValues(t, 4, card)

		// Add more elements to hll2
		addCnt, err = rdb.PFAdd(ctx, "hll2", "e", "f", "g").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		card, err = rdb.PFCount(ctx, "hll2").Result()
		require.NoError(t, err)
		require.EqualValues(t, 3, card)

		// merge to hll3
		mergeCmd, err = rdb.PFMerge(ctx, "hll3", "hll", "hll1", "hll2").Result()
		require.NoError(t, err)
		require.EqualValues(t, "OK", mergeCmd)

		// Count the merged key
		card, err = rdb.PFCount(ctx, "hll3").Result()
		require.NoError(t, err)
		require.EqualValues(t, 7, card)

		// Add more elements to hll4
		addCnt, err = rdb.PFAdd(ctx, "hll4", "h", "i", "j").Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, addCnt)

		// Merge all to existing hll4
		mergeCmd, err = rdb.PFMerge(ctx, "hll4", "hll", "hll1", "hll2", "hll3").Result()
		require.NoError(t, err)
		require.EqualValues(t, "OK", mergeCmd)

		// Count the merged key
		card, err = rdb.PFCount(ctx, "hll4").Result()
		require.NoError(t, err)
		require.EqualValues(t, 10, card)
	})

	t.Run("PFMERGE into existing dest preserves dest data", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "dest", "src1", "src2").Err())

		_, err := rdb.PFAdd(ctx, "dest", "a", "b", "c").Result()
		require.NoError(t, err)
		_, err = rdb.PFAdd(ctx, "src1", "d", "e").Result()
		require.NoError(t, err)
		_, err = rdb.PFAdd(ctx, "src2", "f").Result()
		require.NoError(t, err)

		// Merge src1 and src2 into existing dest
		_, err = rdb.PFMerge(ctx, "dest", "src1", "src2").Result()
		require.NoError(t, err)

		// dest should have all 6 elements
		card, err := rdb.PFCount(ctx, "dest").Result()
		require.NoError(t, err)
		require.EqualValues(t, 6, card)

		// Merge again into dest to verify idempotency
		_, err = rdb.PFMerge(ctx, "dest", "src1").Result()
		require.NoError(t, err)
		card, err = rdb.PFCount(ctx, "dest").Result()
		require.NoError(t, err)
		require.EqualValues(t, 6, card)
	})

	// PFMERGE must load dest metadata using the namespace-prefixed key (same as storage).
	// If GetMetadata used the raw user key, it would miss the record, keep default Metadata (expire=0),
	// and the following PFMERGE would rewrite metadata without preserving TTL (TTL becomes "no expiry").
	t.Run("PFMERGE into existing dest preserves TTL", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "DEL", "dest_ttl", "src_ttl").Err())

		_, err := rdb.PFAdd(ctx, "dest_ttl", "a", "b").Result()
		require.NoError(t, err)
		ok, err := rdb.Expire(ctx, "dest_ttl", 3600*time.Second).Result()
		require.NoError(t, err)
		require.True(t, ok)

		ttlBefore := rdb.TTL(ctx, "dest_ttl").Val()
		require.Greater(t, ttlBefore, time.Duration(0))

		_, err = rdb.PFAdd(ctx, "src_ttl", "c").Result()
		require.NoError(t, err)
		_, err = rdb.PFMerge(ctx, "dest_ttl", "src_ttl").Result()
		require.NoError(t, err)

		ttlAfter := rdb.TTL(ctx, "dest_ttl").Val()
		require.Greater(t, ttlAfter, time.Duration(0),
			"PFMERGE must keep dest TTL when dest already exists (metadata must be read with ns-prefixed key)")
	})
}
