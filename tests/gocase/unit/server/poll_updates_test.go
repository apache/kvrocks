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

package server

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

type PollUpdatesResult struct {
	LatestSeq int64
	NextSeq   int64
	Updates   []string
}

func sliceToPollUpdatesResult(t *testing.T, slice []interface{}) *PollUpdatesResult {
	require.Len(t, slice, 8)

	require.Equal(t, "latest_sequence", slice[0])
	latestSeq, ok := slice[1].(int64)
	require.True(t, ok)

	require.Equal(t, "format", slice[2])
	require.Equal(t, "RAW", slice[3])
	require.Equal(t, "updates", slice[4])
	updates := make([]string, 0)
	if slice[5] != nil {
		fields, ok := slice[5].([]interface{})
		require.True(t, ok)
		for _, field := range fields {
			str, ok := field.(string)
			require.True(t, ok)
			updates = append(updates, str)
		}
	}

	require.Equal(t, "next_sequence", slice[6])
	nextSeq, ok := slice[7].(int64)
	require.True(t, ok)

	return &PollUpdatesResult{
		LatestSeq: latestSeq,
		NextSeq:   nextSeq,
		Updates:   updates,
	}
}

func TestPollUpdates_Basic(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{})
	defer srv0.Close()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	srv1 := util.StartServer(t, map[string]string{})
	defer srv1.Close()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()

	t.Run("Make sure the command POLLUPDATES works well", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			rdb0.Set(ctx, fmt.Sprintf("key-%d", i), i, 0)
		}

		updates := make([]string, 0)
		slice, err := rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 6).Slice()
		require.NoError(t, err)
		pollUpdates := sliceToPollUpdatesResult(t, slice)
		require.EqualValues(t, 10, pollUpdates.LatestSeq)
		require.EqualValues(t, 6, pollUpdates.NextSeq)
		require.Len(t, pollUpdates.Updates, 6)
		updates = append(updates, pollUpdates.Updates...)

		slice, err = rdb0.Do(ctx, "POLLUPDATES", pollUpdates.NextSeq, "MAX", 6).Slice()
		require.NoError(t, err)
		pollUpdates = sliceToPollUpdatesResult(t, slice)
		require.EqualValues(t, 10, pollUpdates.LatestSeq)
		require.EqualValues(t, 10, pollUpdates.NextSeq)
		require.Len(t, pollUpdates.Updates, 4)
		updates = append(updates, pollUpdates.Updates...)

		for i := 0; i < 10; i++ {
			batch, err := hex.DecodeString(updates[i])
			require.NoError(t, err)
			applied, err := rdb1.Do(ctx, "APPLYBATCH", batch).Bool()
			require.NoError(t, err)
			require.True(t, applied)
			require.EqualValues(t, strconv.Itoa(i), rdb1.Get(ctx, fmt.Sprintf("key-%d", i)).Val())
		}
	})

	t.Run("Runs POLLUPDATES with invalid arguments", func(t *testing.T) {
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", -1).Err(),
			"ERR out of numeric range")
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 1001).Err(),
			"ERR out of numeric range")
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 0, "FORMAT", "COMMAND").Err(),
			"ERR invalid FORMAT option, only support RAW")
		require.ErrorContains(t, rdb0.Do(ctx, "POLLUPDATES", 12, "FORMAT", "RAW").Err(),
			"ERR next sequence is out of range")
		require.Error(t, rdb0.Do(ctx, "POLLUPDATES", 1, "FORMAT", "EXTRA").Err())
	})
}

func TestPollUpdates_WithStrict(t *testing.T) {
	ctx := context.Background()

	srv0 := util.StartServer(t, map[string]string{})
	defer srv0.Close()
	rdb0 := srv0.NewClient()
	defer func() { require.NoError(t, rdb0.Close()) }()

	srv1 := util.StartServer(t, map[string]string{})
	defer srv1.Close()
	rdb1 := srv1.NewClient()
	defer func() { require.NoError(t, rdb1.Close()) }()

	// The latest sequence is 2 after running the HSET command, 1 for the metadata and 1 for the field
	require.NoError(t, rdb0.HSet(ctx, "h0", "f0", "v0").Err())
	// The latest sequence is 3 after running the SET command
	require.NoError(t, rdb0.Set(ctx, "k0", "v0", 0).Err())

	// PollUpdates with strict mode should return an error if the sequence number is mismatched
	err := rdb0.Do(ctx, "POLLUPDATES", 1, "MAX", 1, "STRICT").Err()
	require.ErrorContains(t, err, "ERR mismatched sequence number")

	// Works well if the sequence number is mismatched but not in strict mode
	require.NoError(t, rdb0.Do(ctx, "POLLUPDATES", 1, "MAX", 1).Err())

	slice, err := rdb0.Do(ctx, "POLLUPDATES", 0, "MAX", 10, "STRICT").Slice()
	require.NoError(t, err)
	pollUpdates := sliceToPollUpdatesResult(t, slice)
	require.EqualValues(t, 3, pollUpdates.LatestSeq)
	require.EqualValues(t, 3, pollUpdates.NextSeq)
	require.Len(t, pollUpdates.Updates, 2)

	for _, update := range pollUpdates.Updates {
		batch, err := hex.DecodeString(update)
		require.NoError(t, err)
		require.NoError(t, rdb1.Do(ctx, "APPLYBATCH", batch).Err())
	}

	require.Equal(t, "v0", rdb1.Get(ctx, "k0").Val())
	require.Equal(t, "v0", rdb1.HGet(ctx, "h0", "f0").Val())
}
