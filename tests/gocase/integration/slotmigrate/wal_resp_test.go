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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slotmigrate

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestSlotMigratePreservesWALToRESPCommands(t *testing.T) {
	ctx := context.Background()
	slot := 2
	tag := fmt.Sprintf("{%s}", util.SlotTable[slot])
	hashKey := "hash" + tag
	setKey := "set" + tag
	listA := "list-a" + tag
	listB := "list-b" + tag

	source := util.StartServer(t, util.KvrocksServerConfigs{
		"cluster-enabled":             "yes",
		"migrate-batch-size-kb":       "1",
		"migrate-batch-rate-limit-mb": "1",
	})
	defer source.Close()
	src := source.NewClient()
	defer func() { require.NoError(t, src.Close()) }()
	sourceID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx00"
	require.NoError(t, src.Do(ctx, "CLUSTERX", "SETNODEID", sourceID).Err())

	destination := util.StartServer(t, util.KvrocksServerConfigs{"cluster-enabled": "yes"})
	defer destination.Close()
	dst := destination.NewClient()
	defer func() { require.NoError(t, dst.Close()) }()
	destinationID := "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx01"
	require.NoError(t, dst.Do(ctx, "CLUSTERX", "SETNODEID", destinationID).Err())

	nodes := fmt.Sprintf("%s %s %d master - 0-10000\n%s %s %d master - 10001-16383",
		sourceID, source.Host(), source.Port(), destinationID, destination.Host(), destination.Port())
	require.NoError(t, src.Do(ctx, "CLUSTERX", "SETNODES", nodes, "1").Err())
	require.NoError(t, dst.Do(ctx, "CLUSTERX", "SETNODES", nodes, "1").Err())

	initialList := []any{"a", "b", "c", "d", "e"}
	for _, key := range []string{listA, listB} {
		require.NoError(t, src.RPush(ctx, key, initialList...).Err())
	}
	// Keep snapshot transfer running while the writes below enter the incremental WAL.
	require.NoError(t, src.Set(ctx, "0-filler"+tag, strings.Repeat("x", 4<<20), 0).Err())
	require.NoError(t, src.Do(ctx, "CLUSTERX", "MIGRATE", slot, destinationID).Err())
	waitForImportState(t, dst, slot, "start")
	requireMigrateState(t, src, slot, SlotMigrationStateStarted)

	hashFields := map[string]string{
		"a-large": strings.Repeat("a", 32<<10),
		"b-small": "small",
		"c-large": strings.Repeat("c", 32<<10),
	}
	setMembers := []string{strings.Repeat("a", 32<<10), strings.Repeat("b", 32<<10), strings.Repeat("c", 32<<10)}
	require.NoError(t, src.HSet(ctx, hashKey, "a-large", hashFields["a-large"], "b-small", hashFields["b-small"],
		"c-large", hashFields["c-large"]).Err())
	require.NoError(t, src.SAdd(ctx, setKey, setMembers[0], setMembers[1], setMembers[2]).Err())
	require.NoError(t, src.LTrim(ctx, listA, 1, 3).Err())
	require.NoError(t, src.LTrim(ctx, listB, 1, 3).Err())
	waitForMigrateStateInDuration(t, src, slot, SlotMigrationStateSuccess, time.Minute)
	waitForImportState(t, dst, slot, SlotImportStateSuccess)

	require.Len(t, dst.HGetAll(ctx, hashKey).Val(), len(hashFields))
	for field, value := range hashFields {
		require.Equal(t, value, dst.HGet(ctx, hashKey, field).Val())
	}
	require.Equal(t, int64(len(setMembers)), dst.SCard(ctx, setKey).Val())
	for i, member := range setMembers {
		require.True(t, dst.SIsMember(ctx, setKey, member).Val(), "missing set member %d", i)
	}
	for _, key := range []string{listA, listB} {
		require.Equal(t, []string{"b", "c", "d"}, dst.LRange(ctx, key, 0, -1).Val())
	}

	result, err := dst.Do(ctx, "POLLUPDATES", 0, "MAX", 1000, "FORMAT", "RESP").Result()
	require.NoError(t, err)
	updates := result.(map[any]any)["updates"].([]any)
	require.Equal(t, "default", updates[0])

	replay := util.StartServer(t, util.KvrocksServerConfigs{})
	defer replay.Close()
	replayClient := replay.NewClient()
	defer func() { require.NoError(t, replayClient.Close()) }()
	for _, key := range []string{listA, listB} {
		require.NoError(t, replayClient.RPush(ctx, key, initialList...).Err())
	}

	for _, rawCommand := range updates[1].([]any) {
		rawTokens := rawCommand.([]any)
		tokens := make([]any, len(rawTokens))
		for i, token := range rawTokens {
			tokens[i] = token.(string)
		}
		if len(tokens) < 2 {
			continue
		}
		key := tokens[1].(string)
		if key == hashKey || key == setKey || key == listA || key == listB {
			if tokens[0] == "HSET" || tokens[0] == "SADD" || tokens[0] == "LTRIM" {
				require.NoError(t, replayClient.Do(ctx, tokens...).Err())
			}
		}
	}

	require.Len(t, replayClient.HGetAll(ctx, hashKey).Val(), len(hashFields))
	for field, value := range hashFields {
		require.Equal(t, value, replayClient.HGet(ctx, hashKey, field).Val())
	}
	require.Equal(t, int64(len(setMembers)), replayClient.SCard(ctx, setKey).Val())
	for i, member := range setMembers {
		require.True(t, replayClient.SIsMember(ctx, setKey, member).Val(), "missing set member %d", i)
	}
	for _, key := range []string{listA, listB} {
		require.Equal(t, []string{"b", "c", "d"}, replayClient.LRange(ctx, key, 0, -1).Val())
	}
}
