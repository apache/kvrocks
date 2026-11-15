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

package replication

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func pinnedSample(info string) (uint64, int64, error) {
	var pinned string
	var age string
	for _, line := range strings.Split(info, "\r\n") {
		if value, found := strings.CutPrefix(line, "block_cache_pinned_usage[default]:"); found {
			pinned = value
		}
		if value, found := strings.CutPrefix(line, "block_cache_pinned_usage_sample_age_ms:"); found {
			age = value
		}
	}
	if pinned == "" && age == "" {
		return 0, -1, nil
	}
	value, err := strconv.ParseUint(pinned, 10, 64)
	if err != nil {
		return 0, 0, err
	}
	ageMS, err := strconv.ParseInt(age, 10, 64)
	if err != nil || ageMS < 0 {
		return 0, 0, fmt.Errorf("invalid sample age %q: %v", age, err)
	}
	return value, ageMS, nil
}

func waitForPinnedSample(t *testing.T, client *redis.Client) {
	t.Helper()
	started := time.Now()
	require.Eventually(t, func() bool {
		info, err := client.Info(context.Background(), "rocksdb").Result()
		if err != nil {
			return false
		}
		_, age, err := pinnedSample(info)
		// Require a background sample newer than this wait, not just the Open sample.
		return err == nil && age >= 0 && age < time.Since(started).Milliseconds()-100
	}, 10*time.Second, 50*time.Millisecond)
}

func TestInfoPinnedUsageSamplingAcrossFullSync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	replica := util.StartServer(t, map[string]string{
		"workers":                        "4",
		"rocksdb.block_cache_size":       "32",
		"use-rsid-psync":                 "yes",
		"slave-empty-db-before-fullsync": "yes",
		"fullsync-recv-file-delay":       "1",
	})
	defer replica.Close()
	client := replica.NewClient()
	defer func() { require.NoError(t, client.Close()) }()
	require.NoError(t, client.Set(ctx, "before-full-sync", "old-generation", 0).Err())
	waitForPinnedSample(t, client)

	stop := make(chan struct{})
	errors := make(chan error, 8)
	var monitors sync.WaitGroup
	for i := 0; i < cap(errors); i++ {
		monitors.Add(1)
		go func() {
			defer monitors.Done()
			monitor := replica.NewClient()
			defer func() { _ = monitor.Close() }()
			for {
				select {
				case <-stop:
					return
				default:
				}
				info, err := monitor.Info(ctx, "persistence", "rocksdb").Result()
				if err == nil {
					var age int64
					_, age, err = pinnedSample(info)
					if err == nil && age < 0 && !strings.Contains(info, "\r\nloading:1\r\n") {
						// Loading can start between the two INFO sections. Confirm that
						// transition instead of accepting missing fields during service.
						info, err = monitor.Info(ctx, "persistence", "rocksdb").Result()
						if err == nil {
							_, age, err = pinnedSample(info)
							if err == nil && age < 0 && !strings.Contains(info, "\r\nloading:1\r\n") {
								err = fmt.Errorf("pinned sample missing outside loading: %s", info)
							}
						}
					}
				}
				if err != nil {
					errors <- err
					return
				}
			}
		}()
	}
	defer func() {
		close(stop)
		monitors.Wait()
		close(errors)
		for err := range errors {
			require.NoError(t, err)
		}
	}()

	for round := 0; round < 2; round++ {
		primary := util.StartServer(t, map[string]string{
			"rocksdb.block_cache_size": "32",
			"use-rsid-psync":           "yes",
		})
		defer primary.Close()
		writer := primary.NewClient()
		defer func() { require.NoError(t, writer.Close()) }()
		value := fmt.Sprintf("generation-%d:%s", round, strings.Repeat("x", 1024))
		pipe := writer.Pipeline()
		for i := 0; i < 1024; i++ {
			pipe.Set(ctx, fmt.Sprintf("sample:%d", i), value, 0)
		}
		_, err := pipe.Exec(ctx)
		require.NoError(t, err)
		require.NoError(t, writer.Do(ctx, "flushmemtable").Err())
		util.SlaveOf(t, client, primary)
		require.Eventually(t, func() bool {
			got, err := client.Get(ctx, "sample:0").Result()
			return err == nil && got == value && util.FindInfoEntry(client, "master_link_status", "replication") == "up"
		}, 30*time.Second, 100*time.Millisecond)
		util.WaitForOffsetSync(t, writer, client, 30*time.Second)
		require.Equal(t, value, client.Get(ctx, "sample:0").Val())
		require.Equal(t, int64(0), client.Exists(ctx, "before-full-sync").Val())
		fullSyncs, err := strconv.Atoi(util.FindInfoEntry(writer, "sync_full"))
		require.NoError(t, err)
		require.Positive(t, fullSyncs)
		waitForPinnedSample(t, client)
		require.NoError(t, writer.Set(ctx, "sample:after", value, 0).Err())
		util.WaitForOffsetSync(t, writer, client, 5*time.Second)
		require.Equal(t, value, client.Get(ctx, "sample:after").Val())
	}
}
