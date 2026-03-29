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

package cms

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestCMSCommands(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		testCMS(t, configs)
	}
}

func testCMS(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "test_cms_key"

	t.Run("InitByDim basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())

		// Check info
		info := rdb.Do(ctx, "cms.info", key).Val()
		infoSlice := info.([]interface{})
		require.Equal(t, "width", infoSlice[0])
		require.Equal(t, int64(1000), infoSlice[1])
		require.Equal(t, "depth", infoSlice[2])
		require.Equal(t, int64(5), infoSlice[3])
	})

	t.Run("InitByDim duplicate key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())
		require.ErrorContains(t, rdb.Do(ctx, "cms.initbydim", key, "100", "3").Err(), "key already exists")
	})

	t.Run("InitByDim invalid arguments", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Error(t, rdb.Do(ctx, "cms.initbydim", key, "0", "5").Err())
		require.Error(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "0").Err())
	})

	t.Run("InitByProb basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		// Per Redis documentation, 'probability' is the failure probability (probability of inflated count)
		// For a 1% failure rate, set probability = 0.01
		require.NoError(t, rdb.Do(ctx, "cms.initbyprob", key, "0.01", "0.01").Err())

		// Check info - width and depth are calculated from error rate and probability
		// RedisBloom formula:
		//   width = ceil(2 / error_rate) = ceil(2 / 0.01) = 200
		//   depth = ceil(log10(probability) / log10(0.5)) = ceil(log10(0.01) / log10(0.5)) = ceil(6.64) = 7
		info := rdb.Do(ctx, "cms.info", key).Val()
		infoSlice := info.([]interface{})
		require.Equal(t, "width", infoSlice[0])
		width := infoSlice[1].(int64)
		require.Equal(t, int64(200), width)
		depth := infoSlice[3].(int64)
		require.Equal(t, int64(7), depth)
	})

	t.Run("InitByProb invalid arguments", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.Error(t, rdb.Do(ctx, "cms.initbyprob", key, "0", "0.99").Err())
		require.Error(t, rdb.Do(ctx, "cms.initbyprob", key, "1", "0.99").Err())
		require.Error(t, rdb.Do(ctx, "cms.initbyprob", key, "0.01", "0").Err())
		require.Error(t, rdb.Do(ctx, "cms.initbyprob", key, "0.01", "1").Err())
	})

	t.Run("Lazy initialization - query returns 0 without explicit init", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())

		// Query without any IncrBy - all buckets are missing but should return 0
		queryResult := rdb.Do(ctx, "cms.query", key, "nonexistent_item").Val()
		querySlice := queryResult.([]interface{})
		require.Equal(t, int64(0), querySlice[0])

		// Query multiple items
		queryResult = rdb.Do(ctx, "cms.query", key, "item1", "item2", "item3").Val()
		querySlice = queryResult.([]interface{})
		require.Equal(t, int64(0), querySlice[0])
		require.Equal(t, int64(0), querySlice[1])
		require.Equal(t, int64(0), querySlice[2])
	})

	t.Run("Lazy initialization - incrby works after lazy init", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())

		// Increment items
		result := rdb.Do(ctx, "cms.incrby", key, "foo", "10", "bar", "20").Val()
		resultSlice := result.([]interface{})
		require.Equal(t, int64(10), resultSlice[0])
		require.Equal(t, int64(20), resultSlice[1])

		// Query items
		queryResult := rdb.Do(ctx, "cms.query", key, "foo", "bar", "baz").Val()
		querySlice := queryResult.([]interface{})
		require.Equal(t, int64(10), querySlice[0])
		require.Equal(t, int64(20), querySlice[1])
		require.Equal(t, int64(0), querySlice[2]) // baz was never incremented
	})

	t.Run("IncrBy multiple times", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())

		require.NoError(t, rdb.Do(ctx, "cms.incrby", key, "item1", "100").Err())
		require.NoError(t, rdb.Do(ctx, "cms.incrby", key, "item1", "50").Err())

		queryResult := rdb.Do(ctx, "cms.query", key, "item1").Val()
		querySlice := queryResult.([]interface{})
		require.Equal(t, int64(150), querySlice[0])
	})

	t.Run("IncrBy non-existent key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nonexistent").Err())
		require.Error(t, rdb.Do(ctx, "cms.incrby", "nonexistent", "foo", "10").Err())
	})

	t.Run("Query non-existent key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nonexistent").Err())
		require.Error(t, rdb.Do(ctx, "cms.query", "nonexistent", "foo").Err())
	})

	t.Run("Info returns correct total count", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.incrby", key, "a", "10", "b", "20", "c", "30").Err())

		info := rdb.Do(ctx, "cms.info", key).Val()
		infoSlice := info.([]interface{})
		// Find count in the result
		for i := 0; i < len(infoSlice); i += 2 {
			if infoSlice[i] == "count" {
				require.Equal(t, int64(60), infoSlice[i+1]) // 10 + 20 + 30
				break
			}
		}
	})

	t.Run("Merge basic test", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cms1", "cms2", "cms_merge").Err())

		// Create three CMS with same dimensions (destination must be initialized)
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms1", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms2", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms_merge", "1000", "5").Err())

		// Add data
		require.NoError(t, rdb.Do(ctx, "cms.incrby", "cms1", "item1", "100").Err())
		require.NoError(t, rdb.Do(ctx, "cms.incrby", "cms2", "item1", "50").Err())

		// Merge
		require.NoError(t, rdb.Do(ctx, "cms.merge", "cms_merge", "2", "cms1", "cms2").Err())

		// Query merged result
		queryResult := rdb.Do(ctx, "cms.query", "cms_merge", "item1").Val()
		querySlice := queryResult.([]interface{})
		require.Equal(t, int64(150), querySlice[0]) // 100 + 50
	})

	t.Run("Merge with weights", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cms3", "cms4", "cms_merge_w").Err())

		// Create three CMS with same dimensions (destination must be initialized)
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms3", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms4", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms_merge_w", "1000", "5").Err())

		// Add data
		require.NoError(t, rdb.Do(ctx, "cms.incrby", "cms3", "item1", "100").Err())
		require.NoError(t, rdb.Do(ctx, "cms.incrby", "cms4", "item1", "50").Err())

		// Merge with weights
		require.NoError(t, rdb.Do(ctx, "cms.merge", "cms_merge_w", "2", "cms3", "cms4", "weights", "2", "3").Err())

		// Query merged result
		queryResult := rdb.Do(ctx, "cms.query", "cms_merge_w", "item1").Val()
		querySlice := queryResult.([]interface{})
		require.Equal(t, int64(350), querySlice[0]) // 100*2 + 50*3 = 350
	})

	t.Run("Merge invalid dimensions", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cms5", "cms6", "cms_merge_err").Err())

		// Create two CMS with different dimensions
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms5", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms6", "2000", "5").Err())

		// Merge should fail
		require.Error(t, rdb.Do(ctx, "cms.merge", "cms_merge_err", "2", "cms5", "cms6").Err())
	})

	t.Run("Merge destination must exist", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cms_src", "cms_dest_not_exist").Err())

		// Create source CMS
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms_src", "1000", "5").Err())

		// Merge to non-existent destination should fail
		require.ErrorContains(t, rdb.Do(ctx, "cms.merge", "cms_dest_not_exist", "1", "cms_src").Err(), "not found")
	})

	t.Run("Merge with negative weights", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cms_neg1", "cms_neg2", "cms_neg_dest").Err())

		// Create three CMS with same dimensions
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms_neg1", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms_neg2", "1000", "5").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "cms_neg_dest", "1000", "5").Err())

		// Add data
		require.NoError(t, rdb.Do(ctx, "cms.incrby", "cms_neg1", "item1", "100").Err())
		require.NoError(t, rdb.Do(ctx, "cms.incrby", "cms_neg2", "item1", "50").Err())

		// Merge with negative weight (100 - 50 = 50)
		require.NoError(t, rdb.Do(ctx, "cms.merge", "cms_neg_dest", "2", "cms_neg1", "cms_neg2", "weights", "1", "-1").Err())

		// Query merged result
		queryResult := rdb.Do(ctx, "cms.query", "cms_neg_dest", "item1").Val()
		querySlice := queryResult.([]interface{})
		require.Equal(t, int64(50), querySlice[0]) // 100 - 50 = 50
	})

	t.Run("Info non-existent key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nonexistent").Err())
		require.Error(t, rdb.Do(ctx, "cms.info", "nonexistent").Err())
	})

	t.Run("CMS overestimates but never underestimates", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "10").Err())

		// Add items with known counts
		for i := 0; i < 100; i++ {
			require.NoError(t, rdb.Do(ctx, "cms.incrby", key, "item"+string(rune('0'+i%10)), "1").Err())
		}

		// Query - CMS should return count >= actual (never underestimate)
		for i := 0; i < 10; i++ {
			queryResult := rdb.Do(ctx, "cms.query", key, "item"+string(rune('0'+i))).Val()
			querySlice := queryResult.([]interface{})
			require.GreaterOrEqual(t, querySlice[0].(int64), int64(10)) // Each item was incremented 10 times
		}
	})

	t.Run("Type command returns cms", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "1000", "5").Err())
		require.Equal(t, "cms", rdb.Type(ctx, key).Val())
	})

	// Concurrent tests - Stress tests for race conditions
	t.Run("Stress Concurrent INCRBY with independent connections", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "stress_cms_indep").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "stress_cms_indep", "10", "2").Err())

		numGoroutines := 100
		incrementsPerGoroutine := 500
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Each goroutine creates its own client connection
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				// Create independent connection for this goroutine
				client := srv.NewClient()
				defer client.Close()
				
				for j := 0; j < incrementsPerGoroutine; j++ {
					_ = client.Do(ctx, "cms.incrby", "stress_cms_indep", "hot_item", "1").Err()
				}
			}()
		}

		wg.Wait()

		// Verify final count
		queryResult := rdb.Do(ctx, "cms.query", "stress_cms_indep", "hot_item").Val()
		querySlice := queryResult.([]interface{})
		expected := int64(numGoroutines * incrementsPerGoroutine)
		actual := querySlice[0].(int64)
		
		if actual != expected {
			t.Errorf("RACE DETECTED: Expected %d, got %d (lost %d updates = %.2f%%)",
				expected, actual, expected-actual, float64(expected-actual)/float64(expected)*100)
		}
		require.Equal(t, expected, actual, "Race condition: lost updates")
	})

	t.Run("Stress Concurrent INCRBY multiple keys", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "stress_multi_cms").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "stress_multi_cms", "20", "3").Err())

		numGoroutines := 200
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		// Each goroutine increments a random item
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					item := "item" + string(rune('0'+(j%5)))
					_ = rdb.Do(ctx, "cms.incrby", "stress_multi_cms", item, "1").Err()
				}
			}(i)
		}

		wg.Wait()

		// Verify total count
		info := rdb.Do(ctx, "cms.info", "stress_multi_cms").Val()
		infoSlice := info.([]interface{})
		var totalCount int64
		for i := 0; i < len(infoSlice); i += 2 {
			if infoSlice[i] == "count" {
				totalCount = infoSlice[i+1].(int64)
				break
			}
		}
		expected := int64(numGoroutines * 100)
		require.Equal(t, expected, totalCount, "Total count mismatch")
	})

	t.Run("Stress Concurrent Read/Write", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "stress_rw_cms").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "stress_rw_cms", "50", "3").Err())

		numWriters := 50
		numReaders := 100
		writesPerWriter := 200
		
		var wg sync.WaitGroup
		wg.Add(numWriters + numReaders)

		// Writers - aggressive writes
		for i := 0; i < numWriters; i++ {
			go func(id int) {
				defer wg.Done()
				for j := 0; j < writesPerWriter; j++ {
					item := "item" + string(rune('0'+(j%10)))
					_ = rdb.Do(ctx, "cms.incrby", "stress_rw_cms", item, "1").Err()
				}
			}(i)
		}

		// Readers - aggressive reads
		for i := 0; i < numReaders; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < 500; j++ {
					_ = rdb.Do(ctx, "cms.query", "stress_rw_cms", "item0").Err()
				}
			}()
		}

		wg.Wait()

		// Verify consistency
		info := rdb.Do(ctx, "cms.info", "stress_rw_cms").Val()
		require.NotNil(t, info)
	})

	t.Run("Stress Concurrent INCRBY and MERGE", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "stress_merge_src", "stress_merge_dest").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "stress_merge_src", "30", "2").Err())
		require.NoError(t, rdb.Do(ctx, "cms.initbydim", "stress_merge_dest", "30", "2").Err())

		numWriters := 30
		numMergers := 20
		var wg sync.WaitGroup
		wg.Add(numWriters + numMergers)

		// Writers to source
		for i := 0; i < numWriters; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					_ = rdb.Do(ctx, "cms.incrby", "stress_merge_src", "item1", "1").Err()
				}
			}()
		}

		// Concurrent merges
		for i := 0; i < numMergers; i++ {
			go func() {
				defer wg.Done()
				_ = rdb.Do(ctx, "cms.merge", "stress_merge_dest", "1", "stress_merge_src").Err()
			}()
		}

		wg.Wait()

		// Check no corruption
		info := rdb.Do(ctx, "cms.info", "stress_merge_dest").Val()
		require.NotNil(t, info)
	})

	t.Run("Atomic verification - repeated runs", func(t *testing.T) {
		// Run multiple times to catch intermittent races
		for run := 0; run < 5; run++ {
			key := "atomic_test_" + string(rune('0'+run))
			require.NoError(t, rdb.Del(ctx, key).Err())
			require.NoError(t, rdb.Do(ctx, "cms.initbydim", key, "5", "2").Err())

			numGoroutines := 50
			incrementsPerGoroutine := 100
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					defer wg.Done()
					for j := 0; j < incrementsPerGoroutine; j++ {
						_ = rdb.Do(ctx, "cms.incrby", key, "x", "1").Err()
					}
				}()
			}

			wg.Wait()

			queryResult := rdb.Do(ctx, "cms.query", key, "x").Val()
			querySlice := queryResult.([]interface{})
			expected := int64(numGoroutines * incrementsPerGoroutine)
			actual := querySlice[0].(int64)
			
			if actual != expected {
				t.Errorf("Run %d: RACE! Expected %d, got %d", run, expected, actual)
			}
			require.Equal(t, expected, actual, "Run %d failed", run)
		}
	})
}
