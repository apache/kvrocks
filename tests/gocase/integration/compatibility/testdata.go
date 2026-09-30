//go:build compat

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

package compatibility

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// SeededRandom provides deterministic random generation for reproducible tests
type SeededRandom struct {
	rng *rand.Rand
}

// NewSeededRandom creates a SeededRandom with the given seed
func NewSeededRandom(seed int64) *SeededRandom {
	return &SeededRandom{rng: rand.New(rand.NewSource(seed))}
}

// String generates a random alphanumeric string of the given length
func (r *SeededRandom) String(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[r.rng.Intn(len(charset))]
	}
	return string(b)
}

// Bytes generates random bytes of the given length
func (r *SeededRandom) Bytes(length int) []byte {
	b := make([]byte, length)
	r.rng.Read(b)
	return b
}

// Int63 returns a random int64 in range [0, upper)
func (r *SeededRandom) Int63(upper int64) int64 {
	return r.rng.Int63n(upper)
}

// Int returns a random int in range [0, upper)
func (r *SeededRandom) Int(upper int) int {
	return r.rng.Intn(upper)
}

// Float64 returns a random float64 in [0, 1)
func (r *SeededRandom) Float64() float64 {
	return r.rng.Float64()
}

// TestData contains all populated test data for verification
type TestData struct {
	Strings      map[string]string
	Hashes       map[string]map[string]string
	Lists        map[string][]string
	Sets         map[string][]string
	ZSets        map[string][]ZSetEntry
	Streams      map[string][]StreamEntry
	Bitmaps      map[string][]byte
	BloomFilters map[string][]string
	JSONs        map[string]string
	HLLs         map[string][]string
	TDigests     map[string][]float64
	TimeSeries   map[string][]TSSample
}

// TSSample represents a time series sample
type TSSample struct {
	Timestamp uint64
	Value     float64
}

// ZSetEntry represents a sorted set member
type ZSetEntry struct {
	Score  float64
	Member string
}

// StreamEntry represents a stream entry
type StreamEntry struct {
	ID    string
	Field string
	Value string
}

// Data type identifiers
const (
	TypeString      = "string"
	TypeHash        = "hash"
	TypeList        = "list"
	TypeSet         = "set"
	TypeZSet        = "zset"
	TypeStream      = "stream"
	TypeBitmap      = "bitmap"
	TypeBloomFilter = "bloomfilter"
	TypeJSON        = "json"
	TypeHyperLogLog = "hyperloglog"
	TypeTDigest     = "tdigest"
	TypeTimeSeries  = "timeseries"
)

// supportedTypesByVersion maps version strings to supported data types.
// Verified against the command registration in each release tag (e.g.
// bf.reserve in v2.6.0, json.set in v2.7.0, pfadd in v2.10.0,
// tdigest.create in v2.14.0; timeseries exists in v2.14.0 source but its
// REDIS_REGISTER_COMMANDS is commented out, so it is not listed there and
// first appears in v2.15.0).
var supportedTypesByVersion = map[string][]string{
	"v2.3.0":  {TypeString, TypeHash, TypeList, TypeSet, TypeZSet, TypeStream, TypeBitmap},
	"v2.6.0":  {TypeString, TypeHash, TypeList, TypeSet, TypeZSet, TypeStream, TypeBitmap, TypeBloomFilter},
	"v2.7.0":  {TypeString, TypeHash, TypeList, TypeSet, TypeZSet, TypeStream, TypeBitmap, TypeBloomFilter, TypeJSON},
	"v2.10.0": {TypeString, TypeHash, TypeList, TypeSet, TypeZSet, TypeStream, TypeBitmap, TypeBloomFilter, TypeJSON, TypeHyperLogLog},
	"v2.14.0": {TypeString, TypeHash, TypeList, TypeSet, TypeZSet, TypeStream, TypeBitmap, TypeBloomFilter, TypeJSON, TypeHyperLogLog, TypeTDigest},
	"v2.15.0": {TypeString, TypeHash, TypeList, TypeSet, TypeZSet, TypeStream, TypeBitmap, TypeBloomFilter, TypeJSON, TypeHyperLogLog, TypeTDigest, TypeTimeSeries},
}

// IsTypeSupported checks if a data type is supported by the given version
func IsTypeSupported(version string, dataType string) bool {
	types, ok := supportedTypesByVersion[version]
	if !ok {
		return false
	}
	for _, t := range types {
		if t == dataType {
			return true
		}
	}
	return false
}

// PopulateTestData populates all supported data types for the given version
func PopulateTestData(ctx context.Context, client *redis.Client, version string) (*TestData, error) {
	data := &TestData{
		Strings:      make(map[string]string),
		Hashes:       make(map[string]map[string]string),
		Lists:        make(map[string][]string),
		Sets:         make(map[string][]string),
		ZSets:        make(map[string][]ZSetEntry),
		Streams:      make(map[string][]StreamEntry),
		Bitmaps:      make(map[string][]byte),
		BloomFilters: make(map[string][]string),
		JSONs:        make(map[string]string),
		HLLs:         make(map[string][]string),
		TDigests:     make(map[string][]float64),
		TimeSeries:   make(map[string][]TSSample),
	}

	// Use version-based seed for determinism
	seed := int64(12345)
	r := NewSeededRandom(seed)

	// Populate strings (seed 0)
	if IsTypeSupported(version, TypeString) {
		var err error
		data.Strings, err = populateStrings(ctx, client, r)
		if err != nil {
			return nil, fmt.Errorf("strings: %w", err)
		}
	}

	// Populate hashes (seed 1)
	if IsTypeSupported(version, TypeHash) {
		r1 := NewSeededRandom(seed + 1)
		var err error
		data.Hashes, err = populateHashes(ctx, client, r1)
		if err != nil {
			return nil, fmt.Errorf("hashes: %w", err)
		}
	}

	// Populate lists (seed 2)
	if IsTypeSupported(version, TypeList) {
		r2 := NewSeededRandom(seed + 2)
		var err error
		data.Lists, err = populateLists(ctx, client, r2)
		if err != nil {
			return nil, fmt.Errorf("lists: %w", err)
		}
	}

	// Populate sets (seed 3)
	if IsTypeSupported(version, TypeSet) {
		r3 := NewSeededRandom(seed + 3)
		var err error
		data.Sets, err = populateSets(ctx, client, r3)
		if err != nil {
			return nil, fmt.Errorf("sets: %w", err)
		}
	}

	// Populate zsets (seed 4)
	if IsTypeSupported(version, TypeZSet) {
		r4 := NewSeededRandom(seed + 4)
		var err error
		data.ZSets, err = populateZSets(ctx, client, r4)
		if err != nil {
			return nil, fmt.Errorf("zsets: %w", err)
		}
	}

	// Populate streams (seed 5)
	if IsTypeSupported(version, TypeStream) {
		r5 := NewSeededRandom(seed + 5)
		var err error
		data.Streams, err = populateStreams(ctx, client, r5)
		if err != nil {
			return nil, fmt.Errorf("streams: %w", err)
		}
	}

	// Populate bitmaps (seed 6)
	if IsTypeSupported(version, TypeBitmap) {
		r6 := NewSeededRandom(seed + 6)
		var err error
		data.Bitmaps, err = populateBitmaps(ctx, client, r6)
		if err != nil {
			return nil, fmt.Errorf("bitmaps: %w", err)
		}
	}

	// Populate bloom filters (seed 7)
	if IsTypeSupported(version, TypeBloomFilter) {
		r7 := NewSeededRandom(seed + 7)
		var err error
		data.BloomFilters, err = populateBloomFilters(ctx, client, r7)
		if err != nil {
			return nil, fmt.Errorf("bloom filters: %w", err)
		}
	}

	// Populate JSON docs (seed 8)
	if IsTypeSupported(version, TypeJSON) {
		r8 := NewSeededRandom(seed + 8)
		var err error
		data.JSONs, err = populateJSONs(ctx, client, r8)
		if err != nil {
			return nil, fmt.Errorf("json: %w", err)
		}
	}

	// Populate hyperloglogs (seed 9)
	if IsTypeSupported(version, TypeHyperLogLog) {
		r9 := NewSeededRandom(seed + 9)
		var err error
		data.HLLs, err = populateHLLs(ctx, client, r9)
		if err != nil {
			return nil, fmt.Errorf("hyperloglogs: %w", err)
		}
	}

	// Populate tdigests (seed 10)
	if IsTypeSupported(version, TypeTDigest) {
		r10 := NewSeededRandom(seed + 10)
		var err error
		data.TDigests, err = populateTDigests(ctx, client, r10)
		if err != nil {
			return nil, fmt.Errorf("tdigests: %w", err)
		}
	}

	// Populate time series (seed 11)
	if IsTypeSupported(version, TypeTimeSeries) {
		r11 := NewSeededRandom(seed + 11)
		var err error
		data.TimeSeries, err = populateTimeSeries(ctx, client, r11)
		if err != nil {
			return nil, fmt.Errorf("time series: %w", err)
		}
	}

	return data, nil
}

// VerifyTestData verifies all test data is preserved
func VerifyTestData(ctx context.Context, client *redis.Client, data *TestData) error {
	seed := int64(12345)

	if data.Strings != nil {
		r := NewSeededRandom(seed)
		if err := verifyStrings(ctx, client, data.Strings, r); err != nil {
			return fmt.Errorf("strings: %w", err)
		}
	}

	if data.Hashes != nil {
		r := NewSeededRandom(seed + 1)
		if err := verifyHashes(ctx, client, data.Hashes, r); err != nil {
			return fmt.Errorf("hashes: %w", err)
		}
	}

	if data.Lists != nil {
		r := NewSeededRandom(seed + 2)
		if err := verifyLists(ctx, client, data.Lists, r); err != nil {
			return fmt.Errorf("lists: %w", err)
		}
	}

	if data.Sets != nil {
		r := NewSeededRandom(seed + 3)
		if err := verifySets(ctx, client, data.Sets, r); err != nil {
			return fmt.Errorf("sets: %w", err)
		}
	}

	if data.ZSets != nil {
		r := NewSeededRandom(seed + 4)
		if err := verifyZSets(ctx, client, data.ZSets, r); err != nil {
			return fmt.Errorf("zsets: %w", err)
		}
	}

	if data.Streams != nil {
		r := NewSeededRandom(seed + 5)
		if err := verifyStreams(ctx, client, data.Streams, r); err != nil {
			return fmt.Errorf("streams: %w", err)
		}
	}

	if data.Bitmaps != nil {
		r := NewSeededRandom(seed + 6)
		if err := verifyBitmaps(ctx, client, data.Bitmaps, r); err != nil {
			return fmt.Errorf("bitmaps: %w", err)
		}
	}

	// Maps are always initialized; empty means the old version did not
	// support the type, so there is nothing to verify.
	if len(data.BloomFilters) > 0 {
		r := NewSeededRandom(seed + 7)
		if err := verifyBloomFilters(ctx, client, data.BloomFilters, r); err != nil {
			return fmt.Errorf("bloom filters: %w", err)
		}
	}

	if len(data.JSONs) > 0 {
		r := NewSeededRandom(seed + 8)
		if err := verifyJSONs(ctx, client, data.JSONs, r); err != nil {
			return fmt.Errorf("json: %w", err)
		}
	}

	if len(data.HLLs) > 0 {
		r := NewSeededRandom(seed + 9)
		if err := verifyHLLs(ctx, client, data.HLLs, r); err != nil {
			return fmt.Errorf("hyperloglogs: %w", err)
		}
	}

	if len(data.TDigests) > 0 {
		r := NewSeededRandom(seed + 10)
		if err := verifyTDigests(ctx, client, data.TDigests, r); err != nil {
			return fmt.Errorf("tdigests: %w", err)
		}
	}

	if len(data.TimeSeries) > 0 {
		r := NewSeededRandom(seed + 11)
		if err := verifyTimeSeries(ctx, client, data.TimeSeries, r); err != nil {
			return fmt.Errorf("time series: %w", err)
		}
	}

	return nil
}

// populateStrings creates random string keys and returns what was stored
func populateStrings(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string]string, error) {
	result := make(map[string]string)

	// Generate N random string keys
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("str:%d", i)
		value := r.String(64) // 64 char random string
		if err := client.Set(ctx, key, value, 0).Err(); err != nil {
			return nil, fmt.Errorf("set %s: %w", key, err)
		}
		result[key] = value
	}

	return result, nil
}

// verifyStrings re-generates expected values and compares
func verifyStrings(ctx context.Context, client *redis.Client, stored map[string]string, r *SeededRandom) error {
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("str:%d", i)
		expected := r.String(64)
		actual, err := client.Get(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("get %s: %w", key, err)
		}
		if actual != expected {
			return fmt.Errorf("key %s: expected %q, got %q", key, expected, actual)
		}
	}
	return nil
}

// populateHashes creates random hash keys and returns what was stored
func populateHashes(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string]map[string]string, error) {
	result := make(map[string]map[string]string)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("hash:%d", i)
		fields := make(map[string]string)
		fieldCount := 3 + r.Int(5)

		for j := 0; j < fieldCount; j++ {
			field := fmt.Sprintf("f%d", j)
			value := r.String(32)
			fields[field] = value
		}

		// Convert to flat args for HSET
		args := make([]interface{}, 0, len(fields)*2)
		for k, v := range fields {
			args = append(args, k, v)
		}
		if err := client.HSet(ctx, key, args...).Err(); err != nil {
			return nil, fmt.Errorf("hset %s: %w", key, err)
		}
		result[key] = fields
	}

	return result, nil
}

// verifyHashes re-generates expected values and compares
func verifyHashes(ctx context.Context, client *redis.Client, stored map[string]map[string]string, r *SeededRandom) error {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("hash:%d", i)
		fieldCount := 3 + r.Int(5)

		// Re-generate expected fields
		fields := make(map[string]string)
		for j := 0; j < fieldCount; j++ {
			field := fmt.Sprintf("f%d", j)
			fields[field] = r.String(32)
		}

		// Compare against stored
		for f, expVal := range fields {
			actVal, err := client.HGet(ctx, key, f).Result()
			if err != nil {
				return fmt.Errorf("hget %s %s: %w", key, f, err)
			}
			if actVal != expVal {
				return fmt.Errorf("hash %s field %s: expected %q, got %q", key, f, expVal, actVal)
			}
		}
	}
	return nil
}

// populateLists creates random list keys and returns what was stored
func populateLists(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]string, error) {
	result := make(map[string][]string)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("list:%d", i)
		elemCount := 5 + r.Int(20)
		elems := make([]string, elemCount)

		for j := 0; j < elemCount; j++ {
			elems[j] = r.String(32)
		}

		args := make([]interface{}, len(elems))
		for j, e := range elems {
			args[j] = e
		}
		if err := client.RPush(ctx, key, args...).Err(); err != nil {
			return nil, fmt.Errorf("rpush %s: %w", key, err)
		}
		result[key] = elems
	}

	return result, nil
}

// verifyLists re-generates expected values and compares
func verifyLists(ctx context.Context, client *redis.Client, stored map[string][]string, r *SeededRandom) error {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("list:%d", i)
		elemCount := 5 + r.Int(20)

		actual, err := client.LRange(ctx, key, 0, -1).Result()
		if err != nil {
			return fmt.Errorf("lrange %s: %w", key, err)
		}

		for j := 0; j < elemCount; j++ {
			expected := r.String(32)
			if actual[j] != expected {
				return fmt.Errorf("list %s[%d]: expected %q, got %q", key, j, expected, actual[j])
			}
		}
	}
	return nil
}

// populateSets creates random set keys and returns what was stored
func populateSets(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]string, error) {
	result := make(map[string][]string)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("set:%d", i)
		memberCount := 10 + r.Int(30)
		seen := make(map[string]bool)
		members := make([]string, 0, memberCount)

		for j := 0; j < memberCount; j++ {
			member := r.String(32)
			for seen[member] {
				member = r.String(32)
			}
			seen[member] = true
			members = append(members, member)
			if err := client.SAdd(ctx, key, member).Err(); err != nil {
				return nil, fmt.Errorf("sadd %s: %w", key, err)
			}
		}
		result[key] = members
	}

	return result, nil
}

// verifySets re-generates expected values and compares (order-independent)
func verifySets(ctx context.Context, client *redis.Client, stored map[string][]string, r *SeededRandom) error {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("set:%d", i)
		memberCount := 10 + r.Int(30)

		actual, err := client.SMembers(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("smembers %s: %w", key, err)
		}

		// Re-generate expected and check membership
		seen := make(map[string]bool)
		for j := 0; j < memberCount; j++ {
			member := r.String(32)
			for seen[member] {
				member = r.String(32)
			}
			seen[member] = true
			found := false
			for _, a := range actual {
				if a == member {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("set %s: missing member %q", key, member)
			}
		}
	}
	return nil
}

// populateZSets creates random sorted set keys and returns what was stored
func populateZSets(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]ZSetEntry, error) {
	result := make(map[string][]ZSetEntry)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("zset:%d", i)
		entryCount := 10 + r.Int(30)
		entries := make([]ZSetEntry, entryCount)

		for j := 0; j < entryCount; j++ {
			member := r.String(32)
			score := r.Float64() * 1000
			entries[j] = ZSetEntry{Score: score, Member: member}
			if err := client.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Err(); err != nil {
				return nil, fmt.Errorf("zadd %s: %w", key, err)
			}
		}
		result[key] = entries
	}

	return result, nil
}

// verifyZSets re-generates expected values and compares
func verifyZSets(ctx context.Context, client *redis.Client, stored map[string][]ZSetEntry, r *SeededRandom) error {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("zset:%d", i)
		entryCount := 10 + r.Int(30)

		actual, err := client.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			return fmt.Errorf("zrange %s: %w", key, err)
		}

		if len(actual) != entryCount {
			return fmt.Errorf("zset %s: expected %d entries, got %d", key, entryCount, len(actual))
		}

		// Re-generate expected entries and build lookup by member
		expected := make(map[string]float64)
		for j := 0; j < entryCount; j++ {
			member := r.String(32)
			score := r.Float64() * 1000
			expected[member] = score
		}

		// Verify all actual entries match expected
		for _, z := range actual {
			member, _ := z.Member.(string)
			expScore, ok := expected[member]
			if !ok {
				return fmt.Errorf("zset %s: unexpected member %q", key, member)
			}
			if z.Score != expScore {
				return fmt.Errorf("zset %s member %q: expected score %.2f, got %.2f",
					key, member, expScore, z.Score)
			}
		}
	}
	return nil
}

// populateStreams creates random stream keys and returns what was stored
func populateStreams(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]StreamEntry, error) {
	result := make(map[string][]StreamEntry)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("stream:%d", i)
		entryCount := 5 + r.Int(15)
		entries := make([]StreamEntry, entryCount)

		for j := 0; j < entryCount; j++ {
			field := fmt.Sprintf("field%d", j)
			value := r.String(32)
			entries[j] = StreamEntry{Field: field, Value: value}

			id, err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: key,
				Values: map[string]interface{}{field: value},
			}).Result()
			if err != nil {
				return nil, fmt.Errorf("xadd %s: %w", key, err)
			}
			entries[j].ID = id
		}
		result[key] = entries
	}

	return result, nil
}

// verifyStreams re-generates expected values and compares
func verifyStreams(ctx context.Context, client *redis.Client, stored map[string][]StreamEntry, r *SeededRandom) error {
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("stream:%d", i)
		entryCount := 5 + r.Int(15)

		actual, err := client.XRange(ctx, key, "-", "+").Result()
		if err != nil {
			return fmt.Errorf("xrange %s: %w", key, err)
		}

		for j := 0; j < entryCount; j++ {
			field := fmt.Sprintf("field%d", j)
			expected := r.String(32)
			if actual[j].Values[field] != expected {
				return fmt.Errorf("stream %s[%d]: expected %s=%q, got %v",
					key, j, field, expected, actual[j].Values)
			}
		}
	}
	return nil
}

// populateBitmaps creates random bitmap keys and returns what was stored
func populateBitmaps(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]byte, error) {
	result := make(map[string][]byte)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("bitmap:%d", i)
		size := 64 + r.Int(256) // 64-320 bytes
		bytes := r.Bytes(size)
		if err := client.Set(ctx, key, string(bytes), 0).Err(); err != nil {
			return nil, fmt.Errorf("set %s: %w", key, err)
		}
		result[key] = bytes
	}

	return result, nil
}

// verifyBitmaps re-generates expected values and compares
func verifyBitmaps(ctx context.Context, client *redis.Client, stored map[string][]byte, r *SeededRandom) error {
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("bitmap:%d", i)
		size := 64 + r.Int(256)
		expected := r.Bytes(size)

		actual, err := client.Get(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("get %s: %w", key, err)
		}
		if actual != string(expected) {
			return fmt.Errorf("bitmap %s: mismatch", key)
		}
	}
	return nil
}

// populateBloomFilters adds random members to bloom filter keys
func populateBloomFilters(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]string, error) {
	result := make(map[string][]string)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("bf:%d", i)
		memberCount := 20 + r.Int(50)
		members := make([]string, memberCount)

		for j := 0; j < memberCount; j++ {
			members[j] = fmt.Sprintf("m%d-%s", j, r.String(16))
			if err := client.BFAdd(ctx, key, members[j]).Err(); err != nil {
				return nil, fmt.Errorf("bf.add %s: %w", key, err)
			}
		}
		result[key] = members
	}

	return result, nil
}

// verifyBloomFilters checks every populated member is still reported present
func verifyBloomFilters(ctx context.Context, client *redis.Client, stored map[string][]string, r *SeededRandom) error {
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("bf:%d", i)
		memberCount := 20 + r.Int(50)

		for j := 0; j < memberCount; j++ {
			member := fmt.Sprintf("m%d-%s", j, r.String(16))
			present, err := client.BFExists(ctx, key, member).Result()
			if err != nil {
				return fmt.Errorf("bf.exists %s %s: %w", key, member, err)
			}
			if !present {
				return fmt.Errorf("bf %s: member %q lost", key, member)
			}
		}
	}
	return nil
}

// populateJSONs stores random nested JSON documents
func populateJSONs(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string]string, error) {
	result := make(map[string]string)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("json:%d", i)
		doc := map[string]interface{}{
			"name":   r.String(16),
			"age":    r.Int(100),
			"tags":   []string{r.String(8), r.String(8)},
			"nested": map[string]interface{}{"flag": r.Int(2) == 0},
		}
		raw, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshal json %s: %w", key, err)
		}
		if err := client.JSONSet(ctx, key, "$", doc).Err(); err != nil {
			return nil, fmt.Errorf("json.set %s: %w", key, err)
		}
		result[key] = string(raw)
	}

	return result, nil
}

// verifyJSONs re-generates expected docs and compares, then checks JSON.DEL
func verifyJSONs(ctx context.Context, client *redis.Client, stored map[string]string, r *SeededRandom) error {
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("json:%d", i)
		doc := map[string]interface{}{
			"name":   r.String(16),
			"age":    r.Int(100),
			"tags":   []string{r.String(8), r.String(8)},
			"nested": map[string]interface{}{"flag": r.Int(2) == 0},
		}
		expected, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("marshal expected %s: %w", key, err)
		}
		actual, err := client.JSONGet(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("json.get %s: %w", key, err)
		}
		if actual != string(expected) {
			return fmt.Errorf("json %s: expected %s, got %s", key, expected, actual)
		}

		deleted, err := client.Do(ctx, "JSON.DEL", key, "$").Int64()
		if err != nil {
			return fmt.Errorf("json.del %s: %w", key, err)
		}
		if deleted != 1 {
			return fmt.Errorf("json.del %s: expected 1, got %d", key, deleted)
		}
	}
	return nil
}

// hllCountTolerance is generous: HLL counts are estimates with ~0.81% error.
const hllCountTolerance = 0.05
const hllMergeTolerance = 0.10

// populateHLLs adds many unique members to two keys (disjoint prefixes)
func populateHLLs(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]string, error) {
	result := make(map[string][]string)

	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("hll:%d", i)
		memberCount := 1000 + r.Int(2000)
		members := make([]string, memberCount)

		for j := 0; j < memberCount; j++ {
			members[j] = fmt.Sprintf("h%d-%s", i, r.String(24))
			if err := client.PFAdd(ctx, key, members[j]).Err(); err != nil {
				return nil, fmt.Errorf("pfadd %s: %w", key, err)
			}
		}
		result[key] = members
	}

	return result, nil
}

// verifyHLLs checks cardinality estimates and PFMERGE union
func verifyHLLs(ctx context.Context, client *redis.Client, stored map[string][]string, r *SeededRandom) error {
	counts := make([]int64, 2)
	for i := 0; i < 2; i++ {
		key := fmt.Sprintf("hll:%d", i)
		memberCount := int64(1000 + r.Int(2000))
		for j := 0; j < int(memberCount); j++ {
			_ = fmt.Sprintf("h%d-%s", i, r.String(24)) // consume the same random sequence
		}

		actual, err := client.PFCount(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("pfcount %s: %w", key, err)
		}
		if diff := float64(actual-memberCount) / float64(memberCount); diff < -hllCountTolerance || diff > hllCountTolerance {
			return fmt.Errorf("hll %s: count %d, expected ~%d", key, actual, memberCount)
		}
		counts[i] = memberCount
	}

	// Merge both keys and check the union cardinality (members are disjoint)
	if err := client.PFMerge(ctx, "hll:merged", "hll:0", "hll:1").Err(); err != nil {
		return fmt.Errorf("pfmerge: %w", err)
	}
	expectedUnion := counts[0] + counts[1]
	union, err := client.PFCount(ctx, "hll:merged").Result()
	if err != nil {
		return fmt.Errorf("pfcount merged: %w", err)
	}
	if diff := float64(union-expectedUnion) / float64(expectedUnion); diff < -hllMergeTolerance || diff > hllMergeTolerance {
		return fmt.Errorf("hll merged: count %d, expected ~%d", union, expectedUnion)
	}
	return nil
}

// populateTDigests creates t-digests with random values
func populateTDigests(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]float64, error) {
	result := make(map[string][]float64)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("td:%d", i)
		if err := client.Do(ctx, "TDIGEST.CREATE", key).Err(); err != nil {
			return nil, fmt.Errorf("tdigest.create %s: %w", key, err)
		}
		valueCount := 100 + r.Int(300)
		values := make([]float64, valueCount)
		args := make([]interface{}, 0, valueCount+1)
		args = append(args, key)
		for j := 0; j < valueCount; j++ {
			values[j] = r.Float64() * 1000
			args = append(args, values[j])
		}
		if err := client.Do(ctx, append([]interface{}{"TDIGEST.ADD"}, args...)...).Err(); err != nil {
			return nil, fmt.Errorf("tdigest.add %s: %w", key, err)
		}
		result[key] = values
	}

	return result, nil
}

// tdigestInfoObservations extracts the exact value count from TDIGEST.INFO.
// kvrocks replies with a RESP2 flat array of field/value pairs.
func tdigestInfoObservations(ctx context.Context, client *redis.Client, key string) (int64, error) {
	info, err := client.Do(ctx, "TDIGEST.INFO", key).Result()
	if err != nil {
		return 0, fmt.Errorf("tdigest.info %s: %w", key, err)
	}
	switch v := info.(type) {
	case []interface{}:
		for j := 0; j+1 < len(v); j += 2 {
			if strings.ToLower(fmt.Sprint(v[j])) == "observations" {
				return strconv.ParseInt(fmt.Sprint(v[j+1]), 10, 64)
			}
		}
	case map[interface{}]interface{}:
		for k, obs := range v {
			if strings.ToLower(fmt.Sprint(k)) == "observations" {
				return strconv.ParseInt(fmt.Sprint(obs), 10, 64)
			}
		}
	}
	return 0, fmt.Errorf("tdigest.info %s: observations field not found", key)
}

// verifyTDigests checks exact value counts, quantile sanity, and TDIGEST.MERGE
func verifyTDigests(ctx context.Context, client *redis.Client, stored map[string][]float64, r *SeededRandom) error {
	counts := make([]int64, 3)
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("td:%d", i)
		valueCount := 100 + r.Int(300)
		values := make([]float64, valueCount)
		for j := 0; j < valueCount; j++ {
			values[j] = r.Float64() * 1000
		}

		obs, err := tdigestInfoObservations(ctx, client, key)
		if err != nil {
			return err
		}
		if obs != int64(valueCount) {
			return fmt.Errorf("tdigest %s: observations %d, expected %d", key, obs, valueCount)
		}
		counts[i] = int64(valueCount)

		// Quantile estimates must stay within the data range
		quants, err := client.Do(ctx, "TDIGEST.QUANTILE", key, 0, 0.5, 1).Result()
		if err != nil {
			return fmt.Errorf("tdigest.quantile %s: %w", key, err)
		}
		qs := make([]float64, 3)
		for j, raw := range quants.([]interface{}) {
			qs[j], err = strconv.ParseFloat(fmt.Sprint(raw), 64)
			if err != nil {
				return fmt.Errorf("tdigest.quantile %s: bad value %v: %w", key, raw, err)
			}
		}
		lo, hi := values[0], values[0]
		for _, v := range values {
			if v < lo {
				lo = v
			}
			if v > hi {
				hi = v
			}
		}
		span := hi - lo
		if qs[0] < lo-0.15*span || qs[0] > lo+0.15*span || qs[2] < hi-0.15*span || qs[2] > hi+0.15*span {
			return fmt.Errorf("tdigest %s: quantiles %v outside expected range [%f, %f]", key, qs, lo, hi)
		}
		if qs[0] > qs[1] || qs[1] > qs[2] {
			return fmt.Errorf("tdigest %s: quantiles not monotonic: %v", key, qs)
		}
	}

	// Merge two digests and check the total observation count
	if err := client.Do(ctx, "TDIGEST.MERGE", "td:merged", 2, "td:0", "td:1").Err(); err != nil {
		return fmt.Errorf("tdigest.merge: %w", err)
	}
	mergedObs, err := tdigestInfoObservations(ctx, client, "td:merged")
	if err != nil {
		return err
	}
	if mergedObs != counts[0]+counts[1] {
		return fmt.Errorf("tdigest merged: observations %d, expected %d", mergedObs, counts[0]+counts[1])
	}
	return nil
}

// tsValueTolerance allows for float round-trip error in double values
const tsValueTolerance = 1e-9

// populateTimeSeries adds random samples to time series keys (TS.ADD
// auto-creates the series, so no TS.CREATE is needed)
func populateTimeSeries(ctx context.Context, client *redis.Client, r *SeededRandom) (map[string][]TSSample, error) {
	result := make(map[string][]TSSample)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("ts:%d", i)
		sampleCount := 20 + r.Int(30)
		base := uint64(1000000000 + i*1000000)
		samples := make([]TSSample, sampleCount)

		for j := 0; j < sampleCount; j++ {
			samples[j] = TSSample{Timestamp: base + uint64(j*1000), Value: r.Float64() * 1000}
			if err := client.Do(ctx, "TS.ADD", key, samples[j].Timestamp, samples[j].Value).Err(); err != nil {
				return nil, fmt.Errorf("ts.add %s: %w", key, err)
			}
		}
		result[key] = samples
	}

	return result, nil
}

// parseTSSample parses a kvrocks TS reply sample: an array of [timestamp, value]
func parseTSSample(raw interface{}) (uint64, float64, error) {
	s, ok := raw.([]interface{})
	if !ok || len(s) != 2 {
		return 0, 0, fmt.Errorf("unexpected TS sample reply: %v", raw)
	}
	ts, err := strconv.ParseUint(fmt.Sprint(s[0]), 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad TS timestamp %v: %w", s[0], err)
	}
	val, err := strconv.ParseFloat(fmt.Sprint(s[1]), 64)
	if err != nil {
		return 0, 0, fmt.Errorf("bad TS value %v: %w", s[1], err)
	}
	return ts, val, nil
}

// verifyTimeSeries checks TS.GET (last sample) and TS.RANGE (all samples)
func verifyTimeSeries(ctx context.Context, client *redis.Client, stored map[string][]TSSample, r *SeededRandom) error {
	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("ts:%d", i)
		sampleCount := 20 + r.Int(30)
		base := uint64(1000000000 + i*1000000)
		expected := make([]TSSample, sampleCount)
		for j := 0; j < sampleCount; j++ {
			expected[j] = TSSample{Timestamp: base + uint64(j*1000), Value: r.Float64() * 1000}
		}

		// TS.GET returns the last sample
		got, err := client.Do(ctx, "TS.GET", key).Result()
		if err != nil {
			return fmt.Errorf("ts.get %s: %w", key, err)
		}
		lastTs, lastVal, err := parseTSSample(got.([]interface{})[0])
		if err != nil {
			return fmt.Errorf("ts.get %s: %w", key, err)
		}
		last := TSSample{Timestamp: lastTs, Value: lastVal}
		expLast := expected[sampleCount-1]
		if last.Timestamp != expLast.Timestamp || math.Abs(last.Value-expLast.Value) > tsValueTolerance {
			return fmt.Errorf("ts %s: last sample (%d, %f), expected (%d, %f)",
				key, last.Timestamp, last.Value, expLast.Timestamp, expLast.Value)
		}

		// TS.RANGE returns all samples in order
		rangeReply, err := client.Do(ctx, "TS.RANGE", key, 0, uint64(1)<<62).Result()
		if err != nil {
			return fmt.Errorf("ts.range %s: %w", key, err)
		}
		samples, ok := rangeReply.([]interface{})
		if !ok || len(samples) != sampleCount {
			return fmt.Errorf("ts %s: range returned %v samples, expected %d", key, len(samples), sampleCount)
		}
		for j := 0; j < sampleCount; j++ {
			ts, val, err := parseTSSample(samples[j])
			if err != nil {
				return fmt.Errorf("ts.range %s[%d]: %w", key, j, err)
			}
			if ts != expected[j].Timestamp || math.Abs(val-expected[j].Value) > tsValueTolerance {
				return fmt.Errorf("ts %s[%d]: (%d, %f), expected (%d, %f)",
					key, j, ts, val, expected[j].Timestamp, expected[j].Value)
			}
		}
	}
	return nil
}
