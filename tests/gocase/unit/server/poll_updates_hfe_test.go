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
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

func TestPollUpdatesHashFieldExpiration(t *testing.T) {
	for _, txn := range []string{"no", "yes"} {
		ctx := context.Background()
		source := util.StartServer(t, map[string]string{"txn-context-enabled": txn, "hash-encoding-mode": "field-expiration"})
		defer source.Close()
		destination := util.StartServer(t, map[string]string{"hash-encoding-mode": "field-expiration"})
		defer destination.Close()
		src, dst := source.NewClient(), destination.NewClient()
		defer func() { require.NoError(t, src.Close()) }()
		defer func() { require.NoError(t, dst.Close()) }()
		t.Run("txn-context="+txn, func(t *testing.T) {
			var sequence int64
			replay := func(t *testing.T, expected ...[]string) {
				t.Helper()
				result, err := src.Do(ctx, "POLLUPDATES", sequence, "MAX", 1000, "FORMAT", "RESP").Result()
				require.NoError(t, err)
				updates := parsePollUpdatesResult(t, result.(map[any]any), true)
				require.Equal(t, updates.LatestSeq, updates.NextSeq)
				if len(expected) == 0 {
					require.Equal(t, sequence, updates.NextSeq)
					require.Empty(t, updates.Updates)
					return
				}
				require.Equal(t, []any{RESPFormat{Namespace: "default", Commands: expected}}, updates.Updates)
				sequence = updates.NextSeq
				for _, command := range updates.Updates[0].(RESPFormat).Commands {
					args := make([]any, len(command))
					for i, arg := range command {
						args[i] = arg
					}
					require.NoError(t, dst.Do(ctx, args...).Err())
				}
			}
			set := func(key, field, value string) []string { return []string{"HSET", key, field, value} }
			expire := func(key, field string, at int64) []string {
				return []string{"HPEXPIREAT", key, strconv.FormatInt(at, 10), "FIELDS", "1", field}
			}
			checkField := func(t *testing.T, key, field, value string, at int64) {
				t.Helper()
				gotValue, err := dst.HGet(ctx, key, field).Result()
				require.NoError(t, err)
				require.Equal(t, value, gotValue)
				got, err := dst.Do(ctx, "HPEXPIRETIME", key, "FIELDS", 1, field).Int64Slice()
				require.NoError(t, err)
				require.Equal(t, []int64{at}, got)
			}

			t.Run("mixed encodings in a transaction", func(t *testing.T) {
				binary := "\x00\x00\x00\x00\x00\x00\x00\x01payload\x00\xff"
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "legacy").Err())
				require.NoError(t, src.HSet(ctx, "legacy", "f", binary).Err())
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
				require.NoError(t, src.HSet(ctx, "hfe", "f", "initial").Err())
				replay(t, set("legacy", "f", binary), set("hfe", "f", "initial"))
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "legacy").Err())
				_, err := src.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.HSet(ctx, "hfe", "f", "updated")
					pipe.HSet(ctx, "legacy", "f", binary+"updated")
					return nil
				})
				require.NoError(t, err)
				replay(t, set("hfe", "f", "updated"), set("legacy", "f", binary+"updated"))
				checkField(t, "hfe", "f", "updated", -1)
				checkField(t, "legacy", "f", binary+"updated", -1)
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
			})

			t.Run("field TTL transitions and subkey-only updates", func(t *testing.T) {
				at := time.Now().Add(time.Minute).UnixMilli()
				for _, step := range []struct {
					args  []any
					value string
					ttl   int64
				}{
					{[]any{"HSET", "counter", "f", "10"}, "10", -1},
					{[]any{"HPEXPIREAT", "counter", at, "FIELDS", 1, "f"}, "10", at},
					{[]any{"HINCRBY", "counter", "f", 1}, "11", at},
					{[]any{"HINCRBYFLOAT", "counter", "f", 0.5}, "11.5", at},
					{[]any{"HSETEX", "counter", "KEEPTTL", "FIELDS", 1, "f", "12"}, "12", at},
					{[]any{"HGETEX", "counter", "PXAT", at + 1000, "FIELDS", 1, "f"}, "12", at + 1000},
					{[]any{"HPERSIST", "counter", "FIELDS", 1, "f"}, "12", -1},
					{[]any{"HSETEX", "counter", "PXAT", at, "FIELDS", 1, "f", "13"}, "13", at},
					{[]any{"HGETEX", "counter", "PERSIST", "FIELDS", 1, "f"}, "13", -1},
					{[]any{"HPEXPIREAT", "counter", at, "FIELDS", 1, "f"}, "13", at},
					{[]any{"HSET", "counter", "f", "14"}, "14", -1},
				} {
					require.NoError(t, src.Do(ctx, step.args...).Err())
					commands := [][]string{set("counter", "f", step.value)}
					if step.ttl > 0 {
						commands = append(commands, expire("counter", "f", step.ttl))
					}
					replay(t, commands...)
					checkField(t, "counter", "f", step.value, step.ttl)
				}
				require.NoError(t, src.HDel(ctx, "counter", "f").Err())
				replay(t, []string{"HDEL", "counter", "f"})
				require.ErrorIs(t, dst.HGet(ctx, "counter", "f").Err(), redis.Nil)
			})

			t.Run("hgetdel effects and replay", func(t *testing.T) {
				for _, mode := range []string{"legacy", "field-expiration"} {
					t.Run(mode, func(t *testing.T) {
						key := "hgetdel-" + mode
						binary := "\x00\x00\x00\x00\x00\x00\x00\x01value\xff"
						fieldAt := time.Now().Add(10 * time.Minute).UnixMilli()
						keyAt := time.Now().Add(20 * time.Minute).Truncate(time.Second).UnixMilli()
						require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", mode).Err())
						require.NoError(t, dst.ConfigSet(ctx, "hash-encoding-mode", mode).Err())
						require.NoError(t, src.HSet(ctx, key, "removed", binary, "live", "ttl", "keeper", "persistent").Err())
						replay(t, set(key, "keeper", "persistent"), set(key, "live", "ttl"), set(key, "removed", binary))
						if mode == "field-expiration" {
							require.NoError(t, src.Do(ctx, "HPEXPIREAT", key, fieldAt, "FIELDS", 2, "removed", "live").Err())
							replay(t, set(key, "removed", binary), expire(key, "removed", fieldAt), set(key, "live", "ttl"), expire(key, "live", fieldAt))
						}
						require.NoError(t, src.Do(ctx, "PEXPIREAT", key, keyAt).Err())
						replay(t, []string{"PEXPIREAT", key, strconv.FormatInt(keyAt, 10)})
						otherMode := "legacy"
						if mode == "legacy" {
							otherMode = "field-expiration"
						}
						require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", otherMode).Err())
						require.NoError(t, dst.ConfigSet(ctx, "hash-encoding-mode", otherMode).Err())

						got, err := src.Do(ctx, "HGETDEL", key, "FIELDS", 3, "removed", "missing", "removed").Result()
						require.NoError(t, err)
						require.Equal(t, []interface{}{binary, nil, nil}, got)
						replay(t, []string{"HDEL", key, "removed"})
						for _, client := range []*redis.Client{src, dst} {
							values, err := client.HGetAll(ctx, key).Result()
							require.NoError(t, err)
							require.Equal(t, map[string]string{"live": "ttl", "keeper": "persistent"}, values)
							at, err := client.Do(ctx, "PEXPIRETIME", key).Int64()
							require.NoError(t, err)
							require.Equal(t, keyAt, at)
							metadata := util.GetKMetadata(t, client, ctx, key)
							require.Equal(t, mode, metadata.Mode)
							require.Equal(t, int64(2), metadata.Size)
							if mode == "field-expiration" {
								require.Equal(t, int64(1), metadata.Persist)
								expires, err := client.Do(ctx, "HPEXPIRETIME", key, "FIELDS", 3, "removed", "live", "keeper").Result()
								require.NoError(t, err)
								require.Equal(t, []interface{}{int64(-2), fieldAt, int64(-1)}, expires)
							}
						}
						got, err = src.Do(ctx, "HGETDEL", key, "FIELDS", 2, "missing", "removed").Result()
						require.NoError(t, err)
						require.Equal(t, []interface{}{nil, nil}, got)
						replay(t)

						got, err = src.Do(ctx, "HGETDEL", key, "FIELDS", 4, "live", "live", "missing", "keeper").Result()
						require.NoError(t, err)
						require.Equal(t, []interface{}{"ttl", nil, nil, "persistent"}, got)
						replay(t, []string{"HDEL", key, "live"}, []string{"HDEL", key, "keeper"}, []string{"DEL", key})
						for _, client := range []*redis.Client{src, dst} {
							exists, err := client.Exists(ctx, key).Result()
							require.NoError(t, err)
							require.Zero(t, exists)
						}
						got, err = src.Do(ctx, "HGETDEL", key, "FIELDS", 2, "live", "missing").Result()
						require.NoError(t, err)
						require.Equal(t, []interface{}{nil, nil}, got)
						replay(t)
					})
				}
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
				require.NoError(t, dst.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
			})

			t.Run("key TTL and hash log arguments coexist", func(t *testing.T) {
				// Legacy metadata stores key expiration with second precision.
				at := time.Now().Add(time.Minute).Truncate(time.Second).UnixMilli()
				require.NoError(t, src.HSet(ctx, "key-ttl", "f", "v").Err())
				require.NoError(t, src.Do(ctx, "PEXPIREAT", "key-ttl", at).Err())
				require.NoError(t, src.HSet(ctx, "key-ttl", "g", "w").Err())
				replay(t, set("key-ttl", "f", "v"), []string{"PEXPIREAT", "key-ttl", strconv.FormatInt(at, 10)}, set("key-ttl", "g", "w"))
				require.Equal(t, at, dst.Do(ctx, "PEXPIRETIME", "key-ttl").Val())
				checkField(t, "key-ttl", "g", "w", -1)
			})

			t.Run("copy and rename preserve field TTL", func(t *testing.T) {
				at := time.Now().Add(time.Minute).UnixMilli()
				require.NoError(t, src.Do(ctx, "HSETEX", "original", "PXAT", at, "FIELDS", 1, "f", "value").Err())
				replay(t, set("original", "f", "value"), expire("original", "f", at))
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "legacy").Err())
				require.EqualValues(t, 1, src.Copy(ctx, "original", "copied", 0, false).Val())
				replay(t, set("copied", "f", "value"), expire("copied", "f", at))
				checkField(t, "copied", "f", "value", at)
				require.NoError(t, src.Rename(ctx, "copied", "renamed").Err())
				replay(t, []string{"DEL", "copied"}, set("renamed", "f", "value"), expire("renamed", "f", at))
				checkField(t, "renamed", "f", "value", at)
				exists, err := dst.Exists(ctx, "copied").Result()
				require.NoError(t, err)
				require.Zero(t, exists)
				require.NoError(t, src.ConfigSet(ctx, "hash-encoding-mode", "field-expiration").Err())
			})

			t.Run("natural expiration and delayed replay", func(t *testing.T) {
				at := time.Now().Add(3 * time.Second).UnixMilli()
				require.NoError(t, src.Do(ctx, "HSETEX", "expiry", "PXAT", at, "FIELDS", 2, "live", "v", "persist", "p").Err())
				replay(t, set("expiry", "live", "v"), expire("expiry", "live", at), set("expiry", "persist", "p"), expire("expiry", "persist", at))
				checkField(t, "expiry", "live", "v", at)
				checkField(t, "expiry", "persist", "p", at)
				require.NoError(t, src.Do(ctx, "HPERSIST", "expiry", "FIELDS", 1, "persist").Err())
				replay(t, set("expiry", "persist", "p"))
				checkField(t, "expiry", "persist", "p", -1)

				// Leave this source write unread until its deadline has passed.
				require.NoError(t, src.Do(ctx, "HSETEX", "expiry", "PXAT", at, "FIELDS", 1, "delayed", "new").Err())
				require.NoError(t, dst.HSet(ctx, "expiry", "delayed", "old").Err())
				checkField(t, "expiry", "delayed", "old", -1)
				require.Eventually(t, func() bool {
					return time.Now().UnixMilli() > at && dst.HGet(ctx, "expiry", "live").Err() == redis.Nil
				}, 10*time.Second, 10*time.Millisecond)
				checkField(t, "expiry", "persist", "p", -1)
				replay(t, set("expiry", "delayed", "new"), expire("expiry", "delayed", at))
				require.ErrorIs(t, dst.HGet(ctx, "expiry", "delayed").Err(), redis.Nil)
			})
		})
	}
}
