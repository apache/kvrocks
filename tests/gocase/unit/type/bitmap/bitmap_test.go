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

package bitmap

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

type BITOP int32

const (
	AND BITOP = 0
	OR  BITOP = 1
	XOR BITOP = 2
)

func Set2SetBit(t *testing.T, rdb *redis.Client, ctx context.Context, key string, bs []byte) {
	buf := bytes.NewBuffer([]byte{})
	for _, v := range bs {
		buf.WriteString(fmt.Sprintf("%08b", v))
	}
	for index, value := range buf.String() {
		require.NoError(t, rdb.SetBit(ctx, key, int64(index), int(value)-int('0')).Err())
	}
}
func GetBitmap(t *testing.T, rdb *redis.Client, ctx context.Context, keys ...string) []string {
	buf := []string{}
	for _, key := range keys {
		cmd := rdb.Get(ctx, key)
		require.NoError(t, cmd.Err())
		buf = append(buf, cmd.Val())
	}
	return buf
}
func TestBitmap(t *testing.T) {

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("GET bitmap string after setbit", func(t *testing.T) {
		require.NoError(t, rdb.SetBit(ctx, "b0", 0, 0).Err())
		require.NoError(t, rdb.SetBit(ctx, "b1", 35, 0).Err())
		Set2SetBit(t, rdb, ctx, "b2", []byte("\xac\x81\x32\x5d\xfe"))
		Set2SetBit(t, rdb, ctx, "b3", []byte("\xff\xff\xff\xff"))
		require.EqualValues(t, []string{"\x00", "\x00\x00\x00\x00\x00", "\xac\x81\x32\x5d\xfe", "\xff\xff\xff\xff"}, GetBitmap(t, rdb, ctx, "b0", "b1", "b2", "b3"))
	})

	t.Run("GET bitmap with out of max size", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "config", "set", "max-bitmap-to-string-mb", 1).Err())
		require.NoError(t, rdb.SetBit(ctx, "b0", 8388609, 0).Err())
		util.ErrorRegexp(t, rdb.Get(ctx, "b0").Err(), "ERR Operation aborted: The size of the bitmap .*")
	})

	t.Run("SETBIT/GETBIT/BITCOUNT/BITPOS boundary check (type bitmap)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "b0").Err())
		maxOffset := 4*1024*1024*1024 - 1
		util.ErrorRegexp(t, rdb.SetBit(ctx, "b0", int64(maxOffset)+1, 1).Err(), ".*out of range.*")
		require.NoError(t, rdb.SetBit(ctx, "b0", int64(maxOffset), 1).Err())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "b0", int64(maxOffset)).Val())
		require.EqualValues(t, 1, rdb.BitCount(ctx, "b0", &redis.BitCount{Start: 0, End: int64(maxOffset) / 8}).Val())
		require.EqualValues(t, maxOffset, rdb.BitPos(ctx, "b0", 1).Val())
	})

	t.Run("BITOP NOT (known string)", func(t *testing.T) {
		Set2SetBit(t, rdb, ctx, "s", []byte("\xaa\x00\xff\x55"))
		require.NoError(t, rdb.BitOpNot(ctx, "dest", "s").Err())
		require.EqualValues(t, []string{"\x55\xff\x00\xaa"}, GetBitmap(t, rdb, ctx, "dest"))
	})

	t.Run("BITOP where dest and target are the same key", func(t *testing.T) {
		Set2SetBit(t, rdb, ctx, "s", []byte("\xaa\x00\xff\x55"))
		require.NoError(t, rdb.BitOpNot(ctx, "s", "s").Err())
		require.EqualValues(t, []string{"\x55\xff\x00\xaa"}, GetBitmap(t, rdb, ctx, "s"))
	})

	t.Run("BITOP AND|OR|XOR don't change the string with single input key", func(t *testing.T) {
		Set2SetBit(t, rdb, ctx, "a", []byte("\x01\x02\xff"))
		require.NoError(t, rdb.BitOpAnd(ctx, "res1", "a").Err())
		require.NoError(t, rdb.BitOpOr(ctx, "res2", "a").Err())
		require.NoError(t, rdb.BitOpXor(ctx, "res3", "a").Err())
		require.EqualValues(t, []string{"\x01\x02\xff", "\x01\x02\xff", "\x01\x02\xff"}, GetBitmap(t, rdb, ctx, "res1", "res2", "res3"))
	})

	t.Run("BITOP missing key is considered a stream of zero", func(t *testing.T) {
		Set2SetBit(t, rdb, ctx, "a", []byte("\x01\x02\xff"))
		require.NoError(t, rdb.BitOpAnd(ctx, "res1", "no-suck-key", "a").Err())
		require.NoError(t, rdb.BitOpOr(ctx, "res2", "no-suck-key", "a", "no-suck-key").Err())
		require.NoError(t, rdb.BitOpXor(ctx, "res3", "no-suck-key", "a").Err())
		require.EqualValues(t, []string{"\x00\x00\x00", "\x01\x02\xff", "\x01\x02\xff"}, GetBitmap(t, rdb, ctx, "res1", "res2", "res3"))
	})

	t.Run("BITOP shorter keys are zero-padded to the key with max length", func(t *testing.T) {
		Set2SetBit(t, rdb, ctx, "a", []byte("\x01\x02\xff\xff"))
		Set2SetBit(t, rdb, ctx, "b", []byte("\x01\x02\xff"))
		require.NoError(t, rdb.BitOpAnd(ctx, "res1", "a", "b").Err())
		require.NoError(t, rdb.BitOpOr(ctx, "res2", "a", "b").Err())
		require.NoError(t, rdb.BitOpXor(ctx, "res3", "a", "b").Err())
		require.EqualValues(t, []string{"\x01\x02\xff\x00", "\x01\x02\xff\xff", "\x00\x00\x00\xff"}, GetBitmap(t, rdb, ctx, "res1", "res2", "res3"))
	})
}
