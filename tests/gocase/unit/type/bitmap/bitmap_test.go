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
	// t.Log(buf, bs)
	for index, value := range buf.String() {
		// t.Log(index, int(value)-int('0'))
		require.NoError(t, rdb.SetBit(ctx, key, int64(index), int(value)-int('0')).Err())
	}
}
func GetBitmap(t *testing.T, rdb *redis.Client, ctx context.Context, keys ...string) []string {
	var buf []string
	for _, key := range keys {
		cmd := rdb.Get(ctx, key)
		require.NoError(t, cmd.Err())
		buf = append(buf, cmd.Val())
	}
	return buf
}
func TestSet(t *testing.T) {

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
}
