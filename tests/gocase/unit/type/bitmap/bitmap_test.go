//go:build !ignore_when_tsan

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
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type BITOP int32

const (
	AND BITOP = 0
	OR  BITOP = 1
	XOR BITOP = 2
	NOT BITOP = 3
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
	buf := make([]string, 0, len(keys))
	for _, key := range keys {
		cmd := rdb.Get(ctx, key)
		require.NoError(t, cmd.Err())
		buf = append(buf, cmd.Val())
	}
	return buf
}
func SimulateBitOp(op BITOP, values ...[]byte) string {
	maxlen := 0
	binaryArray := make([]string, 0, len(values))
	for _, value := range values {
		if maxlen < len(value)*8 {
			maxlen = len(value) * 8
		}
	}
	for _, value := range values {
		buf := bytes.NewBuffer([]byte{})
		for _, v := range value {
			buf.WriteString(fmt.Sprintf("%08b", v))
		}
		tmp := buf.String() + strings.Repeat("0", maxlen-len(buf.String()))
		binaryArray = append(binaryArray, tmp)
	}
	var binaryResult []byte
	for i := 0; i < maxlen; i++ {
		x := binaryArray[0][i]
		if op == NOT {
			if x == '0' {
				x = '1'
			} else {
				x = '0'
			}
		}
		for j := 1; j < len(binaryArray); j++ {
			left := int(x - '0')
			right := int(binaryArray[j][i] - '0')
			switch op {
			case AND:
				left = left & right
			case XOR:
				left = left ^ right
			case OR:
				left = left | right
			}
			if left == 0 {
				x = '0'
			} else {
				x = '1'
			}
		}
		binaryResult = append(binaryResult, x)
	}

	var result []byte
	for i := 0; i < len(binaryResult); i += 8 {
		sum := 0
		for j := 0; j < 8; j++ {
			sum = sum*2 + int(binaryResult[i+j]-'0')
		}
		result = append(result, byte(sum))
	}
	return string(result)
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
		cmd := rdb.Del(ctx, "b0")
		require.NoError(t, cmd.Err())
		var maxOffset int64 = math.MaxUint32
		cmd = rdb.SetBit(ctx, "b0", maxOffset+1, 1)
		util.ErrorRegexp(t, cmd.Err(), ".*out of range.*")
		cmd = rdb.SetBit(ctx, "b0", maxOffset, 1)
		require.NoError(t, cmd.Err())
		cmd = rdb.GetBit(ctx, "b0", maxOffset)
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 1, cmd.Val())
		cmd = rdb.BitCount(ctx, "b0", &redis.BitCount{Start: 0, End: maxOffset / 8})
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 1, cmd.Val())
		cmd = rdb.BitPos(ctx, "b0", 1)
		require.NoError(t, cmd.Err())
		require.EqualValues(t, maxOffset, cmd.Val())
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

	for _, op := range []BITOP{AND, OR, XOR} {
		t.Run("BITOP fuzzing "+strconv.Itoa(int(op)), func(t *testing.T) {
			for i := 0; i < 10; i++ {
				require.NoError(t, rdb.FlushAll(ctx).Err())
				numVec := util.RandomInt(10) + 1
				var vec [][]byte
				var veckeys []string
				for j := 0; j < int(numVec); j++ {
					str := util.RandString(0, 1000, util.Binary)
					vec = append(vec, []byte(str))
					veckeys = append(veckeys, "vector_"+strconv.Itoa(j))
					Set2SetBit(t, rdb, ctx, "vector_"+strconv.Itoa(j), []byte(str))
				}
				switch op {
				case AND:
					require.NoError(t, rdb.BitOpAnd(ctx, "target", veckeys...).Err())
					require.EqualValues(t, SimulateBitOp(AND, vec...), rdb.Get(ctx, "target").Val())
				case OR:
					require.NoError(t, rdb.BitOpOr(ctx, "target", veckeys...).Err())
					require.EqualValues(t, SimulateBitOp(OR, vec...), rdb.Get(ctx, "target").Val())
				case XOR:
					require.NoError(t, rdb.BitOpXor(ctx, "target", veckeys...).Err())
					require.EqualValues(t, SimulateBitOp(XOR, vec...), rdb.Get(ctx, "target").Val())
				}

			}
		})
	}

	t.Run("BITOP NOT fuzzing", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			require.NoError(t, rdb.Del(ctx, "str").Err())
			str := util.RandString(0, 1000, util.Binary)
			Set2SetBit(t, rdb, ctx, "str", []byte(str))
			require.NoError(t, rdb.BitOpNot(ctx, "target", "str").Err())
			require.EqualValues(t, SimulateBitOp(NOT, []byte(str)), rdb.Get(ctx, "target").Val())
		}
	})

	t.Run("BITOP with non string source key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "c").Err())
		Set2SetBit(t, rdb, ctx, "a", []byte("\xaa\x00\xff\x55"))
		Set2SetBit(t, rdb, ctx, "b", []byte("\xaa\x00\xff\x55"))
		require.NoError(t, rdb.LPush(ctx, "c", "foo").Err())
		util.ErrorRegexp(t, rdb.BitOpXor(ctx, "dest", "a", "b", "c", "d").Err(), ".*WRONGTYPE.*")
	})

	t.Run("BITOP with empty string after non empty string (Redis issue #529)", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		Set2SetBit(t, rdb, ctx, "a", []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
		require.EqualValues(t, 32, rdb.BitOpOr(ctx, "x", "a", "b").Val())
	})
}
