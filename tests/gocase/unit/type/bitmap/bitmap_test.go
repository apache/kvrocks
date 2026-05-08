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
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
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
		_, _ = fmt.Fprintf(buf, "%08b", v)
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
			_, _ = fmt.Fprintf(buf, "%08b", v)
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

	t.Run("GETEX bitmap no option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.SetBit(ctx, "foo", 0, 0).Err())
		require.Equal(t, "\x00", rdb.GetEx(ctx, "foo", 0).Val())

		// Make sure the expiration time is not erased by plain GETEX.
		require.NoError(t, rdb.Expire(ctx, "foo", 10*time.Second).Err())
		require.Equal(t, "\x00", rdb.Do(ctx, "getex", "foo").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX bitmap EX/EXAT/PX/PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.SetBit(ctx, "foo", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "ex", 10).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)

		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.SetBit(ctx, "foo", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(10*time.Second).Unix()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)

		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.SetBit(ctx, "foo", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "px", 10*1000).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)

		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.SetBit(ctx, "foo", 0, 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX bitmap PERSIST option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.SetBit(ctx, "foo", 0, 0).Err())
		require.EqualValues(t, -1, rdb.TTL(ctx, "foo").Val())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "persist").Err())
		require.EqualValues(t, -1, rdb.TTL(ctx, "foo").Val())

		require.NoError(t, rdb.Expire(ctx, "foo", 10*time.Second).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "persist").Err())
		require.EqualValues(t, -1, rdb.TTL(ctx, "foo").Val())
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

	t.Run("BITCOUNT BIT/BYTE option check(type bitmap bitmap_string)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Do(ctx, "SET", "foo", "hello").Err())
		cmd := rdb.Do(ctx, "BITCOUNT", "foo", 0, -1, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 21, cmd.Val())
		require.NoError(t, rdb.Do(ctx, "SETBIT", "foo", 1024*8+2, 1).Err())
		require.NoError(t, rdb.Do(ctx, "SETBIT", "foo", 2*1024*8+1, 1).Err())
		cmd = rdb.Do(ctx, "BITCOUNT", "foo", 0, -1, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 23, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "foo", 0, 1024*8+2, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 22, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "foo", 40, 1024*8+2, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 1, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "foo", 0, 0, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 0, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "foo", 1024*8+2, 2*1024*8+1, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 2, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "foo", -1, -1, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 0, cmd.Val())
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, rdb.Del(ctx, "bar").Err())
		require.NoError(t, rdb.Do(ctx, "SETBIT", "bar", 0, 1).Err())
		require.NoError(t, rdb.Do(ctx, "SETBIT", "bar", 100, 1).Err())
		require.NoError(t, rdb.Do(ctx, "SETBIT", "bar", 1024*8+2, 1).Err())
		require.NoError(t, rdb.Do(ctx, "SETBIT", "bar", 2*1024*8+1, 1).Err())
		cmd = rdb.Do(ctx, "BITCOUNT", "bar", 0, 0, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 1, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "bar", 0, 100, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 2, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "bar", 100, 1024*8+2, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 2, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "bar", 1024*8+2, 2*1024*8+2, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 2, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "bar", 0, -1, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 4, cmd.Val())
		cmd = rdb.Do(ctx, "BITCOUNT", "bar", -1, -1, "BIT")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 0, cmd.Val())
		require.NoError(t, rdb.Del(ctx, "bar").Err())
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

	t.Run("BITOP Boundary Check", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "str").Err())
		str := util.RandStringWithSeed(0, 1000, util.Binary, 2701)
		Set2SetBit(t, rdb, ctx, "str", []byte(str))
		require.NoError(t, rdb.BitOpNot(ctx, "target", "str").Err())
		require.EqualValues(t, SimulateBitOp(NOT, []byte(str)), rdb.Get(ctx, "target").Val())
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

	t.Run("BITFIELD and BITFIELD_RO on string type", func(t *testing.T) {
		str := "zhe ge ren hen lan, shen me dou mei you liu xia."
		require.NoError(t, rdb.Set(ctx, "str", str, 0).Err())
		for _, command := range []string{"BITFIELD", "BITFIELD_RO"} {
			res := rdb.Do(ctx, command, "str", "GET", "u8", "32", "GET", "u8", "40")
			require.NoError(t, res.Err())
			require.EqualValues(t, []interface{}{int64(str[4]), int64(str[5])}, res.Val())
		}

		res := rdb.BitField(ctx, "str", "GET", "u8", "32", "SET", "u8", "32", 'r', "GET", "u8", "32")
		require.NoError(t, res.Err())
		require.EqualValues(t, str[4], res.Val()[0])
		require.EqualValues(t, str[4], res.Val()[1])
		require.EqualValues(t, 'r', res.Val()[2])
		require.ErrorContains(t, rdb.Do(ctx, "BITFIELD_RO", "str", "GET", "u8", "32", "SET", "u8", "32", 'r', "GET", "u8", "32").Err(), "BITFIELD_RO only supports the GET subcommand")

		res = rdb.BitField(ctx, "str", "INCRBY", "u8", "32", 2)
		require.NoError(t, res.Err())
		require.EqualValues(t, 't', res.Val()[0])
		require.ErrorContains(t, rdb.Do(ctx, "BITFIELD_RO", "str", "INCRBY", "u8", "32", 2).Err(), "BITFIELD_RO only supports the GET subcommand")
	})

	t.Run("BITFIELD positional offset #N syntax", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "bf_pos").Err())

		// #0 with u16 = offset 0, #1 = offset 16, #2 = offset 32
		res := rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u16", "#0", 100)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(0)}, res.Val())

		res = rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u16", "#1", 200)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(0)}, res.Val())

		res = rdb.Do(ctx, "BITFIELD", "bf_pos", "GET", "u16", "#0", "GET", "u16", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(100), int64(200)}, res.Val())

		// INCRBY with #N
		res = rdb.Do(ctx, "BITFIELD", "bf_pos", "INCRBY", "u16", "#0", 1)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(101)}, res.Val())

		// OVERFLOW SAT with #N
		res = rdb.Do(ctx, "BITFIELD", "bf_pos", "OVERFLOW", "SAT", "INCRBY", "u16", "#1", 65535)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(65535)}, res.Val())

		// BITFIELD_RO GET with #N
		for _, command := range []string{"BITFIELD", "BITFIELD_RO"} {
			res = rdb.Do(ctx, command, "bf_pos", "GET", "u16", "#0")
			require.NoError(t, res.Err())
			require.EqualValues(t, []interface{}{int64(101)}, res.Val())
		}
	})

	t.Run("BITFIELD positional offset #N invalid and boundary cases", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "bf_pos").Err())

		// bare '#' with no number
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u16", "#", 1).Err(), ".*out of range.*")

		// non-numeric after '#'
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u16", "#abc", 1).Err(), ".*out of range.*")

		// negative index
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u16", "#-1", 1).Err(), ".*out of range.*")

		// overflow: #268435456 * 16 bits = 4294967296 > UINT32_MAX
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u16", "#268435456", 1).Err(), ".*out of range.*")

		// overflow with u8: #536870912 * 8 = 4294967296 > UINT32_MAX
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u8", "#536870912", 1).Err(), ".*out of range.*")

		// overflow with u32: #134217728 * 32 = 4294967296 > UINT32_MAX
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u32", "#134217728", 1).Err(), ".*out of range.*")

		// overflow with i64: #67108864 * 64 = 4294967296 > UINT32_MAX
		util.ErrorRegexp(t, rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "i64", "#67108864", 1).Err(), ".*out of range.*")

		// just below overflow with u8: #536870911 * 8 = 4294967288 <= UINT32_MAX — must not error
		res := rdb.Do(ctx, "BITFIELD", "bf_pos", "SET", "u8", "#0", 255)
		require.NoError(t, res.Err())
	})

	t.Run("BITFIELD positional offset #N with signed types", func(t *testing.T) {
		// i8: #0 = offset 0, #1 = offset 8
		require.NoError(t, rdb.Del(ctx, "bf_i8").Err())
		res := rdb.Do(ctx, "BITFIELD", "bf_i8", "SET", "i8", "#0", -10)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(0)}, res.Val())

		res = rdb.Do(ctx, "BITFIELD", "bf_i8", "SET", "i8", "#1", 42)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(0)}, res.Val())

		res = rdb.Do(ctx, "BITFIELD", "bf_i8", "GET", "i8", "#0", "GET", "i8", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(-10), int64(42)}, res.Val())

		// INCRBY with signed i8 and #N
		res = rdb.Do(ctx, "BITFIELD", "bf_i8", "INCRBY", "i8", "#1", -2)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(40)}, res.Val())

		// i32: #0 = offset 0, #1 = offset 32
		require.NoError(t, rdb.Del(ctx, "bf_i32").Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_i32", "SET", "i32", "#0", -100000)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_i32", "SET", "i32", "#1", 999999)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_i32", "GET", "i32", "#0", "GET", "i32", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(-100000), int64(999999)}, res.Val())
	})

	t.Run("BITFIELD positional offset #N with various unsigned widths", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "bf_widths").Err())

		// u8: #N * 8
		res := rdb.Do(ctx, "BITFIELD", "bf_widths", "SET", "u8", "#0", 255)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "SET", "u8", "#1", 128)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "GET", "u8", "#0", "GET", "u8", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(255), int64(128)}, res.Val())

		// u32: #N * 32
		require.NoError(t, rdb.Del(ctx, "bf_widths").Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "SET", "u32", "#0", 1000000)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "SET", "u32", "#1", 2000000)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "GET", "u32", "#0", "GET", "u32", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(1000000), int64(2000000)}, res.Val())

		// u1: #N * 1 — single bit fields
		require.NoError(t, rdb.Del(ctx, "bf_widths").Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "SET", "u1", "#0", 1)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "SET", "u1", "#1", 1)
		require.NoError(t, res.Err())
		res = rdb.Do(ctx, "BITFIELD", "bf_widths", "GET", "u1", "#0", "GET", "u1", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(1), int64(1)}, res.Val())
	})

	t.Run("BITFIELD positional offset #N mixed with absolute offset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "bf_mix").Err())

		// mix #N positional and absolute offset in same command
		res := rdb.Do(ctx, "BITFIELD", "bf_mix", "SET", "u8", "#0", 10, "SET", "u8", "8", 20)
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(0), int64(0)}, res.Val())

		// #0 = offset 0, absolute 8 = offset 8 — should be same as #1
		res = rdb.Do(ctx, "BITFIELD", "bf_mix", "GET", "u8", "#0", "GET", "u8", "#1")
		require.NoError(t, res.Err())
		require.EqualValues(t, []interface{}{int64(10), int64(20)}, res.Val())
	})

	t.Run("BITPOS BIT option check", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\xf0", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "mykey", 1, 7, 15, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 8, cmd.Val())
	})

	t.Run("BITPOS BIT not found check check", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\xf0", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "mykey", 0, 0, 5, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 0, cmd.Val())
	})

	t.Run("BITPOS BIT not found check check", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\xf0", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "mykey", 0, 2, 3, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 2, cmd.Val())
	})

	t.Run("BITPOS BYTE option produces same result as default byte-indexed mode", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\xf0", 0).Err())
		byteResult := rdb.BitPos(ctx, "mykey", 1, 1)
		require.NoError(t, byteResult.Err())
		explicitByte := rdb.BitPosSpan(ctx, "mykey", 1, 1, -1, "byte")
		require.NoError(t, explicitByte.Err())
		require.EqualValues(t, byteResult.Val(), explicitByte.Val())
	})

	t.Run("BITPOS BYTE option is case-insensitive", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\xf0", 0).Err())
		lower, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, -1, "byte").Int64()
		require.NoError(t, err)
		upper, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, -1, "BYTE").Int64()
		require.NoError(t, err)
		require.EqualValues(t, lower, upper)
	})

	t.Run("BITPOS BIT vs BYTE gives different results for same range", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\xf0", 0).Err())
		bitResult := rdb.BitPosSpan(ctx, "mykey", 1, 0, 15, "bit")
		require.NoError(t, bitResult.Err())
		require.EqualValues(t, 8, bitResult.Val())
		byteResult := rdb.BitPosSpan(ctx, "mykey", 1, 0, 15, "byte")
		require.NoError(t, byteResult.Err())
		require.EqualValues(t, 8, byteResult.Val())
		bitResult2 := rdb.BitPosSpan(ctx, "mykey", 1, 0, 7, "bit")
		require.NoError(t, bitResult2.Err())
		require.EqualValues(t, -1, bitResult2.Val())
		byteResult2 := rdb.BitPosSpan(ctx, "mykey", 1, 0, 7, "byte")
		require.NoError(t, byteResult2.Err())
		require.EqualValues(t, 8, byteResult2.Val())
	})

	t.Run("BITPOS rejects invalid option", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, -1, "INVALID").Err()
		require.Error(t, err)
	})

	t.Run("BITPOS rejects extra arguments after BYTE/BIT", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, -1, "BIT", "extra").Err()
		require.Error(t, err)
	})

	t.Run("BITPOS rejects BIT unit without end offset", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x80", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, "BIT").Err()
		require.ErrorContains(t, err, "not started as an integer")
	})

	t.Run("BITPOS rejects BYTE unit without end offset", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x80", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, "BYTE").Err()
		require.ErrorContains(t, err, "not started as an integer")
	})

	t.Run("BITPOS rejects non-integer bit argument", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x80", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", "x").Err()
		require.ErrorContains(t, err, "The bit argument must be 1 or 0")
	})

	t.Run("BITPOS rejects non-integer bit argument with BIT unit", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x80", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", "x", 0, 0, "BIT").Err()
		require.ErrorContains(t, err, "The bit argument must be 1 or 0")
	})

	t.Run("BITPOS rejects bit argument of 2", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 2).Err()
		require.ErrorContains(t, err, "The bit argument must be 1 or 0")
	})

	t.Run("BITPOS rejects bit argument of -1", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", -1).Err()
		require.ErrorContains(t, err, "The bit argument must be 1 or 0")
	})

	t.Run("BITPOS rejects non-integer start offset", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 1, "abc").Err()
		require.ErrorContains(t, err, "not started as an integer")
	})

	t.Run("BITPOS rejects non-integer end offset", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, "abc").Err()
		require.ErrorContains(t, err, "not started as an integer")
	})

	t.Run("BITPOS bit=1 with nonexistent key returns -1", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nosuchkey").Err())
		val, err := rdb.Do(ctx, "BITPOS", "nosuchkey", 1).Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	t.Run("BITPOS bit=0 with nonexistent key returns 0", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "nosuchkey").Err())
		val, err := rdb.Do(ctx, "BITPOS", "nosuchkey", 0).Int64()
		require.NoError(t, err)
		require.EqualValues(t, 0, val)
	})

	t.Run("BITPOS BYTE with negative start", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\x00\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0, -2, -1, "BYTE").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)
	})

	t.Run("BITPOS BIT with negative start and end", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\x00\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0, -16, -9, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)
	})

	t.Run("BITPOS returns -1 when start > end after normalization", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\x00\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 2, 1, "BYTE").Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	t.Run("BITPOS BIT returns -1 when start > end after normalization", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\x00\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 16, 8, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	t.Run("BITPOS BYTE bit=0 with all-ones string and explicit end returns -1", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\xff\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0, 0, 2, "BYTE").Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	t.Run("BITPOS BIT bit=0 with all-ones string and explicit end returns -1", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\xff\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0, 0, 23, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	t.Run("BITPOS BYTE bit=0 without end extends past string (finds trailing zero)", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\xff\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0).Int64()
		require.NoError(t, err)
		require.EqualValues(t, 24, val)
	})

	t.Run("BITPOS BYTE with end beyond string length clamps correctly", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\x00", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, 100, "BYTE").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)
	})

	t.Run("BITPOS BIT with end beyond string length clamps correctly", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\xff\x00", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 0, 999, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)
	})

	t.Run("BITPOS BYTE with only start argument", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\x00\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 2).Int64()
		require.NoError(t, err)
		require.EqualValues(t, 16, val)
	})

	t.Run("BITPOS BYTE with start past string returns -1 for bit=1", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 5).Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	t.Run("BITPOS on wrong type returns WRONGTYPE error", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mylist").Err())
		require.NoError(t, rdb.LPush(ctx, "mylist", "a").Err())
		err := rdb.Do(ctx, "BITPOS", "mylist", 1).Err()
		require.ErrorContains(t, err, "WRONGTYPE")
	})

	t.Run("BITPOS BYTE bit=0 finds first zero in middle byte", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\x0f\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0, 0, 2, "BYTE").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)
	})

	t.Run("BITPOS BIT bit=0 finds first zero within bit range", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\xff\x0f\xff", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 0, 8, 15, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)
	})

	t.Run("BITPOS BIT single bit precision check", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "\x00\x80", 0).Err())
		val, err := rdb.Do(ctx, "BITPOS", "mykey", 1, 8, 8, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, 8, val)

		val, err = rdb.Do(ctx, "BITPOS", "mykey", 1, 9, 15, "BIT").Int64()
		require.NoError(t, err)
		require.EqualValues(t, -1, val)
	})

	/* Test cases adapted from redis test cases : https://github.com/redis/redis/blob/unstable/tests/unit/bitops.tcl
	 */
	t.Run("BITPOS bit=0 with empty key returns 0", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "str").Err())
		cmd := rdb.BitPosSpan(ctx, "str", 0, 0, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 0, cmd.Val())
	})

	t.Run("BITPOS bit=0 with string less than 1 word works", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\xff\xf0\x00", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 0, 0, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 12, cmd.Val())
	})

	t.Run("BITPOS bit=1 with string less than 1 word works", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\x00\x0f\x00", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 1, 0, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 12, cmd.Val())
	})

	t.Run("BITPOS bit=0 starting at unaligned address", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\xff\xf0\x00", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 0, 1, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 12, cmd.Val())
	})

	t.Run("BITPOS bit=1 starting at unaligned address", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\x00\x0f\xff", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 1, 1, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 12, cmd.Val())
	})

	t.Run("BITPOS bit=0 unaligned+full word+reminder", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\xff\xff\xff", 0).Err())
		require.NoError(t, rdb.Append(ctx, "str", "\xff\xff\xff\xff\xff\xff\xff\xff").Err())
		require.NoError(t, rdb.Append(ctx, "str", "\xff\xff\xff\xff\xff\xff\xff\xff").Err())
		require.NoError(t, rdb.Append(ctx, "str", "\xff\xff\xff\xff\xff\xff\xff\xff").Err())
		require.NoError(t, rdb.Append(ctx, "str", "\x0f").Err())
		// Test values 1, 9, 17, 25, 33, 41, 49, 57, 65
		for i := 0; i < 9; i++ {
			if i == 6 {
				cmd := rdb.BitPosSpan(ctx, "str", 0, 41, -1, "bit")
				require.NoError(t, cmd.Err())
				require.EqualValues(t, 216, cmd.Val())
			} else {
				cmd := rdb.BitPosSpan(ctx, "str", 0, int64(i*8)+1, -1, "bit")
				require.NoError(t, cmd.Err())
				require.EqualValues(t, 216, cmd.Val())
			}
		}
	})

	t.Run("BITPOS bit=1 unaligned+full word+reminder", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\x00\x00\x00", 0).Err())
		require.NoError(t, rdb.Append(ctx, "str", "\x00\x00\x00\x00\x00\x00\x00\x00").Err())
		require.NoError(t, rdb.Append(ctx, "str", "\x00\x00\x00\x00\x00\x00\x00\x00").Err())
		require.NoError(t, rdb.Append(ctx, "str", "\x00\x00\x00\x00\x00\x00\x00\x00").Err())
		require.NoError(t, rdb.Append(ctx, "str", "\xf0").Err())
		// Test values 1, 9, 17, 25, 33, 41, 49, 57, 65
		for i := 0; i < 9; i++ {
			if i == 6 {
				cmd := rdb.BitPosSpan(ctx, "str", 1, 41, -1, "bit")
				require.NoError(t, cmd.Err())
				require.EqualValues(t, 216, cmd.Val())
			} else {
				cmd := rdb.BitPosSpan(ctx, "str", 1, int64(i*8)+1, -1, "bit")
				require.NoError(t, cmd.Err())
				require.EqualValues(t, 216, cmd.Val())
			}
		}
	})

	t.Run("BITPOS bit=1 returns -1 if string is all 0 bits", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "", 0).Err())
		for i := 0; i < 20; i++ {
			cmd := rdb.BitPosSpan(ctx, "str", 1, 0, -1, "bit")
			require.NoError(t, cmd.Err())
			require.EqualValues(t, -1, cmd.Val())
			require.NoError(t, rdb.Append(ctx, "str", "\x00").Err())
		}
	})

	t.Run("BITPOS bit=0 works with intervals", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\x00\xff\x00", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 0, 0, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 0, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 0, 8, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 16, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 0, 16, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 16, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 0, 16, 200, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 16, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 0, 8, 8, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, -1, cmd.Val())
	})

	t.Run("BITPOS bit=1 works with intervals", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\x00\xff\x00", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 1, 0, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 8, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 1, 8, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 8, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 1, 16, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, -1, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 1, 16, 200, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, -1, cmd.Val())
		cmd = rdb.BitPosSpan(ctx, "str", 1, 8, 8, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, 8, cmd.Val())
	})

	t.Run("BITPOS bit=0 changes behavior if end is given", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "str", "\xff\xff\xff", 0).Err())
		cmd := rdb.BitPosSpan(ctx, "str", 0, 0, -1, "bit")
		require.NoError(t, cmd.Err())
		require.EqualValues(t, -1, cmd.Val())
	})

	t.Run("BITPOS bit=1 fuzzy testing using SETBIT", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "str").Err())
		var maxInt int64 = 524288
		var firstOnePos int64 = -1
		for j := 0; j < 1000; j++ {
			cmd := rdb.BitPosSpan(ctx, "str", 1, 0, -1, "bit")
			require.NoError(t, cmd.Err())
			require.EqualValues(t, firstOnePos, cmd.Val())
			pos := util.RandomInt(maxInt)
			require.NoError(t, rdb.SetBit(ctx, "str", int64(pos), 1).Err())
			if firstOnePos == -1 || firstOnePos > pos {
				firstOnePos = pos
			}
		}
	})

	t.Run("BITPOS bit=0 fuzzy testing using SETBIT", func(t *testing.T) {
		var maxInt int64 = 524288
		firstZeroPos := maxInt
		require.NoError(t, rdb.Set(ctx, "str", strings.Repeat("\xff", int(maxInt/8)), 0).Err())
		for j := 0; j < 1000; j++ {
			cmd := rdb.BitPosSpan(ctx, "str", 0, 0, -1, "bit")
			require.NoError(t, cmd.Err())
			if firstZeroPos == maxInt {
				require.EqualValues(t, -1, cmd.Val())
			} else {
				require.EqualValues(t, firstZeroPos, cmd.Val())
			}
			pos := util.RandomInt(maxInt)
			require.NoError(t, rdb.SetBit(ctx, "str", int64(pos), 0).Err())
			if firstZeroPos > pos {
				firstZeroPos = pos
			}
		}
	})

}
