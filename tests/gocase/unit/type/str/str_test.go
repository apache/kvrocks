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

package str

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()
	c := srv.NewTCPClient()
	defer func() { require.NoError(t, c.Close()) }()

	t.Run("SET and GET an item", func(t *testing.T) {
		key := "x"
		value := "foo"
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("SET and GET an empty item", func(t *testing.T) {
		key := "x"
		value := ""
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("Very big payload in GET/SET", func(t *testing.T) {
		key := "x"
		value := strings.Repeat("abcd", 1000000)
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("Very big payload random access", func(t *testing.T) {
		var values []string
		for i := 0; i < 100; i++ {
			value := util.RandString(1, 100000, util.Alpha)
			values = append(values, value)

			require.NoError(t, rdb.Set(ctx, "key_"+strconv.Itoa(i), value, 0).Err())
		}

		for i := 0; i < 1000; i++ {
			numElements := util.RandomInt(100)
			key := "key_" + strconv.FormatInt(numElements, 10)
			value := rdb.Get(ctx, key).Val()
			require.Equal(t, values[numElements], value)
		}
	})

	t.Run("SET 10000 numeric keys and access all them in reverse order", func(t *testing.T) {
		for i := 0; i < 10000; i++ {
			key := strconv.Itoa(i)
			value := key
			require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		}

		for i := 9999; i >= 0; i-- {
			key := strconv.Itoa(i)
			value := key
			require.EqualValues(t, value, rdb.Get(ctx, key).Val())
		}
	})

	t.Run("SETNX target key missing", func(t *testing.T) {
		require.NoError(t, rdb.SetNX(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	})

	t.Run("SETNX target key exists", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.SetNX(ctx, "foo", "bared", 0).Err())
		require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	})

	t.Run("SETNX against not-expired volatile key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "10", 10000*time.Second).Err())
		require.NoError(t, rdb.SetNX(ctx, "x", "20", 0).Err())
		require.Equal(t, "10", rdb.Get(ctx, "x").Val())
	})

	t.Run("SETNX against expired volatile key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "10", 1*time.Second).Err())
		require.Eventually(t, func() bool {
			require.NoError(t, rdb.SetNX(ctx, "x", "20", 0).Err())
			return rdb.Get(ctx, "x").Val() == "20"
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("GETDEL command", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.GetDel(ctx, "foo").Val())
		require.Equal(t, "", rdb.GetDel(ctx, "foo").Val())
	})

	t.Run("MGET command", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "FOO", 0).Err())
		require.NoError(t, rdb.Set(ctx, "bar", "BAR", 0).Err())
		require.Equal(t, []interface{}{"FOO", "BAR"}, rdb.MGet(ctx, "foo", "bar").Val())
	})

	t.Run("MGET against non existing key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "FOO", 0).Err())
		require.NoError(t, rdb.Set(ctx, "bar", "BAR", 0).Err())
		require.Equal(t, []interface{}{"FOO", nil, "BAR"}, rdb.MGet(ctx, "foo", "baazz", "bar").Val())
	})

	t.Run("MGET against non-string key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "FOO", 0).Err())
		require.NoError(t, rdb.Set(ctx, "bar", "BAR", 0).Err())
		require.NoError(t, rdb.SAdd(ctx, "myset", "ciao", "bau").Err())
		require.Equal(t, []interface{}{"FOO", nil, "BAR", nil}, rdb.MGet(ctx, "foo", "baazz", "bar", "myset").Val())
	})

	t.Run("GETSET set new value", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		oldValue := rdb.GetSet(ctx, "foo", "xyz").Val()
		newValue := rdb.Get(ctx, "foo").Val()
		require.Equal(t, "", oldValue)
		require.Equal(t, "xyz", newValue)
	})

	t.Run("GETSET replace old value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		oldValue := rdb.GetSet(ctx, "foo", "xyz").Val()
		newValue := rdb.Get(ctx, "foo").Val()
		require.Equal(t, "bar", oldValue)
		require.Equal(t, "xyz", newValue)
	})

	t.Run("MSET base case", func(t *testing.T) {
		require.NoError(t, rdb.MSet(ctx, map[string]interface{}{
			"x": "10",
			"y": "foo bar",
			"z": "x x x x x x x\n\n\r\n",
		}).Err())
		require.Equal(t, []interface{}{"10", "foo bar", "x x x x x x x\n\n\r\n"}, rdb.MGet(ctx, "x", "y", "z").Val())
	})

	t.Run("MSET wrong number of args", func(t *testing.T) {
		r := rdb.MSet(ctx, "x", "10", "y", "foo bar", "z")
		require.ErrorContains(t, r.Err(), "wrong number")
	})

	t.Run("MSETNX with already existent key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "10", 0).Err())
		require.EqualValues(t, 0, rdb.Exists(ctx, "x1").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "y2").Val())
		require.EqualValues(t, 1, rdb.Exists(ctx, "x").Val())
		r := rdb.MSetNX(ctx, map[string]interface{}{
			"x1": "xxx",
			"y2": "yyy",
			"x":  "20",
		})
		require.Equal(t, false, r.Val())
	})

	t.Run("MSETNX with not existing keys", func(t *testing.T) {
		require.EqualValues(t, 0, rdb.Exists(ctx, "x1").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "y2").Val())
		r := rdb.MSetNX(ctx, map[string]interface{}{
			"x1": "xxx",
			"y2": "yyy",
		})
		require.Equal(t, true, r.Val())
	})

	t.Run("STRLEN against non-existing key", func(t *testing.T) {
		require.EqualValues(t, 0, rdb.Exists(ctx, "notakey").Val())
		require.EqualValues(t, 0, rdb.StrLen(ctx, "notakey").Val())
	})

	t.Run("STRLEN against integer-encoded value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "myinteger", -555, 0).Err())
		require.EqualValues(t, 4, rdb.StrLen(ctx, "myinteger").Val())
	})

	t.Run("STRLEN against plain string", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mystring", "foozzz0123456789 baz", 0).Err())
		require.EqualValues(t, 20, rdb.StrLen(ctx, "mystring").Val())
	})

	t.Run("SETBIT against key with wrong type", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.LPush(ctx, "mykey", "foo").Err())
		r := rdb.SetBit(ctx, "mykey", 0, 1)
		require.ErrorContains(t, r.Err(), "WRONGTYPE")
	})

	t.Run("SETBIT with out of range bit offset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 4*1024*1024*1024+2, 1).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", -1, 1).Err(), "out of range")
	})

	t.Run("SETBIT with non-bit argument", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, -1).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, 2).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, 10).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, 20).Err(), "out of range")
	})

	t.Run("SETBIT/GETBIT/BITCOUNT/BITPOS boundary check (type string)", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "", 0).Err())
		var maxOffset int64 = 4*1024*1024*1024 - 1
		require.NoError(t, rdb.SetBit(ctx, "mykey", maxOffset, 1).Err())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "mykey", maxOffset).Val())
		require.EqualValues(t, 1, rdb.BitCount(ctx, "mykey", &redis.BitCount{Start: 0, End: maxOffset / 8}).Val())
		require.EqualValues(t, maxOffset, rdb.BitPos(ctx, "mykey", 1).Val())
	})

	t.Run("GETBIT against string-encoded key", func(t *testing.T) {
		// Single byte with 2nd and 3rd bit set
		require.NoError(t, rdb.Set(ctx, "mykey", "`", 0).Err())

		// In-range
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 0).Val())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "mykey", 1).Val())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "mykey", 2).Val())
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 3).Val())

		// Out-range
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 8).Val())
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 100).Val())
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 10000).Val())
	})

	t.Run("GETBIT against integer-encoded key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", 1, 0).Err())

		// Ascii "1" is integer 49 = 00 11 00 01
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 0).Val())
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 1).Val())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "mykey", 2).Val())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "mykey", 3).Val())

		// Out-range
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 8).Val())
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 100).Val())
		require.EqualValues(t, 0, rdb.GetBit(ctx, "mykey", 10000).Val())
	})

	t.Run("SETRANGE against non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.EqualValues(t, 3, rdb.SetRange(ctx, "mykey", 0, "foo").Val())
		require.Equal(t, "foo", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.EqualValues(t, 0, rdb.SetRange(ctx, "mykey", 0, "").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "mykey").Val())

		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.EqualValues(t, 4, rdb.SetRange(ctx, "mykey", 1, "foo").Val())
		require.Equal(t, "\000foo", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SETRANGE against string-encoded key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "foo", 0).Err())
		require.EqualValues(t, 3, rdb.SetRange(ctx, "mykey", 0, "b").Val())
		require.Equal(t, "boo", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Set(ctx, "mykey", "foo", 0).Err())
		require.EqualValues(t, 3, rdb.SetRange(ctx, "mykey", 0, "").Val())
		require.Equal(t, "foo", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Set(ctx, "mykey", "foo", 0).Err())
		require.EqualValues(t, 3, rdb.SetRange(ctx, "mykey", 1, "b").Val())
		require.Equal(t, "fbo", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Set(ctx, "mykey", "foo", 0).Err())
		require.EqualValues(t, 7, rdb.SetRange(ctx, "mykey", 4, "bar").Val())
		require.Equal(t, "foo\000bar", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SETRANGE against integer-encoded key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", 1234, 0).Err())
		require.EqualValues(t, 4, rdb.SetRange(ctx, "mykey", 0, "2").Val())
		require.Equal(t, "2234", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Set(ctx, "mykey", 1234, 0).Err())
		require.EqualValues(t, 4, rdb.SetRange(ctx, "mykey", 0, "").Val())
		require.Equal(t, "1234", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Set(ctx, "mykey", 1234, 0).Err())
		require.EqualValues(t, 4, rdb.SetRange(ctx, "mykey", 1, "3").Val())
		require.Equal(t, "1334", rdb.Get(ctx, "mykey").Val())

		require.NoError(t, rdb.Set(ctx, "mykey", 1234, 0).Err())
		require.EqualValues(t, 6, rdb.SetRange(ctx, "mykey", 5, "2").Val())
		require.Equal(t, "1234\0002", rdb.Get(ctx, "mykey").Val())
	})

	t.Run("SETRANGE against key with wrong type", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.LPush(ctx, "mykey", "foo").Err())
		r := rdb.SetRange(ctx, "mykey", 0, "bar")
		require.ErrorContains(t, r.Err(), "WRONGTYPE")
	})

	t.Run("GETRANGE against non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.EqualValues(t, "", rdb.GetRange(ctx, "mykey", 0, -1).Val())
	})

	t.Run("GETRANGE against string value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello World", 0).Err())
		require.EqualValues(t, "Hell", rdb.GetRange(ctx, "mykey", 0, 3).Val())
		require.EqualValues(t, "ll", rdb.GetRange(ctx, "mykey", 2, 3).Val())
		require.EqualValues(t, "Hello World", rdb.GetRange(ctx, "mykey", 0, -1).Val())
		require.EqualValues(t, "orld", rdb.GetRange(ctx, "mykey", -4, -1).Val())
		require.EqualValues(t, "", rdb.GetRange(ctx, "mykey", 5, 3).Val())
		require.EqualValues(t, " World", rdb.GetRange(ctx, "mykey", 5, 5000).Val())
		require.EqualValues(t, "Hello World", rdb.GetRange(ctx, "mykey", -5000, 10000).Val())
	})

	t.Run("GETRANGE against integer-encoded value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", 1234, 0).Err())
		require.EqualValues(t, "123", rdb.GetRange(ctx, "mykey", 0, 2).Val())
		require.EqualValues(t, "1234", rdb.GetRange(ctx, "mykey", 0, -1).Val())
		require.EqualValues(t, "234", rdb.GetRange(ctx, "mykey", -3, -1).Val())
		require.EqualValues(t, "", rdb.GetRange(ctx, "mykey", 5, 3).Val())
		require.EqualValues(t, "4", rdb.GetRange(ctx, "mykey", 3, 5000).Val())
		require.EqualValues(t, "1234", rdb.GetRange(ctx, "mykey", -5000, 10000).Val())
	})

	t.Run("Extended SET can detect syntax errors", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "non-existing-option"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "syntax error")
	})

	t.Run("Extended SET NX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "1", "nx"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		require.NoError(t, c.WriteArgs("SET", "foo", "2", "nx"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "$-1", r)

		require.Equal(t, "1", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET XX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "1", "xx"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "$-1", r)

		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "2", "xx"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		require.Equal(t, "2", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "ex", "10"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		ttl := rdb.TTL(ctx, "foo").Val()
		require.True(t, ttl <= 10*time.Second && ttl > 5*time.Second)
	})

	t.Run("Extended SET PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "px", "10000"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		ttl := rdb.TTL(ctx, "foo").Val()
		require.True(t, ttl <= 10*time.Second && ttl > 5*time.Second)
	})

	t.Run("Extended SET EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireAt := strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "exat", expireAt))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		ttl := rdb.TTL(ctx, "foo").Val()
		require.True(t, ttl <= 10*time.Second && ttl > 5*time.Second)
	})

	t.Run("Extended SET EXAT option with expired timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "exat", "1"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())

		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())

		expireAt := strconv.FormatInt(time.Now().Add(-5*time.Second).Unix(), 10)
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "exat", expireAt))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireAt := strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "pxat", expireAt))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		ttl := rdb.TTL(ctx, "foo").Val()
		require.True(t, ttl <= 10*time.Second && ttl > 5*time.Second)
	})

	t.Run("Extended SET PXAT option with expired timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "pxat", "1"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())

		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())

		expireAt := strconv.FormatInt(time.Now().Add(-5*time.Second).UnixMilli(), 10)
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "pxat", expireAt))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET with incorrect use of multi options should result in syntax err", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "ex", "10", "px", "10000"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "syntax err")

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "NX", "XX"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "syntax err")
	})

	t.Run("Extended SET with incorrect expire value", func(t *testing.T) {
		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "ex", "1234xyz"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "not an integer")

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "ex", "0"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid expire time")

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "exat", "1234xyz"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "not an integer")

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "exat", "0"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid expire time")

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "pxat", "1234xyz"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "not an integer")

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "pxat", "0"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "invalid expire time")
	})

	t.Run("Extended SET using multiple options at once", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())

		require.NoError(t, c.WriteArgs("SET", "foo", "bar", "xx", "px", "10000"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, "+OK", r)

		ttl := rdb.TTL(ctx, "foo").Val()
		require.True(t, ttl <= 10*time.Second && ttl > 5*time.Second)
	})

	t.Run("GETRANGE with huge ranges, Github issue #1844", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.GetRange(ctx, "foo", 0, 2094967291).Val())
	})

	t.Run("CAS normal case", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())

		require.NoError(t, c.WriteArgs("CAS", "cas_key", "old_value", "new_value"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, ":-1", r)

		require.EqualValues(t, 0, rdb.Exists(ctx, "cas_key").Val())
		require.NoError(t, rdb.Set(ctx, "cas_key", "old_value", 0).Err())

		require.NoError(t, c.WriteArgs("CAS", "cas_key", "old_val", "new_value"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, ":0", r)
		require.Equal(t, "old_value", rdb.Get(ctx, "cas_key").Val())

		require.NoError(t, c.WriteArgs("CAS", "cas_key", "old_value", "new_value"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, ":1", r)
		require.Equal(t, "new_value", rdb.Get(ctx, "cas_key").Val())
	})

	t.Run("CAS wrong key type", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a_list_key").Err())
		require.NoError(t, rdb.LPush(ctx, "a_list_key", "123").Err())

		require.NoError(t, c.WriteArgs("CAS", "a_list_key", "123", "234"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "WRONGTYPE")
	})

	t.Run("CAS invalid param num", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())

		require.NoError(t, c.WriteArgs("CAS", "cas_key", "123"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "ERR wrong number of arguments")

		require.NoError(t, c.WriteArgs("CAS", "cas_key", "123", "234", "ex"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "ERR wrong number of arguments")
	})

	t.Run("CAS expire", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())

		require.NoError(t, c.WriteArgs("CAS", "cas_key", "123", "234", "ex", "1"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, r, ":1")

		require.Equal(t, "234", rdb.Get(ctx, "cas_key").Val())

		require.Eventually(t, func() bool {
			return rdb.Get(ctx, "cas_key").Val() == ""
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("CAD normal case", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cad_key").Err())

		require.NoError(t, c.WriteArgs("CAD", "cad_key", "123"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, ":-1", r)

		require.NoError(t, rdb.Set(ctx, "cad_key", "123", 0).Err())

		require.NoError(t, c.WriteArgs("CAD", "cad_key", "234"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, ":0", r)

		require.NoError(t, c.WriteArgs("CAD", "cad_key", "123"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Equal(t, ":1", r)

		require.Equal(t, "", rdb.Get(ctx, "cad_key").Val())
	})

	t.Run("CAD invalid param num", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "cad_key", "123", 0).Err())

		require.NoError(t, c.WriteArgs("CAD", "cad_key"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "ERR wrong number of arguments")

		require.NoError(t, c.WriteArgs("CAD", "cad_key", "123", "234"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "ERR wrong number of arguments")
	})
}
