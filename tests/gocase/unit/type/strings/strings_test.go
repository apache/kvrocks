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

package strings

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:    "txn-context-enabled",
			Options: []string{"yes", "no"},
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		testString(t, configs)
	}
}
func testString(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SET and GET an item", func(t *testing.T) {
		key := "x"
		value := "foobar"
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
		var payload []string
		for i := 0; i < 100; i++ {
			buf := util.RandString(1, 100000, util.Alpha)
			payload = append(payload, buf)
			require.NoError(t, rdb.Set(ctx, "bigpayload_"+strconv.Itoa(i), buf, 0).Err())
		}

		for i := 0; i < 1000; i++ {
			index := util.RandomInt(100)
			key := "bigpayload_" + strconv.FormatInt(index, 10)
			buf := rdb.Get(ctx, key).Val()
			require.Equal(t, payload[index], buf)
		}
	})

	t.Run("SET 10000 numeric keys and access all them in reverse order", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
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
		require.NoError(t, rdb.Del(ctx, "novar").Err())
		require.True(t, rdb.SetNX(ctx, "novar", "foobared", 0).Val())
		require.Equal(t, "foobared", rdb.Get(ctx, "novar").Val())
	})

	t.Run("SETNX target key exists", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "novar", "foobared", 0).Err())
		require.False(t, rdb.SetNX(ctx, "novar", "blabla", 0).Val())
		require.Equal(t, "foobared", rdb.Get(ctx, "novar").Val())
	})

	t.Run("SETNX against not-expired volatile key", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "x", "10", 0).Err())
		require.NoError(t, rdb.Expire(ctx, "x", 10000*time.Second).Err())
		require.False(t, rdb.SetNX(ctx, "x", "20", 0).Val())
		require.Equal(t, "10", rdb.Get(ctx, "x").Val())
	})

	t.Run("SETNX against expired volatile key", func(t *testing.T) {
		// Make it very unlikely for the key this test uses to be expired by the
		// active expiry cycle. This is tightly coupled to the implementation of
		// active expiry and dbAdd() but currently the only way to test that
		// SETNX expires a key when it should have been.
		for x := 0; x < 9999; x++ {
			require.NoError(t, rdb.SetEx(ctx, "key-"+"x", "value", 3600*time.Second).Err())
		}

		// This will be one of 10000 expiring keys. A cycle is executed every
		// 100ms, sampling 10 keys for being expired or not.  This key will be
		// expired for at most 1s when we wait 2s, resulting in a total sample
		// of 100 keys. The probability of the success of this test being a
		// false positive is therefore approx. 1%.
		require.NoError(t, rdb.Set(ctx, "x", "10", 0).Err())
		require.NoError(t, rdb.Expire(ctx, "x", time.Second).Err())

		// Wait for the key to expire
		time.Sleep(2 * time.Second)

		require.NoError(t, rdb.SetNX(ctx, "x", "20", 0).Err())
		require.Equal(t, "20", rdb.Get(ctx, "x").Val())
	})

	t.Run("GETEX EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.GetEx(ctx, "foo", 10*time.Second).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX Duplicate EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.GetEx(ctx, "foo", 10*time.Second).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "ex", 1, "ex", 10).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.GetEx(ctx, "foo", 10*time.Second).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX Duplicate PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "px", 1, "px", 10000).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(10*time.Second).Unix()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX Duplicate EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(100*time.Second).Unix(), "exat", time.Now().Add(10*time.Second).Unix()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX Duplicate PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(1000*time.Second).UnixMilli(), "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX PERSIST option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 10*time.Second).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
		require.NoError(t, rdb.Do(ctx, "getex", "foo", "persist").Err())
		require.EqualValues(t, -1, rdb.TTL(ctx, "foo").Val())
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "ex", 10, "persist").Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "px", 10000, "persist").Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "persist").Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(100*time.Second).Unix(), "persist").Err(), "syntax err")

		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "persist", "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "persist", "px", 10000).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "persist", "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "persist", "exat", time.Now().Add(100*time.Second).Unix()).Err(), "syntax err")

	})

	t.Run("GETEX with incorrect use of multi options should result in syntax err", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "px", 100, "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "px", 100, "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "px", 100, "exat", time.Now().Add(10*time.Second).Unix()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "ex", 10, "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "ex", 10, "exat", time.Now().Add(10*time.Second).Unix()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "exat", time.Now().Add(10*time.Second).Unix()).Err(), "syntax err")

		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "ex", 10, "px", 100).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "px", 100).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(10*time.Second).Unix(), "px", 100).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(10*time.Second).Unix(), "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "getex", "foo", "exat", time.Now().Add(10*time.Second).Unix(), "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")
	})

	t.Run("GETEX no option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.GetEx(ctx, "foo", 0).Val())

		// Make sure the expiration time is not erased.
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 10*time.Second).Err())
		require.Equal(t, "bar", rdb.Do(ctx, "getex", "foo").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "foo").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("GETEX syntax errors", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "getex", "foo", "non-existent-option").Err(), ".*syntax*.")
	})

	t.Run("GETEX no arguments", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "getex").Err(), ".*wrong number of arguments*.")
	})

	t.Run("GETEX against wrong type", func(t *testing.T) {
		rdb.Del(ctx, "foo")
		rdb.LPush(ctx, "foo", "bar")
		util.ErrorRegexp(t, rdb.Do(ctx, "getex", "foo").Err(), ".*WRONGTYPE.*")
		require.EqualValues(t, 1, rdb.Exists(ctx, "foo").Val())
		require.Equal(t, "list", rdb.Type(ctx, "foo").Val())
	})

	t.Run("GETDEL command", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.GetDel(ctx, "foo").Val())
		require.Equal(t, "", rdb.GetDel(ctx, "foo").Val())
	})

	t.Run("DelEX command no args", func(t *testing.T) {
		key := "test-string-key69"
		value := "test-strings-value69"
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())

		require.Equal(t, int64(1), rdb.Do(ctx, "DELEX", key).Val())
		require.Equal(t, "", rdb.Get(ctx, key).Val())

		require.NoError(t, rdb.Do(ctx, "DelEX", key).Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "DelEX", key).Val())

		require.Equal(t, "", rdb.Get(ctx, "random").Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", "random").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "DELEX", "random").Val())
	})

	t.Run("DelEX command with args", func(t *testing.T) {
		key := "test-string-key69"
		value := "Hello world"
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())

		r := rdb.Do(ctx, "DelEX", key, "random", "random", "random").Err()
		require.ErrorContains(t, r, "wrong number")

		r = rdb.Do(ctx, "DelEX", key, "random", "random").Err()
		require.ErrorContains(t, r, "syntax error")

		digest := "b6acb9d84a38ff74"
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifdeq", "xxxxxxxxxxxxxxxx").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "DelEX", key, "ifdeq", "xxxxxxxxxxxxxxxx").Val())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifdeq", digest).Err())
		require.Equal(t, "", rdb.Get(ctx, value).Val())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "DELEX", key, "ifdeq", digest).Val())
		require.Equal(t, "", rdb.Get(ctx, value).Val())

		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifdne", digest).Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "DelEX", key, "ifdne", digest).Val())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifdne", "xxxxxxxxxxxxxxxx").Err())
		require.Equal(t, "", rdb.Get(ctx, value).Val())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "DelEX", key, "ifdne", "xxxxxxxxxxxxxxxx").Val())
		require.Equal(t, "", rdb.Get(ctx, value).Val())

		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifeq", "random").Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "DelEX", key, "ifeq", "random").Val())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifeq", value).Err())
		require.Equal(t, "", rdb.Get(ctx, value).Val())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "DelEX", key, "ifeq", value).Val())
		require.Equal(t, "", rdb.Get(ctx, value).Val())

		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifne", value).Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "DelEX", key, "ifne", value).Val())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
		require.NoError(t, rdb.Do(ctx, "DelEX", key, "ifne", "random").Err())
		require.Equal(t, "", rdb.Get(ctx, value).Val())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "DelEX", key, "ifne", "random").Val())
		require.Equal(t, "", rdb.Get(ctx, value).Val())
	})

	t.Run("DelEX IFDEQ and IFDNE accept uppercase digest", func(t *testing.T) {
		key := "test-string-key-uppercase-digest"
		value := "Hello world"
		var digest string

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		digest = strings.ToUpper(rdb.Do(ctx, "DIGEST", key).Val().(string))
		require.Equal(t, int64(1), rdb.Do(ctx, "DelEX", key, "ifdeq", digest).Val())
		require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())

		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		digest = strings.ToUpper(rdb.Do(ctx, "DIGEST", key).Val().(string))
		require.Equal(t, int64(0), rdb.Do(ctx, "DelEX", key, "ifdne", digest).Val())
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("DelEX IFDEQ and IFDNE reject invalid digest length", func(t *testing.T) {
		key := "test-string-key-invalid-digest"
		value := "Hello world"

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "DelEX", key, "ifdeq", "123456789012345").Err(),
			"exactly 16 hexadecimal characters")
		require.Equal(t, value, rdb.Get(ctx, key).Val())

		require.ErrorContains(t, rdb.Do(ctx, "DelEX", key, "ifdne", "123456789012345").Err(),
			"exactly 16 hexadecimal characters")
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("MGET command", func(t *testing.T) {
		require.NoError(t, rdb.FlushDB(ctx).Err())
		require.NoError(t, rdb.Set(ctx, "foo", "BAR", 0).Err())
		require.NoError(t, rdb.Set(ctx, "bar", "FOO", 0).Err())
		require.Equal(t, []interface{}{"BAR", "FOO"}, rdb.MGet(ctx, "foo", "bar").Val())
	})

	t.Run("MGET against non existing key", func(t *testing.T) {
		require.Equal(t, []interface{}{"BAR", nil, "FOO"}, rdb.MGet(ctx, "foo", "baazz", "bar").Val())
	})

	t.Run("MGET against non-string key", func(t *testing.T) {
		require.NoError(t, rdb.SAdd(ctx, "myset", "ciao", "bau").Err())
		require.Equal(t, []interface{}{"BAR", nil, "FOO", nil}, rdb.MGet(ctx, "foo", "baazz", "bar", "myset").Val())
	})

	t.Run("GETSET set new value", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "", rdb.GetSet(ctx, "foo", "xyz").Val())
		require.Equal(t, "xyz", rdb.Get(ctx, "foo").Val())
	})

	t.Run("GETSET replace old value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.GetSet(ctx, "foo", "xyz").Val())
		require.Equal(t, "xyz", rdb.Get(ctx, "foo").Val())
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

	t.Run("MSETEX wrong args", func(t *testing.T) {
		r := rdb.Do(ctx, "msetex", "0").Err()
		require.ErrorContains(t, r, "wrong number")
		r = rdb.Do(ctx, "msetex", "0", "a", "1").Err()
		require.ErrorContains(t, r, "value is out of range, must be positive")
		r = rdb.Do(ctx, "msetex", "3", "a", "1", "b", "2").Err()
		require.ErrorContains(t, r, "wrong number")
		r = rdb.Do(ctx, "msetex", "1", "a", "1", "b", "2", "xx").Err()
		require.ErrorContains(t, r, "syntax error")
		r = rdb.Do(ctx, "msetex", "1", "a", "1", "ex", "-1").Err()
		require.ErrorContains(t, r, "out of numeric range")
		r = rdb.Do(ctx, "msetex", "1", "a", "1", "ex", "10", "keepttl").Err()
		require.ErrorContains(t, r, "syntax error")
	})

	t.Run("MSETEX with NX|XX", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "xx1", "xx2").Err())
		res := rdb.MSetEX(ctx, redis.MSetEXArgs{Condition: redis.NX}, "xx1", "1", "xx2", "2")
		require.EqualValues(t, 1, res.Val())
		require.Equal(t, "1", rdb.Get(ctx, "xx1").Val())

		require.NoError(t, rdb.Set(ctx, "xx3", "pre", 0).Err())
		res = rdb.MSetEX(ctx, redis.MSetEXArgs{Condition: redis.NX}, "xx3", "a", "xx4", "b")
		require.EqualValues(t, 0, res.Val())
		require.Equal(t, "pre", rdb.Get(ctx, "xx3").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "xx4").Val())

		res = rdb.MSetEX(ctx, redis.MSetEXArgs{
			Condition: redis.XX,
			Expiration: &redis.ExpirationOption{
				Mode: redis.EX, Value: 10,
			}}, "xx1", "new1", "xx2", "new2")
		require.EqualValues(t, 1, res.Val())
		require.Equal(t, "new1", rdb.Get(ctx, "xx1").Val())
		require.Equal(t, "new2", rdb.Get(ctx, "xx2").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "xx1").Val(), 9*time.Second, 10*time.Second)
		util.BetweenValues(t, rdb.TTL(ctx, "xx2").Val(), 9*time.Second, 10*time.Second)
	})

	t.Run("MSETEX with TTL", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a", "b").Err())
		res := rdb.MSetEX(ctx, redis.MSetEXArgs{
			Condition: redis.NX,
			Expiration: &redis.ExpirationOption{
				Mode:  redis.EX,
				Value: 2,
			}}, "a", "1", "b", "2")
		require.EqualValues(t, 1, res.Val())
		require.Equal(t, "1", rdb.Get(ctx, "a").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a").Val(), 1*time.Second, 2*time.Second)
		util.BetweenValues(t, rdb.TTL(ctx, "b").Val(), 1*time.Second, 2*time.Second)

		res = rdb.MSetEX(ctx, redis.MSetEXArgs{
			Condition: redis.XX,
			Expiration: &redis.ExpirationOption{
				Mode:  redis.PX,
				Value: 3000,
			}}, "a", "10", "d", "20")
		require.EqualValues(t, 0, res.Val())
		require.Equal(t, "1", rdb.Get(ctx, "a").Val())

		res = rdb.MSetEX(ctx, redis.MSetEXArgs{
			Condition: redis.XX,
			Expiration: &redis.ExpirationOption{
				Mode:  redis.PX,
				Value: 3000,
			}}, "a", "10", "b", "20")
		require.EqualValues(t, 1, res.Val())
		require.Equal(t, "10", rdb.Get(ctx, "a").Val())
		require.Equal(t, "20", rdb.Get(ctx, "b").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "a").Val(), 2*time.Second, 3*time.Second)
		util.BetweenValues(t, rdb.TTL(ctx, "b").Val(), 2*time.Second, 3*time.Second)
	})

	t.Run("MSETEX with KEEPTTL", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "k1", "v", 5*time.Second).Err())
		require.NoError(t, rdb.Del(ctx, "k2").Err())
		res := rdb.MSetEX(ctx, redis.MSetEXArgs{
			Expiration: &redis.ExpirationOption{
				Mode: redis.KEEPTTL,
			}}, "k1", "v2", "k2", "v3")
		require.EqualValues(t, 1, res.Val())
		require.Equal(t, "v2", rdb.Get(ctx, "k1").Val())
		require.Equal(t, "v3", rdb.Get(ctx, "k2").Val())
		util.BetweenValues(t, rdb.TTL(ctx, "k1").Val(), 4*time.Second, 5*time.Second)
		require.EqualValues(t, -1, rdb.TTL(ctx, "k2").Val())
	})

	t.Run("MSETEX with TXN", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "k1", "k2").Err())
		res := rdb.MSetEX(ctx, redis.MSetEXArgs{}, "k1", "v1")
		require.EqualValues(t, 1, res.Val())
		txn := rdb.TxPipeline()
		txn.MSetEX(ctx, redis.MSetEXArgs{Condition: redis.XX}, "k1", "v10", "k2", "v20")
		_, err := txn.Exec(ctx)
		require.NoError(t, err)
		require.Equal(t, "v1", rdb.Get(ctx, "k1").Val())
		require.Equal(t, "", rdb.Get(ctx, "k2").Val())
	})

	t.Run("MSETNX with already existent key", func(t *testing.T) {
		r := rdb.MSetNX(ctx, map[string]interface{}{
			"x1": "xxx",
			"y2": "yyy",
			"x":  "20",
		})
		require.False(t, r.Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "x1").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "y2").Val())
	})

	t.Run("MSETNX with not existing keys", func(t *testing.T) {
		r := rdb.MSetNX(ctx, map[string]interface{}{
			"x1": "xxx",
			"y2": "yyy",
		})
		require.True(t, r.Val())
		require.Equal(t, "xxx", rdb.Get(ctx, "x1").Val())
		require.Equal(t, "yyy", rdb.Get(ctx, "y2").Val())
	})

	t.Run("MSETNX with already existent key - same key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.NoError(t, rdb.Set(ctx, "x", "v0", 0).Err())
		require.Equal(t, int64(0), rdb.Do(ctx, "MSETNX", "x", "v1", "x", "v2").Val())
		require.EqualValues(t, 1, rdb.Exists(ctx, "x").Val())
		require.Equal(t, "v0", rdb.Get(ctx, "x").Val())
	})

	t.Run("MSETNX with not existing keys - same key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "x").Err())
		require.Equal(t, int64(1), rdb.Do(ctx, "MSETNX", "x", "v1", "x", "v2").Val())
		require.EqualValues(t, 1, rdb.Exists(ctx, "x").Val())
		require.Equal(t, "v2", rdb.Get(ctx, "x").Val())
	})

	t.Run("STRLEN against non-existing key", func(t *testing.T) {
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
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 0, 1).Err(), "WRONGTYPE")
	})

	t.Run("SETBIT with out of range bit offset", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 4*1024*1024*1024+2, 1).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", -1, 1).Err(), "an integer")
	})

	t.Run("SETBIT with non-bit argument", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, -1).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, 2).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, 10).Err(), "out of range")
		require.ErrorContains(t, rdb.SetBit(ctx, "mykey", 1, 20).Err(), "out of range")
	})

	t.Run("SETBIT/GETBIT/BITCOUNT/BITPOS boundary check (type string)", func(t *testing.T) {
		// When setting the last possible bit (offset equal to 2^32 -1) and the string value stored at key
		// does not yet hold a string value, or holds a small string value, Kvrocks needs to allocate all
		// intermediate memory which can block the server for some time. See also https://redis.io/commands/setbit/.
		rdb := srv.NewClientWithOption(&redis.Options{
			ReadTimeout: time.Minute,
		})
		defer func() { require.NoError(t, rdb.Close()) }()

		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.NoError(t, rdb.Set(ctx, "mykey", "", 0).Err())
		var maxOffset int64 = math.MaxUint32
		require.NoError(t, rdb.SetBit(ctx, "mykey", maxOffset, 1).Err())
		require.EqualValues(t, 1, rdb.GetBit(ctx, "mykey", maxOffset).Val())
		require.EqualValues(t, 1, rdb.BitCount(ctx, "mykey", &redis.BitCount{Start: 0, End: maxOffset / 8}).Val())
		// Last byte should contain 1 bit.
		require.EqualValues(t, 1, rdb.BitCount(ctx, "mykey", &redis.BitCount{Start: -1, End: -1}).Val())
		// 0 - Last byte should contain 1 bit.
		require.EqualValues(t, 1, rdb.BitCount(ctx, "mykey", &redis.BitCount{Start: -100, End: -1}).Val())
		// The first byte shouldn't contain any bits
		require.EqualValues(t, 0, rdb.BitCount(ctx, "mykey", &redis.BitCount{Start: -100, End: -100}).Val())
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
		require.ErrorContains(t, rdb.SetRange(ctx, "mykey", 0, "bar").Err(), "WRONGTYPE")
	})

	t.Run("SETRANGE with negative offset", func(t *testing.T) {
		require.ErrorContains(t, rdb.SetRange(ctx, "setrange_negative_offset", -1, "bar").Err(),
			"value is not an integer or out of range")
		require.ErrorContains(t, rdb.SetRange(ctx, "setrange_negative_offset", -2147483599, "bar").Err(),
			"value is not an integer or out of range")
	})

	t.Run("SETRANGE with offset + value length too large", func(t *testing.T) {
		protoMaxBulkLen := int64(1024 * 1024)
		require.NoError(t, rdb.ConfigSet(ctx, "proto-max-bulk-len", strconv.FormatInt(protoMaxBulkLen, 10)).Err())
		require.ErrorContains(t, rdb.SetRange(ctx, "setrange_out_of_range", protoMaxBulkLen, "world").Err(),
			"string exceeds maximum allowed size")

		// it should be able to set the value if the length is protoMaxBulkLen
		require.NoError(t, rdb.SetRange(ctx, "setrange_out_of_range", protoMaxBulkLen-5, "world").Err())
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

	// Since go-redis does not support SUBSTR, use Do to call the SUBSTR command.
	t.Run("SUBSTR against non-existing key", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "mykey").Err())
		require.Nil(t, rdb.Do(ctx, "SUBSTR", "mykey", 0, -1).Val())
	})

	t.Run("SUBSTR against string value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", "Hello World", 0).Err())
		require.EqualValues(t, "Hell", rdb.Do(ctx, "SUBSTR", "mykey", 0, 3).Val())
		require.EqualValues(t, "ll", rdb.Do(ctx, "SUBSTR", "mykey", 2, 3).Val())
		require.EqualValues(t, "Hello World", rdb.Do(ctx, "SUBSTR", "mykey", 0, -1).Val())
		require.EqualValues(t, "orld", rdb.Do(ctx, "SUBSTR", "mykey", -4, -1).Val())
		require.Nil(t, rdb.Do(ctx, "SUBSTR", "mykey", 5, 3).Val())
		require.EqualValues(t, " World", rdb.Do(ctx, "SUBSTR", "mykey", 5, 5000).Val())
		require.EqualValues(t, "Hello World", rdb.Do(ctx, "SUBSTR", "mykey", -5000, 10000).Val())
	})

	t.Run("SUBSTR against integer-encoded value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "mykey", 1234, 0).Err())
		require.EqualValues(t, "123", rdb.Do(ctx, "SUBSTR", "mykey", 0, 2).Val())
		require.EqualValues(t, "1234", rdb.Do(ctx, "SUBSTR", "mykey", 0, -1).Val())
		require.EqualValues(t, "234", rdb.Do(ctx, "SUBSTR", "mykey", -3, -1).Val())
		require.Nil(t, rdb.Do(ctx, "SUBSTR", "mykey", 5, 3).Val())
		require.EqualValues(t, "4", rdb.Do(ctx, "SUBSTR", "mykey", 3, 5000).Val())
		require.EqualValues(t, "1234", rdb.Do(ctx, "SUBSTR", "mykey", -5000, 10000).Val())
	})

	t.Run("Extended SET can detect syntax errors", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "non-existing-option").Err(), "syntax error")
	})

	t.Run("Extended SET NX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "1", "nx").Val())
		require.Nil(t, rdb.Do(ctx, "SET", "foo", "2", "nx").Val())
		require.Equal(t, "1", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET XX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Nil(t, rdb.Do(ctx, "SET", "foo", "1", "xx").Val())
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "2", "xx").Val())
		require.Equal(t, "2", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "ex", "10").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET Duplicate EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "ex", "1", "ex", "2", "ex", "10").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "px", "10000").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET Duplicate PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "px", "100", "px", "1000", "px", "10000").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireAt := strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "exat", expireAt).Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET Duplicate EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireFirst := strconv.FormatInt(time.Now().Add(1*time.Second).Unix(), 10)
		expireSecond := strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "exat", expireFirst, "exat", expireSecond).Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET EXAT option with expired timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "exat", "1").Val())
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())

		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())

		expireAt := strconv.FormatInt(time.Now().Add(-5*time.Second).Unix(), 10)
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "exat", expireAt).Val())
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireAt := strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "pxat", expireAt).Val())

		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET Duplicate PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireFirst := strconv.FormatInt(time.Now().Add(1*time.Second).UnixMilli(), 10)
		expireSecond := strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "pxat", expireFirst, "pxat", expireSecond).Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET PXAT option with expired timestamp", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "pxat", "1").Val())
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())

		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())

		expireAt := strconv.FormatInt(time.Now().Add(-5*time.Second).UnixMilli(), 10)
		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "pxat", expireAt).Val())
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET with incorrect use of multi options should result in syntax err", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "px", 100, "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "px", 100, "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "px", 100, "exat", time.Now().Add(10*time.Second).Unix()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "ex", 10, "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "ex", 10, "exat", time.Now().Add(10*time.Second).Unix()).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "exat", time.Now().Add(10*time.Second).Unix()).Err(), "syntax err")

		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "ex", 10, "px", 100).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "px", 100).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "exat", time.Now().Add(10*time.Second).Unix(), "px", 100).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "pxat", time.Now().Add(10*time.Second).UnixMilli(), "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "exat", time.Now().Add(10*time.Second).Unix(), "ex", 10).Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "exat", time.Now().Add(10*time.Second).Unix(), "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err(), "syntax err")

		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "NX", "XX").Err(), "syntax err")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "XX", "NX").Err(), "syntax err")
	})

	t.Run("Extended SET with incorrect expire value", func(t *testing.T) {
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "ex", "1234xyz").Err(), "non-integer")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "ex", "0").Err(), "out of numeric range")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "exat", "1234xyz").Err(), "non-integer")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "exat", "0").Err(), "out of numeric range")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "pxat", "1234xyz").Err(), "non-integer")
		require.ErrorContains(t, rdb.Do(ctx, "SET", "foo", "bar", "pxat", "0").Err(), "out of numeric range")
	})

	t.Run("Extended SET using multiple options at once", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())

		require.Equal(t, "OK", rdb.Do(ctx, "SET", "foo", "bar", "xx", "px", "10000").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET KEEPTTL and EX/PX/EXAT/PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Error(t, rdb.Do(ctx, "SET", "foo", "xx", "keepttl", "ex", "100").Err())
		require.Error(t, rdb.Do(ctx, "SET", "foo", "xx", "keepttl", "px", "100").Err())
		require.Error(t, rdb.Do(ctx, "SET", "foo", "xx", "keepttl", "exat", "100").Err())
		require.Error(t, rdb.Do(ctx, "SET", "foo", "xx", "keepttl", "pxat", "100").Err())
	})

	t.Run("Extended SET KEEPTTL WITH option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "OK", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{KeepTTL: true}).Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		require.Equal(t, time.Duration(-1), ttl)
		require.Equal(t, "OK", rdb.Set(ctx, "foo", "bar", 10*time.Second).Val())
		require.Equal(t, "OK", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{KeepTTL: true}).Val())
		ttl = rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET GET option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "", rdb.SetArgs(ctx, "foo", "bar", redis.SetArgs{Get: true}).Val())
		require.Equal(t, "bar", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true}).Val())
		require.Equal(t, "xx", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET GET and NX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true, Mode: "NX"}).Val())
		require.Equal(t, "xx", rdb.Get(ctx, "foo").Val())
		require.Equal(t, "OK", rdb.Set(ctx, "foo", "bar", 0).Val())
		require.Equal(t, "bar", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true, Mode: "NX"}).Val())
		require.Equal(t, "bar", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET GET and XX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true, Mode: "XX"}).Val())
		require.Equal(t, "", rdb.Get(ctx, "foo").Val())
		require.Equal(t, "OK", rdb.Set(ctx, "foo", "bar", 0).Val())
		require.Equal(t, "bar", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true, Mode: "XX"}).Val())
		require.Equal(t, "xx", rdb.Get(ctx, "foo").Val())
	})

	t.Run("Extended SET GET and KEEPTTL option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, "", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true, KeepTTL: true}).Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		require.Equal(t, time.Duration(-1), ttl)
		require.Equal(t, "OK", rdb.Set(ctx, "foo", "bar", 10*time.Second).Val())
		require.Equal(t, "bar", rdb.SetArgs(ctx, "foo", "xx", redis.SetArgs{Get: true, KeepTTL: true}).Val())
		ttl = rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET GET and EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, nil, rdb.Do(ctx, "SET", "foo", "bar", "ex", "10", "get").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET GET and PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())
		require.Equal(t, nil, rdb.Do(ctx, "SET", "foo", "bar", "px", "10000", "get").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET GET and EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireAt := strconv.FormatInt(time.Now().Add(10*time.Second).Unix(), 10)
		require.Equal(t, nil, rdb.Do(ctx, "SET", "foo", "bar", "exat", expireAt, "get").Val())
		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("Extended SET GET and PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "foo").Err())

		expireAt := strconv.FormatInt(time.Now().Add(10*time.Second).UnixMilli(), 10)
		require.Equal(t, nil, rdb.Do(ctx, "SET", "foo", "bar", "pxat", expireAt, "get").Val())

		ttl := rdb.TTL(ctx, "foo").Val()
		util.BetweenValues(t, ttl, 5*time.Second, 10*time.Second)
	})

	t.Run("GETRANGE with huge ranges, Github issue redis/redis#1844", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "foo", "bar", 0).Err())
		require.Equal(t, "bar", rdb.GetRange(ctx, "foo", 0, 2094967291).Val())
	})

	t.Run("CAS normal case", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())

		require.EqualValues(t, -1, rdb.Do(ctx, "CAS", "cas_key", "old_value", "new_value").Val())
		require.EqualValues(t, 0, rdb.Exists(ctx, "cas_key").Val())
		require.Equal(t, "OK", rdb.Set(ctx, "cas_key", "old_value", 0).Val())
		require.EqualValues(t, 0, rdb.Do(ctx, "CAS", "cas_key", "old_val", "new_value").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, "CAS", "cas_key", "old_value", "new_value").Val())
	})

	t.Run("CAS wrong key type", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "a_list_key").Err())
		require.NoError(t, rdb.LPush(ctx, "a_list_key", "123").Err())
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "a_list_key", "123", "234").Err(), "WRONGTYPE")
	})

	t.Run("CAS invalid param num", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123").Err(), "ERR wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex").Err(), "no more")
	})

	t.Run("CAS expire", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())

		require.EqualValues(t, 1, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", "1").Val())
		require.Equal(t, "234", rdb.Get(ctx, "cas_key").Val())

		time.Sleep(2 * time.Second)

		require.Equal(t, "", rdb.Get(ctx, "cas_key").Val())
	})

	t.Run("CAS expire EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.EqualValues(t, 1, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", 10).Val())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS expire Duplicate EX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.EqualValues(t, 1, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", 100, "ex", 10).Val())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS expire PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.EqualValues(t, 1, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "px", 10000).Val())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS expire Duplicate PX option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.EqualValues(t, 1, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "px", 1000, "px", 10000).Val())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS expire EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.NoError(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "exat", time.Now().Add(10*time.Second).Unix()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS Duplicate EXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.NoError(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "exat", time.Now().Add(100*time.Second).Unix(), "exat", time.Now().Add(10*time.Second).Unix()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS expire PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.NoError(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS Duplicate PXAT option", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.NoError(t, rdb.Do(ctx, "cas", "cas_key", "123", "234", "pxat", time.Now().Add(100*time.Second).UnixMilli(), "pxat", time.Now().Add(10*time.Second).UnixMilli()).Err())
		util.BetweenValues(t, rdb.TTL(ctx, "cas_key").Val(), 5*time.Second, 10*time.Second)
	})

	t.Run("CAS expire mutually exclusive options exist at the same time", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "cas_key").Err())
		require.NoError(t, rdb.Set(ctx, "cas_key", "123", 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", 100, "px", 100000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", 100, "ex", 10, "px", 10000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "px", 10000, "ex", 100, "ex", 10, "px", 10000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", 100, "exat", 100000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "ex", 100, "pxat", 100000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "px", 100, "exat", 100000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "px", 100, "pxat", 100000).Err(), "syntax error")
		require.ErrorContains(t, rdb.Do(ctx, "CAS", "cas_key", "123", "234", "exat", 100, "pxat", 100000).Err(), "syntax error")
	})

	t.Run("CAD normal case", func(t *testing.T) {
		require.EqualValues(t, -1, rdb.Do(ctx, "CAD", "cad_key", "123").Val())
		require.NoError(t, rdb.Set(ctx, "cad_key", "123", 0).Err())
		require.EqualValues(t, 0, rdb.Do(ctx, "CAD", "cad_key", "234").Val())
		require.EqualValues(t, 1, rdb.Do(ctx, "CAD", "cad_key", "123").Val())
		require.Equal(t, "", rdb.Get(ctx, "cad_key").Val())
	})

	t.Run("CAD invalid param num", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "cad_key", "123", 0).Err())

		require.ErrorContains(t, rdb.Do(ctx, "CAD", "cad_key").Err(), "ERR wrong number of arguments")
		require.ErrorContains(t, rdb.Do(ctx, "CAD", "cad_key", "123", "234").Err(), "ERR wrong number of arguments")
	})

	rna1 := "CACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTTCGTCCGGGTGTG"
	rna2 := "ATTAAAGGTTTATACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT"
	rnalcs := "ACCTTCCCAGGTAACAAACCAACCAACTTTCGATCTCTTGTAGATCTGTTCTCTAAACGAACTTTAAAATCTGTGTGGCTGTCACTCGGCTGCATGCTTAGTGCACTCACGCAGTATAATTAATAACTAATTACTGTCGTTGACAGGACACGAGTAACTCGTCTATCTTCTGCAGGCTGCTTACGGTTTCGTCCGTGTTGCAGCCGATCATCAGCACATCTAGGTTT"

	t.Run("LCS basic", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "virus1", rna1, 0).Err())
		require.NoError(t, rdb.Set(ctx, "virus2", rna2, 0).Err())
		require.Equal(t, rnalcs, rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2"}).Val().MatchString)
	})

	t.Run("LCS len", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "virus1", rna1, 0).Err())
		require.NoError(t, rdb.Set(ctx, "virus2", rna2, 0).Err())
		require.Equal(t, int64(len(rnalcs)), rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2", Len: true}).Val().Len)
	})

	t.Run("LCS indexes", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "virus1", rna1, 0).Err())
		require.NoError(t, rdb.Set(ctx, "virus2", rna2, 0).Err())
		matches := rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2", Idx: true}).Val().Matches
		require.Equal(t, []redis.LCSMatchedPosition{
			{
				Key1: redis.LCSPosition{Start: 238, End: 238},
				Key2: redis.LCSPosition{Start: 239, End: 239},
			},
			{
				Key1: redis.LCSPosition{Start: 236, End: 236},
				Key2: redis.LCSPosition{Start: 238, End: 238},
			},
			{
				Key1: redis.LCSPosition{Start: 229, End: 230},
				Key2: redis.LCSPosition{Start: 236, End: 237},
			},
			{
				Key1: redis.LCSPosition{Start: 224, End: 224},
				Key2: redis.LCSPosition{Start: 235, End: 235},
			},
			{
				Key1: redis.LCSPosition{Start: 1, End: 222},
				Key2: redis.LCSPosition{Start: 13, End: 234},
			},
		}, matches)
	})

	t.Run("LCS indexes with match len", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "virus1", rna1, 0).Err())
		require.NoError(t, rdb.Set(ctx, "virus2", rna2, 0).Err())
		matches := rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2", Idx: true, WithMatchLen: true}).Val().Matches
		require.Equal(t, []redis.LCSMatchedPosition{
			{
				Key1:     redis.LCSPosition{Start: 238, End: 238},
				Key2:     redis.LCSPosition{Start: 239, End: 239},
				MatchLen: 1,
			},
			{
				Key1:     redis.LCSPosition{Start: 236, End: 236},
				Key2:     redis.LCSPosition{Start: 238, End: 238},
				MatchLen: 1,
			},
			{
				Key1:     redis.LCSPosition{Start: 229, End: 230},
				Key2:     redis.LCSPosition{Start: 236, End: 237},
				MatchLen: 2,
			},
			{
				Key1:     redis.LCSPosition{Start: 224, End: 224},
				Key2:     redis.LCSPosition{Start: 235, End: 235},
				MatchLen: 1,
			},
			{
				Key1:     redis.LCSPosition{Start: 1, End: 222},
				Key2:     redis.LCSPosition{Start: 13, End: 234},
				MatchLen: 222,
			},
		}, matches)
	})

	t.Run("LCS indexes with match len and minimum match len", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "virus1", rna1, 0).Err())
		require.NoError(t, rdb.Set(ctx, "virus2", rna2, 0).Err())
		matches := rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2", Idx: true, WithMatchLen: true, MinMatchLen: 5}).Val().Matches
		require.Equal(t, []redis.LCSMatchedPosition{
			{
				Key1:     redis.LCSPosition{Start: 1, End: 222},
				Key2:     redis.LCSPosition{Start: 13, End: 234},
				MatchLen: 222,
			},
		}, matches)
	})

	t.Run("LCS empty", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "virus1", rna1, 0).Err())
		require.NoError(t, rdb.Set(ctx, "virus2", "", 0).Err())

		require.Equal(t, rna1, rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus1"}).Val().MatchString)
		require.Equal(t, "", rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2"}).Val().MatchString)
		require.Equal(t, "", rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus2", Key2: "virus1"}).Val().MatchString)
		require.Equal(t, "", rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus2", Key2: "virus2"}).Val().MatchString)

		require.Equal(t, int64(0), rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2"}).Val().Len)
		require.Equal(t, []redis.LCSMatchedPosition{}, rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2", Idx: true}).Val().Matches)
		require.Equal(t, []redis.LCSMatchedPosition{}, rdb.LCS(ctx, &redis.LCSQuery{Key1: "virus1", Key2: "virus2", Idx: true, WithMatchLen: true}).Val().Matches)
	})

	t.Run("DelEX IFDEQ and IFDNE reject invalid digest length", func(t *testing.T) {
		key := "test-string-key-invalid-digest"
		value := "Hello world"

		require.NoError(t, rdb.Del(ctx, key).Err())
		require.NoError(t, rdb.Set(ctx, key, value, 0).Err())
		require.ErrorContains(t, rdb.Do(ctx, "DelEX", key, "ifdeq", "123456789012345").Err(),
			"exactly 16 hexadecimal characters")
		require.Equal(t, value, rdb.Get(ctx, key).Val())

		require.ErrorContains(t, rdb.Do(ctx, "DelEX", key, "ifdne", "123456789012345").Err(),
			"exactly 16 hexadecimal characters")
		require.Equal(t, value, rdb.Get(ctx, key).Val())
	})

	t.Run("IFEQ missing cmp_value returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "IFEQ").Err()
		require.Error(t, err)
	})

	t.Run("IFNE missing cmp_value returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "IFNE").Err()
		require.Error(t, err)
	})

	t.Run("IFDEQ missing cmp_value returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "IFDEQ").Err()
		require.Error(t, err)
	})

	t.Run("IFDNE missing cmp_value returns error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "IFDNE").Err()
		require.Error(t, err)
	})

	t.Run("NX and IFEQ together returns syntax error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "NX", "IFEQ", "x").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "syntax")
	})

	t.Run("XX and IFNE together returns syntax error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "XX", "IFNE", "x").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "syntax")
	})

	t.Run("IFEQ and IFDEQ together returns syntax error", func(t *testing.T) {
		err := rdb.Do(ctx, "SET", "k", "v", "IFEQ", "x", "IFDEQ", "y").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "syntax")
	})

	t.Run("WRONGTYPE error when key is not a string", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "listkey").Err())
		require.NoError(t, rdb.RPush(ctx, "listkey", "a").Err())
		err := rdb.Do(ctx, "SET", "listkey", "v", "IFEQ", "a").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "WRONGTYPE")
		require.NoError(t, rdb.Del(ctx, "listkey").Err())
	})

	t.Run("IFEQ: key not found returns nil", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "ifeq1").Err())
		res := rdb.Do(ctx, "SET", "ifeq1", "new", "IFEQ", "anything").Val()
		require.Nil(t, res)
	})

	t.Run("IFEQ: value matches writes and returns OK", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifeq2", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifeq2", "world", "IFEQ", "hello").Val()
		require.Equal(t, "OK", res)
		require.Equal(t, "world", rdb.Get(ctx, "ifeq2").Val())
	})

	t.Run("IFEQ: value mismatches returns nil and no write", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifeq3", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifeq3", "world", "IFEQ", "wrong").Val()
		require.Nil(t, res)
		require.Equal(t, "hello", rdb.Get(ctx, "ifeq3").Val())
	})

	t.Run("IFNE: key not found writes and returns OK", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "ifne1").Err())
		res := rdb.Do(ctx, "SET", "ifne1", "created", "IFNE", "anything").Val()
		require.Equal(t, "OK", res)
		require.Equal(t, "created", rdb.Get(ctx, "ifne1").Val())
	})

	t.Run("IFNE: value matches returns nil and no write", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifne2", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifne2", "world", "IFNE", "hello").Val()
		require.Nil(t, res)
		require.Equal(t, "hello", rdb.Get(ctx, "ifne2").Val())
	})

	t.Run("IFNE: value mismatches writes and returns OK", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifne3", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifne3", "world", "IFNE", "wrong").Val()
		require.Equal(t, "OK", res)
		require.Equal(t, "world", rdb.Get(ctx, "ifne3").Val())
	})

	t.Run("IFDEQ: key not found returns nil", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "ifdeq1").Err())
		res := rdb.Do(ctx, "SET", "ifdeq1", "new", "IFDEQ", "xxxxxxxxxxxxxxxx").Val()
		require.Nil(t, res)
	})

	t.Run("IFDEQ: digest matches writes and returns OK", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifdeq2", "hello", 0).Err())
		digest, err := rdb.Do(ctx, "DIGEST", "ifdeq2").Result()
		require.NoError(t, err)
		res := rdb.Do(ctx, "SET", "ifdeq2", "world", "IFDEQ", digest).Val()
		require.Equal(t, "OK", res)
		require.Equal(t, "world", rdb.Get(ctx, "ifdeq2").Val())
	})

	t.Run("IFDEQ: digest mismatches returns nil and no write", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifdeq3", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifdeq3", "world", "IFDEQ", "xxxxxxxxxxxxxxxx").Val()
		require.Nil(t, res)
		require.Equal(t, "hello", rdb.Get(ctx, "ifdeq3").Val())
	})

	t.Run("IFDNE: key not found writes and returns OK", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "ifdne1").Err())
		res := rdb.Do(ctx, "SET", "ifdne1", "created", "IFDNE", "xxxxxxxxxxxxxxxx").Val()
		require.Equal(t, "OK", res)
		require.Equal(t, "created", rdb.Get(ctx, "ifdne1").Val())
	})

	t.Run("IFDNE: digest matches returns nil and no write", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifdne2", "hello", 0).Err())
		digest, err := rdb.Do(ctx, "DIGEST", "ifdne2").Result()
		require.NoError(t, err)
		res := rdb.Do(ctx, "SET", "ifdne2", "world", "IFDNE", digest).Val()
		require.Nil(t, res)
		require.Equal(t, "hello", rdb.Get(ctx, "ifdne2").Val())
	})

	t.Run("IFDNE: digest mismatches writes and returns OK", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifdne3", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifdne3", "world", "IFDNE", "xxxxxxxxxxxxxxxx").Val()
		require.Equal(t, "OK", res)
		require.Equal(t, "world", rdb.Get(ctx, "ifdne3").Val())
	})

	t.Run("IFEQ with GET: condition met returns old value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifeq-get1", "old", 0).Err())
		res, err := rdb.Do(ctx, "SET", "ifeq-get1", "new", "IFEQ", "old", "GET").Result()
		require.NoError(t, err)
		require.Equal(t, "old", res)
		require.Equal(t, "new", rdb.Get(ctx, "ifeq-get1").Val())
	})

	t.Run("IFEQ with GET: condition not met returns old value", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifeq-get2", "hello", 0).Err())
		res, err := rdb.Do(ctx, "SET", "ifeq-get2", "new", "IFEQ", "wrong", "GET").Result()
		require.NoError(t, err)
		require.Equal(t, "hello", res)
		require.Equal(t, "hello", rdb.Get(ctx, "ifeq-get2").Val())
	})

	t.Run("IFEQ with EX: condition met sets TTL", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifeq-ex1", "hello", 0).Err())
		res, err := rdb.Do(ctx, "SET", "ifeq-ex1", "world", "IFEQ", "hello", "EX", "10").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
		ttl := rdb.TTL(ctx, "ifeq-ex1").Val()
		require.Greater(t, ttl, 8*time.Second)
		require.LessOrEqual(t, ttl, 10*time.Second)
	})

	t.Run("IFEQ with EX: condition not met leaves TTL unchanged", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifeq-ex2", "hello", 5*time.Second).Err())
		res := rdb.Do(ctx, "SET", "ifeq-ex2", "world", "IFEQ", "wrong", "EX", "100").Val()
		require.Nil(t, res)
		ttl := rdb.TTL(ctx, "ifeq-ex2").Val()
		require.Greater(t, ttl, time.Duration(0))
		require.LessOrEqual(t, ttl, 5*time.Second)
	})

	t.Run("IFDEQ consistent with DIGEST command output", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "digest-check", "somevalue", 0).Err())
		digest, err := rdb.Do(ctx, "DIGEST", "digest-check").Result()
		require.NoError(t, err)
		res, err := rdb.Do(ctx, "SET", "digest-check", "newvalue", "IFDEQ", digest).Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
	})

	t.Run("IFDEQ accepts uppercase digest", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifdeq-upper", "hello", 0).Err())
		digest, err := rdb.Do(ctx, "DIGEST", "ifdeq-upper").Text()
		require.NoError(t, err)

		res, err := rdb.Do(ctx, "SET", "ifdeq-upper", "world", "IFDEQ", strings.ToUpper(digest)).Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
		require.Equal(t, "world", rdb.Get(ctx, "ifdeq-upper").Val())
	})

	t.Run("IFDNE treats uppercase digest as a match", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifdne-upper", "hello", 0).Err())
		digest, err := rdb.Do(ctx, "DIGEST", "ifdne-upper").Text()
		require.NoError(t, err)

		res := rdb.Do(ctx, "SET", "ifdne-upper", "world", "IFDNE", strings.ToUpper(digest)).Val()
		require.Nil(t, res)
		require.Equal(t, "hello", rdb.Get(ctx, "ifdne-upper").Val())
	})

	t.Run("IFDEQ and IFDNE reject malformed digest lengths", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifd-bad-digest", "hello", 0).Err())

		testCases := []struct {
			name   string
			option string
			digest string
		}{
			{name: "IFDEQ short digest", option: "IFDEQ", digest: "1234567890abcde"},
			{name: "IFDNE short digest", option: "IFDNE", digest: "1234567890abcde"},
			{name: "IFDEQ long digest", option: "IFDEQ", digest: "01234567890abcdef"},
			{name: "IFDNE long digest", option: "IFDNE", digest: "01234567890abcdef"},
			{name: "IFDEQ empty digest", option: "IFDEQ", digest: ""},
			{name: "IFDNE empty digest", option: "IFDNE", digest: ""},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := rdb.Do(ctx, "SET", "ifd-bad-digest", "world", tc.option, tc.digest).Err()
				require.ErrorContains(t, err, "must be exactly 16 hexadecimal characters")
				require.Equal(t, "hello", rdb.Get(ctx, "ifd-bad-digest").Val())
			})
		}
	})

	t.Run("IFDEQ and IFDNE: non-hex 16-char digest treated as non-match", func(t *testing.T) {
		require.NoError(t, rdb.Set(ctx, "ifd-nonhex", "hello", 0).Err())
		res := rdb.Do(ctx, "SET", "ifd-nonhex", "world", "IFDEQ", "GGGGGGGGGGGGGGGG").Val()
		require.Nil(t, res, "IFDEQ with non-hex 16-char digest should return nil (no match)")
		require.Equal(t, "hello", rdb.Get(ctx, "ifd-nonhex").Val())

		res = rdb.Do(ctx, "SET", "ifd-nonhex", "world", "IFDNE", "GGGGGGGGGGGGGGGG").Val()
		require.Equal(t, "OK", res, "IFDNE with non-hex 16-char digest should write (no match)")
		require.Equal(t, "world", rdb.Get(ctx, "ifd-nonhex").Val())
	})

	t.Run("IFEQ writes when value matches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop1-" + strconv.Itoa(i)
			val := util.RandString(1, 20, util.Alpha)
			newVal := util.RandString(1, 20, util.Alpha)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res, err := rdb.Do(ctx, "SET", key, newVal, "IFEQ", val).Result()
			require.NoError(t, err)
			require.Equal(t, "OK", res, "IFEQ should write when cmp_value matches current value")
			require.Equal(t, newVal, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFEQ does not write when value mismatches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop2-" + strconv.Itoa(i)
			val := "value-" + strconv.Itoa(i)
			wrong := "wrong-" + strconv.Itoa(i)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res := rdb.Do(ctx, "SET", key, "new", "IFEQ", wrong).Val()
			require.Nil(t, res, "IFEQ should return nil when cmp_value does not match")
			require.Equal(t, val, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFNE writes when value mismatches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop3-" + strconv.Itoa(i)
			val := "value-" + strconv.Itoa(i)
			wrong := "wrong-" + strconv.Itoa(i)
			newVal := "new-" + strconv.Itoa(i)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res, err := rdb.Do(ctx, "SET", key, newVal, "IFNE", wrong).Result()
			require.NoError(t, err)
			require.Equal(t, "OK", res, "IFNE should write when cmp_value does not match current value")
			require.Equal(t, newVal, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFNE does not write when value matches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop4-" + strconv.Itoa(i)
			val := util.RandString(1, 20, util.Alpha)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res := rdb.Do(ctx, "SET", key, "new", "IFNE", val).Val()
			require.Nil(t, res, "IFNE should return nil when cmp_value matches current value")
			require.Equal(t, val, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFDEQ writes when digest matches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop5-" + strconv.Itoa(i)
			val := util.RandString(1, 20, util.Alpha)
			newVal := util.RandString(1, 20, util.Alpha)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			digest, err := rdb.Do(ctx, "DIGEST", key).Result()
			require.NoError(t, err)
			res, err := rdb.Do(ctx, "SET", key, newVal, "IFDEQ", digest).Result()
			require.NoError(t, err)
			require.Equal(t, "OK", res, "IFDEQ should write when digest matches")
			require.Equal(t, newVal, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFDEQ does not write when digest mismatches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop6-" + strconv.Itoa(i)
			val := util.RandString(1, 20, util.Alpha)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res := rdb.Do(ctx, "SET", key, "new", "IFDEQ", "xxxxxxxxxxxxxxxx").Val()
			require.Nil(t, res, "IFDEQ should return nil when digest does not match")
			require.Equal(t, val, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFDNE writes when digest mismatches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop7-" + strconv.Itoa(i)
			val := util.RandString(1, 20, util.Alpha)
			newVal := util.RandString(1, 20, util.Alpha)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res, err := rdb.Do(ctx, "SET", key, newVal, "IFDNE", "xxxxxxxxxxxxxxxx").Result()
			require.NoError(t, err)
			require.Equal(t, "OK", res, "IFDNE should write when digest does not match")
			require.Equal(t, newVal, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("IFDNE does not write when digest matches (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop8-" + strconv.Itoa(i)
			val := util.RandString(1, 20, util.Alpha)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			digest, err := rdb.Do(ctx, "DIGEST", key).Result()
			require.NoError(t, err)
			res := rdb.Do(ctx, "SET", key, "new", "IFDNE", digest).Val()
			require.Nil(t, res, "IFDNE should return nil when digest matches")
			require.Equal(t, val, rdb.Get(ctx, key).Val())
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("TTL unchanged when condition not met (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop9-" + strconv.Itoa(i)
			val := "value-" + strconv.Itoa(i)
			require.NoError(t, rdb.Set(ctx, key, val, 10*time.Second).Err())
			res := rdb.Do(ctx, "SET", key, "new", "IFEQ", "wrong", "EX", "9999").Val()
			require.Nil(t, res)
			ttl := rdb.TTL(ctx, key).Val()
			require.Greater(t, ttl, time.Duration(0), "TTL should remain positive after failed conditional SET")
			require.LessOrEqual(t, ttl, 10*time.Second)
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("TTL correctly set when condition met (100 iterations)", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			key := "prop10-" + strconv.Itoa(i)
			val := "value-" + strconv.Itoa(i)
			require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
			res, err := rdb.Do(ctx, "SET", key, "new", "IFEQ", val, "EX", "30").Result()
			require.NoError(t, err)
			require.Equal(t, "OK", res)
			ttl := rdb.TTL(ctx, key).Val()
			require.Greater(t, ttl, 28*time.Second)
			require.LessOrEqual(t, ttl, 30*time.Second)
			require.NoError(t, rdb.Del(ctx, key).Err())
		}
	})

	t.Run("Extended SET GET and NX option on wrong type", func(t *testing.T) {
		require.NoError(t, rdb.Del(ctx, "listkey").Err())
		require.NoError(t, rdb.LPush(ctx, "listkey", "v1").Err())
		require.ErrorContains(t, rdb.Do(ctx, "SET", "listkey", "v", "NX", "GET").Err(), "WRONGTYPE")
	})
}
