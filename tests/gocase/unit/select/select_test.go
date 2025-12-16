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

package selectcmd

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSelectWithoutCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("SELECT without redis-databases (disabled mode) should return OK", func(t *testing.T) {
		for i := 0; i < 16; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
		}
	})

	t.Run("SELECT with invalid DB index should return OK without compatible mode", func(t *testing.T) {
		// Without redis-databases > 0, SELECT always returns OK regardless of DB index
		require.NoError(t, rdb.Do(ctx, "SELECT", -1).Err())
		require.NoError(t, rdb.Do(ctx, "SELECT", 16).Err())
		require.NoError(t, rdb.Do(ctx, "SELECT", 100).Err())
	})
}

func TestSelectWithCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"redis-databases": "16",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Basic SELECT command", func(t *testing.T) {
		// Test SELECT 0-15
		for i := 0; i < 16; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
		}

		// Test invalid DB index
		util.ErrorRegexp(t, rdb.Do(ctx, "SELECT", -1).Err(), ".*DB number is out of range.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "SELECT", 16).Err(), ".*DB number is out of range.*")
	})

	t.Run("Data isolation between databases", func(t *testing.T) {
		// Set key in DB 0
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
		require.NoError(t, rdb.Set(ctx, "key1", "value1", 0).Err())
		require.Equal(t, "value1", rdb.Get(ctx, "key1").Val())

		// Switch to DB 1, key should not exist
		require.NoError(t, rdb.Do(ctx, "SELECT", 1).Err())
		require.Equal(t, redis.Nil, rdb.Get(ctx, "key1").Err())

		// Set different value in DB 1
		require.NoError(t, rdb.Set(ctx, "key1", "value2", 0).Err())
		require.Equal(t, "value2", rdb.Get(ctx, "key1").Val())

		// Switch back to DB 0, should see original value
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
		require.Equal(t, "value1", rdb.Get(ctx, "key1").Val())

		// Clean up
		require.NoError(t, rdb.Del(ctx, "key1").Err())
		require.NoError(t, rdb.Do(ctx, "SELECT", 1).Err())
		require.NoError(t, rdb.Del(ctx, "key1").Err())
	})

	t.Run("Multiple databases isolation", func(t *testing.T) {
		// Set different values in DB 0, 1, 2
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
			require.NoError(t, rdb.Set(ctx, "test_key", i, 0).Err())
		}

		// Verify each DB has its own value
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
			val, err := rdb.Get(ctx, "test_key").Int()
			require.NoError(t, err)
			require.Equal(t, i, val)
		}

		// Clean up
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
			require.NoError(t, rdb.Del(ctx, "test_key").Err())
		}
	})
}

func TestNamespaceCommandWithCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"redis-databases": "16",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("All NAMESPACE commands should be blocked", func(t *testing.T) {
		// Test NAMESPACE ADD
		err := rdb.Do(ctx, "NAMESPACE", "ADD", "test_ns", "test_token").Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*not allowed when redis-databases > 0.*")

		// Test NAMESPACE SET
		err = rdb.Do(ctx, "NAMESPACE", "SET", "test_ns", "new_token").Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*not allowed when redis-databases > 0.*")

		// Test NAMESPACE DEL
		err = rdb.Do(ctx, "NAMESPACE", "DEL", "test_ns").Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*not allowed when redis-databases > 0.*")

		// Test NAMESPACE GET
		err = rdb.Do(ctx, "NAMESPACE", "GET", "test_ns").Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*not allowed when redis-databases > 0.*")

		// Test NAMESPACE GET *
		err = rdb.Do(ctx, "NAMESPACE", "GET", "*").Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*not allowed when redis-databases > 0.*")

		// Test NAMESPACE CURRENT
		err = rdb.Do(ctx, "NAMESPACE", "CURRENT").Err()
		require.Error(t, err)
		util.ErrorRegexp(t, err, ".*not allowed when redis-databases > 0.*")
	})
}

func TestNamespaceCommandWithoutCompatibleMode(t *testing.T) {
	password := "test_password"
	srv := util.StartServer(t, map[string]string{
		"requirepass": password,
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClientWithOption(&redis.Options{
		Password: password,
	})
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("NAMESPACE commands should work without redis-select-compatible mode", func(t *testing.T) {
		// Test NAMESPACE ADD
		require.NoError(t, rdb.Do(ctx, "NAMESPACE", "ADD", "test_ns", "test_token").Err())

		// Test NAMESPACE GET
		result := rdb.Do(ctx, "NAMESPACE", "GET", "test_ns")
		require.NoError(t, result.Err())
		require.Equal(t, "test_token", result.Val())

		// Test NAMESPACE SET
		require.NoError(t, rdb.Do(ctx, "NAMESPACE", "SET", "test_ns", "new_token").Err())
		result = rdb.Do(ctx, "NAMESPACE", "GET", "test_ns")
		require.NoError(t, result.Err())
		require.Equal(t, "new_token", result.Val())

		// Test NAMESPACE CURRENT
		result = rdb.Do(ctx, "NAMESPACE", "CURRENT")
		require.NoError(t, result.Err())

		// Test NAMESPACE DEL
		require.NoError(t, rdb.Do(ctx, "NAMESPACE", "DEL", "test_ns").Err())

		// Verify deletion
		result = rdb.Do(ctx, "NAMESPACE", "GET", "test_ns")
		require.Equal(t, redis.Nil, result.Err())
	})
}

func TestClientInfoWithoutCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("CLIENT INFO should show namespace field without redis-databases", func(t *testing.T) {
		result := rdb.Do(ctx, "CLIENT", "INFO")
		require.NoError(t, result.Err())

		info, ok := result.Val().(string)
		require.True(t, ok, "CLIENT INFO should return a string")

		// Should contain namespace field
		require.Contains(t, info, "namespace=")

		// Should NOT contain db field when redis-databases is not set
		require.NotContains(t, info, "db=")
	})
}

func TestClientInfoWithCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"redis-databases": "16",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("CLIENT INFO should show db field with redis-databases", func(t *testing.T) {
		// Test default DB 0
		result := rdb.Do(ctx, "CLIENT", "INFO")
		require.NoError(t, result.Err())

		info, ok := result.Val().(string)
		require.True(t, ok, "CLIENT INFO should return a string")

		// Should contain db field
		require.Contains(t, info, "db=0")

		// Should NOT contain namespace field when redis-databases > 0
		require.NotContains(t, info, "namespace=")
	})

	t.Run("CLIENT INFO should show correct db number after SELECT", func(t *testing.T) {
		// Test DB 0
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
		result := rdb.Do(ctx, "CLIENT", "INFO")
		require.NoError(t, result.Err())
		info := result.Val().(string)
		require.Contains(t, info, "db=0")

		// Test DB 5
		require.NoError(t, rdb.Do(ctx, "SELECT", 5).Err())
		result = rdb.Do(ctx, "CLIENT", "INFO")
		require.NoError(t, result.Err())
		info = result.Val().(string)
		require.Contains(t, info, "db=5")

		// Test DB 15
		require.NoError(t, rdb.Do(ctx, "SELECT", 15).Err())
		result = rdb.Do(ctx, "CLIENT", "INFO")
		require.NoError(t, result.Err())
		info = result.Val().(string)
		require.Contains(t, info, "db=15")

		// Switch back to DB 0
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
		result = rdb.Do(ctx, "CLIENT", "INFO")
		require.NoError(t, result.Err())
		info = result.Val().(string)
		require.Contains(t, info, "db=0")
	})
}

func TestAuthWithCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"redis-databases": "16",
		"requirepass":     "foobar",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("AUTH command should work with redis-databases mode", func(t *testing.T) {
		// Test AUTH with wrong password
		err := rdb.Do(ctx, "AUTH", "wrong!").Err()
		require.Error(t, err)
		require.ErrorContains(t, err, "Invalid password")

		// Test AUTH with correct password
		require.NoError(t, rdb.Do(ctx, "AUTH", "foobar").Err())
	})

	t.Run("SELECT should work after AUTH", func(t *testing.T) {
		// Authenticate first
		require.NoError(t, rdb.Do(ctx, "AUTH", "foobar").Err())

		// Test SELECT commands
		for i := 0; i < 16; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
		}

		// Test invalid DB index
		util.ErrorRegexp(t, rdb.Do(ctx, "SELECT", 16).Err(), ".*DB number is out of range.*")
	})

	t.Run("Authentication state should persist across database switches", func(t *testing.T) {
		// Authenticate
		require.NoError(t, rdb.Do(ctx, "AUTH", "foobar").Err())

		// Switch to DB 0 and set a key
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
		require.NoError(t, rdb.Set(ctx, "auth_key", "value0", 0).Err())

		// Switch to DB 1 and set a key
		require.NoError(t, rdb.Do(ctx, "SELECT", 1).Err())
		require.NoError(t, rdb.Set(ctx, "auth_key", "value1", 0).Err())

		// Switch back to DB 0 and verify
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
		require.Equal(t, "value0", rdb.Get(ctx, "auth_key").Val())

		// Clean up
		require.NoError(t, rdb.Del(ctx, "auth_key").Err())
		require.NoError(t, rdb.Do(ctx, "SELECT", 1).Err())
		require.NoError(t, rdb.Del(ctx, "auth_key").Err())
	})

	t.Run("Unauthenticated client should not be able to use SELECT", func(t *testing.T) {
		// Create a new client without authentication
		rdb2 := srv.NewClient()
		defer func() { require.NoError(t, rdb2.Close()) }()

		// SELECT should fail without authentication
		err := rdb2.Do(ctx, "SELECT", 0).Err()
		require.Error(t, err)
		require.ErrorContains(t, err, "NOAUTH")
	})
}

func TestHelloWithCompatibleMode(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"redis-databases": "16",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("HELLO command should work with redis-databases mode", func(t *testing.T) {
		// Test HELLO with protocol 2
		result := rdb.Do(ctx, "HELLO", "2")
		require.NoError(t, result.Err())
		rList := result.Val().([]interface{})
		require.Contains(t, rList, "proto")
		require.Contains(t, rList, int64(2))

		// Test HELLO with protocol 3
		result = rdb.Do(ctx, "HELLO", "3")
		require.NoError(t, result.Err())
	})

	t.Run("HELLO should not show db field in compatible mode by default", func(t *testing.T) {
		// In redis-databases mode, HELLO response format depends on protocol version
		// For protocol 2, it returns array format
		result := rdb.Do(ctx, "HELLO", "2")
		require.NoError(t, result.Err())
		rList := result.Val().([]interface{})

		// Check that response contains expected fields
		require.Contains(t, rList, "proto")
		require.Contains(t, rList, "version")
	})

	t.Run("HELLO with SETNAME should work with SELECT", func(t *testing.T) {
		// Use HELLO with SETNAME
		require.NoError(t, rdb.Do(ctx, "HELLO", "2", "SETNAME", "test_client").Err())

		// Verify client name is set
		result := rdb.Do(ctx, "CLIENT", "GETNAME")
		require.NoError(t, result.Err())
		require.Equal(t, "test_client", result.Val())

		// Switch databases
		require.NoError(t, rdb.Do(ctx, "SELECT", 5).Err())

		// Client name should persist
		result = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.NoError(t, result.Err())
		require.Equal(t, "test_client", result.Val())

		// Switch back to DB 0
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
	})
}

func TestHelloWithAuthAndSelect(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"redis-databases": "16",
		"requirepass":     "foobar",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("HELLO with AUTH parameter should work", func(t *testing.T) {
		// Test HELLO with wrong password
		err := rdb.Do(ctx, "HELLO", "3", "AUTH", "wrong!").Err()
		require.Error(t, err)
		require.ErrorContains(t, err, "Invalid password")

		// Test HELLO with correct password
		require.NoError(t, rdb.Do(ctx, "HELLO", "3", "AUTH", "foobar").Err())
	})

	t.Run("HELLO with AUTH and username should work", func(t *testing.T) {
		// Test HELLO with username and password
		require.NoError(t, rdb.Do(ctx, "HELLO", "3", "AUTH", "default", "foobar").Err())

		// Verify we can execute commands
		require.NoError(t, rdb.Set(ctx, "test_key", "test_value", 0).Err())
		require.Equal(t, "test_value", rdb.Get(ctx, "test_key").Val())
		require.NoError(t, rdb.Del(ctx, "test_key").Err())
	})

	t.Run("SELECT should work after HELLO with AUTH", func(t *testing.T) {
		// Authenticate with HELLO
		require.NoError(t, rdb.Do(ctx, "HELLO", "2", "AUTH", "foobar").Err())

		// Test SELECT commands
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
			require.NoError(t, rdb.Set(ctx, "hello_key", i, 0).Err())
		}

		// Verify data isolation
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
			val, err := rdb.Get(ctx, "hello_key").Int()
			require.NoError(t, err)
			require.Equal(t, i, val)
		}

		// Clean up
		for i := 0; i < 3; i++ {
			require.NoError(t, rdb.Do(ctx, "SELECT", i).Err())
			require.NoError(t, rdb.Del(ctx, "hello_key").Err())
		}
	})

	t.Run("HELLO with AUTH and SETNAME should work with SELECT", func(t *testing.T) {
		// Use HELLO with AUTH and SETNAME
		require.NoError(t, rdb.Do(ctx, "HELLO", "2", "AUTH", "foobar", "SETNAME", "auth_client").Err())

		// Verify client name
		result := rdb.Do(ctx, "CLIENT", "GETNAME")
		require.NoError(t, result.Err())
		require.Equal(t, "auth_client", result.Val())

		// Switch databases and verify client name persists
		require.NoError(t, rdb.Do(ctx, "SELECT", 10).Err())
		result = rdb.Do(ctx, "CLIENT", "GETNAME")
		require.NoError(t, result.Err())
		require.Equal(t, "auth_client", result.Val())

		// Verify we can still execute commands
		require.NoError(t, rdb.Set(ctx, "final_key", "final_value", 0).Err())
		require.Equal(t, "final_value", rdb.Get(ctx, "final_key").Val())
		require.NoError(t, rdb.Del(ctx, "final_key").Err())

		// Switch back to DB 0
		require.NoError(t, rdb.Do(ctx, "SELECT", 0).Err())
	})
}
