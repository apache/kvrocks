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

package restore

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestRestore_String(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	key := "foo"
	value := "\x00\x03bar\n\x00\xe6\xbeI`\xeef\xfd\x17"
	require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
	require.Equal(t, "bar", rdb.Get(ctx, key).Val())
	require.EqualValues(t, -1, rdb.TTL(ctx, key).Val())

	// Cannot restore to an existing key.
	newValue := "\x00\x03new\n\x000tA\x15\x9ch\x17|"
	require.EqualError(t, rdb.Restore(ctx, key, 0, newValue).Err(),
		"ERR target key name already exists.")

	// Restore exists key with the replacement flag
	require.NoError(t, rdb.RestoreReplace(ctx, key, 10*time.Second, newValue).Err())
	require.Equal(t, "new", rdb.Get(ctx, key).Val())
	require.Greater(t, rdb.TTL(ctx, key).Val(), 5*time.Second)
	require.LessOrEqual(t, rdb.TTL(ctx, key).Val(), 10*time.Second)
}

func TestRestore_ListWithListPackEncoding(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	rand.Seed(time.Now().Unix())
	key := util.RandString(0, 10, util.Alpha)
	value := "\x12\x01\x02\xc3%@z\az\x00\x00\x00\a\x00\xb5x\xe0+\x00\x026\xa2y\xe0\x18\x00\x02#\x8ez\xe0\x04\x00\t\x0f\x01\x01\x02\x01\x03\x01\x04\x01\xff\n\x00\x89\x14\xff>\xf8F\x0e="
	require.NoError(t, rdb.Restore(ctx, key, 0, value).Err())
	require.EqualValues(t, 7, rdb.LLen(ctx, key).Val())
	require.EqualValues(t, []string{
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
		"yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy",
		"zzzzzzzzzzzzzz",
		"1", "2", "3", "4",
	}, rdb.LRange(ctx, key, 0, -1).Val())
	require.EqualValues(t, -1, rdb.TTL(ctx, key).Val())
}
