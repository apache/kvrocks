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

package client

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestClientSetInfo(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("CLIENT SETINFO LIB-NAME sets library name", func(t *testing.T) {
		res, err := rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "my-lib").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
	})

	t.Run("CLIENT SETINFO LIB-VER sets library version", func(t *testing.T) {
		res, err := rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-VER", "1.2.3").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)
	})

	t.Run("CLIENT INFO shows lib-name and lib-ver", func(t *testing.T) {
		info, err := rdb.Do(ctx, "CLIENT", "INFO").Result()
		require.NoError(t, err)
		infoStr, ok := info.(string)
		require.True(t, ok)
		require.Contains(t, infoStr, "lib-name=my-lib")
		require.Contains(t, infoStr, "lib-ver=1.2.3")
	})

	t.Run("CLIENT LIST shows lib-name and lib-ver", func(t *testing.T) {
		list, err := rdb.Do(ctx, "CLIENT", "LIST").Result()
		require.NoError(t, err)
		listStr, ok := list.(string)
		require.True(t, ok)
		require.Contains(t, listStr, "lib-name=my-lib")
		require.Contains(t, listStr, "lib-ver=1.2.3")
	})

	t.Run("CLIENT SETINFO is case-insensitive for attribute name", func(t *testing.T) {
		res, err := rdb.Do(ctx, "CLIENT", "SETINFO", "lib-name", "lower-lib").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)

		info, err := rdb.Do(ctx, "CLIENT", "INFO").Result()
		require.NoError(t, err)
		require.Contains(t, info, "lib-name=lower-lib")
	})

	t.Run("CLIENT SETINFO with empty value clears the field", func(t *testing.T) {
		res, err := rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "").Result()
		require.NoError(t, err)
		require.Equal(t, "OK", res)

		info, err := rdb.Do(ctx, "CLIENT", "INFO").Result()
		require.NoError(t, err)
		require.Contains(t, info.(string), "lib-name=")
		require.False(t, strings.Contains(info.(string), "lib-name=lower-lib"))
	})

	t.Run("CLIENT SETINFO rejects unknown attribute", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "SETINFO", "UNKNOWN", "value").Err()
		require.Error(t, err)
		require.Contains(t, err.Error(), "Unrecognized option")
	})

	t.Run("CLIENT SETINFO rejects value with spaces", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "my lib").Err()
		require.Error(t, err)
	})

	t.Run("CLIENT SETINFO wrong number of arguments", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME").Err()
		require.Error(t, err)
	})
}
