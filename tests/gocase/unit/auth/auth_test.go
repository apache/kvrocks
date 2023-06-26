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

package auth

import (
	"context"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestNoAuth(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("AUTH fails if there is no password configured server side", func(t *testing.T) {
		r := rdb.Do(ctx, "AUTH", "foo")
		require.ErrorContains(t, r.Err(), "no password")
	})
}

func TestAuth(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"requirepass": "foobar",
	})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("AUTH fails when a wrong password is given", func(t *testing.T) {
		r := rdb.Do(ctx, "AUTH", "wrong!")
		require.ErrorContains(t, r.Err(), "invalid password")
	})

	t.Run("Arbitrary command gives an error when AUTH is required", func(t *testing.T) {
		r := rdb.Set(ctx, "foo", "bar", 0)
		require.ErrorContains(t, r.Err(), "NOAUTH Authentication required.")
	})

	t.Run("AUTH succeeds when the right password is given", func(t *testing.T) {
		r := rdb.Do(ctx, "AUTH", "foobar")
		require.Equal(t, "OK", r.Val())
	})

	t.Run("Once AUTH succeeded we can actually send commands to the server", func(t *testing.T) {
		require.Equal(t, "OK", rdb.Set(ctx, "foo", 100, 0).Val())
		require.EqualValues(t, 101, rdb.Incr(ctx, "foo").Val())
	})
}
