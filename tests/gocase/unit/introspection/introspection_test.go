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

package command

import (
	"context"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestIntrospection(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("PING", func(t *testing.T) {
		c := srv.NewTCPClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.Write("*1\r\n$4\r\nPING\r\n"))
		r, err := c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "+PONG")
		require.NoError(t, c.Write("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n"))
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "$4")
		r, err = c.ReadLine()
		require.NoError(t, err)
		require.Contains(t, r, "PONG")
		require.EqualError(t, rdb.Do(ctx, "ping", "hello", "redis").Err(), "ERR wrong number of arguments")
	})
}
