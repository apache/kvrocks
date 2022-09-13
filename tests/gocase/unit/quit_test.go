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

package unit

import (
	"fmt"
	"strings"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func formatCommand(args []string) string {
	suffix := "\r\n"
	str := "*" + fmt.Sprintf("%d", len(args)) + suffix
	for _, arg := range args {
		str += "$" + fmt.Sprintf("%d", len(arg)) + suffix
		str += arg + suffix
	}
	return str
}

func TestQuit(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	rdb := srv.NewTCPClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("QUIT returns OK", func(t *testing.T) {
		// reconnect
		require.NoError(t, rdb.Close())
		rdb = srv.NewTCPClient()
		require.NoError(t, rdb.Write(formatCommand([]string{"quit"})))
		require.NoError(t, rdb.Write(formatCommand([]string{"ping"})))
		resQuit, errQuit := rdb.ReadLine()
		require.Equal(t, "+OK", resQuit)
		require.NoError(t, errQuit)
		resPing, errPing := rdb.ReadLine()
		require.Equal(t, "", resPing)
		require.Error(t, errPing)
	})

	t.Run("Pipelined commands after QUIT must not be executed", func(t *testing.T) {
		// reconnect
		require.NoError(t, rdb.Close())
		rdb = srv.NewTCPClient()
		require.NoError(t, rdb.Write(formatCommand([]string{"quit"})))
		require.NoError(t, rdb.Write(formatCommand([]string{"set", "foo", "bar"})))
		resQuit, errQuit := rdb.ReadLine()
		require.Equal(t, "+OK", resQuit)
		require.NoError(t, errQuit)
		resSet, errSet := rdb.ReadLine()
		require.Equal(t, "", resSet)
		require.Error(t, errSet)
		// reconnect
		require.NoError(t, rdb.Close())
		rdb = srv.NewTCPClient()
		require.NoError(t, rdb.Write(formatCommand([]string{"get", "foo"})))
		resGet, errGet := rdb.ReadLine()
		require.Equal(t, "$-1", resGet)
		require.NoError(t, errGet)
	})

	t.Run("Pipelined commands after QUIT that exceed read buffer size", func(t *testing.T) {
		// reconnect
		require.NoError(t, rdb.Close())
		rdb = srv.NewTCPClient()
		require.NoError(t, rdb.Write(formatCommand([]string{"quit"})))
		require.NoError(t, rdb.Write(formatCommand([]string{"set", "foo", strings.Repeat("x", 1024)})))
		resQuit, errQuit := rdb.ReadLine()
		require.Equal(t, "+OK", resQuit)
		require.NoError(t, errQuit)
		resSet, errSet := rdb.ReadLine()
		require.Equal(t, "", resSet)
		require.Error(t, errSet)
		// reconnect
		require.NoError(t, rdb.Close())
		rdb = srv.NewTCPClient()
		require.NoError(t, rdb.Write(formatCommand([]string{"get", "foo"})))
		resGet, errGet := rdb.ReadLine()
		require.Equal(t, "$-1", resGet)
		require.NoError(t, errGet)
	})

}
