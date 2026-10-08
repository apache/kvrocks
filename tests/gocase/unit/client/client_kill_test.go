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

func TestClientKillParseEdgeCases(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("CLIENT KILL with keyword but missing value returns error", func(t *testing.T) {
		keywords := []string{"addr", "id", "skipme", "type"}
		for _, kw := range keywords {
			err := rdb.Do(ctx, "CLIENT", "KILL", kw).Err()
			require.Error(t, err, "CLIENT KILL %s should fail", kw)
		}
	})

	t.Run("CLIENT KILL with odd number of new-format args returns syntax error", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "KILL", "addr", "127.0.0.1:1234", "type").Err()
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "syntax") || strings.Contains(err.Error(), "Syntax"),
			"expected syntax error, got: %s", err.Error())
	})

	t.Run("CLIENT KILL with unknown option returns syntax error", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "KILL", "unknown", "value").Err()
		require.Error(t, err)
		require.True(t, strings.Contains(err.Error(), "syntax") || strings.Contains(err.Error(), "Syntax"),
			"expected syntax error, got: %s", err.Error())
	})

	t.Run("CLIENT KILL with valid new-format options succeeds", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "KILL", "addr", "127.0.0.1:1234").Err()
		require.NoError(t, err)
	})

	t.Run("CLIENT KILL with multiple valid options succeeds", func(t *testing.T) {
		err := rdb.Do(ctx, "CLIENT", "KILL", "type", "normal", "skipme", "yes").Err()
		require.NoError(t, err)
	})
}
