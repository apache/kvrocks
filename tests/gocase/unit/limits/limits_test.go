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

package limits

import (
	"strings"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestNetworkLimits(t *testing.T) {
	srv := util.StartServer(t, map[string]string{
		"maxclients": "10",
	})
	defer srv.Close()

	t.Run("check if maxclients works refusing connections", func(t *testing.T) {
		var clean []func()
		defer func() {
			for _, f := range clean {
				f()
			}
		}()

		for i := 0; i < 50; i++ {
			c := srv.NewTCPClient()
			clean = append(clean, func() { require.NoError(t, c.Close()) })
			require.NoError(t, c.WriteArgs("PING"))
			r, err := c.ReadLine()
			require.NoError(t, err)
			if strings.Contains(r, "ERR") {
				require.Regexp(t, ".*ERR max.*reached.*", r)
				require.Contains(t, []int{9, 10}, i)
				return
			}
			require.Equal(t, "+PONG", r)
		}

		require.Fail(t, "maxclients doesn't work refusing connections")
	})
}
