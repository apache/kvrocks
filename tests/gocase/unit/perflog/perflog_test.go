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

package ping

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestPerflog(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	t.Run("PerfLog", func(t *testing.T) {
		c := srv.NewClient()
		defer func() { require.NoError(t, c.Close()) }()
		require.NoError(t, c.ConfigSet(ctx, "profiling-sample-commands", "set").Err())
		require.NoError(t, c.ConfigSet(ctx, "profiling-sample-ratio", "100").Err())
		require.NoError(t, c.ConfigSet(ctx, "profiling-sample-record-threshold-ms", "0").Err())

		for i := 0; i < 10; i++ {
			require.NoError(t, c.Set(ctx, fmt.Sprintf("key-%d", i), "value", 0).Err())
		}
		require.EqualValues(t, 10, c.Do(ctx, "perflog", "len").Val())
	})
}
