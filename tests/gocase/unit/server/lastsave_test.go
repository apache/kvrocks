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

package server

import (
	"context"
	"regexp"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestLastSaveCommand(t *testing.T) {
	ctx := context.Background()

	srv := util.StartServer(t, map[string]string{})
	rdb := srv.NewClient()
	defer func() {
		require.NoError(t, rdb.Close())
	}()

	t.Run("LASTSAVE unix timestamp", func(t *testing.T) {
		result, err := rdb.Do(ctx, "LASTSAVE").Result()
		require.NoError(t, err)

		timestamp, ok := result.(int64)
		require.True(t, ok, "Expected an integer timestamp")
		require.Greater(t, timestamp, int64(0), "Timestamp should be a positive number")
	})

	t.Run("LASTSAVE with format", func(t *testing.T) {
		result, err := rdb.Do(ctx, "LASTSAVE", "ISO8601").Result()
		require.NoError(t, err)

		timestampStr, ok := result.(string)
		require.True(t, ok, "Expected LASTSAVE ISO8601 to return a string timestamp")

		matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4}$`, timestampStr)
		require.True(t, matched, "Timestamp should be in ISO8601 format")
	})
	t.Run("LASTSAVE with format and case sensitivity", func(t *testing.T) {
		result, err := rdb.Do(ctx, "LASTSAVE", "iso8601").Result()
		require.NoError(t, err)

		timeStampStr, ok := result.(string)
		require.True(t, ok, "Expected LASTSAVE iso8601 to return a string timestamp")

		matched, _ := regexp.MatchString(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{4}$`, timeStampStr)
		require.True(t, matched, "Timestamp should be in ISO8601 format")
	})

}
