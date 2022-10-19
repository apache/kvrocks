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

package tls

import (
	"context"
	"crypto/tls"
	"flag"
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

var enableTlsTests = flag.Bool("enableTlsTests", false, "enable TLS-related test cases")

func TestTLS(t *testing.T) {
	if *enableTlsTests {
		srv := util.StartTLSServer(t, map[string]string{})
		defer srv.Close()

		ctx := context.Background()

		rdb := srv.NewTLSClient()
		defer func() { require.NoError(t, rdb.Close()) }()

		t.Run("TLS: Not accepting non-TLS connections on a TLS port", func(t *testing.T) {
			c := srv.NewTLSClientWithOption(&redis.Options{})
			defer func() { require.NoError(t, c.Close()) }()
			require.Error(t, c.Ping(ctx).Err())
		})

		t.Run("TLS: Verify tls-auth-clients behaves as expected", func(t *testing.T) {
			c := srv.NewTLSClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
			defer func() { require.NoError(t, c.Close()) }()
			require.Error(t, c.Ping(ctx).Err())

			require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "no").Err())
			require.NoError(t, c.Close())
			c = srv.NewTLSClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
			require.Equal(t, "PONG", c.Ping(ctx).Val())

			require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "optional").Err())
			require.NoError(t, c.Close())
			c = srv.NewTLSClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
			require.Equal(t, "PONG", c.Ping(ctx).Val())

			require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "yes").Err())
			require.NoError(t, c.Close())
			c = srv.NewTLSClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}})
			require.Error(t, c.Ping(ctx).Err())
		})
	}
}
