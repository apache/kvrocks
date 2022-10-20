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
	"testing"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
)

func TestTLS(t *testing.T) {
	if !util.TLSEnable() {
		t.Skip("TLS tests run only if tls enabled.")
	}

	ctx := context.Background()

	srv := util.StartTLSServer(t, map[string]string{})
	defer srv.Close()

	defaultTLSConfig, err := util.DefaultTLSConfig()
	require.NoError(t, err)

	rdb := srv.NewClientWithOption(&redis.Options{TLSConfig: defaultTLSConfig, Addr: srv.TLSAddr()})
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("TLS: Not accepting non-TLS connections on a TLS port", func(t *testing.T) {
		c := srv.NewClientWithOption(&redis.Options{TLSConfig: defaultTLSConfig})
		defer func() { require.NoError(t, c.Close()) }()
		require.Error(t, c.Ping(ctx).Err())
	})

	t.Run("TLS: Verify tls-auth-clients behaves as expected", func(t *testing.T) {
		c := srv.NewClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}, Addr: srv.TLSAddr()})
		defer func() { require.NoError(t, c.Close()) }()
		require.Error(t, c.Ping(ctx).Err())

		require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "no").Err())
		require.NoError(t, c.Close())
		c = srv.NewClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}, Addr: srv.TLSAddr()})
		require.Equal(t, "PONG", c.Ping(ctx).Val())

		require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "optional").Err())
		require.NoError(t, c.Close())
		c = srv.NewClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}, Addr: srv.TLSAddr()})
		require.Equal(t, "PONG", c.Ping(ctx).Val())

		require.NoError(t, rdb.ConfigSet(ctx, "tls-auth-clients", "yes").Err())
		require.NoError(t, c.Close())
		c = srv.NewClientWithOption(&redis.Options{TLSConfig: &tls.Config{InsecureSkipVerify: true}, Addr: srv.TLSAddr()})
		require.Error(t, c.Ping(ctx).Err())
	})

	t.Run("TLS: Verify tls-protocols behaves as expected", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "TLSv1.2").Err())

		tlsConfig, err := util.DefaultTLSConfig()
		require.NoError(t, err)

		tlsConfig.MaxVersion = tls.VersionTLS11

		c := srv.NewClientWithOption(&redis.Options{TLSConfig: tlsConfig, Addr: srv.TLSAddr()})
		defer func() { require.NoError(t, c.Close()) }()
		require.Error(t, c.Ping(ctx).Err())

		tlsConfig.MaxVersion = tls.VersionTLS12

		require.NoError(t, c.Close())
		c = srv.NewClientWithOption(&redis.Options{TLSConfig: tlsConfig, Addr: srv.TLSAddr()})
		require.Equal(t, "PONG", c.Ping(ctx).Val())
	})

	t.Run("TLS: Verify tls-ciphers behaves as expected", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "TLSv1.2").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT:-AES128-SHA256").Err())

		tlsConfig, err := util.DefaultTLSConfig()
		require.NoError(t, err)

		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA256}

		c := srv.NewClientWithOption(&redis.Options{TLSConfig: tlsConfig, Addr: srv.TLSAddr()})
		defer func() { require.NoError(t, c.Close()) }()

		require.Error(t, c.Ping(ctx).Err())

		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_256_GCM_SHA384}

		require.NoError(t, c.Close())
		c = srv.NewClientWithOption(&redis.Options{TLSConfig: tlsConfig, Addr: srv.TLSAddr()})

		require.Equal(t, "PONG", c.Ping(ctx).Val())

		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT").Err())

		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_128_CBC_SHA256}

		require.NoError(t, c.Close())
		c = srv.NewClientWithOption(&redis.Options{TLSConfig: tlsConfig, Addr: srv.TLSAddr()})

		require.NoError(t, c.Ping(ctx).Err())
		require.Equal(t, "PONG", c.Ping(ctx).Val())

		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT").Err())
	})

	t.Run("TLS: Verify tls-prefer-server-ciphers behaves as expected", func(t *testing.T) {
		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "TLSv1.2").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "AES128-SHA256:AES256-GCM-SHA384").Err())

		tlsConfig, err := util.DefaultTLSConfig()
		require.NoError(t, err)

		tlsConfig.CipherSuites = []uint16{tls.TLS_RSA_WITH_AES_256_GCM_SHA384, tls.TLS_RSA_WITH_AES_128_CBC_SHA256}

		c := srv.NewTCPTLSClient(tlsConfig)
		defer func() { require.NoError(t, c.Close()) }()

		require.NoError(t, c.WriteArgs("PING"))
		c.MustMatch(t, "PONG")
		require.Equal(t, c.TLSState().CipherSuite, tls.TLS_RSA_WITH_AES_256_GCM_SHA384)

		require.NoError(t, rdb.ConfigSet(ctx, "tls-prefer-server-ciphers", "yes").Err())

		require.NoError(t, c.Close())
		c = srv.NewTCPTLSClient(tlsConfig)

		require.NoError(t, c.WriteArgs("PING"))
		c.MustMatch(t, "PONG")
		require.Equal(t, c.TLSState().CipherSuite, tls.TLS_RSA_WITH_AES_128_CBC_SHA256)

		require.NoError(t, rdb.ConfigSet(ctx, "tls-protocols", "").Err())
		require.NoError(t, rdb.ConfigSet(ctx, "tls-ciphers", "DEFAULT").Err())
	})
}
