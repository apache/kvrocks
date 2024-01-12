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

package pubsub

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestPubSubShard(t *testing.T) {
	ctx := context.Background()

	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	csrv := util.StartServer(t, map[string]string{"cluster-enabled": "yes"})
	defer csrv.Close()
	crdb := csrv.NewClient()
	defer func() { require.NoError(t, crdb.Close()) }()

	nodeID := "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
	require.NoError(t, crdb.Do(ctx, "clusterx", "SETNODEID", nodeID).Err())
	clusterNodes := fmt.Sprintf("%s %s %d master - 0-16383", nodeID, csrv.Host(), csrv.Port())
	require.NoError(t, crdb.Do(ctx, "clusterx", "SETNODES", clusterNodes, "1").Err())

	rdbs := []*redis.Client{rdb, crdb}

	t.Run("SSUBSCRIBE PING", func(t *testing.T) {
		pubsub := rdb.SSubscribe(ctx, "somechannel")
		receiveType(t, pubsub, &redis.Subscription{})
		require.NoError(t, pubsub.Ping(ctx))
		require.NoError(t, pubsub.Ping(ctx))
		require.NoError(t, pubsub.SUnsubscribe(ctx, "somechannel"))
		require.Equal(t, "PONG", rdb.Ping(ctx).Val())
		receiveType(t, pubsub, &redis.Pong{})
		receiveType(t, pubsub, &redis.Pong{})
	})

	t.Run("SSUBSCRIBE/SUNSUBSCRIBE basic", func(t *testing.T) {
		for _, c := range rdbs {
			pubsub := c.SSubscribe(ctx, "singlechannel")
			defer pubsub.Close()

			msg := receiveType(t, pubsub, &redis.Subscription{})
			require.EqualValues(t, 1, msg.Count)
			require.EqualValues(t, "singlechannel", msg.Channel)
			require.EqualValues(t, "ssubscribe", msg.Kind)

			err := pubsub.SSubscribe(ctx, "multichannel1{tag1}", "multichannel2{tag1}", "multichannel1{tag1}")
			require.Nil(t, err)
			require.EqualValues(t, 2, receiveType(t, pubsub, &redis.Subscription{}).Count)
			require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)
			require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)

			err = pubsub.SSubscribe(ctx, "multichannel3{tag1}", "multichannel4{tag2}")
			require.Nil(t, err)
			if c == rdb {
				require.EqualValues(t, 4, receiveType(t, pubsub, &redis.Subscription{}).Count)
				require.EqualValues(t, 5, receiveType(t, pubsub, &redis.Subscription{}).Count)
			} else {
				// note: when cluster enabled, shard channels in single command must belong to the same slot
				// reference: https://redis.io/commands/ssubscribe
				_, err = pubsub.Receive(ctx)
				require.EqualError(t, err, "ERR CROSSSLOT Keys in request don't hash to the same slot")
			}

			err = pubsub.SUnsubscribe(ctx, "multichannel3{tag1}", "multichannel4{tag2}", "multichannel5{tag2}")
			require.Nil(t, err)
			if c == rdb {
				require.EqualValues(t, 4, receiveType(t, pubsub, &redis.Subscription{}).Count)
				require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)
				require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)
			} else {
				require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)
				require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)
				require.EqualValues(t, 3, receiveType(t, pubsub, &redis.Subscription{}).Count)
			}

			err = pubsub.SUnsubscribe(ctx)
			require.Nil(t, err)
			msg = receiveType(t, pubsub, &redis.Subscription{})
			require.EqualValues(t, 2, msg.Count)
			require.EqualValues(t, "sunsubscribe", msg.Kind)
			require.EqualValues(t, 1, receiveType(t, pubsub, &redis.Subscription{}).Count)
			require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
		}
	})

	t.Run("SSUBSCRIBE/SUNSUBSCRIBE with empty channel", func(t *testing.T) {
		for _, c := range rdbs {
			pubsub := c.SSubscribe(ctx)
			defer pubsub.Close()

			err := pubsub.SUnsubscribe(ctx, "foo", "bar")
			require.Nil(t, err)
			require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
			require.EqualValues(t, 0, receiveType(t, pubsub, &redis.Subscription{}).Count)
		}
	})

	t.Run("SHARDNUMSUB returns numbers, not strings", func(t *testing.T) {
		require.EqualValues(t, map[string]int64{
			"abc": 0,
			"def": 0,
		}, rdb.PubSubShardNumSub(ctx, "abc", "def").Val())
	})

	t.Run("PUBSUB SHARDNUMSUB/SHARDCHANNELS", func(t *testing.T) {
		for _, c := range rdbs {
			pubsub := c.SSubscribe(ctx, "singlechannel")
			defer pubsub.Close()
			receiveType(t, pubsub, &redis.Subscription{})

			err := pubsub.SSubscribe(ctx, "multichannel1{tag1}", "multichannel2{tag1}", "multichannel3{tag1}")
			require.Nil(t, err)
			receiveType(t, pubsub, &redis.Subscription{})
			receiveType(t, pubsub, &redis.Subscription{})
			receiveType(t, pubsub, &redis.Subscription{})

			pubsub1 := c.SSubscribe(ctx, "multichannel1{tag1}")
			defer pubsub1.Close()

			sc := c.PubSubShardChannels(ctx, "")
			require.EqualValues(t, len(sc.Val()), 4)
			sc = c.PubSubShardChannels(ctx, "multi*")
			require.EqualValues(t, len(sc.Val()), 3)

			sn := c.PubSubShardNumSub(ctx)
			require.EqualValues(t, len(sn.Val()), 0)
			sn = c.PubSubShardNumSub(ctx, "singlechannel", "multichannel1{tag1}", "multichannel2{tag1}", "multichannel3{tag1}")
			for i, k := range sn.Val() {
				if i == "multichannel1{tag1}" {
					require.EqualValues(t, k, 2)
				} else {
					require.EqualValues(t, k, 1)
				}
			}
		}
	})
}
