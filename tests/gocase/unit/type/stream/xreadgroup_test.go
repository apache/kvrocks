package stream

import (
	"context"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestXReadGroup(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "txn-context-enabled",
			Options:    []string{"yes", "no"},
			ConfigType: util.YesNo,
		},
	}

	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)

	for _, configs := range configsMatrix {
		testXReadGroup(t, configs)
	}
}

func testXReadGroup(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("XREADGROUP with CLAIM option", func(t *testing.T) {
		streamName := "mystream_claim"
		groupName := "mygroup_claim"
		consumerName1 := "consumer1"
		consumerName2 := "consumer2"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		id1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v1"}}).Val()

		// Consumer 1 reads it
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName1,
			Streams:  []string{streamName, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// Consumer 2 tries to claim it with 50ms idle time
		// XREADGROUP GROUP mygroup consumer2 COUNT 1 CLAIM 50 STREAMS mystream >
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "1", "CLAIM", "50", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		// Verify response structure
		// Expected: [[streamName, [[id, [k, v1], idle, count]]]]
		// Note: The exact types depend on the go-redis parsing of interface{}
		
		streams := res.([]interface{})
		require.Len(t, streams, 1)
		
		stream := streams[0].([]interface{})
		require.Equal(t, streamName, stream[0])
		
		messages := stream[1].([]interface{})
		require.Len(t, messages, 1)
		
		msg := messages[0].([]interface{})
		require.Len(t, msg, 4, "Message should have 4 elements: id, fields, idle, count")
		
		require.Equal(t, id1, msg[0])
		// msg[1] is fields, msg[2] is idle, msg[3] is count
		
		// Verify ownership change
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamName,
			Group:  groupName,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)
		
		found := false
		for _, p := range pending {
			if p.ID == id1 {
				require.Equal(t, consumerName2, p.Consumer)
				found = true
				break
			}
		}
		require.True(t, found, "Message should be claimed by consumer2")
	})

	t.Run("XREADGROUP CLAIM ordering guarantees", func(t *testing.T) {
		streamName := "mystream_ordering"
		groupName := "mygroup_ordering"
		consumerName1 := "consumer1"
		consumerName2 := "consumer2"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		// Add multiple messages
		id1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v1"}}).Val()
		time.Sleep(50 * time.Millisecond)
		id2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v2"}}).Val()
		time.Sleep(50 * time.Millisecond)
		id3 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v3"}}).Val()

		// Consumer1 reads first two messages
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName1,
			Streams:  []string{streamName, ">"},
			Count:    2,
		}).Result()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// Consumer2 claims with COUNT 10 - should get idle entries first (ordered by idle time), then new entries
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "10", "CLAIM", "50", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streams := res.([]interface{})
		require.Len(t, streams, 1)

		stream := streams[0].([]interface{})
		messages := stream[1].([]interface{})

		// Should get: id1 (longest idle), id2 (shorter idle), id3 (new message)
		require.Len(t, messages, 3)

		msg1 := messages[0].([]interface{})
		require.Equal(t, id1, msg1[0], "First message should be id1 (longest idle)")
		require.Len(t, msg1, 4, "Claimed message should have 4 elements")

		msg2 := messages[1].([]interface{})
		require.Equal(t, id2, msg2[0], "Second message should be id2")
		require.Len(t, msg2, 4, "Claimed message should have 4 elements")

		msg3 := messages[2].([]interface{})
		require.Equal(t, id3, msg3[0], "Third message should be id3 (new)")
		require.Len(t, msg3, 4, "New message in CLAIM mode should also have 4 elements")
	})

	t.Run("XREADGROUP CLAIM with NOACK", func(t *testing.T) {
		streamName := "mystream_noack"
		groupName := "mygroup_noack"
		consumerName1 := "consumer1"
		consumerName2 := "consumer2"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		id1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v1"}}).Val()

		// Consumer1 reads it without ACK
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName1,
			Streams:  []string{streamName, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// Consumer2 claims with NOACK - claimed entries should still be added to PEL
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "1", "CLAIM", "50", "NOACK", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streams := res.([]interface{})
		require.Len(t, streams, 1)

		stream := streams[0].([]interface{})
		messages := stream[1].([]interface{})
		require.Len(t, messages, 1)

		msg := messages[0].([]interface{})
		require.Equal(t, id1, msg[0])
		require.Len(t, msg, 4, "Claimed message should have 4 elements even with NOACK")

		// NOACK doesn't apply to claimed entries - they should still be in PEL
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamName,
			Group:  groupName,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)

		found := false
		for _, p := range pending {
			if p.ID == id1 {
				require.Equal(t, consumerName2, p.Consumer, "Message should be owned by consumer2")
				found = true
				break
			}
		}
		require.True(t, found, "Claimed message should be in PEL even with NOACK")
	})

	t.Run("XREADGROUP CLAIM min idle time filter", func(t *testing.T) {
		streamName := "mystream_filter"
		groupName := "mygroup_filter"
		consumerName1 := "consumer1"
		consumerName2 := "consumer2"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		_ = rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v1"}}).Val()
		_ = rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v2"}}).Val()

		// Consumer1 reads both
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName1,
			Streams:  []string{streamName, ">"},
			Count:    2,
		}).Result()
		require.NoError(t, err)

		time.Sleep(50 * time.Millisecond)

		// Try to claim with 1000ms idle time - should not claim anything, only get new messages
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "10", "CLAIM", "1000", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streams := res.([]interface{})
		require.Len(t, streams, 1)

		stream := streams[0].([]interface{})
		messages := stream[1].([]interface{})

		// Should get 0 messages (no claimed entries, no new entries)
		require.Len(t, messages, 0, "Should not claim messages below min-idle-time")
	})

	t.Run("XREADGROUP CLAIM delivery count increment", func(t *testing.T) {
		streamName := "mystream_count"
		groupName := "mygroup_count"
		consumerName1 := "consumer1"
		consumerName2 := "consumer2"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		id1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v1"}}).Val()

		// Consumer1 reads it (delivery count = 1)
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName1,
			Streams:  []string{streamName, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// Consumer2 claims it (delivery count should become 2)
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "1", "CLAIM", "50", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streams := res.([]interface{})
		stream := streams[0].([]interface{})
		messages := stream[1].([]interface{})
		msg := messages[0].([]interface{})

		require.Equal(t, id1, msg[0])
		// msg[3] should be delivery count
		deliveryCount := msg[3].(int64)
		require.Equal(t, int64(1), deliveryCount, "Delivery count should be 1 (from first delivery)")

		// Verify with XPENDING
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamName,
			Group:  groupName,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, int64(2), pending[0].RetryCount, "PEL delivery count should be 2 after claim")
	})
}

