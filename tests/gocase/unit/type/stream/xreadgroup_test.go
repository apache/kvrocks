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

		// Add messages
		// Message 1: multiple key-values
		id1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k1", "v1", "k2", "v2"}}).Val()
		// Message 2: single key-value
		id2 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k3", "v3"}}).Val()

		// Consumer 1 reads them using Do to verify the raw standard format (2 elements)
		// XREADGROUP GROUP mygroup consumer1 COUNT 2 STREAMS mystream >
		resRaw, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName1, "COUNT", "2", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streamsRaw := resRaw.([]interface{})
		require.Len(t, streamsRaw, 1)
		streamRaw := streamsRaw[0].([]interface{})
		messagesRaw := streamRaw[1].([]interface{})
		require.Len(t, messagesRaw, 2)

		msg1Raw := messagesRaw[0].([]interface{})
		require.Len(t, msg1Raw, 2, "Standard XREADGROUP message should have 2 elements: id, fields")

		msg2Raw := messagesRaw[1].([]interface{})
		require.Len(t, msg2Raw, 2, "Standard XREADGROUP message should have 2 elements: id, fields")

		// Sleep to satisfy min-idle-time of 1ms
		time.Sleep(2 * time.Millisecond)

		// Consumer 2 claims them with 1ms idle time
		// XREADGROUP GROUP mygroup consumer2 COUNT 2 CLAIM 1 STREAMS mystream >
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "2", "CLAIM", "1", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		// Verify response structure
		streams := res.([]interface{})
		require.Len(t, streams, 1)

		stream := streams[0].([]interface{})
		require.Equal(t, streamName, stream[0])

		messages := stream[1].([]interface{})
		require.Len(t, messages, 2)

		// Verify Message 1
		msg1 := messages[0].([]interface{})
		require.Len(t, msg1, 4, "Message should have 4 elements: id, fields, idle, count")
		require.Equal(t, id1, msg1[0])

		fields1 := msg1[1].([]interface{})
		require.Len(t, fields1, 4) // k1, v1, k2, v2
		require.Equal(t, "k1", fields1[0])
		require.Equal(t, "v1", fields1[1])
		require.Equal(t, "k2", fields1[2])
		require.Equal(t, "v2", fields1[3])

		// Idle time check
		// Note: The user example shows idle time and count as strings.
		// We handle both string and int64 to be safe, or check what we get.
		// For now, we just assert they are present.
		// Idle time check
		require.NotNil(t, msg1[2]) // idle
		idleTime := msg1[2].(int64)
		require.GreaterOrEqual(t, idleTime, int64(2), "Idle time should be >= 2ms")
		require.NotNil(t, msg1[3]) // count

		// Verify Message 2
		msg2 := messages[1].([]interface{})
		require.Len(t, msg2, 4)
		require.Equal(t, id2, msg2[0])

		fields2 := msg2[1].([]interface{})
		require.Len(t, fields2, 2)
		require.Equal(t, "k3", fields2[0])
		require.Equal(t, "v3", fields2[1])

		// Verify ownership change
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamName,
			Group:  groupName,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)

		found1 := false
		found2 := false
		for _, p := range pending {
			if p.ID == id1 {
				require.Equal(t, consumerName2, p.Consumer)
				found1 = true
			}
			if p.ID == id2 {
				require.Equal(t, consumerName2, p.Consumer)
				found2 = true
			}
		}
		require.True(t, found1, "Message 1 should be claimed by consumer2")
		require.True(t, found2, "Message 2 should be claimed by consumer2")
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
		require.Equal(t, int64(2), deliveryCount, "Delivery count should be 2 (1 from read + 1 from claim)")

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

	t.Run("XREADGROUP CLAIM with multiple retries", func(t *testing.T) {
		streamName := "mystream_retries"
		groupName := "mygroup_retries"
		consumerName1 := "consumer1"
		consumerName2 := "consumer2"

		require.NoError(t, rdb.Del(ctx, streamName).Err())
		require.NoError(t, rdb.XGroupCreateMkStream(ctx, streamName, groupName, "0").Err())

		id1 := rdb.XAdd(ctx, &redis.XAddArgs{Stream: streamName, Values: []string{"k", "v1"}}).Val()

		// 1. Consumer1 reads it (delivery count = 1)
		_, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: consumerName1,
			Streams:  []string{streamName, ">"},
			Count:    1,
		}).Result()
		require.NoError(t, err)

		time.Sleep(50 * time.Millisecond)

		// 2. Consumer2 claims it (PEL count becomes 2)
		res, err := rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName2, "COUNT", "1", "CLAIM", "20", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streams := res.([]interface{})
		stream := streams[0].([]interface{})
		messages := stream[1].([]interface{})
		msg := messages[0].([]interface{})
		require.Equal(t, id1, msg[0])
		// Expect count 2
		require.Equal(t, int64(2), msg[3].(int64), "Delivery count should be 2 after first claim")
		require.GreaterOrEqual(t, msg[2].(int64), int64(50), "Idle time should be >= 50ms")

		time.Sleep(50 * time.Millisecond)

		// 3. Consumer1 claims it back (PEL count becomes 3)
		res, err = rdb.Do(ctx, "XREADGROUP", "GROUP", groupName, consumerName1, "COUNT", "1", "CLAIM", "20", "STREAMS", streamName, ">").Result()
		require.NoError(t, err)

		streams = res.([]interface{})
		stream = streams[0].([]interface{})
		messages = stream[1].([]interface{})
		msg = messages[0].([]interface{})
		require.Equal(t, id1, msg[0])
		// Expect count 3
		require.Equal(t, int64(3), msg[3].(int64), "Delivery count should be 3 after second claim")
		require.GreaterOrEqual(t, msg[2].(int64), int64(50), "Idle time should be >= 50ms")

		// 4. Verify PEL has count 3
		pending, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamName,
			Group:  groupName,
			Start:  "-",
			End:    "+",
			Count:  10,
		}).Result()
		require.NoError(t, err)
		require.Len(t, pending, 1)
		require.Equal(t, int64(3), pending[0].RetryCount)
	})
}
