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

package replication

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func notificationSync(t *testing.T, writer, reader *redis.Client) {
	t.Helper()
	ctx := context.Background()
	marker := fmt.Sprintf("%s:%d", t.Name(), time.Now().UnixNano())
	require.NoError(t, writer.Set(ctx, "notification:barrier", marker, 0).Err())
	require.Eventually(t, func() bool {
		value, err := reader.Get(ctx, "notification:barrier").Result()
		if err != nil || value != marker || util.FindInfoEntry(reader, "master_link_status", "replication") != "up" {
			return false
		}
		primary, primaryErr := strconv.ParseUint(util.FindInfoEntry(writer, "master_repl_offset", "replication"), 10, 64)
		replica, replicaErr := strconv.ParseUint(util.FindInfoEntry(reader, "slave_repl_offset", "replication"), 10, 64)
		return primaryErr == nil && replicaErr == nil && primary > 0 && primary == replica
	}, 10*time.Second, 10*time.Millisecond)
}

func notificationPair(t *testing.T, workers int) (*util.KvrocksServer, *redis.Client, *redis.Client) {
	t.Helper()
	configs := map[string]string{"workers": strconv.Itoa(workers), "rocksdb.block_cache_size": "32", "use-rsid-psync": "yes"}
	primary := util.StartServer(t, configs)
	t.Cleanup(primary.Close)
	writer := primary.NewClient()
	t.Cleanup(func() { require.NoError(t, writer.Close()) })
	require.NoError(t, writer.Set(context.Background(), "notification:initial", t.Name(), 0).Err())
	replica := util.StartServer(t, map[string]string{
		"workers": strconv.Itoa(workers), "rocksdb.block_cache_size": "32", "use-rsid-psync": "yes",
		"slave-empty-db-before-fullsync": "yes",
	})
	t.Cleanup(replica.Close)
	reader := replica.NewClient()
	t.Cleanup(func() { require.NoError(t, reader.Close()) })
	util.SlaveOf(t, reader, primary)
	util.WaitForSync(t, reader)
	notificationSync(t, writer, reader)
	return replica, writer, reader
}

func notificationSubscribe(t *testing.T, client *redis.Client, channels ...string) *redis.PubSub {
	t.Helper()
	sub := client.Subscribe(context.Background(), channels...)
	t.Cleanup(func() { require.NoError(t, sub.Close()) })
	for _, channel := range channels {
		got, err := sub.ReceiveTimeout(context.Background(), 5*time.Second)
		require.NoError(t, err)
		require.IsType(t, &redis.Subscription{}, got)
		require.Equal(t, channel, got.(*redis.Subscription).Channel)
	}
	return sub
}

func notificationMessages(t *testing.T, sub *redis.PubSub, fence string) [][2]string {
	t.Helper()
	messages := make([][2]string, 0)
	for range 20 {
		got, err := sub.ReceiveTimeout(context.Background(), 5*time.Second)
		require.NoError(t, err)
		require.IsType(t, &redis.Message{}, got)
		message := got.(*redis.Message)
		if message.Channel == fence {
			require.Equal(t, "done", message.Payload)
			return messages
		}
		messages = append(messages, [2]string{message.Channel, message.Payload})
	}
	t.Fatal("notification fence not received")
	return nil
}

func notificationExecute(t *testing.T, writer *redis.Client, transaction bool, commands ...[]any) {
	t.Helper()
	pipe := writer.Pipeline()
	if transaction {
		pipe = writer.TxPipeline()
	}
	for _, command := range commands {
		pipe.Do(context.Background(), command...)
	}
	_, err := pipe.Exec(context.Background())
	require.NoError(t, err)
}

func TestReplicationPublishBatchNotifications(t *testing.T) {
	t.Parallel()
	_, writer, reader := notificationPair(t, 4)
	for _, name := range []string{"standalone", "pipeline", "same-channel", "two-channels", "publish-set", "set-publish", "publish-xadd", "xadd-publish"} {
		t.Run(name, func(t *testing.T) {
			first, second, fence := t.Name()+":first", t.Name()+":second", t.Name()+":fence"
			subs := []*redis.PubSub{notificationSubscribe(t, writer, first, second, fence), notificationSubscribe(t, reader, first, second, fence)}
			publish := []any{"PUBLISH", first, "same-payload"}
			commands := make([][]any, 0, 4)
			commands = append(commands, publish)
			expected := [][2]string{{first, "same-payload"}}
			switch name {
			case "pipeline", "same-channel":
				commands = append(commands[:0], publish, publish, []any{"PUBLISH", first, "last"})
				expected = append(expected, expected[0], [2]string{first, "last"})
			case "two-channels":
				commands = append(commands, []any{"PUBLISH", second, "second-message"})
				expected = append(expected, [2]string{second, "second-message"})
			case "publish-set":
				commands = append(commands, []any{"SET", t.Name(), "value"})
			case "set-publish":
				commands = append(commands[:0], []any{"SET", t.Name(), "value"}, publish)
			case "publish-xadd":
				commands = append(commands, []any{"XADD", t.Name(), "1-0", "f", "value"})
			case "xadd-publish":
				commands = append(commands[:0], []any{"XADD", t.Name(), "1-0", "f", "value"}, publish)
			}
			notificationExecute(t, writer, name != "standalone" && name != "pipeline", commands...)
			notificationSync(t, writer, reader)
			require.NoError(t, writer.Publish(context.Background(), fence, "done").Err())
			for _, sub := range subs {
				require.Equal(t, expected, notificationMessages(t, sub, fence))
			}
		})
	}
}

type notificationRead struct {
	streams []redis.XStream
	err     error
}

func notificationBlockedRead(t *testing.T, replica *util.KvrocksServer, streams ...string) <-chan notificationRead {
	t.Helper()
	client := replica.NewClientWithOption(&redis.Options{MaxRetries: -1, ReadTimeout: 10 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	result := make(chan notificationRead, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		got, err := client.XRead(ctx, &redis.XReadArgs{Streams: streams, Block: 5 * time.Second}).Result()
		result <- notificationRead{got, err}
	}()
	t.Cleanup(func() {
		cancel()
		_ = client.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("blocked notification reader did not exit")
		}
	})
	return result
}

func notificationWaitBlocked(t *testing.T, reader *redis.Client, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		return util.FindInfoEntry(reader, "blocked_clients", "clients") == strconv.Itoa(count)
	}, 3*time.Second, 10*time.Millisecond)
}

func notificationReadResult(t *testing.T, result <-chan notificationRead, keys []string, id string) {
	t.Helper()
	select {
	case got := <-result:
		require.NoError(t, got.err)
		expected := make([]redis.XStream, 0, len(keys))
		for _, key := range keys {
			expected = append(expected, redis.XStream{Stream: key, Messages: []redis.XMessage{{ID: id, Values: map[string]any{"f": "real"}}}})
		}
		require.Equal(t, expected, got.streams)
	case <-time.After(8 * time.Second):
		t.Fatal("notification reader did not return")
	}
}

func notificationStillBlocked(t *testing.T, result <-chan notificationRead) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatalf("reader woke without a new visible entry: %+v", got)
	case <-time.After(150 * time.Millisecond):
	}
}

func TestReplicationWakesBlockingXRead(t *testing.T) {
	t.Parallel()
	for _, workers := range []int{1, 4} {
		replica, writer, reader := notificationPair(t, workers)
		t.Run(fmt.Sprintf("workers-%d", workers), func(t *testing.T) {
			for _, start := range []string{"0-0", "1-0", "$"} {
				t.Run(start, func(t *testing.T) {
					key := t.Name()
					if start != "0-0" {
						require.NoError(t, writer.XAdd(context.Background(), &redis.XAddArgs{Stream: key, ID: "1-0", Values: map[string]any{"f": "seed"}}).Err())
						notificationSync(t, writer, reader)
					}
					results := make([]<-chan notificationRead, 0, 3)
					for range 3 {
						results = append(results, notificationBlockedRead(t, replica, key, start))
					}
					notificationWaitBlocked(t, reader, 3)
					notificationExecute(t, writer, false, []any{"XADD", key, "2-0", "f", "real"})
					notificationSync(t, writer, reader)
					for _, result := range results {
						notificationReadResult(t, result, []string{key}, "2-0")
					}
				})
			}
			t.Run("mixed-batch", func(t *testing.T) {
				first, second, channel, fence := t.Name()+":a", t.Name()+":b", t.Name()+":channel", t.Name()+":fence"
				sub := notificationSubscribe(t, reader, channel, fence)
				results := []<-chan notificationRead{
					notificationBlockedRead(t, replica, first, "0-0"),
					notificationBlockedRead(t, replica, second, "0-0"),
					notificationBlockedRead(t, replica, first, second, "0-0", "0-0"),
				}
				notificationWaitBlocked(t, reader, 3)
				notificationExecute(t, writer, true, []any{"PUBLISH", channel, "message"},
					[]any{"XADD", first, "1-0", "f", "real"}, []any{"XADD", second, "1-0", "f", "real"}, []any{"SET", t.Name(), "marker"})
				notificationSync(t, writer, reader)
				require.NoError(t, writer.Publish(context.Background(), fence, "done").Err())
				require.Equal(t, [][2]string{{channel, "message"}}, notificationMessages(t, sub, fence))
				for i, keys := range [][]string{{first}, {second}, {first, second}} {
					notificationReadResult(t, results[i], keys, "1-0")
				}
			})
		})
	}
}

func TestReplicationStreamNotificationFinalState(t *testing.T) {
	t.Parallel()
	replica, writer, reader := notificationPair(t, 4)
	for _, name := range []string{"xdel", "del", "point-trim", "range-trim", "recreate", "wrong-type", "maxlen-zero", "bookkeeping"} {
		t.Run(name, func(t *testing.T) {
			key := t.Name()
			last := 1
			if name == "range-trim" {
				last = 130
			}
			for i := 1; i <= last; i++ {
				require.NoError(t, writer.XAdd(context.Background(), &redis.XAddArgs{Stream: key, ID: fmt.Sprintf("%d-0", i), Values: map[string]any{"f": "seed"}}).Err())
			}
			if name == "bookkeeping" {
				require.NoError(t, writer.XGroupCreate(context.Background(), key, "group", "0-0").Err())
			}
			notificationSync(t, writer, reader)
			result := notificationBlockedRead(t, replica, key, fmt.Sprintf("%d-0", last))
			notificationWaitBlocked(t, reader, 1)
			id := fmt.Sprintf("%d-0", last+1)
			commands := [][]any{{"XADD", key, id, "f", "removed"}}
			switch name {
			case "xdel":
				commands = append(commands, []any{"XDEL", key, id})
			case "del":
				commands = append(commands, []any{"DEL", key})
			case "point-trim", "range-trim":
				commands = append(commands, []any{"XTRIM", key, "MAXLEN", "0"})
			case "recreate":
				commands = append(commands, []any{"DEL", key}, []any{"XADD", key, "1-0", "f", "seed"})
			case "wrong-type":
				commands = append(commands, []any{"SET", key, "not-a-stream"})
			case "maxlen-zero":
				commands = [][]any{{"XADD", key, "MAXLEN", "0", id, "f", "removed"}}
			case "bookkeeping":
				commands = [][]any{{"XGROUP", "SETID", key, "group", "0-0"}, {"XGROUP", "CREATECONSUMER", key, "group", "consumer"},
					{"XREADGROUP", "GROUP", "group", "consumer", "STREAMS", key, ">"}, {"XACK", key, "group", "1-0"}}
			}
			notificationExecute(t, writer, name != "bookkeeping", commands...)
			notificationSync(t, writer, reader)
			notificationStillBlocked(t, result)
			if name == "wrong-type" {
				require.NoError(t, writer.Del(context.Background(), key).Err())
			}
			id = fmt.Sprintf("%d-0", last+2)
			notificationExecute(t, writer, false, []any{"XADD", key, id, "f", "real"})
			notificationReadResult(t, result, []string{key}, id)
		})
	}
}

func TestReplicationNotificationLifecycle(t *testing.T) {
	t.Parallel()
	replica, writer, reader := notificationPair(t, 4)
	checkNotifications := func(t *testing.T, writer *redis.Client) {
		t.Helper()
		key, channel, fence := t.Name()+":stream", t.Name()+":channel", t.Name()+":fence"
		sub := notificationSubscribe(t, reader, channel, fence)
		result := notificationBlockedRead(t, replica, key, "0-0")
		notificationWaitBlocked(t, reader, 1)
		notificationExecute(t, writer, true, []any{"PUBLISH", channel, "new"}, []any{"XADD", key, "1-0", "f", "real"})
		notificationSync(t, writer, reader)
		require.NoError(t, writer.Publish(context.Background(), fence, "done").Err())
		require.Equal(t, [][2]string{{channel, "new"}}, notificationMessages(t, sub, fence))
		notificationReadResult(t, result, []string{key}, "1-0")
	}
	t.Run("initial", func(t *testing.T) { checkNotifications(t, writer) })
	require.NoError(t, reader.ConfigRewrite(context.Background()).Err())
	replica.Restart()
	util.WaitForSync(t, reader)
	t.Run("restart", func(t *testing.T) { checkNotifications(t, writer) })

	other := util.StartServer(t, map[string]string{"use-rsid-psync": "yes", "rocksdb.block_cache_size": "32"})
	t.Cleanup(other.Close)
	otherWriter := other.NewClient()
	t.Cleanup(func() { require.NoError(t, otherWriter.Close()) })
	require.NoError(t, otherWriter.Set(context.Background(), "notification:new-primary", "new", 0).Err())
	require.NoError(t, otherWriter.Do(context.Background(), "flushmemtable").Err())
	util.SlaveOf(t, reader, other)
	require.Eventually(t, func() bool {
		value, err := reader.Get(context.Background(), "notification:new-primary").Result()
		return err == nil && value == "new" && util.FindInfoEntry(reader, "master_link_status") == "up"
	}, 30*time.Second, 50*time.Millisecond)
	notificationSync(t, otherWriter, reader)
	require.Equal(t, "new", reader.Get(context.Background(), "notification:new-primary").Val())
	require.Equal(t, int64(0), reader.Exists(context.Background(), "notification:initial").Val())
	fullSyncs, err := strconv.Atoi(util.FindInfoEntry(otherWriter, "sync_full"))
	require.NoError(t, err)
	require.Positive(t, fullSyncs)
	t.Run("full-sync", func(t *testing.T) { checkNotifications(t, otherWriter) })
}

func TestReplicationPropagatedCommandNotifications(t *testing.T) {
	t.Parallel()
	_, writer, reader := notificationPair(t, 1)
	ctx := context.Background()
	sha, err := writer.ScriptLoad(ctx, "return 42").Result()
	require.NoError(t, err)
	notificationSync(t, writer, reader)
	require.EqualValues(t, 42, reader.EvalShaRO(ctx, sha, nil).Val())
	require.NoError(t, writer.ScriptFlush(ctx).Err())
	notificationSync(t, writer, reader)
	require.ErrorContains(t, reader.EvalShaRO(ctx, sha, nil).Err(), "NOSCRIPT")

	code := "#!lua name=notification_lib\nredis.register_function('notification_answer', function() return 42 end)"
	require.NoError(t, writer.Do(ctx, "FUNCTION", "LOAD", code).Err())
	notificationSync(t, writer, reader)
	got, err := reader.Do(ctx, "FCALL_RO", "notification_answer", "0").Result()
	require.NoError(t, err)
	require.EqualValues(t, 42, got)
	channel, fence := t.Name()+":channel", t.Name()+":fence"
	sub := notificationSubscribe(t, reader, channel, fence)
	notificationExecute(t, writer, true, []any{"FUNCTION", "DELETE", "notification_lib"},
		[]any{"PUBLISH", channel, "after-delete"}, []any{"SET", t.Name(), "marker"})
	notificationSync(t, writer, reader)
	require.NoError(t, writer.Publish(ctx, fence, "done").Err())
	require.Equal(t, [][2]string{{channel, "after-delete"}}, notificationMessages(t, sub, fence))
	require.ErrorContains(t, reader.Do(ctx, "FCALL_RO", "notification_answer", "0").Err(), "No such function name found in storage")
}
