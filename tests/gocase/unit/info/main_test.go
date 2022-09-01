package command

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/stretchr/testify/require"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCommand(t *testing.T) {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6666"})

	FindInfoEntry := func(t *testing.T, section string, key string) string {
		r := rdb.Info(ctx, section)
		p := regexp.MustCompile(fmt.Sprintf("%s:(.+)", key))
		ms := p.FindStringSubmatch(r.Val())
		require.Len(t, ms, 2)
		return strings.TrimSpace(ms[1])
	}

	MustAtoi := func(t *testing.T, s string) int {
		i, err := strconv.Atoi(s)
		require.NoError(t, err)
		return i
	}

	t.Run("get rocksdb ops by INFO", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			k := fmt.Sprintf("key%d", i)
			v := fmt.Sprintf("value%d", i)
			for j := 0; j < 500; j++ {
				rdb.LPush(ctx, k, v)
				rdb.LRange(ctx, k, 0, 1)
			}
			time.Sleep(time.Second)
		}

		r := FindInfoEntry(t, "rocksdb", "put_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
		r = FindInfoEntry(t, "rocksdb", "get_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
		r = FindInfoEntry(t, "rocksdb", "seek_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
		r = FindInfoEntry(t, "rocksdb", "next_per_sec")
		require.Greater(t, MustAtoi(t, r), 0)
	})

	t.Run("get bgsave information by INFO", func(t *testing.T) {
		require.Equal(t, "0", FindInfoEntry(t, "persistence", "bgsave_in_progress"))
		require.Equal(t, "-1", FindInfoEntry(t, "persistence", "last_bgsave_time"))
		require.Equal(t, "ok", FindInfoEntry(t, "persistence", "last_bgsave_status"))
		require.Equal(t, "-1", FindInfoEntry(t, "persistence", "last_bgsave_time_sec"))

		r := rdb.Do(ctx, "bgsave")
		v, err := r.Text()
		require.NoError(t, err)
		require.Equal(t, "OK", v)

		require.Eventually(t, func() bool {
			e := MustAtoi(t, FindInfoEntry(t, "persistence", "bgsave_in_progress"))
			return e == 0
		}, 5*time.Second, 100*time.Millisecond)

		lastBgsaveTime := MustAtoi(t, FindInfoEntry(t, "persistence", "last_bgsave_time"))
		require.Greater(t, lastBgsaveTime, 1640507660)
		require.Equal(t, "ok", FindInfoEntry(t, "persistence", "last_bgsave_status"))
		lastBgsaveTimeSec := MustAtoi(t, FindInfoEntry(t, "persistence", "last_bgsave_time_sec"))
		require.GreaterOrEqual(t, lastBgsaveTimeSec, 0)
		require.Less(t, lastBgsaveTimeSec, 3)
	})
}
