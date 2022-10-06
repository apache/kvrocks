package expire

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/apache/incubator-kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestExpire(t *testing.T) {
	svr := util.StartServer(t, map[string]string{})
	defer svr.Close()

	ctx := context.Background()
	rdb := svr.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("EXPIRE - set timeouts multiple times", func(t *testing.T) {
		rdb.Set(ctx, "x", "foobar", 0)
		require.Equal(t, 1, rdb.Expire(ctx, "x", 5*time.Second).Val())
		ttl := rdb.TTL(ctx, "x").Val()
		require.LessOrEqual(t, 5, ttl)
		require.GreaterOrEqual(t, 4, ttl)
		require.Equal(t, 1, rdb.Expire(ctx, "x", 10*time.Second).Val())
		require.Equal(t, 10*time.Second, rdb.TTL(ctx, "x").Val())
		rdb.Expire(ctx, "x", 2*time.Second)
	})

	t.Run("EXPIRE - It should be still possible to read 'x'", func(t *testing.T) {
		require.Equal(t, "foobar", rdb.Get(ctx, "x").Val())
	})

	t.Run("EXPIRE - write on expire should work", func(t *testing.T) {
		rdb.Del(ctx, "x")
		rdb.LPush(ctx, "x", "foo")
		rdb.Expire(ctx, "x", 1000*time.Second)
		rdb.LPush(ctx, "x", "bar")
		require.Equal(t, []string{"bar", "foo"}, rdb.LRange(ctx, "x", 0, -1).Val())
	})

	t.Run("EXPIREAT - Check for EXPIRE alike behavior", func(t *testing.T) {
		rdb.Del(ctx, "x")
		rdb.Set(ctx, "x", "foo", 0)
		rdb.ExpireAt(ctx, "x", time.Now().Add(15*time.Second))
		ttl := rdb.TTL(ctx, "x").Val()
		require.GreaterOrEqual(t, 13, ttl)
		require.LessOrEqual(t, 16, ttl)
	})

	t.Run("SETEX - Set + Expire combo operation. Check for TTL", func(t *testing.T) {
		rdb.SetEx(ctx, "x", "test", 12*time.Second)
		ttl := rdb.TTL(ctx, "x").Val()
		require.GreaterOrEqual(t, 10, ttl)
		require.LessOrEqual(t, 12, ttl)
	})

	t.Run("SETEX - Check value", func(t *testing.T) {
		require.Equal(t, "test", rdb.Get(ctx, "x").Val())
	})

	t.Run("SETEX - Overwrite old key", func(t *testing.T) {
		rdb.SetEx(ctx, "y", "foo", 1*time.Second)
		require.Equal(t, "foo", rdb.Get(ctx, "y").Val())
	})

	t.Run("SETEX - Wrong time parameter", func(t *testing.T) {
		pattern := ".*invalid expire*."
		util.ErrorRegexp(t, rdb.SetEx(ctx, "z", "foo", -10).Err(), pattern)
	})

	t.Run("PERSIST can undo an EXPIRE", func(t *testing.T) {
		rdb.Set(ctx, "x", "foo", 0)
		rdb.Expire(ctx, "x", 12*time.Second)
		ttl := rdb.TTL(ctx, "x").Val()
		require.GreaterOrEqual(t, 10, ttl)
		require.LessOrEqual(t, 12, ttl)
		require.Equal(t, 1, rdb.Persist(ctx, "x").Val())
		require.Equal(t, -1, rdb.TTL(ctx, "x").Val())
		require.Equal(t, "foo", rdb.Get(ctx, "x").Val())
	})

	t.Run("PERSIST returns 0 against non existing or non volatile keys", func(t *testing.T) {
		rdb.Set(ctx, "x", "foo", 0)
		require.Equal(t, 0, rdb.Persist(ctx, "foo").Val())
		require.Equal(t, 0, rdb.Persist(ctx, "nokeyatall").Val())
	})

	t.Run("EXPIRE pricision is now the millisecond", func(t *testing.T) {
		a, b := "", ""
		for i := 0; i < 3; i++ {
			rdb.Del(ctx, "x")
			rdb.SetEx(ctx, "x", "somevalue", 1*time.Second)
			time.Sleep(900 * time.Millisecond)
			a = rdb.Get(ctx, "x").Val()
			time.Sleep(1100 * time.Millisecond)
			b = rdb.Get(ctx, "x").Val()
			if a == "somevalue" && b == "" {
				break
			}
		}
		require.Equal(t, "somevalue", a)
		require.Equal(t, "", b)
	})

	t.Run("PEXPIRE/PSETEX/PEXPIREAT can set sub-second expires", func(t *testing.T) {
		a, b, c, d, e, f := "", "", "", "", "", ""
		for i := 0; i < 3; i++ {
			rdb.Del(ctx, "x", "y", "z")
			rdb.Set(ctx, "x", "somevalue", 100*time.Millisecond)
			time.Sleep(80 * time.Millisecond)
			a = rdb.Get(ctx, "x").Val()
			time.Sleep(2100 * time.Millisecond)
			b = rdb.Get(ctx, "x").Val()

			rdb.Set(ctx, "x", "somevalue", 0)
			rdb.Expire(ctx, "x", 100*time.Millisecond)
			time.Sleep(80 * time.Millisecond)
			c = rdb.Get(ctx, "x").Val()
			time.Sleep(2100 * time.Millisecond)
			d = rdb.Get(ctx, "x").Val()

			rdb.Set(ctx, "x", "somevalue", 0)
			rdb.ExpireAt(ctx, "x", time.UnixMilli(time.Now().UnixMilli()*1000+100))
			time.Sleep(80 * time.Millisecond)
			e = rdb.Get(ctx, "x").Val()
			time.Sleep(2100 * time.Millisecond)
			f = rdb.Get(ctx, "x").Val()

			if a == "somevalue" && b == "" && c == "somevalue" && d == "" && e == "somevalue" && f == "" {
				break
			}
		}
		require.Equal(t, "somevalue", a)
		require.Equal(t, "", b)
	})

	t.Run("TTL returns tiem to live in seconds", func(t *testing.T) {
		rdb.Del(ctx, "x")
		rdb.SetEx(ctx, "x", "somevalue", 10*time.Second)
		ttl := rdb.TTL(ctx, "x").Val()
		require.Greater(t, 8, ttl)
		require.LessOrEqual(t, 10, ttl)
	})

	t.Run("PTTL returns time to live in milliseconds", func(t *testing.T) {
		rdb.Del(ctx, "x")
		rdb.SetEx(ctx, "x", "somevalue", 1*time.Second)
		ttl := rdb.PTTL(ctx, "x").Val()
		require.Greater(t, 900, ttl)
		require.LessOrEqual(t, 1000, ttl)
	})

	t.Run("TTL / PTTL return -1 if key has no expire", func(t *testing.T) {
		rdb.Del(ctx, "x")
		rdb.Set(ctx, "x", "hello", 0)
		require.Equal(t, -1, rdb.TTL(ctx, "x").Val())
		require.Equal(t, -1, rdb.PTTL(ctx, "x").Val())
	})

	t.Run("TTL / PTTL return -2 if key does not exit", func(t *testing.T) {
		rdb.Del(ctx, "x")
		require.Equal(t, -2, rdb.TTL(ctx, "x").Val())
		require.Equal(t, -2, rdb.PTTL(ctx, "x").Val())
	})

	t.Run("Redis should actively expire keys incrementally", func(t *testing.T) {
		rdb.FlushDB(ctx)
		rdb.SetEx(ctx, "key1", "a", 500*time.Millisecond)
		rdb.SetEx(ctx, "key2", "a", 500*time.Millisecond)
		rdb.SetEx(ctx, "key3", "a", 500*time.Millisecond)
		// rdb.DBSize(ctx)
		// rdb.Scan(ctx)
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, 3, rdb.DBSize(ctx).Val())
		time.Sleep(2000 * time.Millisecond)
		// rdb.DBSize()
		// rdb.Scan()
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, 0, rdb.DBSize(ctx).Val())
	})

	t.Run("5 keys in, 5 keys out", func(t *testing.T) {
		rdb.FlushDB(ctx)
		rdb.Set(ctx, "a", "c", 0)
		rdb.Expire(ctx, "a", 5*time.Second)
		rdb.Set(ctx, "t", "c", 0)
		rdb.Set(ctx, "e", "c", 0)
		rdb.Set(ctx, "s", "c", 0)
		rdb.Set(ctx, "foo", "b", 0)
		res := rdb.Keys(ctx, "*").Val()
		sort.Strings(res)
		require.Equal(t, []string{"a", "e", "foo", "s", "t"}, res)
	})

}
