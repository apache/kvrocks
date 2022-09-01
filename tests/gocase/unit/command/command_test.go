package command

import (
	"context"
	"github.com/stretchr/testify/require"
	"gocase/util"
	"testing"
)

func TestCommand(t *testing.T) {
	srv, err := util.StartServer(t, map[string]string{})
	require.NoError(t, err)
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.Client()

	t.Run("Kvrocks supports 180 commands currently", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "COUNT")
		v, err := r.Int()
		require.NoError(t, err)
		require.Equal(t, 180, v)
	})

	t.Run("acquire GET command info by COMMAND INFO", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "INFO", "GET")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
		require.Equal(t, "get", v[0])
		require.EqualValues(t, 2, v[1])
		require.Equal(t, []interface{}{"readonly"}, v[2])
		require.EqualValues(t, 1, v[3])
		require.EqualValues(t, 1, v[4])
		require.EqualValues(t, 1, v[5])
	})

	t.Run("command entry length check", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND")
		vs, err := r.Slice()
		require.NoError(t, err)
		v := vs[0].([]interface{})
		require.Len(t, v, 6)
	})

	t.Run("get keys of commands by COMMAND GETKEYS", func(t *testing.T) {
		r := rdb.Do(ctx, "COMMAND", "GETKEYS", "GET", "test")
		vs, err := r.Slice()
		require.NoError(t, err)
		require.Len(t, vs, 1)
		require.Equal(t, "test", vs[0])
	})
}
