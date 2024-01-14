package applybatch

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestApplyBatch_Basic(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Make sure the apply batch command works", func(t *testing.T) {
		// SET a 1
		batch, err := hex.DecodeString("04000000000000000100000003013105010D0B5F5F6E616D6573706163656106010000000031")
		require.NoError(t, err)
		r := rdb.Do(ctx, "ApplyBatch", string(batch))
		val, err := r.Int64()
		require.NoError(t, err)
		require.EqualValues(t, len(batch), val)
		require.Equal(t, "1", rdb.Get(ctx, "a").Val())

		// HSET hash field value
		batch, err = hex.DecodeString("05000000000000000200000003013201210B5F5F6E616D65737061636500000004686173683076F331696342A76669656C640576616C75650501100B5F5F6E616D657370616365686173681102000000003076F331696342A700000002")
		require.NoError(t, err)
		r = rdb.Do(ctx, "ApplyBatch", string(batch))
		val, err = r.Int64()
		require.NoError(t, err)
		require.EqualValues(t, len(batch), val)
		require.Equal(t, "value", rdb.HGet(ctx, "hash", "field").Val())
	})
}
