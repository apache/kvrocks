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

package scripting

import (
	"context"
	_ "embed"
	"strings"
	"testing"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

//go:embed mylib1.lua
var luaMylib1 string

//go:embed mylib2.lua
var luaMylib2 string

func TestFunction(t *testing.T) {
	srv := util.StartServer(t, map[string]string{})
	defer srv.Close()

	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("FUNCTION LOAD errors", func(t *testing.T) {
		code := strings.Join(strings.Split(luaMylib1, "\n")[1:], "\n")
		util.ErrorRegexp(t, rdb.Do(ctx, "FUNCTION", "LOAD", code).Err(), ".*Shebang statement.*")

		code2 := "#!lua\n" + code
		util.ErrorRegexp(t, rdb.Do(ctx, "FUNCTION", "LOAD", code2).Err(), ".*Expect library name.*")

		code2 = "#!lua name=$$$\n" + code
		util.ErrorRegexp(t, rdb.Do(ctx, "FUNCTION", "LOAD", code2).Err(), ".*valid library name.*")
	})

	t.Run("FUNCTION LOAD and FCALL mylib1", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Err(), ".*No such function name.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib1).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Val(), int64(2))
		require.Equal(t, rdb.Do(ctx, "FCALL", "add", 0, 122, 111).Val(), int64(233))
	})

	t.Run("FUNCTION LIST and FUNCTION LISTFUNC mylib1", func(t *testing.T) {
		list := rdb.Do(ctx, "FUNCTION", "LIST", "WITHCODE").Val().([]interface{})
		require.Equal(t, list[1].(string), "mylib1")
		require.Equal(t, list[3].(string), luaMylib1)
		require.Equal(t, len(list), 4)

		list = rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		require.Equal(t, list[1].(string), "add")
		require.Equal(t, list[3].(string), "mylib1")
		require.Equal(t, list[5].(string), "inc")
		require.Equal(t, list[7].(string), "mylib1")
		require.Equal(t, len(list), 8)
	})

	t.Run("FUNCTION LOAD and FCALL mylib2", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Err(), ".*No such function name.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib2).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Val(), "Hello, x!")
		require.Equal(t, rdb.Do(ctx, "FCALL", "reverse", 0, "abc").Val(), "cba")
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 2).Val(), int64(3))
	})

	t.Run("FUNCTION LIST and FUNCTION LISTFUNC mylib2", func(t *testing.T) {
		list := rdb.Do(ctx, "FUNCTION", "LIST", "WITHCODE").Val().([]interface{})
		require.Equal(t, list[1].(string), "mylib1")
		require.Equal(t, list[3].(string), luaMylib1)
		require.Equal(t, list[5].(string), "mylib2")
		require.Equal(t, list[7].(string), luaMylib2)
		require.Equal(t, len(list), 8)

		list = rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		require.Equal(t, list[1].(string), "add")
		require.Equal(t, list[3].(string), "mylib1")
		require.Equal(t, list[5].(string), "hello")
		require.Equal(t, list[7].(string), "mylib2")
		require.Equal(t, list[9].(string), "inc")
		require.Equal(t, list[11].(string), "mylib1")
		require.Equal(t, list[13].(string), "reverse")
		require.Equal(t, list[15].(string), "mylib2")
		require.Equal(t, len(list), 16)
	})

	t.Run("FUNCTION DELETE", func(t *testing.T) {
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "yy").Val(), "Hello, yy!")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "DELETE", "mylib2").Err())
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Err(), ".*No such function name.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "reverse", 0, "x").Err(), ".*No such function name.*")
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 3).Val(), int64(4))

		list := rdb.Do(ctx, "FUNCTION", "LIST", "WITHCODE").Val().([]interface{})
		require.Equal(t, list[1].(string), "mylib1")
		require.Equal(t, list[3].(string), luaMylib1)
		require.Equal(t, len(list), 4)

		list = rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		require.Equal(t, list[1].(string), "add")
		require.Equal(t, list[3].(string), "mylib1")
		require.Equal(t, list[5].(string), "inc")
		require.Equal(t, list[7].(string), "mylib1")
		require.Equal(t, len(list), 8)
	})

	t.Run("FUNCTION LOAD REPLACE", func(t *testing.T) {
		code := strings.ReplaceAll(luaMylib2, "name=mylib2", "name=mylib1")
		util.ErrorRegexp(t, rdb.Do(ctx, "FUNCTION", "LOAD", code).Err(), ".*library already exists.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", "REPLACE", code).Err())

		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "xxx").Val(), "Hello, xxx!")
		require.Equal(t, rdb.Do(ctx, "FCALL", "reverse", 0, "xyz").Val(), "zyx")
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Err(), ".*No such function name.*")

		list := rdb.Do(ctx, "FUNCTION", "LIST", "WITHCODE").Val().([]interface{})
		require.Equal(t, list[1].(string), "mylib1")
		require.Equal(t, list[3].(string), code)
		require.Equal(t, len(list), 4)

		list = rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		require.Equal(t, list[1].(string), "hello")
		require.Equal(t, list[3].(string), "mylib1")
		require.Equal(t, list[5].(string), "reverse")
		require.Equal(t, list[7].(string), "mylib1")
		require.Equal(t, len(list), 8)
	})
}
