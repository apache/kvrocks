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

	"github.com/redis/go-redis/v9"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

//go:embed mylib1.lua
var luaMylib1 string

//go:embed mylib2.lua
var luaMylib2 string

//go:embed mylib3.lua
var luaMylib3 string

type ListFuncResult struct {
	Name    string
	Library string
}

func decodeListFuncResult(t *testing.T, v interface{}) ListFuncResult {
	switch res := v.(type) {
	case []interface{}:
		require.EqualValues(t, 4, len(res))
		require.EqualValues(t, "function_name", res[0])
		require.EqualValues(t, "from_library", res[2])
		return ListFuncResult{
			Name:    res[1].(string),
			Library: res[3].(string),
		}
	case map[interface{}]interface{}:
		require.EqualValues(t, 2, len(res))
		return ListFuncResult{
			Name:    res["function_name"].(string),
			Library: res["from_library"].(string),
		}
	}
	require.Fail(t, "unexpected type")
	return ListFuncResult{}
}

type ListLibResult struct {
	Name      string
	Engine    string
	Functions []interface{}
}

func decodeListLibResult(t *testing.T, v interface{}) ListLibResult {
	switch res := v.(type) {
	case []interface{}:
		require.EqualValues(t, 6, len(res))
		require.EqualValues(t, "library_name", res[0])
		require.EqualValues(t, "engine", res[2])
		require.EqualValues(t, "functions", res[4])
		return ListLibResult{
			Name:      res[1].(string),
			Engine:    res[3].(string),
			Functions: res[5].([]interface{}),
		}
	case map[interface{}]interface{}:
		require.EqualValues(t, 3, len(res))
		return ListLibResult{
			Name:      res["library_name"].(string),
			Engine:    res["engine"].(string),
			Functions: res["functions"].([]interface{}),
		}
	}
	require.Fail(t, "unexpected type")
	return ListLibResult{}
}

func TestFunctionsWithRESP3(t *testing.T) {
	testFunctions(t, "yes")
}

func TestFunctionsWithoutRESP2(t *testing.T) {
	testFunctions(t, "no")
}

var testFunctions = func(t *testing.T, enabledRESP3 string) {
	srv := util.StartServer(t, map[string]string{
		"resp3-enabled": enabledRESP3,
	})
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
		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(libraries))
		require.Equal(t, "mylib1", libraries[0].Name)
		require.Equal(t, luaMylib1, libraries[0].Code)

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		require.EqualValues(t, 2, len(list))
		f1 := decodeListFuncResult(t, list[0])
		require.Equal(t, "add", f1.Name)
		require.Equal(t, "mylib1", f1.Library)
		f2 := decodeListFuncResult(t, list[1])
		require.Equal(t, "inc", f2.Name)
		require.Equal(t, "mylib1", f2.Library)
	})

	t.Run("FUNCTION LOAD and FCALL mylib2", func(t *testing.T) {
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Err(), ".*No such function name.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib2).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Val(), "Hello, x!")
		require.Equal(t, rdb.Do(ctx, "FCALL", "reverse", 0, "abc").Val(), "cba")
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 2).Val(), int64(3))
	})

	t.Run("FUNCTION LIST and FUNCTION LISTFUNC mylib2", func(t *testing.T) {
		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 2, len(libraries))

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		expected := []ListFuncResult{
			{Name: "add", Library: "mylib1"},
			{Name: "hello", Library: "mylib2"},
			{Name: "inc", Library: "mylib1"},
			{Name: "reverse", Library: "mylib2"},
		}
		require.EqualValues(t, len(expected), len(list))
		for i, f := range expected {
			actual := decodeListFuncResult(t, list[i])
			require.Equal(t, f.Name, actual.Name)
			require.Equal(t, f.Library, actual.Library)
		}
	})

	t.Run("FUNCTION DELETE", func(t *testing.T) {
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "yy").Val(), "Hello, yy!")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "DELETE", "mylib2").Err())
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "hello", 0, "x").Err(), ".*No such function name.*")
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "reverse", 0, "x").Err(), ".*No such function name.*")
		require.Equal(t, rdb.Do(ctx, "FCALL", "inc", 0, 3).Val(), int64(4))

		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(libraries))
		require.Equal(t, "mylib1", libraries[0].Name)

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		expected := []ListFuncResult{
			{Name: "add", Library: "mylib1"},
			{Name: "inc", Library: "mylib1"},
		}
		require.EqualValues(t, len(expected), len(list))
		for i, f := range expected {
			actual := decodeListFuncResult(t, list[i])
			require.Equal(t, f.Name, actual.Name)
			require.Equal(t, f.Library, actual.Library)
		}
	})

	t.Run("FUNCTION LOAD REPLACE", func(t *testing.T) {
		code := strings.ReplaceAll(luaMylib2, "name=mylib2", "name=mylib1")
		util.ErrorRegexp(t, rdb.Do(ctx, "FUNCTION", "LOAD", code).Err(), ".*library already exists.*")
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", "REPLACE", code).Err())

		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "xxx").Val(), "Hello, xxx!")
		require.Equal(t, rdb.Do(ctx, "FCALL", "reverse", 0, "xyz").Val(), "zyx")
		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL", "inc", 0, 1).Err(), ".*No such function name.*")

		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 1, len(libraries))
		require.Equal(t, "mylib1", libraries[0].Name)

		list := rdb.Do(ctx, "FUNCTION", "LISTFUNC").Val().([]interface{})
		expected := []ListFuncResult{
			{Name: "hello", Library: "mylib1"},
			{Name: "reverse", Library: "mylib1"},
		}
		require.EqualValues(t, len(expected), len(list))
		for i, f := range expected {
			actual := decodeListFuncResult(t, list[i])
			require.Equal(t, f.Name, actual.Name)
			require.Equal(t, f.Library, actual.Library)
		}
	})

	t.Run("FCALL_RO", func(t *testing.T) {
		require.NoError(t, rdb.Do(ctx, "FUNCTION", "LOAD", luaMylib3).Err())

		require.NoError(t, rdb.Set(ctx, "x", 1, 0).Err())
		require.Equal(t, rdb.Do(ctx, "FCALL", "myget", 1, "x").Val(), "1")
		require.Equal(t, rdb.Do(ctx, "FCALL_RO", "myget", 1, "x").Val(), "1")

		require.Equal(t, rdb.Do(ctx, "FCALL", "myset", 1, "x", 2).Val(), "OK")
		require.Equal(t, rdb.Get(ctx, "x").Val(), "2")

		util.ErrorRegexp(t, rdb.Do(ctx, "FCALL_RO", "myset", 1, "x", 3).Err(), ".*Write commands are not allowed.*")
	})

	t.Run("Restart server and test again", func(t *testing.T) {
		srv.Restart()

		require.Equal(t, rdb.Do(ctx, "FCALL", "myget", 1, "x").Val(), "2")
		require.Equal(t, rdb.Do(ctx, "FCALL", "hello", 0, "xxx").Val(), "Hello, xxx!")

		libraries, err := rdb.FunctionList(ctx, redis.FunctionListQuery{
			WithCode: true,
		}).Result()
		require.NoError(t, err)
		require.EqualValues(t, 2, len(libraries))
		require.Equal(t, libraries[0].Name, "mylib1")
		require.Equal(t, libraries[1].Name, "mylib3")
	})

	t.Run("FUNCTION LISTLIB", func(t *testing.T) {
		r := rdb.Do(ctx, "FUNCTION", "LISTLIB", "mylib1").Val()
		require.EqualValues(t, ListLibResult{
			Name: "mylib1", Engine: "lua", Functions: []interface{}{"hello", "reverse"},
		}, decodeListLibResult(t, r))

		r = rdb.Do(ctx, "FUNCTION", "LISTLIB", "mylib3").Val()
		require.EqualValues(t, ListLibResult{
			Name: "mylib3", Engine: "lua", Functions: []interface{}{"myget", "myset"},
		}, decodeListLibResult(t, r))
	})
}
