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

package kmetadata

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

type kMetadataResponse struct {
	expire  int64  `redis:"expire"`
	size    int64  `redis:"size"`
	ktype   string `redis:"type"`
	flags   int64  `redis:"flags"`
	version int64  `redis:"version"`
	mode    string `redis:"mode"`
	format  string `redis:"format"`
	persist int64  `redis:"persist"`
	lower   int64  `redis:"lower"`
	upper   int64  `redis:"upper"`
	head    int64  `redis:"head"`
	tail    int64  `redis:"tail"`
}

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	default:
		return 0, fmt.Errorf("value is not a number, got %T", val)
	}
}

func ExtractKMetadataResponse(result interface{}) (*kMetadataResponse, error) {
	resultMap, ok := result.(map[interface{}]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[interface{}]interface{}, got %T", result)
	}

	response := &kMetadataResponse{}

	// Convert numeric fields
	for field, target := range map[string]*int64{
		"expire":  &response.expire,
		"size":    &response.size,
		"flags":   &response.flags,
		"version": &response.version,
		"persist": &response.persist,
		"lower":   &response.lower,
		"upper":   &response.upper,
		"head":    &response.head,
		"tail":    &response.tail,
	} {
		if val, ok := resultMap[field]; ok {
			converted, err := toInt64(val)
			if err != nil {
				return nil, fmt.Errorf("%s: %v", field, err)
			}
			*target = converted
		}
	}

	for field, target := range map[string]*string{
		"type":   &response.ktype,
		"mode":   &response.mode,
		"format": &response.format,
	} {
		if val, ok := resultMap[field]; ok {
			if strVal, ok := val.(string); ok {
				*target = strVal
			} else {
				return nil, fmt.Errorf("%s is not a string, got %T", field, val)
			}
		}
	}

	return response, nil
}

func MustKMetadataMap(t *testing.T, result interface{}) map[interface{}]interface{} {
	t.Helper()

	resultMap, ok := result.(map[interface{}]interface{})
	require.Truef(t, ok, "expected map[interface{}]interface{}, got %T", result)
	return resultMap
}

func TestKMetadata(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:    "resp3-enabled",
			Options: []string{"yes"},
		},
		{
			Name:    "hash-encoding-mode",
			Options: []string{"legacy", "field-expiration"},
		},
		{
			Name:    "json-storage-format",
			Options: []string{"json", "cbor"},
		},
	}
	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)
	for _, configs := range configsMatrix {
		testKMetadata(t, configs)
	}
}

var testKMetadata = func(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Test KMetadata for String type", func(t *testing.T) {
		key := "string_" + util.RandString(1, 10, util.Alpha)
		val := util.RandString(1, 10, util.Alpha)
		require.NoError(t, rdb.Set(ctx, key, val, 0).Err())
		// Test KMetadata for string type
		r := rdb.Do(ctx, "kmetadata", key)
		result, err := r.Result()
		require.NoError(t, err)

		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "string", metaResponse.ktype)
		require.Equal(t, int64(0), metaResponse.version)
		require.Equal(t, int64(0), metaResponse.size)
	})

	t.Run("Test KMetadata for hash type", func(t *testing.T) {
		key := "hash_" + util.RandString(1, 10, util.Alpha)
		require.NoError(t, rdb.HSet(ctx, key, "f0", "v0", "f1", "v1").Err())
		r := rdb.Do(ctx, "kmetadata", key)
		result, err := r.Result()
		require.NoError(t, err)

		resultMap := MustKMetadataMap(t, result)
		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "hash", metaResponse.ktype)
		require.NotEqual(t, int64(0), metaResponse.version)
		require.Equal(t, int64(2), metaResponse.size)
		require.Equal(t, configs["hash-encoding-mode"], metaResponse.mode)
		if configs["hash-encoding-mode"] == "field-expiration" {
			require.Equal(t, int64(2), metaResponse.persist)
			require.Equal(t, int64(0), metaResponse.lower)
			require.Equal(t, int64(0), metaResponse.upper)
			require.Contains(t, resultMap, "persist")
			require.Contains(t, resultMap, "lower")
			require.Contains(t, resultMap, "upper")
		} else {
			require.NotContains(t, resultMap, "persist")
			require.NotContains(t, resultMap, "lower")
			require.NotContains(t, resultMap, "upper")
		}
	})

	t.Run("Test KMetadata for set type", func(t *testing.T) {
		setName := "set_" + util.RandString(1, 10, util.Alpha)
		require.NoError(t, rdb.SAdd(ctx, setName, "e0", "e1", "e2", "e3").Err())
		r := rdb.Do(ctx, "kmetadata", setName)
		result, err := r.Result()
		require.NoError(t, err)

		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "set", metaResponse.ktype)
		require.NotEqual(t, int64(0), metaResponse.version)
		require.Equal(t, int64(4), metaResponse.size)
	})

	t.Run("Test KMetadata for zset type", func(t *testing.T) {
		zsetName := "zset_" + util.RandString(1, 10, util.Alpha)
		members := []redis.Z{
			{Score: 1.0, Member: "m0"},
			{Score: 2.0, Member: "m1"},
			{Score: 3.0, Member: "m2"},
		}
		rdb.ZAdd(ctx, zsetName, members...)
		r := rdb.Do(ctx, "kmetadata", zsetName)
		result, err := r.Result()
		require.NoError(t, err)

		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "zset", metaResponse.ktype)
		require.NotEqual(t, int64(0), metaResponse.version)
		require.Equal(t, int64(3), metaResponse.size)
	})

	t.Run("Test KMetadata for Bitmap type", func(t *testing.T) {
		bitMapKey := "bitmap_" + util.RandString(1, 10, util.Alpha)
		require.NoError(t, rdb.SetBit(ctx, bitMapKey, 0, 1).Err())
		r := rdb.Do(ctx, "kmetadata", bitMapKey)
		result, err := r.Result()
		require.NoError(t, err)

		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "bitmap", metaResponse.ktype)
		require.NotEqual(t, int64(0), metaResponse.version)
		require.Equal(t, int64(1), metaResponse.size)
	})

	t.Run("Test KMetadata for List type", func(t *testing.T) {
		listKey := "list_" + util.RandString(1, 10, util.Alpha)
		const listInitialIndex = int64(^uint64(0) >> 1)

		require.NoError(t, rdb.LPush(ctx, listKey, "a", "b").Err())
		r := rdb.Do(ctx, "kmetadata", listKey)
		result, err := r.Result()
		require.NoError(t, err)

		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "list", metaResponse.ktype)
		require.NotEqual(t, int64(0), metaResponse.version)
		require.Equal(t, int64(2), metaResponse.size)
		require.Equal(t, listInitialIndex-2, metaResponse.head)
		require.Equal(t, listInitialIndex, metaResponse.tail)
	})

	t.Run("Test KMetadata for JSON type", func(t *testing.T) {
		jsonKey := "json_" + util.RandString(1, 10, util.Alpha)
		require.NoError(t, rdb.Do(ctx, "JSON.SET", jsonKey, "$", `{"x":1,"y":2}`).Err())

		r := rdb.Do(ctx, "kmetadata", jsonKey)
		result, err := r.Result()
		require.NoError(t, err)

		metaResponse, err := ExtractKMetadataResponse(result)
		require.NoError(t, err)
		require.Equal(t, "ReJSON-RL", metaResponse.ktype)
		require.Equal(t, configs["json-storage-format"], metaResponse.format)
		require.Equal(t, int64(0), metaResponse.version)
		require.Equal(t, int64(0), metaResponse.size)
	})

	t.Run("Test Key not present", func(t *testing.T) {
		notFoundKey := "not_found_" + util.RandString(1, 10, util.Alpha)
		r := rdb.Do(ctx, "kmetadata", notFoundKey)
		require.NotNil(t, r.Err())
	})
}
