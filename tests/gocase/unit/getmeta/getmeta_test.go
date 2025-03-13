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

package getmeta

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

type getMetaResponse struct {
	ttl     int64  `redis:"ttl"`
	size    int64  `redis:"size"`
	ktype   string `redis:"type"`
	flags   int64  `redis:"flags"`
	version int64  `redis:"version"`
}

func ExtractGetMetaResponse(result interface{}) (*getMetaResponse, error) {
	// Check if result is a map
	resultMap, ok := result.(map[interface{}]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[interface{}]interface{}, got %T", result)
	}

	response := &getMetaResponse{}

	// Extract TTL field
	if val, ok := resultMap["ttl"]; ok {
		switch v := val.(type) {
		case int64:
			response.ttl = v
		case int:
			response.ttl = int64(v)
		case float64:
			response.ttl = int64(v)
		default:
			return nil, fmt.Errorf("ttl is not a number, got %T", val)
		}
	}

	// Extract Size field
	if val, ok := resultMap["size"]; ok {
		switch v := val.(type) {
		case int64:
			response.size = v
		case int:
			response.size = int64(v)
		case float64:
			response.size = int64(v)
		default:
			return nil, fmt.Errorf("size is not a number, got %T", val)
		}
	}

	// Extract Type field
	if val, ok := resultMap["type"]; ok {
		if strVal, ok := val.(string); ok {
			response.ktype = strVal
		} else {
			return nil, fmt.Errorf("type is not a string, got %T", val)
		}
	}

	// Extract Flags field
	if val, ok := resultMap["flags"]; ok {
		switch v := val.(type) {
		case int64:
			response.flags = v
		case int:
			response.flags = int64(v)
		case float64:
			response.flags = int64(v)
		default:
			return nil, fmt.Errorf("flags is not a number, got %T", val)
		}
	}

	// Extract Version field
	if val, ok := resultMap["version"]; ok {
		switch v := val.(type) {
		case int64:
			response.version = v
		case int:
			response.version = int64(v)
		case float64:
			response.version = int64(v)
		default:
			return nil, fmt.Errorf("version is not a number, got %T", val)
		}
	}

	return response, nil
}

func TestGetMeta(t *testing.T) {
	configOptions := []util.ConfigOptions{
		{
			Name:       "resp3-enabled",
			Options:    []string{"yes"},
			ConfigType: util.YesNo,
		},
	}
	configsMatrix, err := util.GenerateConfigsMatrix(configOptions)
	require.NoError(t, err)
	for _, configs := range configsMatrix {
		testGetMeta(t, configs)
	}
}

var testGetMeta = func(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Test GetMeta for String type", func(t *testing.T) {
		key := "__avoid_collisions__" + "_getMetaString_" + util.RandString(0, 8, util.Alpha)
		val := "__avoid_collisions__" + "_getMetaString_" + util.RandString(0, 8, util.Alpha)
		rdb.Set(ctx, key, val, 0)
		r := rdb.Do(ctx, "getmeta", key)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractGetMetaResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "string", metaResponse.ktype)
		assert.Equal(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(0), metaResponse.size)
	})

	t.Run("Test GetMeta for hash type", func(t *testing.T) {
		key := "__avoid_collisions__" + "_getMetaHash_" + util.RandString(0, 8, util.Alpha)
		f1 := "__avoid_collisions__" + "_getMetaHash_" + util.RandString(0, 8, util.Alpha)
		v1 := "__avoid_collisions__" + "_getMetaHash_" + util.RandString(0, 8, util.Alpha)
		f2 := "__avoid_collisions__" + "_getMetaHash_" + util.RandString(0, 8, util.Alpha)
		v2 := "__avoid_collisions__" + "_getMetaHash_" + util.RandString(0, 8, util.Alpha)
		rdb.HSet(ctx, key, f1, v1, f2, v2)
		r := rdb.Do(ctx, "getmeta", key)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractGetMetaResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "hash", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(2), metaResponse.size)
	})

	t.Run("Test GetMeta for set type", func(t *testing.T) {
		setName := "__avoid_collisions__" + "_getMetaSet_" + util.RandString(0, 8, util.Alpha)
		item1 := "__avoid_collisions__" + "_getMetaSet_" + util.RandString(0, 8, util.Alpha)
		item2 := "__avoid_collisions__" + "_getMetaSet_" + util.RandString(0, 8, util.Alpha)
		item3 := "__avoid_collisions__" + "_getMetaSet_" + util.RandString(0, 8, util.Alpha)
		item4 := "__avoid_collisions__" + "_getMetaSet_" + util.RandString(0, 8, util.Alpha)
		rdb.SAdd(ctx, setName, item1, item2, item3, item4)
		r := rdb.Do(ctx, "getmeta", setName)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractGetMetaResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "set", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(4), metaResponse.size)
	})

	t.Run("Test GetMeta for zset type", func(t *testing.T) {
		zsetName := "__avoid_collisions__" + "_getMetaZSet_" + util.RandString(0, 8, util.Alpha)
		members := []redis.Z{
			{
				Score:  1.0,
				Member: "__avoid_collisions__" + "_getMetaZSet_" + util.RandString(0, 8, util.Alpha),
			},
			{
				Score:  2.0,
				Member: "__avoid_collisions__" + "_getMetaZSet_" + util.RandString(0, 8, util.Alpha),
			},
			{
				Score:  3.0,
				Member: "__avoid_collisions__" + "_getMetaZSet_" + util.RandString(0, 8, util.Alpha),
			},
		}
		rdb.ZAdd(ctx, zsetName, members...)
		r := rdb.Do(ctx, "getmeta", zsetName)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractGetMetaResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "zset", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(3), metaResponse.size)
	})

	t.Run("Test GetMeta for Bitmap type", func(t *testing.T) {
		bitMapKey := "__avoid_collisions__" + "_getMetaBitMap_" + util.RandString(0, 8, util.Alpha)
		rdb.SetBit(ctx, bitMapKey, 0, 1)
		r := rdb.Do(ctx, "getmeta", bitMapKey)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractGetMetaResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "bitmap", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(1), metaResponse.size)
	})

	t.Run("Test GetMeta for List type", func(t *testing.T) {
		listKey := "__avoid_collisions__" + "_getMetaList_" + util.RandString(0, 8, util.Alpha)
		item1 := "__avoid_collisions__" + "_getMetaList_" + util.RandString(0, 8, util.Alpha)
		item2 := "__avoid_collisions__" + "_getMetaList_" + util.RandString(0, 8, util.Alpha)
		rdb.RPush(ctx, listKey, item1, item2)
		r := rdb.Do(ctx, "getmeta", listKey)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractGetMetaResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "list", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(2), metaResponse.size)
	})

	t.Run("Test Key not present", func(t *testing.T) {
		notFoundKey := "__avoid_collisions__" + "_getMetaNotFound_" + util.RandString(0, 8, util.Alpha)
		r := rdb.Do(ctx, "getmeta", notFoundKey)
		val := r.Val()
		assert.Equal(t, nil, val)
		assert.Error(t, r.Err())
	})

}
