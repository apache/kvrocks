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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

type kMetadataResponse struct {
	expire  int64  `redis:"expire"`
	size    int64  `redis:"size"`
	ktype   string `redis:"type"`
	flags   int64  `redis:"flags"`
	version int64  `redis:"version"`
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
	} {
		if val, ok := resultMap[field]; ok {
			converted, err := toInt64(val)
			if err != nil {
				return nil, fmt.Errorf("%s: %v", field, err)
			}
			*target = converted
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

	return response, nil
}

func TestKMetadata(t *testing.T) {
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
		key := "__avoid_collisions__" + "_KMetadataString_" + util.RandString(1, 10, util.Alpha)
		val := "__avoid_collisions__" + "_KMetadataString_" + util.RandString(1, 10, util.Alpha)
		rdb.Set(ctx, key, val, 0)
		r := rdb.Do(ctx, "kmetadata", key)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractKMetadataResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "string", metaResponse.ktype)
		assert.Equal(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(0), metaResponse.size)
	})

	t.Run("Test KMetadata for hash type", func(t *testing.T) {
		key := "__avoid_collisions__" + "_kMetadataHash_" + util.RandString(1, 10, util.Alpha)
		f1 := "__avoid_collisions__" + "_kMetadataHash_" + util.RandString(1, 10, util.Alpha)
		v1 := "__avoid_collisions__" + "_kMetadataHash_" + util.RandString(1, 10, util.Alpha)
		f2 := "__avoid_collisions__" + "_kMetadataHash_" + util.RandString(1, 10, util.Alpha)
		v2 := "__avoid_collisions__" + "_kMetadataHash_" + util.RandString(1, 10, util.Alpha)
		rdb.HSet(ctx, key, f1, v1, f2, v2)
		r := rdb.Do(ctx, "kmetadata", key)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractKMetadataResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "hash", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(2), metaResponse.size)
	})

	t.Run("Test KMetadata for set type", func(t *testing.T) {
		setName := "__avoid_collisions__" + "_kMetadataSet_" + util.RandString(1, 10, util.Alpha)
		item1 := "__avoid_collisions__" + "_kMetadataSet_" + util.RandString(1, 10, util.Alpha)
		item2 := "__avoid_collisions__" + "_kMetadataSet_" + util.RandString(1, 10, util.Alpha)
		item3 := "__avoid_collisions__" + "_kMetadataSet_" + util.RandString(1, 10, util.Alpha)
		item4 := "__avoid_collisions__" + "_kMetadataSet_" + util.RandString(1, 10, util.Alpha)
		rdb.SAdd(ctx, setName, item1, item2, item3, item4)
		r := rdb.Do(ctx, "kmetadata", setName)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractKMetadataResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "set", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(4), metaResponse.size)
	})

	t.Run("Test KMetadata for zset type", func(t *testing.T) {
		zsetName := "__avoid_collisions__" + "_kMetadataZSet_" + util.RandString(1, 10, util.Alpha)
		members := []redis.Z{
			{
				Score:  1.0,
				Member: "__avoid_collisions__" + "_kMetadataZSet_" + util.RandString(1, 10, util.Alpha),
			},
			{
				Score:  2.0,
				Member: "__avoid_collisions__" + "_kMetadataZSet_" + util.RandString(1, 10, util.Alpha),
			},
			{
				Score:  3.0,
				Member: "__avoid_collisions__" + "_kMetadataZSet_" + util.RandString(1, 10, util.Alpha),
			},
		}
		rdb.ZAdd(ctx, zsetName, members...)
		r := rdb.Do(ctx, "kmetadata", zsetName)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractKMetadataResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "zset", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(3), metaResponse.size)
	})

	t.Run("Test KMetadata for Bitmap type", func(t *testing.T) {
		bitMapKey := "__avoid_collisions__" + "_kMetadataBitMap_" + util.RandString(1, 10, util.Alpha)
		rdb.SetBit(ctx, bitMapKey, 0, 1)
		r := rdb.Do(ctx, "kmetadata", bitMapKey)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractKMetadataResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "bitmap", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(1), metaResponse.size)
	})

	t.Run("Test KMetadata for List type", func(t *testing.T) {
		listKey := "__avoid_collisions__" + "_kMetadataList_" + util.RandString(1, 10, util.Alpha)
		item1 := "__avoid_collisions__" + "_kMetadataList_" + util.RandString(1, 10, util.Alpha)
		item2 := "__avoid_collisions__" + "_kMetadataList_" + util.RandString(1, 10, util.Alpha)
		rdb.RPush(ctx, listKey, item1, item2)
		r := rdb.Do(ctx, "kmetadata", listKey)
		result, err := r.Result()
		if err != nil {
			t.Fatalf("Command failed: %v", err)
		}
		metaResponse, err := ExtractKMetadataResponse(result)
		if err != nil {
			t.Fatalf("Failed to extract response: %v", err)
		}
		assert.Equal(t, "list", metaResponse.ktype)
		assert.NotEqual(t, int64(0), metaResponse.version)
		assert.Equal(t, int64(2), metaResponse.size)
	})

	t.Run("Test Key not present", func(t *testing.T) {
		notFoundKey := "__avoid_collisions__" + "_kMetadataNotFound_" + util.RandString(1, 10, util.Alpha)
		r := rdb.Do(ctx, "kmetadata", notFoundKey)
		val := r.Val()
		assert.Equal(t, nil, val)
		assert.Error(t, r.Err())
	})

}
