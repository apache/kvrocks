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

package util

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type KMetadataResponse struct {
	Expire  int64
	Size    int64
	Type    string
	Flags   int64
	Version int64
	Mode    string
	Format  string
	Persist int64
	Lower   int64
	Upper   int64
	Head    int64
	Tail    int64
}

func kMetadataToInt64(val interface{}) (int64, error) {
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

func ExtractKMetadataResponse(result interface{}) (*KMetadataResponse, error) {
	resultMap, ok := result.(map[interface{}]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[interface{}]interface{}, got %T", result)
	}

	response := &KMetadataResponse{}
	for field, target := range map[string]*int64{
		"expire":  &response.Expire,
		"size":    &response.Size,
		"flags":   &response.Flags,
		"version": &response.Version,
		"persist": &response.Persist,
		"lower":   &response.Lower,
		"upper":   &response.Upper,
		"head":    &response.Head,
		"tail":    &response.Tail,
	} {
		if val, ok := resultMap[field]; ok {
			converted, err := kMetadataToInt64(val)
			if err != nil {
				return nil, fmt.Errorf("%s: %v", field, err)
			}
			*target = converted
		}
	}

	for field, target := range map[string]*string{
		"type":   &response.Type,
		"mode":   &response.Mode,
		"format": &response.Format,
	} {
		if val, ok := resultMap[field]; ok {
			strVal, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("%s is not a string, got %T", field, val)
			}
			*target = strVal
		}
	}

	return response, nil
}

func GetKMetadata(t testing.TB, rdb *redis.Client, ctx context.Context, key string) KMetadataResponse {
	t.Helper()

	result, err := rdb.Do(ctx, "kmetadata", key).Result()
	require.NoError(t, err)
	meta, err := ExtractKMetadataResponse(result)
	require.NoError(t, err)
	return *meta
}
