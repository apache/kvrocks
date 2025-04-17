//go:build !(ignore_when_tsan || ignore_when_asan || ignore_when_ubsan)

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

package sst

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/kvrocks/tests/gocase/util"
)

const (
	DefaultKvrocksNamespace       = "__namespace"
	metaDataEncodingMask    uint8 = 0x82
	versionCounterBits            = 11
)

type Metadata struct {
	Flags   uint8
	Expire  uint64
	Version uint64
	Size    uint64
}

type SSTResponse struct {
	filesLoaded int64 `redis:"files_loaded"`
}

func NewMetadata() *Metadata {
	src := rand.NewSource(time.Now().UnixNano())
	r := rand.New(src)

	timestamp := uint64(time.Now().UnixMicro())
	counter := uint64(r.Int63())
	version := (timestamp << versionCounterBits) + (counter % (1 << versionCounterBits))

	return &Metadata{
		Flags:   metaDataEncodingMask,
		Version: version,
	}
}

func (m *Metadata) Encode() []byte {
	buf := make([]byte, 25) // 1 + 8 + 8 + 8 bytes
	buf[0] = m.Flags
	binary.BigEndian.PutUint64(buf[1:], m.Expire)
	binary.BigEndian.PutUint64(buf[9:], m.Version)
	binary.BigEndian.PutUint64(buf[17:], m.Size)
	return buf
}

func encodeInternalKey(namespace, key, field string, version uint64) []byte {
	nsLen := len(namespace)
	keyLen := len(key)
	fieldLen := len(field)

	// Pre-calculate total size: 1 byte for ns size + ns + 4 bytes for key size + key + 8 bytes for version + field
	out := make([]byte, 1+nsLen+4+keyLen+8+fieldLen)

	out[0] = uint8(nsLen)
	copy(out[1:], namespace)
	binary.BigEndian.PutUint32(out[1+nsLen:], uint32(keyLen))
	copy(out[5+nsLen:], key)
	binary.BigEndian.PutUint64(out[5+nsLen+keyLen:], version)
	copy(out[13+nsLen+keyLen:], field)

	return out
}

func encodeRedisHashKey(namespace, userKey string) []byte {
	totalLen := 1 + len(namespace) + len(userKey)
	buf := make([]byte, totalLen)
	buf[0] = uint8(len(namespace))
	copy(buf[1:], namespace)
	copy(buf[1+len(namespace):], userKey)
	return buf
}

func createSSTFile(filename string, data map[string]string) error {
	envOpts := grocksdb.NewDefaultEnvOptions()
	sstWriterOpts := grocksdb.NewDefaultOptions()
	sstWriterOpts.SetCompression(grocksdb.CompressionType(2))
	sstWriter := grocksdb.NewSSTFileWriter(envOpts, sstWriterOpts)
	defer sstWriter.Destroy()

	err := sstWriter.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open SST file writer: %v", err)
	}

	// Get all keys and sort them
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Add keys in sorted order
	for _, k := range keys {
		err = sstWriter.Add([]byte(k), []byte(data[k]))
		if err != nil {
			log.Printf("Error adding key %s to SST: %v", k, err)
		}
	}

	err = sstWriter.Finish()
	if err != nil {
		return fmt.Errorf("failed to finish SST file: %v", err)
	}
	return nil
}

func makeTempDir() (string, error) {
	return os.MkdirTemp("", "sst_test_*")
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

func ExtractSSTResponse(result interface{}) (*SSTResponse, error) {
	resultMap, ok := result.(map[interface{}]interface{})
	if !ok {
		return nil, fmt.Errorf("expected map[interface{}]interface{}, got %T", result)
	}
	response := &SSTResponse{}
	for field, target := range map[string]*int64{
		"files_loaded": &response.filesLoaded,
	} {
		if val, ok := resultMap[field]; ok {
			converted, err := toInt64(val)
			if err != nil {
				return nil, fmt.Errorf("%s: %v", field, err)
			}
			*target = converted
		}
	}
	return response, nil
}

func TestSSTLoad(t *testing.T) {
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
		testSSTLoad(t, configs)
	}
}

var testSSTLoad = func(t *testing.T, configs util.KvrocksServerConfigs) {
	srv := util.StartServer(t, configs)
	defer srv.Close()
	ctx := context.Background()
	rdb := srv.NewClient()
	defer func() { require.NoError(t, rdb.Close()) }()

	t.Run("Test load cmd with no folder", func(t *testing.T) {
		r := rdb.Do(ctx, "sst", "load")
		assert.Error(t, r.Err())
	})

	t.Run("Test wrong subcommand", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		r := rdb.Do(ctx, "sst", "wrong-sub-command", dir)
		assert.Error(t, r.Err())
	})

	t.Run("Test wrong load option", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		r := rdb.Do(ctx, "sst", "load", dir, "wrong-load-option")
		assert.Error(t, r.Err())
	})

	t.Run("Test empty folder", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)
		r := rdb.Do(ctx, "sst", "load", dir)
		assert.NoError(t, r.Err())
		resp, err := ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), resp.filesLoaded)
	})

	t.Run("Test load redis hash keys", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		namespace := DefaultKvrocksNamespace
		data := map[string][]map[string]string{
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
		}
		keys := make(map[string]string, len(data))
		metaKeys := make(map[string]string, len(data))
		for hashK := range data {
			meta := NewMetadata()
			fields := data[hashK]
			for _, field := range fields {
				for fieldK, fieldV := range field {
					internalKey := encodeInternalKey(namespace, hashK, fieldK, meta.Version)
					keys[string(internalKey)] = fieldV
				}
			}
			hashKey := encodeRedisHashKey(namespace, hashK)
			meta.Size++
			metaKeys[string(hashKey)] = string(meta.Encode())
		}

		err = createSSTFile(filepath.Join(dir, "kvrocks_default.sst"), keys)
		assert.NoError(t, err)
		err = createSSTFile(filepath.Join(dir, "kvrocks_metadata.sst"), metaKeys)
		assert.NoError(t, err)

		r := rdb.Do(ctx, "sst", "load", dir)
		assert.NoError(t, r.Err())
		resp, err := ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(2), resp.filesLoaded)

		//verify files didn't get moved
		_, err = os.Stat(filepath.Join(dir, "kvrocks_default.sst"))
		assert.NoError(t, err)
		_, err = os.Stat(filepath.Join(dir, "kvrocks_metadata.sst"))
		assert.NoError(t, err)

		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK, expectedVal := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.NoError(t, val.Err())
					assert.Equal(t, expectedVal, val.Val(), "Hash field value mismatch for key:%s field:%s", hashK, fieldK)
				}
			}
		}
	})

	t.Run("Test load and update redis hash keys", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		namespace := DefaultKvrocksNamespace
		data := map[string][]map[string]string{
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
		}
		keys := make(map[string]string, len(data))
		metaKeys := make(map[string]string, len(data))
		for hashK := range data {
			meta := NewMetadata()
			fields := data[hashK]
			for _, field := range fields {
				for fieldK, fieldV := range field {
					internalKey := encodeInternalKey(namespace, hashK, fieldK, meta.Version)
					keys[string(internalKey)] = fieldV
				}
			}
			hashKey := encodeRedisHashKey(namespace, hashK)
			meta.Size++
			metaKeys[string(hashKey)] = string(meta.Encode())
		}

		err = createSSTFile(filepath.Join(dir, "kvrocks_default.sst"), keys)
		assert.NoError(t, err)
		err = createSSTFile(filepath.Join(dir, "kvrocks_metadata.sst"), metaKeys)
		assert.NoError(t, err)

		r := rdb.Do(ctx, "sst", "load", dir)
		assert.NoError(t, r.Err())
		resp, err := ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(2), resp.filesLoaded)

		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK, expectedVal := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.NoError(t, val.Err())
					assert.Equal(t, expectedVal, val.Val(), "Hash field value mismatch for key:%s field:%s", hashK, fieldK)
				}
			}
		}
		// update the keys fields via redis interface
		for hashK := range data {
			fields := data[hashK]
			for _, field := range fields {
				for fieldK := range field {
					newVal := "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)
					r := rdb.HSet(ctx, hashK, fieldK, newVal)
					assert.NoError(t, r.Err())
					field[fieldK] = newVal
				}
			}
		}
		// validate the updates made via redis interface
		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK, expectedVal := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.NoError(t, val.Err())
					assert.Equal(t, expectedVal, val.Val(), "Hash field value mismatch for key:%s field:%s", hashK, fieldK)
				}
			}
		}
		// update the values and sst load
		for hashK := range data {
			fields := data[hashK]
			for _, field := range fields {
				for fieldK := range field {
					newVal := "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)
					field[fieldK] = newVal
				}
			}
		}

		keys = make(map[string]string, len(data))
		metaKeys = make(map[string]string, len(data))
		for hashK := range data {
			meta := NewMetadata()
			fields := data[hashK]
			for _, field := range fields {
				for fieldK, fieldV := range field {
					internalKey := encodeInternalKey(namespace, hashK, fieldK, meta.Version)
					keys[string(internalKey)] = fieldV
				}
			}
			hashKey := encodeRedisHashKey(namespace, hashK)
			meta.Size++
			metaKeys[string(hashKey)] = string(meta.Encode())
		}

		err = createSSTFile(filepath.Join(dir, "kvrocks_default.sst"), keys)
		assert.NoError(t, err)
		err = createSSTFile(filepath.Join(dir, "kvrocks_metadata.sst"), metaKeys)
		assert.NoError(t, err)

		r = rdb.Do(ctx, "sst", "load", dir)
		assert.NoError(t, r.Err())
		resp, err = ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(2), resp.filesLoaded)

		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK, expectedVal := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.NoError(t, val.Err())
					assert.Equal(t, expectedVal, val.Val(), "Hash field value mismatch for key:%s field:%s", hashK, fieldK)
				}
			}
		}
	})

	t.Run("Test load redis hash keys with move option", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		namespace := DefaultKvrocksNamespace
		data := map[string][]map[string]string{
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
		}
		keys := make(map[string]string, len(data))
		metaKeys := make(map[string]string, len(data))
		for hashK := range data {
			meta := NewMetadata()
			fields := data[hashK]
			for _, field := range fields {
				for fieldK, fieldV := range field {
					internalKey := encodeInternalKey(namespace, hashK, fieldK, meta.Version)
					keys[string(internalKey)] = fieldV
				}
			}
			hashKey := encodeRedisHashKey(namespace, hashK)
			meta.Size++
			metaKeys[string(hashKey)] = string(meta.Encode())
		}

		err = createSSTFile(filepath.Join(dir, "kvrocks_default.sst"), keys)
		assert.NoError(t, err)
		err = createSSTFile(filepath.Join(dir, "kvrocks_metadata.sst"), metaKeys)
		assert.NoError(t, err)

		r := rdb.Do(ctx, "sst", "load", dir, "movefiles", "yes")
		assert.NoError(t, r.Err())
		resp, err := ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(2), resp.filesLoaded)

		//verify files did get moved
		_, err = os.Stat(filepath.Join(dir, "kvrocks_default.sst"))
		assert.True(t, os.IsNotExist(err))
		_, err = os.Stat(filepath.Join(dir, "kvrocks_metadata.sst"))
		assert.True(t, os.IsNotExist(err))

		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK, expectedVal := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.NoError(t, val.Err())
					assert.Equal(t, expectedVal, val.Val(), "Hash field value mismatch for key:%s field:%s", hashK, fieldK)
				}
			}
		}
	})

	t.Run("Test load redis hash keys with no metadata entries", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		namespace := DefaultKvrocksNamespace
		data := map[string][]map[string]string{
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
		}
		keys := make(map[string]string, len(data))
		metaKeys := make(map[string]string, len(data))
		for hashK := range data {
			meta := NewMetadata()
			fields := data[hashK]
			for _, field := range fields {
				for fieldK, fieldV := range field {
					internalKey := encodeInternalKey(namespace, hashK, fieldK, meta.Version)
					keys[string(internalKey)] = fieldV
				}
			}
			hashKey := encodeRedisHashKey(namespace, hashK)
			meta.Size++
			metaKeys[string(hashKey)] = string(meta.Encode())
		}

		err = createSSTFile(filepath.Join(dir, "kvrocks_default.sst"), keys)
		assert.NoError(t, err)

		r := rdb.Do(ctx, "sst", "load", dir)
		assert.NoError(t, r.Err())
		resp, err := ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(1), resp.filesLoaded)

		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.Error(t, val.Err())
				}
			}
		}
	})

	t.Run("Test load redis hash keys with expiration", func(t *testing.T) {
		dir, err := makeTempDir()
		require.NoError(t, err)
		defer os.RemoveAll(dir)

		namespace := DefaultKvrocksNamespace
		data := map[string][]map[string]string{
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
			"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): {
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
				{"__avoid_collisions__" + util.RandString(1, 10, util.Alpha): "__avoid_collisions__" + util.RandString(1, 10, util.Alpha)},
			},
		}
		keys := make(map[string]string, len(data))
		metaKeys := make(map[string]string, len(data))
		for hashK := range data {
			meta := NewMetadata()
			meta.Expire = uint64(time.Now().Add(5 * time.Second).UnixMilli())
			fields := data[hashK]
			for _, field := range fields {
				for fieldK, fieldV := range field {
					internalKey := encodeInternalKey(namespace, hashK, fieldK, meta.Version)
					keys[string(internalKey)] = fieldV
				}
			}
			hashKey := encodeRedisHashKey(namespace, hashK)
			meta.Size++
			metaKeys[string(hashKey)] = string(meta.Encode())
		}

		err = createSSTFile(filepath.Join(dir, "kvrocks_default.sst"), keys)
		assert.NoError(t, err)
		err = createSSTFile(filepath.Join(dir, "kvrocks_metadata.sst"), metaKeys)
		assert.NoError(t, err)

		r := rdb.Do(ctx, "sst", "load", dir)
		assert.NoError(t, r.Err())
		resp, err := ExtractSSTResponse(r.Val())
		assert.NoError(t, err)
		assert.Equal(t, int64(2), resp.filesLoaded)

		// verify keys have expiration
		for hashK := range data {
			expireDuration := rdb.ExpireTime(ctx, hashK)
			assert.NotEmpty(t, expireDuration.Val().Milliseconds())
		}
		//verify keys have expired
		time.Sleep(5 * time.Second)
		for hashK, fields := range data {
			for _, field := range fields {
				for fieldK := range field {
					val := rdb.HGet(ctx, hashK, fieldK)
					assert.Error(t, val.Err())
				}
			}
		}
	})
}
